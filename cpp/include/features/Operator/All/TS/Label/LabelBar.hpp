#pragma once

// =============================================================================
// LabelBar - All 档价格收益标签 (分钟 VWAP 口径; bar1m 无盘口 → 无冲击成本列)
// =============================================================================
//   标签 (lb_long_<h> / lb_short_<h>, 组见 Common/Label/LabelGroups.hpp, 与 Pool 档同名同组) = 纯价格收益, 不含冲击与税佣:
//     做多: (exit − entry) / entry        做空: (entry − exit) / entry
//   entry(m) = 行 m 之后首个有成交的连续竞价分钟 r ∈ [m+1, kContCloseL1) 的 VWAP (bar1m amount / volume):
//     行 m 的特征在分钟 m 末才可知, 最早在分钟 m+1 内成交; 无成交分钟顺延 (稀疏 / 盘中停牌),
//     09:30 前的行 (含 09:25 撮合) 顺延到开盘后首个有成交分钟; 集合竞价 bar 不作 entry (与 Pool 档"集合竞价簿不可成交"同约).
//     之后再无连续竞价成交 (行 m ≥ kContCloseL1 − 1 / 尾盘无成交) → 收盘价建仓 (分钟 / 收盘档即建即平 = 0).
//   exit:
//     MIN   行 m 持有 n 分钟 = entry(m+n) (同一基准, 相邻行精确伸缩, 与 Pool 档同构); m+n 过收盘 → 收盘价 (T0 当日平仓)
//     CLOSE 当日收盘价 = 末个有成交 bar 的 close (有收盘竞价即撮合价)
//     OPEN  T+n 开盘价 = 09:25 撮合 bar 的价, 无撮合则首个有成交连续竞价 bar 的 open; 当日无开盘 (停牌) → 顺延,
//           最多再等 n 日 (D+2n 仍无 → 最近收盘价); 越过区间末 → 最后一日收盘价
//   全日无成交 (停牌 / 无 bar1m): 分钟 / 收盘档全行留 0, 当日不悬挂; 开盘档到期照常结算.
//   价格均为 bar1m 原始价 (不复权): 开盘档跨除权日的收益含除权缺口, 与 Pool 档同约.
//
// 非 DAG 节点 (未来标签需回填, 不走 Node), CoreBar 驱动, 接口与 LabelReturn 对仗:
//   minute_closed(l1, writer)   有成交的分钟 (onMinute 之后) 一调: 截开盘价, 连续竞价分钟的 VWAP 填给之前悬着的行 = 这些行的 entry,
//                               行 e 的 entry 到位即行 e−n 的分钟档 exit 到位, 顺带写 (e−n, n).
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreBar 保证).
//   finish_all                  回测区间末: 悬挂日全部按最近一次收盘价结算, 全部释放.
// 开盘档跨日回填 / 悬挂日环 / 写句柄延迟归还与 LabelReturn 完全相同 (PEND_DAYS = 2·N_max + 1).
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Operator/Common/Label/LabelGroups.hpp"
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>

class LabelBar {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_KIND);
  static constexpr size_t GROUP_SIZE = 2;                           // 一个 hold 组: [long, short]
  static constexpr size_t L1_LABEL_COUNT = HOLD_COUNT * GROUP_SIZE; // LB 块总列数
  static constexpr size_t hold_col(size_t h) { return h * GROUP_SIZE; }
  static_assert(HOLD_COUNT <= 32, "开盘档结算位图用 uint32_t");

  // 开盘档最大 N: 日 D 的 T+N 组最晚在 D+2N 结算 → 同时悬挂的日数 ≤ PEND_DAYS (含当日)
  static constexpr size_t OPEN_MAX = [] {
    size_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m = std::max(m, LABEL_N[h]);
    return m;
  }();
  static constexpr size_t PEND_DAYS = 2 * OPEN_MAX + 1;
  static_assert([] {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] != HoldKind::CLOSE && LABEL_N[h] < 1)
        return false;
    return true;
  }(),
                "分钟档 / 开盘档的 n 须 ≥ 1");
  // FeatureStore 池下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进
  static constexpr size_t MIN_POOL_SLOTS = PEND_DAYS + 1;

  // 09:25 撮合 bar (L1 kAuctionL1) / 连续竞价 [kContOpenL1, kContCloseL1) = [09:30, 14:57) / 收盘竞价 (L1 252..254)
  static constexpr size_t kAuctionL1 = (L2::MORNING_MATCHING_START_HOUR * 60 + L2::MORNING_MATCHING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContOpenL1 = (L2::CONTINUOUS_TRADING_MORNING_START_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContCloseL1 = MORNING_MINUTES + (AFTERNOON_END_MIN - AFTERNOON_START_MIN);
  static_assert(kAuctionL1 == 10 && kContOpenL1 == 15 && kContCloseL1 == 252);

  explicit LabelBar(const MinuteData &md) : md_(md) {}

  // 有成交的分钟 bar 结算后 (CoreBar 只在 volume > 0 的分钟调). writer(col, n, l1, values, days_ago), 此处 days_ago 恒 0
  template <class Writer>
  inline void minute_closed(size_t l1, Writer &&writer) {
    last_px_ = md_.close.back(); // 当日末笔成交价 (收盘价候选)
    has_px_ = true;
    if (l1 == kAuctionL1) {
      assert(!open_ok_ && "09:25 撮合 bar 之前不该有开盘价");
      open_px_ = last_px_;
      open_ok_ = true;
      return;
    }
    if (l1 < kContOpenL1 || l1 >= kContCloseL1)
      return; // 集合竞价 bar 不作 entry
    if (!open_ok_) {
      open_px_ = md_.open.back();
      open_ok_ = true;
    }
    const float vwap = md_.bid_amount.back() / static_cast<float>(md_.bid_volume.back());
    Pend &p = pend_[cur_];
    for (; p.n_entry < l1; ++p.n_entry)
      p.entry[p.n_entry] = vwap; // 行 m < l1 (m+1 ≤ l1) 悬着的 entry 全部到位
    float values[GROUP_SIZE];
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      if (LABEL_KIND[h] != HoldKind::MIN)
        continue;
      for (size_t m = next_label_l1_[h]; m + LABEL_N[h] < p.n_entry; m = ++next_label_l1_[h]) {
        fill_values(p.entry[m], p.entry[m + LABEL_N[h]], values);
        writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), size_t{0});
      }
    }
  }

  // ---- 日历 (日期轴每一日各调一次, 无数据日也调) ----

  // 新的一日: 悬挂环前进. 被顶掉的槽必已结清 (日 D 最晚 D+2N 结算, 环长 2N+1)
  inline void day_begin() {
    cur_ = (cur_ + 1) % PEND_DAYS;
    Pend &p = pend_[cur_];
    assert(!p.active && "悬挂日环被顶掉的日尚未结清 (PEND_DAYS 与到期规则不符)");
    p.n_entry = 0;
    p.open_mask = 0;
  }

  // 收盘结算: 有成交 → 尾部行按收盘价建仓, 分钟档尾部 (exit 过收盘) / 收盘档全行 exit = 收盘价, 有开盘档则悬挂;
  // 无成交 → 不写不悬挂. 之后开盘档到期结算. release(days_ago) = 该日全部组已写完
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    Pend &p = pend_[cur_];
    if (has_px_) {
      const float close_px = last_px_;
      last_close_ = close_px;
      has_last_ = true;
      for (size_t m = p.n_entry; m < TRADE_MINUTES_PER_DAY; ++m)
        p.entry[m] = close_px; // 其后再无连续竞价成交: 收盘价建仓
      p.n_entry = TRADE_MINUTES_PER_DAY;
      float values[GROUP_SIZE];
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] == HoldKind::OPEN)
          continue;
        const bool is_min = LABEL_KIND[h] == HoldKind::MIN;
        for (size_t m = is_min ? next_label_l1_[h] : 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(p.entry[m], close_px, values);
          writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), size_t{0});
        }
        if (is_min)
          next_label_l1_[h] = TRADE_MINUTES_PER_DAY;
      }
      p.active = kOpenMask != 0; // 有开盘档才悬挂
    } else {
      p.active = false;
    }
    settle_open(writer, release, false);
  }

  // 回测区间末: 悬挂日全部按最近一次收盘价结算 (持有到最后一日收盘), 全部释放
  template <class Writer, class Release>
  inline void finish_all(Writer &&writer, Release &&release) {
    settle_open(writer, release, true);
  }

  // 每日重置: 行游标 / 当日开盘 / 当日末笔归零
  inline void reset() {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
    open_ok_ = false;
    has_px_ = false;
  }

private:
  static constexpr uint32_t kOpenMask = [] {
    uint32_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m |= 1u << h;
    return m;
  }();

  // 悬挂日槽: 一日各行 entry 价 (全部组共用) + 开盘档结算进度
  struct Pend {
    bool active = false;                  // 有开盘档未结清 (写句柄未归还)
    uint32_t open_mask = 0;               // 已结算的开盘档 (位 = h)
    size_t n_entry = 0;                   // 已定 entry 的行数 (前缀)
    float entry[TRADE_MINUTES_PER_DAY]{}; // 各行 entry 价
  };

  static inline void fill_values(float entry, float exit_px, float *values) {
    values[0] = calc_return(entry, exit_px, true);
    values[1] = calc_return(entry, exit_px, false);
  }

  // 建仓 / 平仓无价 → 交易成不了, 收益 0
  static inline float calc_return(float entry, float exit_px, bool is_long) {
    if (entry < 1e-6f || exit_px < 1e-6f)
      return 0.0f;
    return is_long ? (exit_px - entry) / entry : (entry - exit_px) / entry;
  }

  // 开盘档到期结算 (与 LabelReturn::settle_open 同): 老日先, 结清的日 release(age); final = 区间末全部按最近收盘价
  template <class Writer, class Release>
  inline void settle_open(Writer &&writer, Release &&release, bool final) {
    float values[GROUP_SIZE];
    for (size_t age = PEND_DAYS; age-- > 0;) {
      Pend &q = pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS];
      if (!q.active) {
        if (age == 0 && !final)
          release(age); // 当日不悬挂 (无成交 / 无开盘档): 句柄当日归还
        continue;
      }
      assert(has_last_ && "悬挂日必有成交 ⇒ last_close_ 有值");
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] != HoldKind::OPEN || (q.open_mask & (1u << h)))
          continue;
        const size_t n = LABEL_N[h];
        float exit_px = 0.0f;
        if (final || age >= 2 * n)
          exit_px = last_close_;
        else if (age >= n && open_ok_)
          exit_px = open_px_;
        else
          continue;
        for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(q.entry[m], exit_px, values);
          writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), age);
        }
        q.open_mask |= 1u << h;
      }
      if (q.open_mask == kOpenMask) {
        q.active = false;
        release(age);
      }
    }
  }

  const MinuteData &md_; // 当前 bar (close / open / bid 侧 = bar1m 总量额)

  size_t next_label_l1_[HOLD_COUNT] = {}; // 分钟档: 各组下一个待写 L1 行
  float last_px_ = 0.0f;                  // 当日末笔成交价 (has_px_ = 当日有过成交; reset 清)
  bool has_px_ = false;
  float open_px_ = 0.0f; // 当日开盘价 (open_ok_ = 已截到; reset 清)
  bool open_ok_ = false;

  // 跨日状态 (reset 不清)
  std::array<Pend, PEND_DAYS> pend_; // 悬挂日环, pend_[cur_] = 当日
  size_t cur_ = PEND_DAYS - 1;       // 首个 day_begin 推到 0
  float last_close_ = 0.0f;          // 最近一次收盘 (最近有成交日的末笔成交价)
  bool has_last_ = false;
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行序 = LB 块布局: 每个 hold 一组 [long, short] (组序 = LABEL_GROUPS 序, 与 hold_col(h) 对应). 英 / 中片段见 LabelGroups.hpp
#define LBAR_NOTE_MIN(n) "行m+" #n "的entry(其后首个有成交连续竞价分钟的VWAP, 与entry同基准, 相邻行精确伸缩); 过收盘则按当日收盘价"
#define LBAR_NOTE_CLOSE(n) "当日收盘价(末笔成交价)"
#define LBAR_NOTE_OPEN(n) "T+" #n "日开盘价(09:25集合竞价撮合价, 无撮合则连续竞价首个有成交bar的开盘价); 当日无开盘则顺延至之后首个有开盘日(最多再等" #n "日, 仍无则取最近收盘价); 越过区间末则持有到最后一日收盘"
#define LBAR_TEX_MIN(n) R"(, \quad P_{exit}=\mathrm{VWAP}_{t_{entry}+)" #n R"(\,\mathrm{min}})"
#define LBAR_TEX_CLOSE(n) R"(, \quad P_{exit}=P^{close}_{D})"
#define LBAR_TEX_OPEN(n) R"(, \quad P_{exit}=P^{open}_{D+)" #n "}"
#define LBAR_ROW(X, CAT1, side, en, cn, formula, name, kind, n)                                       \
  X(lb_##side##_##name, CAT1, ret, en " " LABEL_EN_##kind(n) " Return", cn LABEL_CN_##kind(n) "收益", \
    cn LABEL_CN_##kind(n) "价格收益(比例; 不含冲击与税佣). entry: 其后首个有成交连续竞价分钟的VWAP, 无成交顺延, 再无则按收盘价建仓. exit: " LBAR_NOTE_##kind(n), formula LBAR_TEX_##kind(n), LABEL)
#define LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, long, "Long", "做多", R"(\frac{P_{exit}-\mathrm{VWAP}_{entry}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_ROW_SHORT(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, short, "Short", "做空", R"(\frac{\mathrm{VWAP}_{entry}-P_{exit}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_GROUP(name, kind, n, X, CAT1) LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW_SHORT(name, kind, n, X, CAT1)

#define FIELDS_L1_LabelBar(X, CAT1) LABEL_GROUPS(LBAR_GROUP, X, CAT1)
