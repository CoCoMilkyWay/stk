#pragma once

// =============================================================================
// LabelHold<Entry> - 持有期标签的档无关骨架 (LabelLob 盘口中间价 / LabelBar 分钟 VWAP 共用)
// =============================================================================
//   档只管 entry 怎么来 (Entry::px = 建仓价; Lob 另带冲击成本), 骨架管持有期组的全部日历与结算:
//     open_at(l1, md)       有成交的分钟 bar 截开盘价 (开盘型 exit): 09:25 撮合 bar 的撮合价, 无撮合则首个连续竞价 bar 的
//                           首笔成交价 (交易所开盘价定义). 返回该 bar 是否连续竞价 (可作 entry)
//     emit_min(writer)      当日 entry 前缀推进后, 写出 exit 已到位的分钟型 (m, n): exit = entry[m+n] (同一基准, 相邻行精确伸缩)
//     close_day(...)        收盘结算: 尾部行按收盘价建仓, 分钟型尾部 (exit 过收盘) / 收盘型全行 exit = 收盘价, 有开盘型则悬挂;
//                           无成交 → 不写不悬挂. 之后开盘型到期结算
//     day_begin / finish_all / reset   日历 / 区间末 / 每日重置
//   开盘型 (T+N) 跨日回填: 日 D 的行 entry 存在悬挂槽里, 等到 D+N 日开盘才写 D 的张量 → D 的写句柄由 CoreTs 持到该日结清
//   (writer 带 days_ago). D+N 无开盘 (停牌 / 全日无成交) → 顺延到之后首个有开盘的日, 最多再等 N 日 (D+2N 仍无 → 最近一次收盘价).
//   悬挂日环长 PEND_DAYS = 2·N_max + 1, 顶掉的槽必已结清 (断言).
//   writer(col, n, l1, values, days_ago) 写 LB 块内偏移 col 起的 n 列; release(days_ago) = 该日全部组已写完, 写句柄可归还.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelGroups.hpp" // LABEL_GROUPS / HoldKind / LABEL_KIND / LABEL_N
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>

template <class Entry>
class LabelHold {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_KIND);
  static constexpr size_t GROUP_SIZE = 2; // 一个 hold 组: [long, short]
  static constexpr size_t hold_col(size_t h) { return h * GROUP_SIZE; }
  static_assert(HOLD_COUNT <= 32, "开盘型结算位图用 uint32_t");

  // 开盘型最大 N: 日 D 的 T+N 组最晚在 D+2N 结算 → 同时悬挂 (张量未结清, 写句柄未归还) 的日数 ≤ PEND_DAYS (含当日)
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
                "分钟型 / 开盘型的 n 须 ≥ 1");
  // FeatureStore 日环下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进 (CS / IO 阶段的 slot 另算, 只影响吞吐不致死锁)
  static constexpr size_t MIN_RING_SLOTS = PEND_DAYS + 1;

  // 09:25 撮合 bar (L1 kAuctionL1) / 连续竞价 [kContOpenL1, kContCloseL1) = [09:30, 14:57) / 收盘竞价 (L1 252..254)
  static constexpr size_t kAuctionL1 = (L2::MORNING_MATCHING_START_HOUR * 60 + L2::MORNING_MATCHING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContOpenL1 = (L2::CONTINUOUS_TRADING_MORNING_START_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContCloseL1 = MORNING_MINUTES + (AFTERNOON_END_MIN - AFTERNOON_START_MIN);
  static_assert(kAuctionL1 == 10 && kContOpenL1 == 15 && kContCloseL1 == 252);

  // ---- 日历 (日期轴每一日各调一次, 无数据日也调) ----

  // 新的一日: 悬挂环前进. 被顶掉的槽必已结清 (日 D 最晚 D+2N 结算, 环长 2N+1)
  inline void day_begin() {
    cur_ = (cur_ + 1) % PEND_DAYS;
    Pend &p = pend_[cur_];
    assert(!p.active && "悬挂日环被顶掉的日尚未结清 (PEND_DAYS 与到期规则不符)");
    p.n_entry = 0;
    p.open_mask = 0;
  }

  // 回测区间末: 悬挂日全部按最近一次收盘价结算 (持有到最后一日收盘), 全部释放
  template <class Writer, class Release>
  inline void finish_all(Writer &&writer, Release &&release) {
    settle_open(writer, release, true);
  }

  // 每日重置: 行游标 / 当日开盘归零
  inline void reset() {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
    open_ok_ = false;
  }

protected:
  // 悬挂日槽: 一日的 entry (全部组共用) + 开盘型结算进度
  struct Pend {
    bool active = false;                // 有开盘型未结清 (写句柄未归还)
    uint32_t open_mask = 0;             // 已结算的开盘型 (位 = h)
    size_t n_entry = 0;                 // 已定 entry 的行数 (行按锚点单调, 前缀)
    Entry entry[TRADE_MINUTES_PER_DAY]; // 各行 entry
  };

  Pend &today() { return pend_[cur_]; }

  // 有成交的分钟 bar 截开盘价; 返回该 bar 是否连续竞价
  inline bool open_at(size_t l1, const MinuteData &md) {
    if (l1 == kAuctionL1) {
      assert(!open_ok_ && "09:25 撮合 bar 之前不该有开盘价");
      open_px_ = md.close.back();
      open_ok_ = true;
      return false;
    }
    if (l1 < kContOpenL1 || l1 >= kContCloseL1)
      return false;
    if (!open_ok_) {
      open_px_ = md.open.back();
      open_ok_ = true;
    }
    return true;
  }

  // 分钟型: 行 e 的 entry 到位 = 行 e−n 的 exit 到位, 每组游标 next_label_l1_ 顺序推进 (days_ago 恒 0)
  template <class Writer>
  inline void emit_min(Writer &&writer) {
    const Pend &p = pend_[cur_];
    float values[GROUP_SIZE];
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      if (LABEL_KIND[h] != HoldKind::MIN)
        continue;
      for (size_t m = next_label_l1_[h]; m + LABEL_N[h] < p.n_entry; m = ++next_label_l1_[h]) {
        fill_values(p.entry[m], p.entry[m + LABEL_N[h]].px, values);
        writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), size_t{0});
      }
    }
  }

  // 收盘结算 (当日有无成交皆调). traded: 当日有收盘价 close_px; close_entry = 按收盘价建仓的 entry (尾部行);
  // on_tail(m, entry) = 尾部行补齐时档自己的附带写 (Lob: 冲击成本列)
  template <class Writer, class Release, class OnTail>
  inline void close_day(bool traded, float close_px, const Entry &close_entry, Writer &&writer, Release &&release, OnTail &&on_tail) {
    Pend &p = pend_[cur_];
    if (traded) {
      last_close_ = close_px;
      has_last_ = true;
      for (size_t m = p.n_entry; m < TRADE_MINUTES_PER_DAY; ++m) {
        p.entry[m] = close_entry; // 其后再无可成交: 收盘价建仓
        on_tail(m, close_entry);
      }
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
      p.active = kOpenMask != 0; // 有开盘型才悬挂
    } else {
      p.active = false;
    }
    settle_open(writer, release, false);
  }

private:
  static constexpr uint32_t kOpenMask = [] {
    uint32_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m |= 1u << h;
    return m;
  }();

  // [long, short]: 建仓价 vs 平仓价 exit_px
  static inline void fill_values(const Entry &entry, float exit_px, float *values) {
    values[0] = calc_return(entry.px, exit_px, true);
    values[1] = calc_return(entry.px, exit_px, false);
  }

  // 单个 label 的价格收益率; 建仓无价 / 平仓无价 → 这笔交易根本成不了, 收益 0 (未建仓 = 无盈亏)
  static inline float calc_return(float entry_px, float exit_px, bool is_long) {
    if (entry_px < 1e-6f || exit_px < 1e-6f)
      return 0.0f;
    return is_long ? (exit_px - entry_px) / entry_px : (entry_px - exit_px) / entry_px;
  }

  // 开盘型到期结算 (close_day 尾 / finish_all): 遍历悬挂日 (老日先, 释放序随日序 —— FeatureStore 的"d+1 计满 ⇒ d 计满"),
  //   到期组写出, 结清的日 release(age). final = 区间末: 不管到期, 全部按最近一次收盘价结算
  template <class Writer, class Release>
  inline void settle_open(Writer &&writer, Release &&release, bool final) {
    float values[GROUP_SIZE];
    for (size_t age = PEND_DAYS; age-- > 0;) {
      Pend &q = pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS];
      if (!q.active) {
        if (age == 0 && !final)
          release(age); // 当日不悬挂 (无成交 / 无开盘型): 句柄当日归还
        continue;
      }
      assert(has_last_ && "悬挂日必有 entry ⇒ 必有过成交 ⇒ last_close_ 有值");
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] != HoldKind::OPEN || (q.open_mask & (1u << h)))
          continue;
        const size_t n = LABEL_N[h];
        float exit_px = 0.0f;
        if (final || age >= 2 * n)
          exit_px = last_close_; // 区间末 / 顺延到期仍无开盘: 最近一次收盘价
        else if (age >= n && open_ok_)
          exit_px = open_px_; // 到期 (或顺延中) 且今日有开盘
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

  size_t next_label_l1_[HOLD_COUNT] = {}; // 分钟型: 各组下一个待写 L1 行 (收盘 / 开盘型不用)
  float open_px_ = 0.0f;                  // 当日开盘价, open_ok_ = 已截到 (reset 清)
  bool open_ok_ = false;

  // 跨日状态 (reset 不清)
  std::array<Pend, PEND_DAYS> pend_; // 悬挂日环, pend_[cur_] = 当日; 日 D 的槽 = (cur_ − age) mod PEND_DAYS
  size_t cur_ = PEND_DAYS - 1;       // 首个 day_begin 推到 0
  float last_close_ = 0.0f;          // 最近一次收盘 (最近有成交日的末笔成交价), 停牌到期 / 区间末的 exit
  bool has_last_ = false;
};
