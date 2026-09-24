#pragma once

// =============================================================================
// LabelReturn - 吃单收益标签: "吃单做多/做空 a 分钟 b 万元" 的收益 (含冲击 + 税佣)
// =============================================================================
//   做多: (exit_vwap·(1-fee_sell) - entry_vwap·(1+fee_buy)) / entry_vwap·(1+fee_buy)
//   做空: (entry_vwap·(1-fee_sell) - exit_vwap·(1+fee_buy)) / entry_vwap·(1-fee_sell)
//   费用: 双边万 1 佣金; 卖出另加印花税, 按日期取当时税率 (2023-08-28 起千 1 → 万 5), reset(date) 时定
//   【不产 NaN】标签一律取"实盘真能做到的那笔交易", 缺口用可成交时刻/价格顶上, 幅值都在收益量纲内, 多日拉取无跳变:
//     分钟档 exit 越过连续竞价末秒 → 持有到收盘 (T0 当日平仓, 见 day_end)
//     entry 锚点在非交易空窗 (09:25 撮合后到 09:30 开盘) → 顺延到最早可成交时刻 (见 get_snapshot_tradable)
//     名义 exit 早于最早可成交时刻 (09:30 前的锚点) → 建仓即平, 只剩 −(税佣 + 冲击)
//     entry 锚点在 14:57 后哨兵分钟 (已无盘口) → 收盘建仓: 分钟档 / 收盘档 即建即平, 开盘档 = 收盘买次日开盘卖 (真实可做)
//     全簿吃不完 / 涨跌停封板该侧全空 → 余量按涨跌停价成交 (与 Book 吃单成本同约, 见 calc_vwap)
//     该侧全空且无涨跌停价可补 (无限制股) → 这笔交易成不了, 收益 0 (未建仓 = 无盈亏)
//
// 非 DAG 节点 (未来标签需回填, 不走 Node). 一份深度快照环 (各金额档吃单 VWAP) 供各路径共用:
//   snapshot(t)                 每次 onDepth 先调: 只记账, VWAP 惰性结算 —— 同一秒内只有最后一次
//                               盘口状态会被消费, 所以等下一个活跃秒的首次更新到来时才从 Depth 环
//                               的上一格结算入环 (materialize offset=1); 查询目标恰为当前秒 (还没
//                               结算) 时从环末现算 (offset=0). 与"每次更新都算"逐值一致, VWAP 计算
//                               次数从每笔盘口更新降到每活跃秒一次. 顺带截首个 ≥ 09:30 活跃秒的簿为当日开盘快照.
//   second(t, l0, v)            L0 秒级 (当前停用, 见文件末): LABEL_L0_HOLD 分钟 × LABEL_L0_AMT 万, 只落 long
//   minute_anchored(t, writer)  L1 分钟锚定惰性回填 (锚点 = 分钟末, 与行 m 特征的可知时刻对齐):
//                               先把 entry 秒已完结的行的 entry 快照存进当日悬挂槽 (所有组共用一份 entry), 再写分钟档已到 exit 的行.
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreSequential 保证).
//                               day_end 结算 分钟档尾部 (exit 过收盘 → 当日最后盘口) + 收盘档 全行 + 开盘档 到期日 (见下), 并释放结清的日.
//   finish_all                  回测区间末: 悬挂日全部按 最近一次可成交盘口 结算 (持有到最后一日收盘), 全部释放.
// 开盘档 (T+N) 跨日回填: 日 D 的行 entry 存在悬挂槽里, 等到 D+N 日开盘 (09:30 起首个活跃秒的簿) 才写 D 的张量 →
//   D 的写句柄由 CoreSequential 持到该日结清 (writer 带 days_ago). D+N 无开盘 (停牌 / 一字板无盘口) → 顺延到之后首个有开盘的日,
//   最多再等 N 日 (D+2N 仍无 → 最近一次可成交盘口). 悬挂日环长 PEND_DAYS = 2·N_max + 1, 顶掉的槽必已结清 (断言).
// 配置 (LABEL_GROUPS / LABEL_AMTS / LABEL_L0_*) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp" // L1_to_L0 (分钟锚定路径)
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <string_view>

// ---- 配置 ----
// 持有期组 (name, KIND, n): name = 列名 token (lb_<side>_<name>_<amt>w; 也是 Stat 持有期键的来源, 见 factor/Stat/Contract.hpp 【持有期键】)
//   MIN   持仓 n 分钟
//   CLOSE 持有到当日收盘 (n 不用, 写 0)
//   OPEN  T+n 开盘平仓 (跨日回填, 见文件头)
#define LABEL_GROUPS(X, ...)                                  \
  X(1m, MIN, 1, __VA_ARGS__)                                  \
  X(5m, MIN, 5, __VA_ARGS__)                                  \
  X(15m, MIN, 15, __VA_ARGS__)                                \
  X(30m, MIN, 30, __VA_ARGS__)                                \
  X(60m, MIN, 60, __VA_ARGS__)                                \
  X(close, CLOSE, 0, __VA_ARGS__) X(t1, OPEN, 1, __VA_ARGS__) \
      X(t3, OPEN, 3, __VA_ARGS__) X(t5, OPEN, 5, __VA_ARGS__)
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) // 下单金额 (万元), 也是快照预计算的档
#define LABEL_L0_HOLD 1                      // L0 秒级标签: 持仓分钟
#define LABEL_L0_AMT 5                       // L0 秒级标签: 金额 (万元), 必须 ∈ LABEL_AMTS
constexpr size_t LABEL_DELAY_SECONDS = 3;    // 下单延迟 (秒)

enum class HoldKind : uint8_t { MIN,
                                CLOSE,
                                OPEN };
#define LABEL_KIND_ONE(name, kind, n, ...) HoldKind::kind,
#define LABEL_N_ONE(name, kind, n, ...) n,
#define LABEL_LIST_ONE(v, ...) v,
inline constexpr HoldKind LABEL_KIND[] = {LABEL_GROUPS(LABEL_KIND_ONE)};
inline constexpr size_t LABEL_N[] = {LABEL_GROUPS(LABEL_N_ONE)};
inline constexpr size_t LABEL_AMOUNT_WAN[] = {LABEL_AMTS(LABEL_LIST_ONE)};
#undef LABEL_KIND_ONE
#undef LABEL_N_ONE
#undef LABEL_LIST_ONE

// 交易费用
constexpr float FEE_COMMISSION = 0.0001f; // 佣金 万1 (双边)
constexpr float STAMP = 0.0010f;

class LabelReturn {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_KIND);
  static constexpr size_t AMT_COUNT = std::size(LABEL_AMOUNT_WAN);
  static constexpr size_t GROUP_SIZE = 2 * AMT_COUNT; // 一个 hold 组: [long × 各金额, short × 各金额]
  static constexpr size_t L1_LABEL_COUNT = HOLD_COUNT * GROUP_SIZE;
  static_assert(HOLD_COUNT <= 32, "开盘档结算位图用 uint32_t");

  // 开盘档最大 N: 日 D 的 T+N 组最晚在 D+2N 结算 → 同时悬挂 (张量未结清, 写句柄未归还) 的日数 ≤ PEND_DAYS (含当日)
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
  // FeatureStore 池下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进 (CS / IO 阶段的 slot 另算, 只影响吞吐不致死锁)
  static constexpr size_t MIN_POOL_SLOTS = PEND_DAYS + 1;
  static constexpr size_t L0_AMT_IDX = [] {
    for (size_t a = 0; a < AMT_COUNT; ++a)
      if (LABEL_AMOUNT_WAN[a] == LABEL_L0_AMT)
        return a;
    return AMT_COUNT;
  }();
  static_assert(L0_AMT_IDX < AMT_COUNT, "LABEL_L0_AMT must be one of LABEL_AMTS");

  LabelReturn(const DepthSeries &bid_price, const DepthSeries &ask_price,
              const DepthSeries &bid_qty, const DepthSeries &ask_qty,
              const float &lim_up, const float &lim_dn)
      : bid_price_(bid_price), ask_price_(ask_price), bid_qty_(bid_qty), ask_qty_(ask_qty),
        lim_up_(lim_up), lim_dn_(lim_dn) {}

  // 记录当前秒有盘口更新 (每次 onDepth 调一次, 先于 second / minute_anchored).
  // 换秒时把上一个活跃秒结算入环: Depth 环的上一格 (offset=1) 正是那一秒最后
  // 一次更新的盘口 —— 与旧实现"每次更新覆盖写"的最终留存值逐位相同.
  inline void snapshot(size_t t) {
    if (pending_l0_ != kNoPending && pending_l0_ != t) {
      Snapshot &s = ring_[pending_l0_ % RING_SIZE];
      materialize(s, pending_l0_, 1);
      if (!open_ok_ && pending_l0_ >= OPEN_L0) { // 当日开盘快照 = 首个 ≥ 09:30 活跃秒的簿 (该秒终值)
        open_snap_ = s;
        open_ok_ = true;
      }
    }
    pending_l0_ = t;
  }

  // L0 秒级: 以 t 为平仓时刻, 反推 label_l0 = t - DELAY - hold 的做多收益 (缺失 = NaN); 时间不足返回 false
  inline bool second(size_t t, size_t &label_l0, float &value) const {
    constexpr size_t hold_sec = LABEL_L0_HOLD * 60;
    constexpr size_t total = LABEL_DELAY_SECONDS + hold_sec;
    if (t < total)
      return false;
    label_l0 = t - total;
    const auto *entry = get_snapshot(label_l0 + LABEL_DELAY_SECONDS);
    const auto *exit = get_snapshot(t);
    value = (entry && exit) ? calc_return(entry, exit, L0_AMT_IDX, true) : kNaN;
    return true;
  }

  // L1 分钟锚定惰性回填: 锚点 = 分钟 m 末 (= m+1 起始秒; 11:29 → 13:00:00), entry = 锚点+DELAY, exit = entry+hold.
  //   L1 行 m 的特征是分钟 m 结束时才可知的 (CoreSequential 顺序), 所以标签只能从 m 末起算, 锚到 m 起始秒会前视一分钟.
  //   entry 快照按行存进当日悬挂槽 (所有组共用): entry 秒完结 (entry_l0 < t) 且 [entry_l0 − 60, t − 1] 内有盘口才算捕获
  //   (锚点后无盘口 = 顺延中, 等下一活跃秒; 一直没有 → day_end 用收盘簿).
  //   分钟档: exit 已过线且 entry 已捕获的行逐个写出 (深度稀疏也不漏分钟, 快照缺口沿用 60s 回溯).
  //   exit 越过连续竞价末秒的行不在此结算 —— 14:57 起全部 tick 钳到盘后哨兵, 中间那段 L0 无人占位,
  //   回溯搭不回 14:56:59, 与"exit 过收盘"本质同类, 一并留给 day_end 按持有到收盘结算.
  //   writer(h, label_l1, values[GROUP_SIZE], days_ago) 负责落盘 (此处 days_ago 恒 0).
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    Pend &p = pend_[cur_];
    while (p.n_entry < TRADE_MINUTES_PER_DAY) {
      const size_t entry_l0 = L1_to_L0(p.n_entry + 1) + LABEL_DELAY_SECONDS; // 末分钟 254 → 15303 (盘后), 永不捕获 → day_end
      if (entry_l0 >= t)
        break; // entry 秒尚未完结: 同秒只消费最后一次盘口
      const Snapshot *e = get_snapshot_tradable(entry_l0, t - 1);
      if (!e)
        break; // 锚点起至 t−1 无盘口 (09:25-09:30 空窗等), 顺延中
      p.entry[p.n_entry] = *e;
      ++p.n_entry;
    }

    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      if (LABEL_KIND[h] != HoldKind::MIN)
        continue;
      const size_t hold_sec = LABEL_N[h] * 60;
      for (;;) {
        const size_t m = next_label_l1_[h];
        if (m >= p.n_entry)
          break; // entry 未捕获 (含 m == 255 全部写完)
        const size_t exit_l0 = L1_to_L0(m + 1) + LABEL_DELAY_SECONDS + hold_sec;
        if (exit_l0 > t || exit_l0 > LAST_CONTINUOUS_L0)
          break;
        const Snapshot *entry = &p.entry[m];
        const Snapshot *exit = get_snapshot(exit_l0);
        if (!exit)
          exit = entry; // 名义平仓时刻早于最早可成交时刻 (09:30 前的锚点): 退化为建仓即平, 只剩成本
        float values[GROUP_SIZE];
        fill_values(entry, exit, values);
        writer(h, m, static_cast<const float *>(values), size_t{0});
        ++next_label_l1_[h];
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

  // 收盘结算 (当日有无盘口皆调):
  //   分钟档尾部 (exit 过收盘, 永不过线) —— T0 头寸必须当日平掉, exit 截到当日最后盘口 (收盘竞价; 14:57 起全部 tick 钳到
  //     L0 15299, 见 TimeIndex), 标签 = "持有到收盘" 的真实可交易收益, 而非缺失. 持有窗口随行号递减, 缩到 0 时自然退化为
  //     −(税佣 + 冲击) —— 不赚钱还扣手续费, 连续无跳变.
  //   收盘档 全行 exit = 当日最后盘口.
  //   开盘档 到期日: 悬挂日 P (age = 今日 − P) 的 T+N 组, age ≥ N 且今日有开盘 → exit = 今日开盘快照; age ≥ 2N 仍无 → 最近一次可成交盘口.
  //   全日无盘口: 分钟 / 收盘档无标签可写 (整日 _meta 皆无效), 当日不悬挂; 开盘档到期照常结算.
  //   writer(h, l1, values, days_ago); release(days_ago) = 该日全部组已写完, 写句柄可归还.
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    Pend &p = pend_[cur_];
    const Snapshot *last = pending_l0_ != kNoPending ? get_snapshot(pending_l0_) : nullptr;
    if (last) {
      const Snapshot close_snap = *last;         // get_snapshot 可能复用 scratch_, 先拷出
      if (!open_ok_ && pending_l0_ >= OPEN_L0) { // 首个 ≥ 09:30 的活跃秒就是最后一个 (尚未入环): 开盘 = 收盘簿
        open_snap_ = close_snap;
        open_ok_ = true;
      }
      last_snap_ = close_snap;
      has_last_ = true;
      for (size_t m = p.n_entry; m < TRADE_MINUTES_PER_DAY; ++m)
        p.entry[m] = close_snap; // 锚点已无盘口 (14:57 后的哨兵分钟 / 至收盘无盘口): 收盘建仓
      p.n_entry = TRADE_MINUTES_PER_DAY;
      float values[GROUP_SIZE];
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] == HoldKind::OPEN)
          continue;
        const bool is_min = LABEL_KIND[h] == HoldKind::MIN;
        for (size_t m = is_min ? next_label_l1_[h] : 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(&p.entry[m], &close_snap, values);
          writer(h, m, static_cast<const float *>(values), size_t{0});
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

  // 回测区间末: 悬挂日全部按最近一次可成交盘口结算 (持有到最后一日收盘), 全部释放
  template <class Writer, class Release>
  inline void finish_all(Writer &&writer, Release &&release) {
    settle_open(writer, release, true);
  }

  // 每日重置: 快照环作废, 行游标归零, 按日期定卖出费率 (印花税)
  inline void reset(std::string_view yyyymmdd) {
    assert(yyyymmdd.size() == 8 && "reset: 日期须为 YYYYMMDD");
    fee_sell_ = FEE_COMMISSION + STAMP;
    for (auto &snap : ring_)
      snap.valid = false;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
    pending_l0_ = kNoPending; // 昨日最后一个活跃秒不结算 (当日 ring 已整体作废)
    open_ok_ = false;
  }

private:
  // 预计算的冲击成本快照
  struct Snapshot {
    float buy_vwap[AMT_COUNT] = {};    // 吃 ask 盘的 VWAP (各金额档)
    float buy_shares[AMT_COUNT] = {};  // 吃 ask 盘能买到的股数
    float sell_vwap[AMT_COUNT] = {};   // 吃 bid 盘的 VWAP
    float sell_shares[AMT_COUNT] = {}; // 吃 bid 盘能卖出的股数
    size_t l0_index = 0;
    bool valid = false;
  };

  // 连续竞价末秒 (14:56:59 → 15119): AFTERNOON_END_MIN 之后的 tick 全钳到盘后哨兵 15299,
  // 中间那段 L0 永无 tick 占位, 任何锚点落进去都查不到快照 (见 TimeIndex)
  static constexpr size_t LAST_CONTINUOUS_L0 = MORNING_SECONDS + (AFTERNOON_END_MIN - AFTERNOON_START_MIN) * 60 - 1;
  static constexpr size_t OPEN_L0 = Clock_to_L0(9, 30, 0); // 连续竞价开盘秒 (900): 开盘档 exit 取此起首个活跃秒的簿

  static constexpr size_t MAX_HOLD = [] {
    size_t m = LABEL_L0_HOLD;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::MIN)
        m = std::max(m, LABEL_N[h]);
    return m;
  }();
  // 环长: 最远回看 = 延迟 + 最长分钟档持仓; +128 覆盖 get_snapshot 的 60s 回溯再留余量 (收盘 / 开盘档不查环, 用悬挂槽的 entry)
  static constexpr size_t RING_SIZE = LABEL_DELAY_SECONDS + MAX_HOLD * 60 + 128;

  // 开盘档位图 (按 h 下标)
  static constexpr uint32_t kOpenMask = [] {
    uint32_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m |= 1u << h;
    return m;
  }();

  // 悬挂日槽: 一日的 entry 快照 (全部组共用) + 开盘档结算进度
  struct Pend {
    bool active = false;                   // 有开盘档未结清 (写句柄未归还)
    uint32_t open_mask = 0;                // 已结算的开盘档 (位 = h)
    size_t n_entry = 0;                    // 已捕获 entry 的行数 (行按锚点单调, 前缀)
    Snapshot entry[TRADE_MINUTES_PER_DAY]; // 各行 entry 快照
  };

  // [long × 各金额, short × 各金额]
  inline void fill_values(const Snapshot *entry, const Snapshot *exit, float *values) const {
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      values[a] = calc_return(entry, exit, a, true);
      values[AMT_COUNT + a] = calc_return(entry, exit, a, false);
    }
  }

  // 开盘档到期结算 (day_end 尾 / finish_all): 遍历悬挂日 (老日先, 释放序随日序 —— FeatureStore 的"d+1 计满 ⇒ d 计满"),
  //   到期组写出, 结清的日 release(age). final = 区间末: 不管到期, 全部按最近一次可成交盘口结算
  template <class Writer, class Release>
  inline void settle_open(Writer &&writer, Release &&release, bool final) {
    float values[GROUP_SIZE];
    for (size_t age = PEND_DAYS; age-- > 0;) {
      Pend &q = pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS];
      if (!q.active) {
        if (age == 0 && !final)
          release(age); // 当日不悬挂 (无盘口 / 无开盘档): 句柄当日归还
        continue;
      }
      assert(has_last_ && "悬挂日必有 entry ⇒ 必有过盘口 ⇒ last_snap_ 有值");
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] != HoldKind::OPEN || (q.open_mask & (1u << h)))
          continue;
        const size_t n = LABEL_N[h];
        const Snapshot *exit = nullptr;
        if (final || age >= 2 * n)
          exit = &last_snap_; // 区间末 / 顺延到期仍无开盘: 最近一次可成交盘口
        else if (age >= n && open_ok_)
          exit = &open_snap_; // 到期 (或顺延中) 且今日有开盘
        if (!exit)
          continue;
        for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(&q.entry[m], exit, values);
          writer(h, m, static_cast<const float *>(values), age);
        }
        q.open_mask |= 1u << h;
      }
      if (q.open_mask == kOpenMask) {
        q.active = false;
        release(age);
      }
    }
  }

  // 单个 label 的收益率; 该侧全空且无涨跌停价可补 (无限制股) → 这笔交易根本成不了, 收益 0 (未建仓 = 无盈亏)
  inline float calc_return(const Snapshot *entry, const Snapshot *exit, size_t amt_idx, bool is_long) const {
    if (is_long) {
      // 做多: entry 买入 (吃 ask), exit 卖出 (吃 bid)
      const float entry_vwap = entry->buy_vwap[amt_idx];
      const float shares = entry->buy_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;
      const float entry_cost = entry_vwap * (1.0f + FEE_COMMISSION);
      const float exit_vwap = interp_vwap(exit->sell_vwap, exit->sell_shares, shares); // 同股数卖出, 档间插值
      if (exit_vwap < 1e-6f)
        return 0.0f;
      const float exit_income = exit_vwap * (1.0f - fee_sell_);
      return (exit_income - entry_cost) / entry_cost;
    } else {
      // 做空: entry 卖出 (吃 bid), exit 买入 (吃 ask)
      const float entry_vwap = entry->sell_vwap[amt_idx];
      const float shares = entry->sell_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;
      const float entry_income = entry_vwap * (1.0f - fee_sell_);
      const float exit_vwap = interp_vwap(exit->buy_vwap, exit->buy_shares, shares);
      if (exit_vwap < 1e-6f)
        return 0.0f;
      const float exit_cost = exit_vwap * (1.0f + FEE_COMMISSION);
      return (entry_income - exit_cost) / entry_income;
    }
  }

  // 从 Depth 环末尾回退 offset 格的盘口状态结算快照 (0 = 当前更新, 1 = 上一次更新)
  void materialize(Snapshot &snap, size_t l0, size_t offset) const {
    snap.l0_index = l0;
    snap.valid = true;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      const float amt = static_cast<float>(LABEL_AMOUNT_WAN[a]) * 10000.0f;
      calc_vwap(ask_price_, ask_qty_, amt, true, offset, lim_up_, snap.buy_vwap[a], snap.buy_shares[a]);    // 吃 ask (买入): 余量按涨停
      calc_vwap(bid_price_, bid_qty_, amt, false, offset, lim_dn_, snap.sell_vwap[a], snap.sell_shares[a]); // 吃 bid (卖出): 余量按跌停
    }
  }

  // 指定 l0 时刻的快照; 深度不是每秒都更新, 向前找 ≤60s 内最近的有效快照
  const Snapshot *get_snapshot(size_t target) const {
    if (target == pending_l0_) { // 当前秒未结算: 从环末现算 (只有锚点查询走到, 频次 ~分钟级)
      materialize(scratch_, target, 0);
      return &scratch_;
    }
    const auto &s = ring_[target % RING_SIZE];
    if (s.valid && s.l0_index == target)
      return &s;
    for (size_t off = 1; off <= 60 && off <= target; ++off) {
      const auto &ss = ring_[(target - off) % RING_SIZE];
      if (ss.valid && ss.l0_index == target - off)
        return &ss;
    }
    return nullptr;
  }

  // 建仓侧快照: 锚点先按常规 60s 回溯; 落在非交易空窗 (09:25 撮合后到 09:30 开盘不受理委托, 全段无盘口更新)
  // 时顺延到 limit 之前首个真实成交时刻 —— "最早能成交的时刻才是建仓点", 比留 NaN 贴近 T0 实盘.
  // 取锚点之后的快照不引入前视: 标签本就是未来量, 行 m 的特征在分钟 m 末已定.
  const Snapshot *get_snapshot_tradable(size_t target, size_t limit) const {
    if (const auto *s = get_snapshot(target))
      return s;
    for (size_t l0 = target + 1; l0 <= limit; ++l0) {
      if (l0 == pending_l0_) { // 当前秒未结算, 同 get_snapshot: 从环末现算
        materialize(scratch_, l0, 0);
        return &scratch_;
      }
      const auto &s = ring_[l0 % RING_SIZE];
      if (s.valid && s.l0_index == l0)
        return &s;
    }
    return nullptr;
  }

  // 模拟吃单: 遍历盘口深度算 VWAP. is_buy: 吃 ask (qty 存负值); 否则吃 bid (正值)
  // offset: 从 Depth 环末尾回退几格取盘口 (所有档的环长同步推进, 下标一致)
  // limit_px: 全簿吃不完时余量的成交价 (买 → 涨停, 卖 → 跌停), 与 Book 的吃单成本同约;
  //           涨跌停封板 (该侧全空, 如涨停无卖盘) 也走这条 —— 恒有定义, 不产 NaN
  static inline void calc_vwap(const DepthSeries &price, const DepthSeries &qty,
                               float amount, bool is_buy, size_t offset, float limit_px, float &vwap, float &shares) {
    assert(price[0].size() > offset && "calc_vwap: Depth 环深度不足 offset");
    float cost = 0.0f, sh = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH && amount > 1e-6f; ++i) {
      const size_t k = price[i].size() - 1 - offset;
      const float p = price[i][k];
      const float q = is_buy ? -qty[i][k] : qty[i][k];
      if (p < 1e-6f || q < 1e-6f)
        continue;
      const float fill = std::min(amount, p * q); // 本档成交金额
      cost += fill;
      sh += fill / p;
      amount -= fill;
    }
    if (amount > 1e-6f && limit_px > 0.0f) { // 余量按涨跌停价成交 (边界 NaN = 无限制股 → 比较恒 false, 不补)
      cost += amount;
      sh += amount / limit_px;
    }
    vwap = (sh > 1e-6f) ? (cost / sh) : 0.0f;
    shares = sh;
  }

  // 按目标股数在预计算金额档之间线性插值 VWAP
  static inline float interp_vwap(const float *vwaps, const float *shares, float target) {
    if (AMT_COUNT == 1 || target <= shares[0])
      return vwaps[0];
    for (size_t i = 1; i < AMT_COUNT; ++i) {
      if (target <= shares[i]) {
        const float r = (target - shares[i - 1]) / (shares[i] - shares[i - 1] + 1e-9f);
        return vwaps[i - 1] + r * (vwaps[i] - vwaps[i - 1]);
      }
    }
    return vwaps[AMT_COUNT - 1]; // 超出范围用最大档
  }

  const DepthSeries &bid_price_;
  const DepthSeries &ask_price_;
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;
  const float &lim_up_, &lim_dn_; // Fund 当日涨跌停价 (盘前已知), 簿子吃不完时补齐余量用

  static constexpr size_t kNoPending = SIZE_MAX;

  std::array<Snapshot, RING_SIZE> ring_;    // 深度快照环
  size_t next_label_l1_[HOLD_COUNT] = {};   // 分钟档: 各组下一个待写 L1 行 (收盘 / 开盘档不用)
  size_t pending_l0_ = kNoPending;          // 当前活跃秒 (有盘口更新, 尚未结算入环)
  mutable Snapshot scratch_;                // pending 秒被查询时的现算暂存
  float fee_sell_ = FEE_COMMISSION + STAMP; // 当日卖出费率 (佣金 + 印花), reset(date) 设定

  // 跨日状态 (reset 不清)
  std::array<Pend, PEND_DAYS> pend_; // 悬挂日环, pend_[cur_] = 当日; 日 D 的槽 = (cur_ − age) mod PEND_DAYS
  size_t cur_ = PEND_DAYS - 1;       // 首个 day_begin 推到 0
  Snapshot open_snap_;               // 当日开盘快照 (首个 ≥ 09:30 活跃秒的簿), open_ok_ = 已截到 (reset 清)
  bool open_ok_ = false;
  Snapshot last_snap_; // 最近一次可成交盘口 (最近有盘口日的收盘簿), 停牌到期 / 区间末的 exit
  bool has_last_ = false;
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成: L1 每个 hold 一组 [long × LABEL_AMTS, short × LABEL_AMTS], 组序 = LABEL_GROUPS 序 (与 GROUP_SIZE / writer 的 h 对应)
// 每种 KIND 的描述片段 (英 / 中 / 备注 / 公式尾), 由 LABEL_ROW 按 kind token 拼接
#define LABEL_EN_MIN(n) #n "min"
#define LABEL_EN_CLOSE(n) "to-Close"
#define LABEL_EN_OPEN(n) "T+" #n " Open"
#define LABEL_CN_MIN(n) #n "分钟"
#define LABEL_CN_CLOSE(n) "至收盘"
#define LABEL_CN_OPEN(n) "至T+" #n "开盘"
#define LABEL_NOTE_MIN(n) "; 尾部不足" #n "分钟则持有到收盘"
#define LABEL_NOTE_CLOSE(n) "; exit=当日最后盘口"
#define LABEL_NOTE_OPEN(n) "; exit=T+" #n "日09:30起首个盘口, 无开盘顺延≤" #n "日后取最近盘口, 区间末持有到最后一日收盘"
#define LABEL_TEX_MIN(n) R"(, T=)" #n R"(\mathrm{min})"
#define LABEL_TEX_CLOSE(n) R"(, T=\mathrm{close}_D)"
#define LABEL_TEX_OPEN(n) R"(, T=\mathrm{open}_{D+)" #n "}"
#define LABEL_ROW(X, CAT1, side, en, cn, formula, name, kind, n, a)                                                                                                                                                \
  X(lb_##side##_##name##_##a##w, CAT1, ret, en " " LABEL_EN_##kind(n) " " #a "w Return", cn LABEL_CN_##kind(n) "收益(" #a "万)", "吃单" cn LABEL_CN_##kind(n) "收益(" #a "万元,含冲击+税佣)" LABEL_NOTE_##kind(n), \
    formula R"(, \quad A=)" #a R"(\mathrm{w})" LABEL_TEX_##kind(n), LABEL)
#define LABEL_ROW_LONG(a, name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, long, "Long", "做多", R"(\frac{\mathrm{VWAP}^{B}_{exit}-\mathrm{VWAP}^{A}_{entry}}{\mathrm{VWAP}^{A}_{entry}})", name, kind, n, a)
#define LABEL_ROW_SHORT(a, name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, short, "Short", "做空", R"(\frac{\mathrm{VWAP}^{B}_{entry}-\mathrm{VWAP}^{A}_{exit}}{\mathrm{VWAP}^{B}_{entry}})", name, kind, n, a)
#define LABEL_GROUP(name, kind, n, X, CAT1) LABEL_AMTS(LABEL_ROW_LONG, name, kind, n, X, CAT1) LABEL_AMTS(LABEL_ROW_SHORT, name, kind, n, X, CAT1)

// L0 秒级标签已停用 (L0 只落 _meta 一列, 秒频张量成本太高); 恢复 = 取消注释 + CoreSequential 加回 second() 回填
// #define FIELDS_L0_LabelReturn(X, CAT1) LABEL_ROW_LONG(LABEL_L0_AMT, 1m, MIN, LABEL_L0_HOLD, X, CAT1)
#define FIELDS_L1_LabelReturn(X, CAT1) LABEL_GROUPS(LABEL_GROUP, X, CAT1)
