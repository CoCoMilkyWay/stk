#pragma once

// =============================================================================
// LabelReturn - 吃单收益标签: "吃单做多/做空 a 分钟 b 万元" 的收益 (含冲击 + 税佣)
// =============================================================================
//   做多: (exit_vwap·(1-fee_sell) - entry_vwap·(1+fee_buy)) / entry_vwap·(1+fee_buy)
//   做空: (entry_vwap·(1-fee_sell) - exit_vwap·(1+fee_buy)) / entry_vwap·(1-fee_sell)
//   费用: 双边万 1 佣金; 卖出另加印花税, 按日期取当时税率 (2023-08-28 起千 1 → 万 5), reset(date) 时定
//   【不产 NaN】标签一律取"实盘真能做到的那笔交易", 缺口用可成交时刻/价格顶上, 幅值都在收益量纲内, 多日拉取无跳变:
//     exit 过收盘 / 越过连续竞价末秒 → 持有到收盘 (T0 必须当日平仓, 见 finish())
//     entry 锚点在非交易空窗 (09:25 撮合后到 09:30 开盘) → 顺延到最早可成交时刻 (见 get_snapshot_tradable)
//     名义 exit 早于最早可成交时刻 (09:30 前的锚点) → 建仓即平, 只剩 −(税佣 + 冲击)
//     全簿吃不完 / 涨跌停封板该侧全空 → 余量按涨跌停价成交 (与 Book 吃单成本同约, 见 calc_vwap)
//     该侧全空且无涨跌停价可补 (无限制股) → 这笔交易成不了, 收益 0 (未建仓 = 无盈亏)
//
// 非 DAG 节点 (未来标签需回填, 不走 Node). 一份深度快照环 (各金额档吃单 VWAP) 供两条路径共用:
//   snapshot(t)                 每次 onDepth 先调: 只记账, VWAP 惰性结算 —— 同一秒内只有最后一次
//                               盘口状态会被消费, 所以等下一个活跃秒的首次更新到来时才从 Depth 环
//                               的上一格结算入环 (materialize offset=1); 查询目标恰为当前秒 (还没
//                               结算) 时从环末现算 (offset=0). 与"每次更新都算"逐值一致, VWAP 计算
//                               次数从每笔盘口更新降到每活跃秒一次.
//   second(t, l0, v)            L0 秒级 (当前停用, 见文件末): LABEL_L0_HOLD 分钟 × LABEL_L0_AMT 万, 只落 long
//   minute_anchored(t, writer)  L1 分钟锚定惰性回填 (锚点 = 分钟末, 与行 m 特征的可知时刻对齐): writer(h, l1, values[GROUP_SIZE])
//   finish(writer)              收盘: 各组尚未写出的尾部行 (exit 过收盘) 按 exit = 当日最后盘口 结算
// 配置 (LABEL_HOLDS / LABEL_AMTS / LABEL_L0_*) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp" // L1_to_L0 (分钟锚定路径)
#include <algorithm>
#include <array>
#include <cassert>
#include <iterator>
#include <string_view>

// ---- 配置 ----
#define LABEL_HOLDS(X, ...) X(5, __VA_ARGS__) X(10, __VA_ARGS__) X(30, __VA_ARGS__) // L1 持仓分钟
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(20, __VA_ARGS__)                     // 下单金额 (万元), 也是快照预计算的档
#define LABEL_L0_HOLD 1                                                             // L0 秒级标签: 持仓分钟
#define LABEL_L0_AMT 5                                                              // L0 秒级标签: 金额 (万元), 必须 ∈ LABEL_AMTS
constexpr size_t LABEL_DELAY_SECONDS = 3;                                           // 下单延迟 (秒)

#define LABEL_LIST_ONE(v, ...) v,
inline constexpr size_t LABEL_HOLD_MINUTES[] = {LABEL_HOLDS(LABEL_LIST_ONE)};
inline constexpr size_t LABEL_AMOUNT_WAN[] = {LABEL_AMTS(LABEL_LIST_ONE)};
#undef LABEL_LIST_ONE

// 交易费用
constexpr float FEE_COMMISSION = 0.0001f; // 佣金 万1 (双边)
constexpr float STAMP = 0.0010f;

class LabelReturn {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_HOLD_MINUTES);
  static constexpr size_t AMT_COUNT = std::size(LABEL_AMOUNT_WAN);
  static constexpr size_t GROUP_SIZE = 2 * AMT_COUNT; // 一个 hold 组: [long × 各金额, short × 各金额]
  static constexpr size_t L1_LABEL_COUNT = HOLD_COUNT * GROUP_SIZE;
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
    if (pending_l0_ != kNoPending && pending_l0_ != t)
      materialize(ring_[pending_l0_ % RING_SIZE], pending_l0_, 1);
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
  //   每次推进: exit 已过线的分钟逐个写出 (深度稀疏也不漏分钟, 快照缺口沿用 60s 回溯).
  //   exit 越过连续竞价末秒的行不在此结算 —— 14:57 起全部 tick 钳到盘后哨兵, 中间那段 L0 无人占位,
  //   回溯搭不回 14:56:59, 与"exit 过收盘"本质同类, 一并留给 finish() 按持有到收盘结算.
  //   writer(h, label_l1, values[GROUP_SIZE]) 负责落盘.
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      const size_t hold_sec = LABEL_HOLD_MINUTES[h] * 60;
      for (;;) {
        const size_t label_l0 = L1_to_L0(next_label_l1_[h] + 1); // 末分钟 254 → 15300 (盘后), exit 永不过线 → 留给 finish
        const size_t entry_l0 = label_l0 + LABEL_DELAY_SECONDS;
        const size_t exit_l0 = entry_l0 + hold_sec;
        if (exit_l0 > t || exit_l0 > LAST_CONTINUOUS_L0)
          break;
        // 建仓顺延上限取当前秒 t: t 必是刚记账的活跃秒 (snapshot(t) 先于本函数), 故顺延必命中
        const auto *entry = get_snapshot_tradable(entry_l0, t);
        assert(entry && "entry 恒有值: 顺延上限 t 即 snapshot(t) 刚记账的活跃秒");
        const auto *exit = get_snapshot(exit_l0);
        if (!exit)
          exit = entry; // 名义平仓时刻早于最早可成交时刻 (09:30 前的锚点): 退化为建仓即平, 只剩成本
        float values[GROUP_SIZE];
        for (size_t a = 0; a < AMT_COUNT; ++a) {
          values[a] = calc_return(entry, exit, a, true);
          values[AMT_COUNT + a] = calc_return(entry, exit, a, false);
        }
        writer(h, next_label_l1_[h], static_cast<const float *>(values));
        ++next_label_l1_[h];
      }
    }
  }

  // 收盘: 各组剩余行 (exit 过收盘, 永不过线) —— T0 头寸必须当日平掉, 所以把 exit 截到当日最后盘口 (收盘竞价;
  // 14:57 起全部 tick 钳到 L0 15299, 见 TimeIndex), 标签 = "持有到收盘" 的真实可交易收益, 而非缺失.
  // 持有窗口随行号递减, 缩到 0 时自然退化为 −(税佣 + 冲击) —— 不赚钱还扣手续费, 连续无跳变.
  template <class Writer>
  inline void finish(Writer &&writer) {
    const Snapshot *last = pending_l0_ != kNoPending ? get_snapshot(pending_l0_) : nullptr;
    if (!last)
      return;                          // 全日无任何盘口更新: 整日 _meta 皆无效, 标签无从谈起
    const Snapshot close_snap = *last; // 下面还要查 entry, 而 get_snapshot 可能复用 scratch_, 先拷出
    float values[GROUP_SIZE];
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      for (; next_label_l1_[h] < TRADE_MINUTES_PER_DAY; ++next_label_l1_[h]) {
        const size_t entry_l0 = L1_to_L0(next_label_l1_[h] + 1) + LABEL_DELAY_SECONDS;
        const Snapshot *entry = get_snapshot(entry_l0);
        if (!entry)
          entry = &close_snap; // 该锚点已无盘口 (14:57 后的哨兵分钟): 退化为收盘即建即平, 只剩成本
        for (size_t a = 0; a < AMT_COUNT; ++a) {
          values[a] = calc_return(entry, &close_snap, a, true);
          values[AMT_COUNT + a] = calc_return(entry, &close_snap, a, false);
        }
        writer(h, next_label_l1_[h], static_cast<const float *>(values));
      }
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

  static constexpr size_t MAX_HOLD = std::max<size_t>(LABEL_L0_HOLD, *std::max_element(std::begin(LABEL_HOLD_MINUTES), std::end(LABEL_HOLD_MINUTES)));
  // 环长: 最远回看 = 延迟 + 最长持仓; +128 覆盖 get_snapshot 的 60s 回溯再留余量
  static constexpr size_t RING_SIZE = LABEL_DELAY_SECONDS + MAX_HOLD * 60 + 128;

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
  size_t next_label_l1_[HOLD_COUNT] = {};   // 分钟锚定路径: 各组下一个待写 L1 行
  size_t pending_l0_ = kNoPending;          // 当前活跃秒 (有盘口更新, 尚未结算入环)
  mutable Snapshot scratch_;                // pending 秒被查询时的现算暂存
  float fee_sell_ = FEE_COMMISSION + STAMP; // 当日卖出费率 (佣金 + 印花), reset(date) 设定
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成: L1 每个 hold 一组 [long × LABEL_AMTS, short × LABEL_AMTS], 组序 = LABEL_HOLDS 序 (与 GROUP_SIZE / minute_anchored 的 h 对应)
#define LABEL_ROW(X, CAT1, side, en, cn, formula, h, a) \
  X(lb_##side##_##h##m_##a##w, CAT1, ret, en " " #h "min " #a "w Return", cn #h "分钟收益(" #a "万)", "吃单" cn #h "分钟收益(" #a "万元,含冲击+税佣); 尾部不足" #h "分钟则持有到收盘", formula R"(, \quad A=)" #a R"(\mathrm{w}, T=)" #h R"(\mathrm{min})", LABEL)
#define LABEL_ROW_LONG(a, h, X, CAT1) LABEL_ROW(X, CAT1, long, "Long", "做多", R"(\frac{\mathrm{VWAP}^{B}_{exit}-\mathrm{VWAP}^{A}_{entry}}{\mathrm{VWAP}^{A}_{entry}})", h, a)
#define LABEL_ROW_SHORT(a, h, X, CAT1) LABEL_ROW(X, CAT1, short, "Short", "做空", R"(\frac{\mathrm{VWAP}^{B}_{entry}-\mathrm{VWAP}^{A}_{exit}}{\mathrm{VWAP}^{B}_{entry}})", h, a)
#define LABEL_GROUP(h, X, CAT1) LABEL_AMTS(LABEL_ROW_LONG, h, X, CAT1) LABEL_AMTS(LABEL_ROW_SHORT, h, X, CAT1)

// L0 秒级标签已停用 (L0 只落 _meta 一列, 秒频张量成本太高); 恢复 = 取消注释 + CoreSequential 加回 second() 回填
// #define FIELDS_L0_LabelReturn(X, CAT1) LABEL_ROW_LONG(LABEL_L0_AMT, LABEL_L0_HOLD, X, CAT1)
#define FIELDS_L1_LabelReturn(X, CAT1) LABEL_HOLDS(LABEL_GROUP, X, CAT1)
