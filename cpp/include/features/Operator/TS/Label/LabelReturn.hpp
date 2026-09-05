#pragma once

// =============================================================================
// LabelReturn - 吃单收益标签: "吃单做多/做空 a 分钟 b 万元" 的收益 (含冲击 + 税佣)
// =============================================================================
//   做多: (exit_vwap·(1-fee_sell) - entry_vwap·(1+fee_buy)) / entry_vwap·(1+fee_buy)
//   做空: (entry_vwap·(1-fee_sell) - exit_vwap·(1+fee_buy)) / entry_vwap·(1-fee_sell)
//   费用: 买入万 1 佣金, 卖出万 11 (印花 + 佣金)
//
// 非 DAG 节点 (未来标签需回填, 不走 Node). 一份深度快照环 (各金额档 VWAP 预计算) 供两条路径共用:
//   snapshot(t)                 每次 onDepth 先调: 存当前盘口的吃单 VWAP
//   second(t, l0, v)            L0 秒级 (当前停用, 见文件末): LABEL_L0_HOLD 分钟 × LABEL_L0_AMT 万, 只落 long
//   minute_anchored(t, writer)  L1 分钟锚定惰性回填: writer(h, l1, values[GROUP_SIZE])
// 配置 (LABEL_HOLDS / LABEL_AMTS / LABEL_L0_*) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp" // L1_to_L0 (分钟锚定路径)
#include <algorithm>
#include <array>
#include <iterator>

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
constexpr float FEE_BUY = 0.0001f;  // 买入佣金 万1
constexpr float FEE_SELL = 0.0011f; // 卖出 万11 (印花万10 + 佣金万1)

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
              const DepthSeries &bid_qty, const DepthSeries &ask_qty)
      : bid_price_(bid_price), ask_price_(ask_price), bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  // 保存当前深度: 预计算各金额档吃单 VWAP (每次 onDepth 调一次, 先于 second / minute_anchored)
  inline void snapshot(size_t t) {
    auto &snap = ring_[t % RING_SIZE];
    snap.l0_index = t;
    snap.valid = true;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      const float amt = static_cast<float>(LABEL_AMOUNT_WAN[a]) * 10000.0f;
      calc_vwap(ask_price_, ask_qty_, amt, true, snap.buy_vwap[a], snap.buy_shares[a]);    // 吃 ask (买入)
      calc_vwap(bid_price_, bid_qty_, amt, false, snap.sell_vwap[a], snap.sell_shares[a]); // 吃 bid (卖出)
    }
  }

  // L0 秒级: 以 t 为平仓时刻, 反推 label_l0 = t - DELAY - hold 的做多收益; 有则返回 true
  inline bool second(size_t t, size_t &label_l0, float &value) const {
    constexpr size_t hold_sec = LABEL_L0_HOLD * 60;
    constexpr size_t total = LABEL_DELAY_SECONDS + hold_sec;
    if (t < total)
      return false;
    label_l0 = t - total;
    const auto *entry = get_snapshot(label_l0 + LABEL_DELAY_SECONDS);
    const auto *exit = get_snapshot(t);
    if (!entry || !exit)
      return false;
    value = calc_return(entry, exit, L0_AMT_IDX, true);
    return value != 0.0f;
  }

  // L1 分钟锚定惰性回填: 锚点 = 分钟 m 起始秒, entry = 锚点+DELAY, exit = entry+hold.
  //   每次推进: exit 已过线的分钟逐个补算 (深度稀疏也不漏分钟, 快照缺口沿用 60s 回溯; 找不到则该分钟无标签).
  //   writer(h, label_l1, values[GROUP_SIZE]) 负责落盘.
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      const size_t hold_sec = LABEL_HOLD_MINUTES[h] * 60;
      for (;;) {
        const size_t label_l0 = L1_to_L0(next_label_l1_[h]);
        const size_t entry_l0 = label_l0 + LABEL_DELAY_SECONDS;
        const size_t exit_l0 = entry_l0 + hold_sec;
        if (exit_l0 > t)
          break;
        const auto *entry = get_snapshot(entry_l0);
        const auto *exit = get_snapshot(exit_l0);
        if (entry && exit) {
          float values[GROUP_SIZE];
          bool any = false;
          for (size_t a = 0; a < AMT_COUNT; ++a) {
            values[a] = calc_return(entry, exit, a, true);
            values[AMT_COUNT + a] = calc_return(entry, exit, a, false);
            any = any || values[a] != 0.0f || values[AMT_COUNT + a] != 0.0f;
          }
          if (any)
            writer(h, next_label_l1_[h], static_cast<const float *>(values));
        }
        ++next_label_l1_[h];
      }
    }
  }

  // 每日重置
  inline void reset() {
    for (auto &snap : ring_)
      snap.valid = false;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
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

  static constexpr size_t MAX_HOLD = std::max<size_t>(LABEL_L0_HOLD, *std::max_element(std::begin(LABEL_HOLD_MINUTES), std::end(LABEL_HOLD_MINUTES)));
  // 环长: 最远回看 = 延迟 + 最长持仓; +128 覆盖 get_snapshot 的 60s 回溯再留余量
  static constexpr size_t RING_SIZE = LABEL_DELAY_SECONDS + MAX_HOLD * 60 + 128;

  // 单个 label 的收益率; 数据不足返回 0
  inline float calc_return(const Snapshot *entry, const Snapshot *exit, size_t amt_idx, bool is_long) const {
    if (is_long) {
      // 做多: entry 买入 (吃 ask), exit 卖出 (吃 bid)
      const float entry_vwap = entry->buy_vwap[amt_idx];
      const float shares = entry->buy_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;
      const float entry_cost = entry_vwap * (1.0f + FEE_BUY);
      const float exit_vwap = interp_vwap(exit->sell_vwap, exit->sell_shares, shares); // 同股数卖出, 档间插值
      if (exit_vwap < 1e-6f)
        return 0.0f;
      const float exit_income = exit_vwap * (1.0f - FEE_SELL);
      return (exit_income - entry_cost) / entry_cost;
    } else {
      // 做空: entry 卖出 (吃 bid), exit 买入 (吃 ask)
      const float entry_vwap = entry->sell_vwap[amt_idx];
      const float shares = entry->sell_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;
      const float entry_income = entry_vwap * (1.0f - FEE_SELL);
      const float exit_vwap = interp_vwap(exit->buy_vwap, exit->buy_shares, shares);
      if (exit_vwap < 1e-6f)
        return 0.0f;
      const float exit_cost = exit_vwap * (1.0f + FEE_BUY);
      return (entry_income - exit_cost) / entry_income;
    }
  }

  // 指定 l0 时刻的快照; 深度不是每秒都更新, 向前找 ≤60s 内最近的有效快照
  const Snapshot *get_snapshot(size_t target) const {
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

  // 模拟吃单: 遍历盘口深度算 VWAP. is_buy: 吃 ask (qty 存负值); 否则吃 bid (正值)
  static inline void calc_vwap(const DepthSeries &price, const DepthSeries &qty,
                               float amount, bool is_buy, float &vwap, float &shares) {
    float cost = 0.0f, sh = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH && amount > 1e-6f; ++i) {
      const float p = price[i].back();
      const float q = is_buy ? -qty[i].back() : qty[i].back();
      if (p < 1e-6f || q < 1e-6f)
        continue;
      const float fill = std::min(amount, p * q); // 本档成交金额
      cost += fill;
      sh += fill / p;
      amount -= fill;
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

  std::array<Snapshot, RING_SIZE> ring_;  // 深度快照环
  size_t next_label_l1_[HOLD_COUNT] = {}; // 分钟锚定路径: 各组下一个待写 L1 行
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成: L1 每个 hold 一组 [long × LABEL_AMTS, short × LABEL_AMTS], 组序 = LABEL_HOLDS 序 (与 GROUP_SIZE / minute_anchored 的 h 对应)
#define LABEL_ROW(X, CAT1, side, en, cn, formula, h, a) \
  X(lb_##side##_##h##m_##a##w, CAT1, FUTURE_RET, NONE, en " " #h "min " #a "w Return", cn #h "分钟收益(" #a "万)", "吃单" cn #h "分钟收益(" #a "万元,含冲击+税佣)", formula R"(, \quad A=)" #a R"(\mathrm{w}, T=)" #h R"(\mathrm{min})", LABEL)
#define LABEL_ROW_LONG(a, h, X, CAT1) LABEL_ROW(X, CAT1, long, "Long", "做多", R"(\frac{P_{exit}^{sell}-P_{entry}^{buy}}{P_{entry}^{buy}})", h, a)
#define LABEL_ROW_SHORT(a, h, X, CAT1) LABEL_ROW(X, CAT1, short, "Short", "做空", R"(\frac{P_{entry}^{sell}-P_{exit}^{buy}}{P_{entry}^{sell}})", h, a)
#define LABEL_GROUP(h, X, CAT1) LABEL_AMTS(LABEL_ROW_LONG, h, X, CAT1) LABEL_AMTS(LABEL_ROW_SHORT, h, X, CAT1)

// L0 秒级标签已停用 (L0 只落 _meta 一列, 秒频张量成本太高); 恢复 = 取消注释 + CoreSequential 加回 second() 回填
// #define FIELDS_L0_LabelReturn(X, CAT1) LABEL_ROW_LONG(LABEL_L0_AMT, LABEL_L0_HOLD, X, CAT1)
#define FIELDS_L1_LabelReturn(X, CAT1) LABEL_HOLDS(LABEL_GROUP, X, CAT1)
