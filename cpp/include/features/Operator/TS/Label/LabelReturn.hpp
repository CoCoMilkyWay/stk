#pragma once

// =============================================================================
// LabelReturn - 吃单收益标签算子
// =============================================================================
// 计算 "吃单做多/做空 a分钟 b万元" 收益标签
//
// 【公式定义】
//   做多: (exit_vwap·(1-fee_sell) - entry_vwap·(1+fee_buy)) / entry_vwap·(1+fee_buy)
//   做空: (entry_vwap·(1-fee_sell) - exit_vwap·(1+fee_buy)) / entry_vwap·(1-fee_sell)
//
// 【触发域】
//   compute: onDepth (每秒保存深度快照)
//   flush:   onTick (按组批量写入, 供Tick_Sequential调用)
//
// 【输入输出】
//   输入: bid_price[0:29] (onDepth), ask_price[0:29] (onDepth), bid_qty[0:29] (onDepth), ask_qty[0:29] (onDepth)
//   输出: lb_long/short_{5m/10m/30m}_{5w/20w} (onTick, 按hold_minutes分组)
//
// 【模板参数】
//   DELAY_SECONDS  - 下单延迟秒数 (3秒)
//   HOLD_MINUTES   - 持仓时长数组 (5, 10, 30分钟)
//   AMOUNT_WAN     - 下单金额数组 (5万, 20万)
//
// 【备注】
//   - 使用环形缓冲区保存深度快照，避免重复计算VWAP
//   - 费用: 买入万1佣金, 卖出万11 (印花+佣金)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/FeaturesDefine.hpp" // L1_to_L0 (分钟锚定路径)
#include <array>

// 交易费用
constexpr float FEE_BUY = 0.0001f;  // 买入佣金 万1
constexpr float FEE_SELL = 0.0011f; // 卖出 万11 (印花万10 + 佣金万1)

// 预计算的冲击成本快照
template <size_t AMT_COUNT>
struct PrecomputedCost {
  float buy_vwap[AMT_COUNT] = {};    // 吃ask盘的VWAP (各金额档)
  float buy_shares[AMT_COUNT] = {};  // 吃ask盘能买到的股数
  float sell_vwap[AMT_COUNT] = {};   // 吃bid盘的VWAP
  float sell_shares[AMT_COUNT] = {}; // 吃bid盘能卖出的股数
  size_t l0_index = 0;
  bool valid = false;
};

// =============================================================================
// LabelReturn 算子
// =============================================================================
// 模板参数:
//   DELAY_SECONDS  - 下单延迟秒数 (label_l0 到 entry_l0)
//   HOLD_COUNT     - 持仓时长种类数
//   HOLD_MINUTES   - 持仓时长数组 (分钟)
//   AMT_COUNT      - 下单金额种类数
//   AMOUNT_WAN     - 下单金额数组 (万元)
// =============================================================================
template <size_t DELAY_SECONDS,
          size_t HOLD_COUNT, const size_t (&HOLD_MINUTES)[HOLD_COUNT],
          size_t AMT_COUNT, const size_t (&AMOUNT_WAN)[AMT_COUNT]>
class LabelReturn {
public:
  // 每组4个label: long_5w, long_20w, short_5w, short_20w
  static constexpr size_t GROUP_SIZE = 2 * AMT_COUNT;
  static constexpr size_t LABEL_COUNT = HOLD_COUNT * GROUP_SIZE;

  static constexpr size_t MAX_HOLD = []() constexpr {
    size_t m = 0;
    for (size_t i = 0; i < HOLD_COUNT; ++i)
      if (HOLD_MINUTES[i] > m)
        m = HOLD_MINUTES[i];
    return m;
  }();
  static constexpr size_t MAX_DELAY = DELAY_SECONDS + MAX_HOLD * 60;
  // +128: 覆盖 get_snapshot 的 60s 回溯 (分钟锚定路径 entry 最早可回看
  // t - MAX_DELAY - 60 附近) 再留余量
  static constexpr size_t BUFFER_SIZE = MAX_DELAY + 128;

  LabelReturn(const CBuffer<float, L2::BLEN> (&bid_price)[L2::LOB_DEPTH],
              const CBuffer<float, L2::BLEN> (&ask_price)[L2::LOB_DEPTH],
              const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
              const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH])
      : bid_price_(bid_price), ask_price_(ask_price),
        bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  // -------------------------------------------------------------------------
  // compute: 每秒调用一次
  //   1. 保存当前深度快照 (预计算各金额的VWAP)
  //   2. 对每个hold_minutes组，检查是否可以计算 (t >= total_delay)
  //   3. 查找entry/exit时刻的快照，计算收益
  // -------------------------------------------------------------------------
  inline void compute(size_t t) {
    save_snapshot(t);

    // 重置输出
    for (size_t i = 0; i < LABEL_COUNT; ++i)
      out_valid_[i] = false;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      out_group_valid_[h] = false;

    // 按组计算
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      size_t hold_sec = HOLD_MINUTES[h] * 60;
      size_t total_delay = DELAY_SECONDS + hold_sec;

      if (t < total_delay)
        continue; // 数据不足

      // 时间点计算
      size_t label_l0 = t - total_delay;          // 标签写入位置
      size_t entry_l0 = label_l0 + DELAY_SECONDS; // 建仓时刻
      size_t exit_l0 = entry_l0 + hold_sec;       // 平仓时刻 (= t)

      // 获取entry/exit的深度快照
      const auto *entry = get_snapshot(entry_l0);
      const auto *exit = get_snapshot(exit_l0);
      if (!entry || !exit)
        continue;

      // 计算该组的4个label
      size_t base = h * GROUP_SIZE;
      for (size_t a = 0; a < AMT_COUNT; ++a) {
        // 做多: 买入建仓，卖出平仓
        float ret_long = calc_return(entry, exit, a, true);
        if (ret_long != 0.0f) {
          out_values_[base + a] = ret_long;
          out_valid_[base + a] = true;
        }
        // 做空: 卖出建仓，买入平仓
        float ret_short = calc_return(entry, exit, a, false);
        if (ret_short != 0.0f) {
          out_values_[base + AMT_COUNT + a] = ret_short;
          out_valid_[base + AMT_COUNT + a] = true;
        }
      }
      out_l0_[h] = label_l0;
      out_group_valid_[h] = true;
    }
  }

  // -------------------------------------------------------------------------
  // compute_minute_anchored: 分钟锚定惰性回填 (L1 标签)
  //   锚点 = 分钟 m 起始秒 (L1_to_L0), entry = 锚点+DELAY, exit = entry+hold.
  //   每次 onDepth 推进: exit 已过线的分钟逐个补算 (深度稀疏也不漏分钟,
  //   快照缺口沿用 get_snapshot 的 60s 回溯; 找不到则该分钟无标签).
  //   writer(h, label_l1, values[GROUP_SIZE]) 负责落盘.
  // -------------------------------------------------------------------------
  template <class Writer>
  inline void compute_minute_anchored(size_t t, Writer &&writer) {
    save_snapshot(t);

    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      const size_t hold_sec = HOLD_MINUTES[h] * 60;
      for (;;) {
        const size_t label_l0 = L1_to_L0(next_label_l1_[h]);
        const size_t entry_l0 = label_l0 + DELAY_SECONDS;
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

  // -------------------------------------------------------------------------
  // flush接口 - 供Tick_Sequential按组批量写入
  // -------------------------------------------------------------------------
  static constexpr size_t hold_count() { return HOLD_COUNT; }
  static constexpr size_t group_size() { return GROUP_SIZE; }
  inline size_t group_l0(size_t h) const { return out_l0_[h]; }
  inline bool group_valid(size_t h) const { return out_group_valid_[h]; }
  inline const float *group_values(size_t h) const { return &out_values_[h * GROUP_SIZE]; }

  // 每日重置
  inline void reset() {
    for (auto &snap : buffer_)
      snap.valid = false;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
  }

private:
  using Snapshot = PrecomputedCost<AMT_COUNT>;

  // -------------------------------------------------------------------------
  // save_snapshot: 保存当前深度，预计算各金额档的VWAP
  // -------------------------------------------------------------------------
  inline void save_snapshot(size_t l0) {
    auto &snap = buffer_[l0 % BUFFER_SIZE];
    snap.l0_index = l0;
    snap.valid = true;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      float amt = static_cast<float>(AMOUNT_WAN[a]) * 10000.0f;
      // 吃ask盘 (买入)
      calc_vwap(ask_price_, ask_qty_, amt, true, snap.buy_vwap[a], snap.buy_shares[a]);
      // 吃bid盘 (卖出)
      calc_vwap(bid_price_, bid_qty_, amt, false, snap.sell_vwap[a], snap.sell_shares[a]);
    }
  }

  // -------------------------------------------------------------------------
  // calc_return: 计算单个label的收益率
  // -------------------------------------------------------------------------
  inline float calc_return(const Snapshot *entry, const Snapshot *exit, size_t amt_idx, bool is_long) {
    if (is_long) {
      // 做多: entry买入(吃ask), exit卖出(吃bid)
      float entry_vwap = entry->buy_vwap[amt_idx];
      float shares = entry->buy_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;

      float entry_cost = entry_vwap * (1.0f + FEE_BUY);
      // exit时用相同股数卖出，需插值得到VWAP
      float exit_vwap = interp_vwap(exit->sell_vwap, exit->sell_shares, shares);
      if (exit_vwap < 1e-6f)
        return 0.0f;

      float exit_income = exit_vwap * (1.0f - FEE_SELL);
      return (exit_income - entry_cost) / entry_cost;
    } else {
      // 做空: entry卖出(吃bid), exit买入(吃ask)
      float entry_vwap = entry->sell_vwap[amt_idx];
      float shares = entry->sell_shares[amt_idx];
      if (entry_vwap < 1e-6f || shares < 1e-6f)
        return 0.0f;

      float entry_income = entry_vwap * (1.0f - FEE_SELL);
      float exit_vwap = interp_vwap(exit->buy_vwap, exit->buy_shares, shares);
      if (exit_vwap < 1e-6f)
        return 0.0f;

      float exit_cost = exit_vwap * (1.0f + FEE_BUY);
      return (entry_income - exit_cost) / entry_income;
    }
  }

  // -------------------------------------------------------------------------
  // get_snapshot: 获取指定l0时刻的快照，容忍深度不更新 (向前查找60秒)
  // -------------------------------------------------------------------------
  const Snapshot *get_snapshot(size_t target) const {
    // 先检查精确匹配
    const auto &s = buffer_[target % BUFFER_SIZE];
    if (s.valid && s.l0_index == target)
      return &s;
    // 深度可能不是每秒都更新，向前找最近的有效快照
    for (size_t off = 1; off <= 60 && off <= target; ++off) {
      const auto &ss = buffer_[(target - off) % BUFFER_SIZE];
      if (ss.valid && ss.l0_index == target - off)
        return &ss;
    }
    return nullptr;
  }

  // -------------------------------------------------------------------------
  // calc_vwap: 模拟吃单，遍历盘口深度计算VWAP
  //   is_buy=true:  吃ask盘 (qty存的是负数)
  //   is_buy=false: 吃bid盘 (qty存的是正数)
  // -------------------------------------------------------------------------
  static inline void calc_vwap(const CBuffer<float, L2::BLEN> (&price)[L2::LOB_DEPTH],
                               const CBuffer<float, L2::BLEN> (&qty)[L2::LOB_DEPTH],
                               float amount, bool is_buy, float &vwap, float &shares) {
    float cost = 0.0f, sh = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH && amount > 1e-6f; ++i) {
      float p = price[i].back();
      float q = is_buy ? -qty[i].back() : qty[i].back(); // ask的qty存负值
      if (p < 1e-6f || q < 1e-6f)
        continue;
      float fill = std::min(amount, p * q); // 本档成交金额
      cost += fill;
      sh += fill / p; // 成交股数
      amount -= fill;
    }
    vwap = (sh > 1e-6f) ? (cost / sh) : 0.0f;
    shares = sh;
  }

  // -------------------------------------------------------------------------
  // interp_vwap: 根据目标股数插值VWAP
  //   预计算了5w/20w两档，平仓时股数可能在两档之间，需要线性插值
  // -------------------------------------------------------------------------
  static inline float interp_vwap(const float *vwaps, const float *shares, float target) {
    if (AMT_COUNT == 1 || target <= shares[0])
      return vwaps[0];
    for (size_t i = 1; i < AMT_COUNT; ++i) {
      if (target <= shares[i]) {
        float r = (target - shares[i - 1]) / (shares[i] - shares[i - 1] + 1e-9f);
        return vwaps[i - 1] + r * (vwaps[i] - vwaps[i - 1]);
      }
    }
    return vwaps[AMT_COUNT - 1]; // 超出范围用最大档
  }

  // 输入: 盘口深度CBuffer引用
  const CBuffer<float, L2::BLEN> (&bid_price_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_price_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];

  // 内部状态
  std::array<Snapshot, BUFFER_SIZE> buffer_; // 深度快照环形缓冲区
  float out_values_[LABEL_COUNT] = {};       // 输出值
  size_t out_l0_[HOLD_COUNT] = {};           // 各组的label_l0
  bool out_valid_[LABEL_COUNT] = {};         // 各label是否有效
  bool out_group_valid_[HOLD_COUNT] = {};    // 各组是否有效
  size_t next_label_l1_[HOLD_COUNT] = {};    // 分钟锚定路径: 各组下一个待写 L1 行
};

// =============================================================================
// 默认配置
// =============================================================================
inline constexpr size_t LABEL_HOLD_MINUTES[] = {5, 10, 30}; // 持仓分钟数
inline constexpr size_t LABEL_AMOUNT_WAN[] = {5, 20};       // 下单金额(万元)
inline constexpr size_t LABEL_DELAY_SECONDS = 3;            // 下单延迟(秒)

// L1 分钟标签 (12 列, compute_minute_anchored 路径)
using LabelReturnOp = LabelReturn<LABEL_DELAY_SECONDS, 3, LABEL_HOLD_MINUTES, 2, LABEL_AMOUNT_WAN>;

// L0 秒级标签: 1 分钟 × 5 万 (compute 秒级回填路径; short 一并算出但只落 long)
inline constexpr size_t LABEL_1M_HOLD_MINUTES[] = {1};
inline constexpr size_t LABEL_1M_AMOUNT_WAN[] = {5};
using LabelReturn1mOp = LabelReturn<LABEL_DELAY_SECONDS, 1, LABEL_1M_HOLD_MINUTES, 1, LABEL_1M_AMOUNT_WAN>;
