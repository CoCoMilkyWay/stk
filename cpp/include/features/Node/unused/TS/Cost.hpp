#pragma once

// =============================================================================
// COST (Impact Cost) - 冲击成本: 吃掉前 N 档的 VWAP 相对中间价的偏离
// =============================================================================
//   cost_buy_N  = VWAP(ask[1:N]) / mid - 1   (买方, 正值)
//   cost_sell_N = 1 - VWAP(bid[1:N]) / mid   (卖方, 正值)
//   IS_BUY: true=吃 ask, false=吃 bid
//   N=1 时 cost_buy_1 ≡ cost_sell_1 = (ask1−bid1)/(ask1+bid1) (相对半价差), 只落 buy 一列.
//   当日尚无盘口 / 该侧 N 档皆空 / mid 无效 → NaN
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS, bool IS_BUY, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Cost {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Cost(const DepthSeries &price,
       const DepthSeries &qty,
       const Series &mid_price)
      : price_(price), qty_(qty), mid_price_(mid_price) {}

  inline void compute() {
    if (qty_[0].empty()) [[unlikely]] { // 当日首次盘口更新前 (Depth 每日清空)
      y[value] = kNaN;
      return;
    }
    float sum_pv = 0.0f;
    float sum_v = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float p = price_[i].back();
      float v = qty_[i].back();
      if constexpr (IS_BUY)
        v = -v; // ask qty 存负值 (Depth 已钳符号, 此处 v ≥ 0)
      sum_pv += p * v;
      sum_v += v;
    }

    float mid = mid_price_.back();
    float cost = kNaN;
    if (sum_v > 1e-6f && mid > 1e-6f) {
      float vwap = sum_pv / sum_v;
      cost = IS_BUY ? vwap / mid - 1.0f : 1.0f - vwap / mid;
    }
    y[value] = cost;
  }

private:
  const DepthSeries &price_;
  const DepthSeries &qty_;
  const Series &mid_price_; // 引用 (按值拷贝会拿到构造时的空环, back() 读垃圾)
};

// =============================================================================
// CostAmt - 按金额吃单的冲击成本: 吃掉 A 元 (沿档位累计成交额, 末档按比例) 的 VWAP 相对中间价的偏离
// =============================================================================
//   cost_buy_A  = VWAP(ask, 吃满 A 元) / mid - 1     cost_sell_A = 1 - VWAP(bid, 吃满 A 元) / mid
//   全部 N 档金额不足 A / 当日尚无盘口 / mid 无效 → NaN.   开源 029 买入 / 卖出冲击成本 (10万 / 100万 / 300万)
// =============================================================================
template <uint32_t AMT_WAN, bool IS_BUY, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CostAmt {
  static constexpr float TARGET = static_cast<float>(AMT_WAN) * 10000.0f; // 元

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  CostAmt(const DepthSeries &price,
          const DepthSeries &qty,
          const Series &mid_price)
      : price_(price), qty_(qty), mid_price_(mid_price) {}

  inline void compute() {
    if (qty_[0].empty()) [[unlikely]] {
      y[value] = kNaN;
      return;
    }
    float sum_pv = 0.0f; // 已吃金额
    float sum_v = 0.0f;  // 已吃股数
    for (size_t i = 0; i < DEPTH_SIZE && sum_pv < TARGET; ++i) {
      const float p = price_[i].back();
      float v = qty_[i].back();
      if constexpr (IS_BUY)
        v = -v;
      if (p <= 0.0f || v <= 0.0f)
        continue;
      const float remain = TARGET - sum_pv;
      const float amt = p * v;
      if (amt >= remain) { // 末档按比例
        sum_v += remain / p;
        sum_pv = TARGET;
      } else {
        sum_v += v;
        sum_pv += amt;
      }
    }
    const float mid = mid_price_.back();
    float cost = kNaN;
    if (sum_pv >= TARGET && sum_v > 0.0f && mid > 1e-6f) {
      const float vwap = sum_pv / sum_v;
      cost = IS_BUY ? vwap / mid - 1.0f : 1.0f - vwap / mid;
    }
    y[value] = cost;
  }

private:
  const DepthSeries &price_;
  const DepthSeries &qty_;
  const Series &mid_price_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Cost_buy_10w(N) N(Cost_buy_10w, (CostAmt<10, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_buy_10w(X, CAT1) \
  X(cost_buy_10w, CAT1, RAW, "Buy Impact Cost 100k", "买方冲击成本10万", "吃掉10万元卖盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(\frac{\mathrm{VWAP}^{A}(10^5)}{P_{\mathrm{mid},t}} - 1)", OP(Cost_buy_10w, None, None))
#define NODE_Cost_buy_100w(N) N(Cost_buy_100w, (CostAmt<100, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_buy_100w(X, CAT1) \
  X(cost_buy_100w, CAT1, RAW, "Buy Impact Cost 1M", "买方冲击成本100万", "吃掉100万元卖盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(\frac{\mathrm{VWAP}^{A}(10^6)}{P_{\mathrm{mid},t}} - 1)", OP(Cost_buy_100w, None, None))
#define NODE_Cost_buy_300w(N) N(Cost_buy_300w, (CostAmt<300, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_buy_300w(X, CAT1) \
  X(cost_buy_300w, CAT1, RAW, "Buy Impact Cost 3M", "买方冲击成本300万", "吃掉300万元卖盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(\frac{\mathrm{VWAP}^{A}(3 \times 10^6)}{P_{\mathrm{mid},t}} - 1)", OP(Cost_buy_300w, None, None))
#define NODE_Cost_sell_10w(N) N(Cost_sell_10w, (CostAmt<10, false>), (Depth.bid_price, Depth.bid_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_sell_10w(X, CAT1) \
  X(cost_sell_10w, CAT1, RAW, "Sell Impact Cost 100k", "卖方冲击成本10万", "吃掉10万元买盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(1 - \frac{\mathrm{VWAP}^{B}(10^5)}{P_{\mathrm{mid},t}})", OP(Cost_sell_10w, None, None))
#define NODE_Cost_sell_100w(N) N(Cost_sell_100w, (CostAmt<100, false>), (Depth.bid_price, Depth.bid_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_sell_100w(X, CAT1) \
  X(cost_sell_100w, CAT1, RAW, "Sell Impact Cost 1M", "卖方冲击成本100万", "吃掉100万元买盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(1 - \frac{\mathrm{VWAP}^{B}(10^6)}{P_{\mathrm{mid},t}})", OP(Cost_sell_100w, None, None))
#define NODE_Cost_sell_300w(N) N(Cost_sell_300w, (CostAmt<300, false>), (Depth.bid_price, Depth.bid_qty, MidPrice.out()), onMinute)
#define FIELDS_L1_Cost_sell_300w(X, CAT1) \
  X(cost_sell_300w, CAT1, RAW, "Sell Impact Cost 3M", "卖方冲击成本300万", "吃掉300万元买盘的执行价vs中间价偏离(比率, 深度不足→NaN)", R"(1 - \frac{\mathrm{VWAP}^{B}(3 \times 10^6)}{P_{\mathrm{mid},t}})", OP(Cost_sell_300w, None, None))

#define NODE_Cost_buy_1(N) N(Cost_buy_1, (Cost<1, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)

#define FIELDS_L1_Cost_buy_1(X, CAT1) \
  X(cost_buy_1, CAT1, RAW, "Buy Impact Cost 1-Level", "买方冲击成本1档", "吃1档卖盘的执行价vs中间价偏离(比率,降频)=相对半价差,买卖对称故只落此列", R"(\frac{\sum_{i=1}^{1} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_1, None, None))

#define NODE_Cost_buy_10(N) N(Cost_buy_10, (Cost<10, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)

#define FIELDS_L1_Cost_buy_10(X, CAT1) \
  X(cost_buy_10, CAT1, RAW, "Buy Impact Cost 10-Level", "买方冲击成本10档", "吃10档卖盘的执行价vs中间价偏离(比率,降频)", R"(\frac{\sum_{i=1}^{10} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_10, None, None))

#define NODE_Cost_buy_5(N) N(Cost_buy_5, (Cost<5, true>), (Depth.ask_price, Depth.ask_qty, MidPrice.out()), onMinute)

#define FIELDS_L1_Cost_buy_5(X, CAT1) \
  X(cost_buy_5, CAT1, RAW, "Buy Impact Cost 5-Level", "买方冲击成本5档", "吃5档卖盘的执行价vs中间价偏离(比率,降频)", R"(\frac{\sum_{i=1}^{5} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_5, None, None))

#define NODE_Cost_sell_10(N) N(Cost_sell_10, (Cost<10, false>), (Depth.bid_price, Depth.bid_qty, MidPrice.out()), onMinute)

#define FIELDS_L1_Cost_sell_10(X, CAT1) \
  X(cost_sell_10, CAT1, RAW, "Sell Impact Cost 10-Level", "卖方冲击成本10档", "吃10档买盘的执行价vs中间价偏离(比率,降频)", R"(1 - \frac{\sum_{i=1}^{10} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,B}})", OP(Cost_sell_10, None, None))

#define NODE_Cost_sell_5(N) N(Cost_sell_5, (Cost<5, false>), (Depth.bid_price, Depth.bid_qty, MidPrice.out()), onMinute)

#define FIELDS_L1_Cost_sell_5(X, CAT1) \
  X(cost_sell_5, CAT1, RAW, "Sell Impact Cost 5-Level", "卖方冲击成本5档", "吃5档买盘的执行价vs中间价偏离(比率,降频)", R"(1 - \frac{\sum_{i=1}^{5} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,B}})", OP(Cost_sell_5, None, None))
