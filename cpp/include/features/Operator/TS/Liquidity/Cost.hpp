#pragma once

// =============================================================================
// COST (Impact Cost) - 冲击成本: 吃掉前 N 档的 VWAP 相对中间价的偏离
// =============================================================================
//   cost_buy_N  = VWAP(ask[1:N]) / mid - 1   (买方, 正值)
//   cost_sell_N = 1 - VWAP(bid[1:N]) / mid   (卖方, 正值)
//   IS_BUY: true=吃 ask, false=吃 bid
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BUY, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Cost {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Cost(const CBuffer<float, L2::BLEN> (&price)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> &mid_price)
      : price_(price), qty_(qty), mid_price_(mid_price) {}

  inline void compute() {
    float sum_pv = 0.0f;
    float sum_v = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float p = price_[i].back();
      float v = qty_[i].back();
      if constexpr (IS_BUY)
        v = -v; // ask qty 存负值
      sum_pv += p * v;
      sum_v += v;
    }

    float mid = mid_price_.back();
    float cost = 0.0f;
    if (sum_v > 1e-6f && mid > 1e-6f) {
      float vwap = sum_pv / sum_v;
      cost = IS_BUY ? vwap / mid - 1.0f : 1.0f - vwap / mid;
    }
    y[value] = cost;
  }

private:
  const CBuffer<float, L2::BLEN> (&price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> &mid_price_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Cost_buy_1(N) N(Cost_buy_1, (Cost<1, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_1(X) \
  X(cost_buy_1, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 1-Level", "买方冲击成本1档", "吃1档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{1} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_1))

#define NODE_Cost_buy_10(N) N(Cost_buy_10, (Cost<10, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_10(X) \
  X(cost_buy_10, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 10-Level", "买方冲击成本10档", "吃10档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{10} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_10))

#define NODE_Cost_buy_5(N) N(Cost_buy_5, (Cost<5, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_5(X) \
  X(cost_buy_5, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 5-Level", "买方冲击成本5档", "吃5档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{5} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_5))

#define NODE_Cost_sell_1(N) N(Cost_sell_1, (Cost<1, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_1(X) \
  X(cost_sell_1, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 1-Level", "卖方冲击成本1档", "吃1档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{1} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,B}})", OP(Cost_sell_1))

#define NODE_Cost_sell_10(N) N(Cost_sell_10, (Cost<10, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_10(X) \
  X(cost_sell_10, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 10-Level", "卖方冲击成本10档", "吃10档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{10} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,B}})", OP(Cost_sell_10))

#define NODE_Cost_sell_5(N) N(Cost_sell_5, (Cost<5, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_5(X) \
  X(cost_sell_5, 1, DATA, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 5-Level", "卖方冲击成本5档", "吃5档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{5} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,B}})", OP(Cost_sell_5))
