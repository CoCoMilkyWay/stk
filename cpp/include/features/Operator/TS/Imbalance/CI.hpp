#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡: 前 N 档的累计买卖失衡率
// =============================================================================
//   CI_N = (Σ V_{i,t}^{M,B} - Σ V_{i,t}^{M,A}) / (Σ V_{i,t}^{M,B} + Σ V_{i,t}^{M,A}), i=1..N
//   值域 [-1,1] (Depth 已钳 bid ≥ 0 / ask ≤ 0), 正值买方占优; 当日尚无盘口 / 两侧皆空 → NaN
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  CI(const DepthSeries &bid_qty,
     const DepthSeries &ask_qty)
      : bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  inline void compute() {
    if (bid_qty_[0].empty()) [[unlikely]] { // 当日首次盘口更新前 (Depth 每日清空)
      y[value] = kNaN;
      return;
    }
    float sum_bid = 0.0f;
    float sum_ask = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      sum_bid += bid_qty_[i].back();
      sum_ask += -ask_qty_[i].back(); // 卖方存负值
    }
    float denom = sum_bid + sum_ask;
    y[value] = denom > 1e-6f ? (sum_bid - sum_ask) / denom : kNaN;
  }

private:
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// #define NODE_Ci_1(N) N(Ci_1, (CI<1>), (Depth.bid_qty, Depth.ask_qty), onDepth)
// #define FIELDS_L0_Ci_1(X, CAT1) X(obi_1, CAT1, RATIO, "Order Book Imbalance 1-Level", "顶部1档失衡", "顶部1档订单失衡率", R"(\frac{V_{1,t}^{M,B} - V_{1,t}^{M,A}}{V_{1,t}^{M,B} + V_{1,t}^{M,A}})", OP(Ci_1, None, None))

#define NODE_Ci_5(N) N(Ci_5, (CI<5>), (Depth.bid_qty, Depth.ask_qty), onMinute)
#define FIELDS_L1_Ci_5(X, CAT1) X(obi_5, CAT1, RATIO, "Order Book Imbalance 5-Level", "累计5档失衡", "累计5档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{5}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_5, None, None))

#define NODE_Ci_10(N) N(Ci_10, (CI<10>), (Depth.bid_qty, Depth.ask_qty), onMinute)
#define FIELDS_L1_Ci_10(X, CAT1) X(obi_10, CAT1, RATIO, "Order Book Imbalance 10-Level", "累计10档失衡", "累计10档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{10}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{10}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_10, None, None))

#define NODE_Ci_30(N) N(Ci_30, (CI<30>), (Depth.bid_qty, Depth.ask_qty), onMinute)
#define FIELDS_L1_Ci_30(X, CAT1) X(obi_30, CAT1, RATIO, "Order Book Imbalance 30-Level", "累计30档失衡", "累计30档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{30}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{30}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_30, None, None))
