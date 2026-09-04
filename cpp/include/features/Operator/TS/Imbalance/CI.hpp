#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡: 前 N 档的累计买卖失衡率
// =============================================================================
//   CI_N = (Σ V_{i,t}^{M,B} - Σ V_{i,t}^{M,A}) / (Σ V_{i,t}^{M,B} + Σ V_{i,t}^{M,A}), i=1..N
//   值域 [-1,1], 正值买方占优
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
    float sum_bid = 0.0f;
    float sum_ask = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      sum_bid += bid_qty_[i].back();
      sum_ask += -ask_qty_[i].back(); // 卖方存负值
    }
    float denom = sum_bid + sum_ask;
    y[value] = denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f;
  }

private:
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ci_1(N) N(Ci_1, (CI<1>), (DepthData.bid_qty, DepthData.ask_qty), onDepth)

#define FIELDS_L0_Ci_1(X) \
  X(ci_1, IMBALANCE, RATIO, NONE, "Cumu Imba 1-Level", "顶部1档失衡", "顶部1档订单失衡率", R"(\frac{V_{1,t}^{M,B} - V_{1,t}^{M,A}}{V_{1,t}^{M,B} + V_{1,t}^{M,A}})", OP(Ci_1))

#define NODE_Ci_10(N) N(Ci_10, (CI<10>), (DepthData.bid_qty, DepthData.ask_qty), onMinute)

#define FIELDS_L1_Ci_10(X) \
  X(ci_10, IMBALANCE, RATIO, NONE, "Cumu Imba 10-Level", "累计10档失衡", "累计10档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{10}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{10}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_10))

#define NODE_Ci_30(N) N(Ci_30, (CI<30>), (DepthData.bid_qty, DepthData.ask_qty), onMinute)

#define FIELDS_L1_Ci_30(X) \
  X(ci_30, IMBALANCE, RATIO, NONE, "Cumu Imba 30-Level", "累计30档失衡", "累计30档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{30}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{30}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_30))

#define NODE_Ci_5(N) N(Ci_5, (CI<5>), (DepthData.bid_qty, DepthData.ask_qty), onMinute)

#define FIELDS_L1_Ci_5(X) \
  X(ci_5, IMBALANCE, RATIO, NONE, "Cumu Imba 5-Level", "累计5档失衡", "累计5档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{5}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_5))
