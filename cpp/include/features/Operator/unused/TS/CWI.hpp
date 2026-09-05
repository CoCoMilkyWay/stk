#pragma once

// =============================================================================
// CWI (Convexity-Weighted Imbalance) - 凸加权失衡: 按档位 i^(-γ) 加权的多档失衡
// =============================================================================
//   w_i = 1 / (i + ε)^γ
//   CWI = Σ w_i·(V_{i,t}^{M,B} - V_{i,t}^{M,A}) / Σ w_i·(V_{i,t}^{M,B} + V_{i,t}^{M,A})
//   GAMMA_X10 = γ×10 (避免浮点模板参数)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

template <int GAMMA_X10, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CWI {
public:
  static constexpr float GAMMA = static_cast<float>(GAMMA_X10) / 10.0f;
  static constexpr float EPSILON = 1e-6f;

  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  CWI(const DepthSeries &bid_qty,
      const DepthSeries &ask_qty)
      : bid_qty_(bid_qty), ask_qty_(ask_qty) {
    for (size_t i = 0; i < DEPTH_SIZE; ++i)
      weights_[i] = 1.0f / std::pow(static_cast<float>(i + 1) + EPSILON, GAMMA);
  }

  inline void compute() {
    float numer = 0.0f;
    float denom = 0.0f;
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();
      float a = -ask_qty_[i].back(); // 卖方存负值
      float w = weights_[i];
      numer += w * (b - a);
      denom += w * (b + a);
    }
    y[value] = denom > 1e-6f ? numer / denom : 0.0f;
  }

private:
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;
  float weights_[DEPTH_SIZE];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Cwi_1(N) N(Cwi_1, (CWI<10>), (DepthData.bid_qty, DepthData.ask_qty), onMinute)

#define FIELDS_L1_Cwi_1(X, CAT1) \
  X(cwi_1, CAT1, RATIO, NONE, "Convexity-weighted Imb γ=1", "凸加权失衡γ=1", "考虑全量, 但是近端更高权重(按档位, 降频)", R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{M,B} - \sum_{i=1}^{N} w_i V_{i,t}^{M,A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 1)", OP(Cwi_1))

#define NODE_Cwi_2(N) N(Cwi_2, (CWI<20>), (DepthData.bid_qty, DepthData.ask_qty), onMinute)

#define FIELDS_L1_Cwi_2(X, CAT1) \
  X(cwi_2, CAT1, RATIO, NONE, "Convexity-weighted Imb γ=2", "凸加权失衡γ=2", "考虑全量, 但是近端更高权重(按档位, 降频)", R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{M,B} - \sum_{i=1}^{N} w_i V_{i,t}^{M,A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 2)", OP(Cwi_2))
