#pragma once

// =============================================================================
// GradImba - 梯度失衡: 对比买卖两侧梯度的绝对值大小
// =============================================================================
//   grad_imba = (|grad^B| - |grad^A|) / (|grad^B| + |grad^A|)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

class GradImba {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  GradImba(const CBuffer<float, L2::BLEN> &bid_grad,
           const CBuffer<float, L2::BLEN> &ask_grad)
      : bid_grad_(bid_grad), ask_grad_(ask_grad) {}

  inline void compute() {
    float abs_b = std::abs(bid_grad_.back());
    float abs_a = std::abs(ask_grad_.back());
    float denom = abs_b + abs_a;
    y[value] = denom > 1e-6f ? (abs_b - abs_a) / denom : 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> &bid_grad_;
  const CBuffer<float, L2::BLEN> &ask_grad_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_GradImba_5_c1(N) N(GradImba_5_c1, (GradImba), (Grad_b_5_c1.out(), Grad_a_5_c1.out()), onMinute, onMinute)

#define FIELDS_L1_GradImba_5_c1(X) \
  X(imba_5_c1, 1, DATA, IMBALANCE, RATIO, NONE, "00/100/00", "Top 5-level Grad Ratio", "买卖五档梯度失衡", "买卖五档梯度失衡(降频)", R"(\frac{|\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B})| - |\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A})|}{|\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B})| + |\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A})|}, \quad N = 5)", OP(GradImba_5_c1))
