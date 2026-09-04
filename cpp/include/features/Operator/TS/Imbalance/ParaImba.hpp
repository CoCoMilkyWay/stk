#pragma once

// =============================================================================
// ParaImba - 抛物线参数失衡: 逐系数对比买卖两侧抛物线系数的绝对值大小
// =============================================================================
//   imba_para_ck = (|c_k^B| - |c_k^A|) / (|c_k^B| + |c_k^A|),  k ∈ {0,1,2}
// =============================================================================

#include "features/DataDefine.hpp"
#include <cmath>

class ParaImba {
public:
  enum Out : size_t { c0,
                      c1,
                      c2,
                      kCount };
  float y[kCount] = {};

  ParaImba(const Series (&bid_coef)[kCount],
           const Series (&ask_coef)[kCount])
      : bid_coef_(bid_coef), ask_coef_(ask_coef) {}

  inline void compute() {
    for (size_t k = 0; k < kCount; ++k) {
      float abs_b = std::abs(bid_coef_[k].back());
      float abs_a = std::abs(ask_coef_[k].back());
      float denom = abs_b + abs_a;
      y[k] = denom > 1e-6f ? (abs_b - abs_a) / denom : 0.0f;
    }
  }

private:
  const Series (&bid_coef_)[kCount];
  const Series (&ask_coef_)[kCount];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_ParaImba(N) N(ParaImba, (ParaImba), (Para_b.outs(), Para_a.outs()), onMinute)

#define FIELDS_L1_ParaImba(X)                                                                                                                                                                                             \
  X(imba_para_c0, IMBALANCE, RATIO, NONE, "Depth Parabola c0 Imba", "买卖抛物线截距失衡", "对比买卖近端流动性(降频)", R"(\frac{|c_{0,t}^{M,B}| - |c_{0,t}^{M,A}|}{|c_{0,t}^{M,B}| + |c_{0,t}^{M,A}|})", OP(ParaImba, c0)) \
  X(imba_para_c1, IMBALANCE, RATIO, NONE, "Depth Parabola c1 Imba", "买卖抛物线斜率失衡", "对比买卖风偏(降频)", R"(\frac{|c_{1,t}^{M,B}| - |c_{1,t}^{M,A}|}{|c_{1,t}^{M,B}| + |c_{1,t}^{M,A}|})", OP(ParaImba, c1))       \
  X(imba_para_c2, IMBALANCE, RATIO, NONE, "Depth Parabola c2 Imba", "买卖抛物线曲率失衡", "对比买卖订单块距离(降频)", R"(\frac{|c_{2,t}^{M,B}| - |c_{2,t}^{M,A}|}{|c_{2,t}^{M,B}| + |c_{2,t}^{M,A}|})", OP(ParaImba, c2))
