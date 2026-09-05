#pragma once

// =============================================================================
// GRAD (Gradient) - 前 N 档深度的平均一阶差分
// =============================================================================
//   GRAD_N = (1/(N-1)) · Σ (V_{i+1} - V_i) = (V_N - V_1) / (N-1)   (裂项)
//   正值: 越远离盘口量越多 (做市类挂单); 负值: 集中在盘口 (冲击类订单)
//   单侧算子: 只收一侧 qty; IS_BID 决定符号 (ask 存负值)
// =============================================================================

#include "features/DataDefine.hpp"

template <size_t N_LEVELS, bool IS_BID>
class Grad {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= L2::LOB_DEPTH, "N_LEVELS must be >= 2");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  explicit Grad(const DepthSeries &qty) : qty_(qty) {}

  inline void compute() {
    float v_first = qty_[0].back(), v_last = qty_[N_LEVELS - 1].back();
    if constexpr (!IS_BID)
      v_first = -v_first, v_last = -v_last;
    y[value] = (v_last - v_first) / static_cast<float>(N_LEVELS - 1);
  }

private:
  const DepthSeries &qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define GRAD_FIELD(X, CAT1, side, S, en, cn, n) \
  X(side##_##n##_c1, CAT1, RAW, NONE, "Top " #n "-level " en " Grad", cn "侧" #n "档梯度", cn "侧梯度(近端斜率)(降频)", R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,)" S R"(} - V_{i,t}^{M,)" S R"(}), \quad N = )" #n, OP(Grad_##side##_##n##_c1))

#define NODE_Grad_a_5_c1(N) N(Grad_a_5_c1, (Grad<5, false>), (DepthData.ask_qty), onMinute)
#define FIELDS_L1_Grad_a_5_c1(X, CAT1) GRAD_FIELD(X, CAT1, a, "A", "Ask", "卖", 5)

#define NODE_Grad_b_5_c1(N) N(Grad_b_5_c1, (Grad<5, true>), (DepthData.bid_qty), onMinute)
#define FIELDS_L1_Grad_b_5_c1(X, CAT1) GRAD_FIELD(X, CAT1, b, "B", "Bid", "买", 5)
