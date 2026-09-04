#pragma once

// =============================================================================
// GRAD (Gradient) - 前 N 档深度的平均一阶差分
// =============================================================================
//   GRAD_N = (1/(N-1)) · Σ (V_{i+1} - V_i) = (V_N - V_1) / (N-1)   (裂项)
//   正值: 越远离盘口量越多 (做市类挂单); 负值: 集中在盘口 (冲击类订单)
//   IS_BID: true=买侧, false=卖侧
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Grad {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS must be >= 2");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Grad(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE])
      : bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  inline void compute() {
    const auto &q = IS_BID ? bid_qty_ : ask_qty_;
    float v_first = q[0].back(), v_last = q[N_LEVELS - 1].back();
    if constexpr (!IS_BID)
      v_first = -v_first, v_last = -v_last; // ask 存负值
    y[value] = (v_last - v_first) / static_cast<float>(N_LEVELS - 1);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Grad_a_5_c1(N) N(Grad_a_5_c1, (Grad<5, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Grad_a_5_c1(X) \
  X(a_5_c1, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Ask Grad", "卖侧五档梯度", "卖侧梯度(近端斜率)(降频)", R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A}), \quad N = 5)", OP(Grad_a_5_c1))

#define NODE_Grad_b_5_c1(N) N(Grad_b_5_c1, (Grad<5, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Grad_b_5_c1(X) \
  X(b_5_c1, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Bid Grad", "买侧五档梯度", "买侧梯度(近端斜率)(降频)", R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B}), \quad N = 5)", OP(Grad_b_5_c1))
