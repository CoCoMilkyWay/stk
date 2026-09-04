#pragma once

// =============================================================================
// Para (Parabola Fit) - 30 档深度二次拟合 V_i ~ c0 + c1·i + c2·i², 最小二乘 (X'X)^-1 X'y
// =============================================================================
//   c0 截距 (近端流动性), c1 斜率 (风偏方向), c2 曲率 (<0 近端订单块; >0 做市类挂单)
//   一侧一个节点, 三个系数共用一次 X'y 累加; IS_BID: true=买侧, false=卖侧
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

namespace para_detail {
struct Inv3 {
  float v[3][3];
};
// (X'X)^-1, X'X = [[n, Σi, Σi²], [Σi, Σi², Σi³], [Σi², Σi³, Σi⁴]], 伴随矩阵法 (对称)
template <size_t N>
constexpr Inv3 make_inv() {
  double s1 = 0, s2 = 0, s3 = 0, s4 = 0;
  for (size_t i = 0; i < N; ++i) {
    double di = static_cast<double>(i);
    s1 += di;
    s2 += di * di;
    s3 += di * di * di;
    s4 += di * di * di * di;
  }
  double n = static_cast<double>(N);
  double det = n * (s2 * s4 - s3 * s3) - s1 * (s1 * s4 - s2 * s3) + s2 * (s1 * s3 - s2 * s2);
  double inv_det = 1.0 / det;
  Inv3 m{};
  m.v[0][0] = static_cast<float>((s2 * s4 - s3 * s3) * inv_det);
  m.v[0][1] = m.v[1][0] = static_cast<float>((s2 * s3 - s1 * s4) * inv_det);
  m.v[0][2] = m.v[2][0] = static_cast<float>((s1 * s3 - s2 * s2) * inv_det);
  m.v[1][1] = static_cast<float>((n * s4 - s2 * s2) * inv_det);
  m.v[1][2] = m.v[2][1] = static_cast<float>((s1 * s2 - n * s3) * inv_det);
  m.v[2][2] = static_cast<float>((n * s2 - s1 * s1) * inv_det);
  return m;
}
} // namespace para_detail

template <bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Para {
  static_assert(DEPTH_SIZE >= 3, "Need at least 3 levels for parabola fit");
  static constexpr para_detail::Inv3 kInv = para_detail::make_inv<DEPTH_SIZE>();

public:
  enum Out : size_t { c0,
                      c1,
                      c2,
                      kCount };
  float y[kCount] = {};

  Para(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE])
      : bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  inline void compute() {
    // X'y = [Σv, Σi·v, Σi²·v]
    float xy0 = 0.0f, xy1 = 0.0f, xy2 = 0.0f;
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float v = IS_BID ? bid_qty_[i].back() : -ask_qty_[i].back(); // ask 存负值
      float fi = static_cast<float>(i);
      xy0 += v;
      xy1 += fi * v;
      xy2 += fi * fi * v;
    }
    for (size_t r = 0; r < kCount; ++r)
      y[r] = kInv.v[r][0] * xy0 + kInv.v[r][1] * xy1 + kInv.v[r][2] * xy2;
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Para_a(N) N(Para_a, (Para<false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Para_a(X)                                                                                                                                                                                                                                           \
  X(a_para_c0, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Ask Depth Parabola c0", "卖侧抛物线截距", "卖侧近端流动性(降频)", R"(c_{0,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)", OP(Para_a, c0))             \
  X(a_para_c1, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Ask Depth Parabola c1", "卖侧抛物线斜率", "卖方风偏(近端还是远端挂单)(降频)", R"(c_{1,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)", OP(Para_a, c1)) \
  X(a_para_c2, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Ask Depth Parabola c2", "卖侧抛物线曲率", "<0:近端有订单块(降频)", R"(c_{2,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)", OP(Para_a, c2))

#define NODE_Para_b(N) N(Para_b, (Para<true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Para_b(X)                                                                                                                                                                                                                                           \
  X(b_para_c0, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Bid Depth Parabola c0", "买侧抛物线截距", "买侧近端流动性(降频)", R"(c_{0,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)", OP(Para_b, c0))             \
  X(b_para_c1, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Bid Depth Parabola c1", "买侧抛物线斜率", "买方风偏(近端还是远端挂单)(降频)", R"(c_{1,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)", OP(Para_b, c1)) \
  X(b_para_c2, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Bid Depth Parabola c2", "买侧抛物线曲率", "<0:近端有订单块(降频)", R"(c_{2,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)", OP(Para_b, c2))
