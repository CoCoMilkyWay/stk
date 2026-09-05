#pragma once

// =============================================================================
// Para (Parabola Fit) - 30 档深度二次拟合 V_i ~ c0 + c1·i + c2·i², 最小二乘 (X'X)^-1 X'y
// =============================================================================
//   c0 截距 (近端流动性), c1 斜率 (风偏方向), c2 曲率 (<0 近端订单块; >0 做市类挂单)
//   一侧一个节点, 三个系数共用一次 X'y 累加; 单侧算子: 只收一侧 qty, IS_BID 决定符号 (ask 存负值)
// =============================================================================

#include "features/DataDefine.hpp"

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

template <bool IS_BID>
class Para {
  static constexpr size_t N = L2::LOB_DEPTH;
  static_assert(N >= 3, "Need at least 3 levels for parabola fit");
  static constexpr para_detail::Inv3 kInv = para_detail::make_inv<N>();

public:
  enum Out : size_t { c0,
                      c1,
                      c2,
                      kCount };
  float y[kCount] = {};

  explicit Para(const DepthSeries &qty) : qty_(qty) {}

  inline void compute() {
    // X'y = [Σv, Σi·v, Σi²·v]
    float xy0 = 0.0f, xy1 = 0.0f, xy2 = 0.0f;
    for (size_t i = 0; i < N; ++i) {
      float v = IS_BID ? qty_[i].back() : -qty_[i].back();
      float fi = static_cast<float>(i);
      xy0 += v;
      xy1 += fi * v;
      xy2 += fi * fi * v;
    }
    for (size_t r = 0; r < kCount; ++r)
      y[r] = kInv.v[r][0] * xy0 + kInv.v[r][1] * xy1 + kInv.v[r][2] * xy2;
  }

private:
  const DepthSeries &qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define PARA_FIELDS(X, CAT1, side, S, en, cn)                                                                                                                                                                                                                                                 \
  X(side##_para_c0, CAT1, RAW, NONE, en " Depth Parabola c0", cn "侧抛物线截距", cn "侧近端流动性(降频)", R"(c_{0,t}^{M,)" S R"(}, \quad \text{where } V_{i,t}^{M,)" S R"(} \sim c_{0,t}^{M,)" S R"(} + c_{1,t}^{M,)" S R"(} i + c_{2,t}^{M,)" S R"(} i^2)", OP(Para_##side, c0))             \
  X(side##_para_c1, CAT1, RAW, NONE, en " Depth Parabola c1", cn "侧抛物线斜率", cn "方风偏(近端还是远端挂单)(降频)", R"(c_{1,t}^{M,)" S R"(}, \quad \text{where } V_{i,t}^{M,)" S R"(} \sim c_{0,t}^{M,)" S R"(} + c_{1,t}^{M,)" S R"(} i + c_{2,t}^{M,)" S R"(} i^2)", OP(Para_##side, c1)) \
  X(side##_para_c2, CAT1, RAW, NONE, en " Depth Parabola c2", cn "侧抛物线曲率", "<0:近端有订单块(降频)", R"(c_{2,t}^{M,)" S R"(}, \quad \text{where } V_{i,t}^{M,)" S R"(} \sim c_{0,t}^{M,)" S R"(} + c_{1,t}^{M,)" S R"(} i + c_{2,t}^{M,)" S R"(} i^2)", OP(Para_##side, c2))

#define NODE_Para_a(N) N(Para_a, (Para<false>), (DepthData.ask_qty), onMinute)
#define FIELDS_L1_Para_a(X, CAT1) PARA_FIELDS(X, CAT1, a, "A", "Ask", "卖")

#define NODE_Para_b(N) N(Para_b, (Para<true>), (DepthData.bid_qty), onMinute)
#define FIELDS_L1_Para_b(X, CAT1) PARA_FIELDS(X, CAT1, b, "B", "Bid", "买")
