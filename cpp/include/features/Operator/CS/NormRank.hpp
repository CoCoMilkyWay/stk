#pragma once

// =============================================================================
// NormRank - 截面 rank → inverse normal: 输出 ~N(0,1), 缺失 → 0
// =============================================================================
//   y_a = Φ⁻¹((rank_a + 1) / (N + 1))   (Beasley-Springer-Moro 近似, 钳 ±6)
// =============================================================================

#include <cstddef>

namespace cs {

struct NormRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n); // CSKernels.cpp
};

} // namespace cs

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_NormRank(X) \
  X(cs_spread_rank, LIQUIDITY, RANK, RANK_ZSCORE, "CS Spread Rank", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))

#define FIELDS_L1_NormRank(X) \
  X(cs_spread_rank, LIQUIDITY, RANK, RANK_ZSCORE, "CS Spread Rank", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))
