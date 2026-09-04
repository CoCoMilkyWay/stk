#pragma once

// =============================================================================
// NeutralRank - 截面 winsor_q(1%,99%) → 中性化 (行业 + log 市值 OLS 残差) → z → pct_rank → 均值填充, 输出 ∈ [0,1]
// =============================================================================
//   忠实性契约: 与 qmt/cpp/src/feature/cs.cpp neutral_pipeline 逐步一致
//   (中位数取法 / z 的 double 累加 / pct_rank 并列均秩 / 均值填充), 保证 qmt 因子效果可复现.
//   中性化 (FWL 等价): 行业组内 demean → 对 demean 后 log(mcap) 标量回归取残差; 行业 0 为独立组.
//   唯一需要上下文的方法 (kNeutral): Ctx 由 CoreCrosssection 每分钟准备一次, 全部 NeutralRank 行复用 (仅 L1).
// =============================================================================

#include <cstddef>

namespace cs {

struct NeutralRank {
  static constexpr bool kNeutral = true;

  // 中性化上下文: 与 y 同下标的 dense 数组 (有效资产子集)
  struct Ctx {
    const float *logmc;    // log(总市值), ≤0 / 非 finite → NaN
    const float *industry; // SW2021 一级行业 ID (0 = 未知, 1..31)
  };

  static void prepare_logmc(float *mcap, std::size_t n);    // mcap → log(mcap) 原地 (CSKernels.cpp)
  static void apply(float *y, std::size_t n, const Ctx &c); // CSKernels.cpp
};

} // namespace cs

// 上下文源列 (L1 字段 code): CoreCrosssection 每分钟按 L1_Field::<token> gather 一次
#define NEUTRAL_RANK_MCAP mcap
#define NEUTRAL_RANK_INDUSTRY industry_l1

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_NeutralRank(X)                                                                                                                                                                \
  X(ep_ttm12, BASIC, RANK, NONE, "Neutral EP", "中性EP", "pct_rank(z(neutralize(winsor_q(1/pe))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pe}))))", CS(1, pe, Reciprocal, NeutralRank))     \
  X(bp_ttm3, BASIC, RANK, NONE, "Neutral BP", "中性BP", "pct_rank(z(neutralize(winsor_q(1/pb))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pb}))))", CS(1, pb, Reciprocal, NeutralRank))      \
  X(sp_ttm12, BASIC, RANK, NONE, "Neutral SP", "中性SP", "pct_rank(z(neutralize(winsor_q(1/ps))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{ps}))))", CS(1, ps, Reciprocal, NeutralRank))     \
  X(cp_ttm12, BASIC, RANK, NONE, "Neutral CP", "中性CP", "pct_rank(z(neutralize(winsor_q(1/pcf))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pcf}))))", CS(1, pcf, Reciprocal, NeutralRank))  \
  X(dy_ttm12, BASIC, RANK, NONE, "Neutral DY", "中性DY", "pct_rank(z(neutralize(winsor_q(dy_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{dy}))))", CS(1, dy_raw, None, NeutralRank))       \
  X(roe_ttm12, BASIC, RANK, NONE, "Neutral ROE", "中性ROE", "pct_rank(z(neutralize(winsor_q(roe_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roe}))))", CS(1, roe_raw, None, NeutralRank)) \
  X(roa_ttm12, BASIC, RANK, NONE, "Neutral ROA", "中性ROA", "pct_rank(z(neutralize(winsor_q(roa_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roa}))))", CS(1, roa_raw, None, NeutralRank))
