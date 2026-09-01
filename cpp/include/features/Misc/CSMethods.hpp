// CSMethods — 通用截面方法库 (per-特征可配置, 配置表见 Minute_Crosssection)
//
// 三种方法, dense 数组原地应用 (valid 子集, 缺失 = NaN):
//   NormRank    rank → inverse normal, 输出 ~N(0,1), 缺失 → 0
//   WinsorRank  winsor_mad(k=3) → z → pct_rank → 均值填充, 输出 ∈[0,1]
//   NeutralRank winsor_q(1%,99%) → 中性化(行业+log市值 OLS 残差)
//                 → z → pct_rank → 均值填充, 输出 ∈[0,1]
//
// 忠实性契约: WinsorRank/NeutralRank 与 qmt/cpp/src/feature/cs.cpp 的
//   factor_pipeline/neutral_pipeline 逐步一致 (中位数取法/z 的 double 累加/
//   pct_rank 并列均秩/均值填充), 保证 qmt 因子效果可复现.
//   实现在 CSMethods.cpp (依赖 NaN 语义, 必须在 CMake PRECISE_MATH 列表).
#pragma once

#include <cstddef>
#include <cstdint>

namespace cs {

enum class Method : std::uint8_t {
  NormRank,    // Φ⁻¹(pctl(x))
  WinsorRank,  // pct_rank(z(winsor_mad(x)))
  NeutralRank, // pct_rank(z(neutralize(winsor_q(x))))
};

enum class Transform : std::uint8_t {
  None,
  Reciprocal, // x → 1/x (x==0 或非 finite → NaN; 估值比率 → 收益率口径, ep/bp/sp/cp)
};

// NeutralRank 的中性化输入: mcap (dense) → log(mcap) 原地; ≤0 / 非 finite → NaN.
// 每分钟调用一次, 供该分钟所有 NeutralRank 特征复用.
void prepare_logmc(float *mcap, std::size_t n);

// 对 dense 截面列原地应用 变换 + CS 方法.
// logmc / industry: 仅 NeutralRank 使用 (长度 n, 与 y 同下标), 其余方法可传 nullptr.
void apply(Method m, Transform tf, float *y, std::size_t n,
           const float *logmc, const float *industry);

} // namespace cs
