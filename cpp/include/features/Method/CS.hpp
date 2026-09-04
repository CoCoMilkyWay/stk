#pragma once

// =============================================================================
// 截面方法 (namespace cs): 无状态 struct, dense 列原地变换. 契约见 DataDefine.hpp, 实现 src/features/Method/CS.cpp (precise-math)
// =============================================================================
//   元素预变换 (CS 行第三参, 先于方法):
//     None         x → x
//     Reciprocal   x → 1/x  (x==0 或非 finite → NaN; 估值比率 → 收益率口径: pe→ep, pb→bp, ps→sp, pcf→cp)
//   截面方法 (CS 行第四参):
//     NormRank     rank → inverse normal, 输出 ~N(0,1), 缺失 → 0.  Φ⁻¹((rank+1)/(N+1)), Beasley-Springer-Moro 近似, 钳 ±6
//     WinsorRank   winsor_mad(k=3) → z → pct_rank → 均值填充, 输出 ∈ [0,1]                    (= qmt factor_pipeline)
//     NeutralRank  winsor_q(1%,99%) → 中性化 (行业 + log 市值 OLS 残差, FWL 等价: 行业组内 demean → 对 demean 后 log(mcap)
//                  标量回归取残差, 行业 0 独立组) → z → pct_rank → 均值填充, 输出 ∈ [0,1]     (= qmt neutral_pipeline)
//                  唯一需要上下文的方法 (kNeutral): Ctx 由 CoreCrosssection 每分钟准备一次, 全部 NeutralRank 行复用 (仅 L1)
//
//   忠实性契约: WinsorRank / NeutralRank 与 qmt/cpp/src/feature/cs.cpp 逐步一致
//   (中位数取法 / z 的 double 累加 / pct_rank 并列均秩 / 均值填充), 保证 qmt 因子效果可复现.
// =============================================================================

#include <cstddef>

namespace cs {

// ---- 元素预变换 ----
struct None {
  static void apply(float *, std::size_t) {}
};
struct Reciprocal {
  static void apply(float *y, std::size_t n);
};

// ---- 截面方法 ----
struct NormRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct WinsorRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct NeutralRank {
  static constexpr bool kNeutral = true;

  // 中性化上下文: 与 y 同下标的 dense 数组 (有效资产子集)
  struct Ctx {
    const float *logmc;    // log(总市值), ≤0 / 非 finite → NaN
    const float *industry; // SW2021 一级行业 ID (0 = 未知, 1..31)
  };

  static void prepare_logmc(float *mcap, std::size_t n); // mcap → log(mcap) 原地
  static void apply(float *y, std::size_t n, const Ctx &c);
};

} // namespace cs

// NeutralRank 上下文源列 (L1 字段 code): CoreCrosssection 每分钟按 L1_Field::<token> gather 一次
#define NEUTRAL_RANK_MCAP mcap
#define NEUTRAL_RANK_INDUSTRY industry_l1
