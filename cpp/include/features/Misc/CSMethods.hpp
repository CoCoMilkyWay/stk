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

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_CrossSection(X) \
  X(cs_spread_rank, 1, ALL, CS, LIQUIDITY, RANK, RANK_ZSCORE, "100/00/00", "CS Spread Rank", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))

#define FIELDS_L1_CrossSection(X)                                                                                                                                                                                                              \
  X(cs_spread_rank, 1, DATA, CS, LIQUIDITY, RANK, RANK_ZSCORE, "00/100/00", "CS Spread Rank", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))                  \
  X(ep_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Neutral EP", "中性EP", "pct_rank(z(neutralize(winsor_q(1/pe))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pe}))))", CS(1, pe, Reciprocal, NeutralRank))                          \
  X(bp_ttm3, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Neutral BP", "中性BP", "pct_rank(z(neutralize(winsor_q(1/pb))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pb}))))", CS(1, pb, Reciprocal, NeutralRank))                           \
  X(sp_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Neutral SP", "中性SP", "pct_rank(z(neutralize(winsor_q(1/ps))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{ps}))))", CS(1, ps, Reciprocal, NeutralRank))                          \
  X(cp_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Neutral CP", "中性CP", "pct_rank(z(neutralize(winsor_q(1/pcf))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pcf}))))", CS(1, pcf, Reciprocal, NeutralRank))                       \
  X(mcap_cs, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Market Cap Factor", "总市值因子", "pct_rank(z(winsor_mad(mcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mcap}))))", CS(1, mcap, None, WinsorRank))                                 \
  X(fmcap_cs, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Float Market Cap Factor", "流通市值因子", "pct_rank(z(winsor_mad(fmcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{fmcap}))))", CS(1, fmcap, None, WinsorRank))                     \
  X(close_cs, 1, DATA, CS, BASIC, RANK, NONE, "00/010/00", "Price Factor", "股价因子", "pct_rank(z(winsor_mad(P_t)))", R"(\mathrm{pctl}(z(\mathrm{w}(P_t))))", CS(1, _ohlc_close, None, WinsorRank))                                           \
  X(dy_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "Neutral DY", "中性DY", "pct_rank(z(neutralize(winsor_q(dy_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{dy}))))", CS(1, dy_raw, None, NeutralRank))                            \
  X(cffoa_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "CFFOA Factor", "现金流改善因子", "pct_rank(z(winsor_mad(cffoa_raw)))(qmt同款,无中性化)", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{cffoa}))))", CS(1, cffoa_raw, None, WinsorRank)) \
  X(roe_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "Neutral ROE", "中性ROE", "pct_rank(z(neutralize(winsor_q(roe_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roe}))))", CS(1, roe_raw, None, NeutralRank))                      \
  X(roa_ttm12, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "Neutral ROA", "中性ROA", "pct_rank(z(neutralize(winsor_q(roa_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roa}))))", CS(1, roa_raw, None, NeutralRank))                      \
  X(mr_bal_cs, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "Margin Buy Factor", "融资余额因子", "pct_rank(z(winsor_mad(mr_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mr\_bal}))))", CS(1, mr_bal, None, WinsorRank))                      \
  X(ms_bal_cs, 1, DATA, CS, BASIC, RANK, NONE, "00/000/00", "Margin Sell Factor", "融券余额因子", "pct_rank(z(winsor_mad(ms_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{ms\_bal}))))", CS(1, ms_bal, None, WinsorRank))
