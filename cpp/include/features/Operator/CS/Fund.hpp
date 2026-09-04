#pragma once

// =============================================================================
// Fund (CS) - 日频基本面的截面因子 (源 = Fund 节点 L1 广播列)
// =============================================================================
//   dy / roe / roa        = pct_rank(z(neutralize(winsor_q(raw))))   行业/市值中性化 (NeutralRank)
//   cffoa / mr_bal / ms_bal = pct_rank(z(winsor_mad(raw)))            qmt 同款, 无中性化 (WinsorRank)
//   方法见 Method/CS.hpp
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_CsFund(X)                                                                                                                                                                                          \
  X(dy_ttm12, BASIC, RANK, NONE, "Neutral DY", "中性DY", "pct_rank(z(neutralize(winsor_q(dy_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{dy}))))", CS(1, dy_raw, None, NeutralRank))                            \
  X(cffoa_ttm12, BASIC, RANK, NONE, "CFFOA Factor", "现金流改善因子", "pct_rank(z(winsor_mad(cffoa_raw)))(qmt同款,无中性化)", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{cffoa}))))", CS(1, cffoa_raw, None, WinsorRank)) \
  X(roe_ttm12, BASIC, RANK, NONE, "Neutral ROE", "中性ROE", "pct_rank(z(neutralize(winsor_q(roe_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roe}))))", CS(1, roe_raw, None, NeutralRank))                      \
  X(roa_ttm12, BASIC, RANK, NONE, "Neutral ROA", "中性ROA", "pct_rank(z(neutralize(winsor_q(roa_raw))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roa}))))", CS(1, roa_raw, None, NeutralRank))                      \
  X(mr_bal_cs, BASIC, RANK, NONE, "Margin Buy Factor", "融资余额因子", "pct_rank(z(winsor_mad(mr_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mr\_bal}))))", CS(1, mr_bal, None, WinsorRank))                      \
  X(ms_bal_cs, BASIC, RANK, NONE, "Margin Sell Factor", "融券余额因子", "pct_rank(z(winsor_mad(ms_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{ms\_bal}))))", CS(1, ms_bal, None, WinsorRank))
