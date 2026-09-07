#pragma once

// =============================================================================
// Fund (CS) - 日频基本面的截面因子 (源 = Fund 节点 L1 广播列)
// =============================================================================
//   dy_ttm_cs / roe_ttm_cs / roa_ttm_cs   = pct_rank(z(neutralize(winsor_q(src))))   行业/市值中性化 (NeutralRank)
//   cfo_chg_ttm_cs / rz_bal_cs / rq_bal_cs = pct_rank(z(winsor_mad(src)))            qmt 同款, 无中性化 (WinsorRank)
//   命名: <TS 源列>_cs; 方法见 Method/CS.hpp
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_CsFund(X, CAT1)                                                                                                                                                                                                   \
  X(dy_ttm_cs, CAT1, RANK, NONE, "Neutral DY TTM", "中性DY", "pct_rank(z(neutralize(winsor_q(dy_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{dy}))))", CS(1, dy_ttm, None, NeutralRank))                                       \
  X(cfo_chg_ttm_cs, CAT1, RANK, NONE, "CFO Change Factor", "现金流改善因子", "pct_rank(z(winsor_mad(cfo_chg_ttm)))(qmt同款,无中性化)", R"(\mathrm{pctl}(z(\mathrm{w}(\Delta\mathrm{cfo}))))", CS(1, cfo_chg_ttm, None, WinsorRank)) \
  X(roe_ttm_cs, CAT1, RANK, NONE, "Neutral ROE TTM", "中性ROE", "pct_rank(z(neutralize(winsor_q(roe_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roe}))))", CS(1, roe_ttm, None, NeutralRank))                                 \
  X(roa_ttm_cs, CAT1, RANK, NONE, "Neutral ROA TTM", "中性ROA", "pct_rank(z(neutralize(winsor_q(roa_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roa}))))", CS(1, roa_ttm, None, NeutralRank))                                 \
  X(rz_bal_cs, CAT1, RANK, NONE, "Margin Buy Factor", "融资余额因子", "pct_rank(z(winsor_mad(rz_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{rz\_bal}))))", CS(1, rz_bal, None, WinsorRank))                                      \
  X(rq_bal_cs, CAT1, RANK, NONE, "Margin Sell Factor", "融券余额因子", "pct_rank(z(winsor_mad(rq_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{rq\_bal}))))", CS(1, rq_bal, None, WinsorRank))
