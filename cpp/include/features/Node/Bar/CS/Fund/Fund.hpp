#pragma once

// =============================================================================
// Fund (CS) - 日频基本面的截面列 (L_1day: 源 = Fund 节点 L_1day 列, 中性化上下文 = L_1min 末行 mcap (收盘市值) / L_1day ind_l1, 见 Method/CS.hpp)
// =============================================================================
//   dy_ttm_cs / roe_ttm_cs / roa_ttm_cs   = pct_rank(z(neutralize(winsor_q(src))))   行业/市值中性化 (NeutralRank)
//   cfo_chg_ttm_cs / rz_bal_cs / rq_bal_cs = pct_rank(z(winsor_mad(src)))            qmt 同款, 无中性化 (WinsorRank)
//   命名: <TS 源列>_cs; 方法见 Method/CS.hpp
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_1day_CsFund(X, CAT1)                                                                                                                                                                                                   \
  X(dy_ttm_cs, CAT1, AUTO, "Neutral DY TTM", "中性DY", "pct_rank(z(neutralize(winsor_q(dy_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{dy}))))", CS(bar, L_1day, dy_ttm, None, NeutralRank))                                       \
  X(cfo_chg_ttm_cs, CAT1, AUTO, "CFO Change Factor", "现金流改善因子", "pct_rank(z(winsor_mad(cfo_chg_ttm)))(qmt同款,无中性化)", R"(\mathrm{pctl}(z(\mathrm{w}(\Delta\mathrm{cfo}))))", CS(bar, L_1day, cfo_chg_ttm, None, WinsorRank)) \
  X(roe_ttm_cs, CAT1, AUTO, "Neutral ROE TTM", "中性ROE", "pct_rank(z(neutralize(winsor_q(roe_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roe}))))", CS(bar, L_1day, roe_ttm, None, NeutralRank))                                 \
  X(roa_ttm_cs, CAT1, AUTO, "Neutral ROA TTM", "中性ROA", "pct_rank(z(neutralize(winsor_q(roa_ttm))))", R"(\mathrm{pctl}(z(\mathrm{neu}(\mathrm{roa}))))", CS(bar, L_1day, roa_ttm, None, NeutralRank))                                 \
  X(rz_bal_cs, CAT1, AUTO, "Margin Buy Factor", "融资余额因子", "pct_rank(z(winsor_mad(rz_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{rz\_bal}))))", CS(bar, L_1day, rz_bal, None, WinsorRank))                                      \
  X(rq_bal_cs, CAT1, AUTO, "Margin Sell Factor", "融券余额因子", "pct_rank(z(winsor_mad(rq_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{rq\_bal}))))", CS(bar, L_1day, rq_bal, None, WinsorRank))
