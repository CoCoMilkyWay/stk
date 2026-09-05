#pragma once

// =============================================================================
// Valuation (CS) - 实时估值的截面因子 (源 = Val 节点 L1 列)
// =============================================================================
//   ep / bp / sp / cp   = pct_rank(z(neutralize(winsor_q(1/pe|pb|ps|pcf))))   收益率口径 + 行业/市值中性化 (Reciprocal + NeutralRank)
//   mcap_cs / fmcap_cs  = pct_rank(z(winsor_mad(mcap|fmcap)))                  市值因子 (WinsorRank)
//   方法见 Method/CS.hpp
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_CsValuation(X, CAT1)                                                                                                                                                        \
  X(ep_ttm12, CAT1, RANK, NONE, "Neutral EP", "中性EP", "pct_rank(z(neutralize(winsor_q(1/pe))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pe}))))", CS(1, pe, Reciprocal, NeutralRank))    \
  X(bp_ttm3, CAT1, RANK, NONE, "Neutral BP", "中性BP", "pct_rank(z(neutralize(winsor_q(1/pb))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pb}))))", CS(1, pb, Reciprocal, NeutralRank))     \
  X(sp_ttm12, CAT1, RANK, NONE, "Neutral SP", "中性SP", "pct_rank(z(neutralize(winsor_q(1/ps))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{ps}))))", CS(1, ps, Reciprocal, NeutralRank))    \
  X(cp_ttm12, CAT1, RANK, NONE, "Neutral CP", "中性CP", "pct_rank(z(neutralize(winsor_q(1/pcf))))", R"(\mathrm{pctl}(z(\mathrm{neu}(1/\mathrm{pcf}))))", CS(1, pcf, Reciprocal, NeutralRank)) \
  X(mcap_cs, CAT1, RANK, NONE, "Market Cap Factor", "总市值因子", "pct_rank(z(winsor_mad(mcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mcap}))))", CS(1, mcap, None, WinsorRank))           \
  X(fmcap_cs, CAT1, RANK, NONE, "Float Market Cap Factor", "流通市值因子", "pct_rank(z(winsor_mad(fmcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{fmcap}))))", CS(1, fmcap, None, WinsorRank))
