#pragma once

// =============================================================================
// Spread (CS) - 价差的截面排名: L_sec 每秒 / L_1min 每分钟 (源均为 L_sec spread, L_1min 取分钟起始秒)
// =============================================================================
//   spread_cs = Φ⁻¹(pctl(spread))   (NormRank, 方法见 Method/CS.hpp)
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_sec_CsSpread(X, CAT1) X(spread_cs, CAT1, RANK, "Spread Factor", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))

#define FIELDS_L_1min_CsSpread(X, CAT1) X(spread_cs, CAT1, RANK, "Spread Factor", "价差截面排名", "spread截面rank→inverse normal", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))", CS(0, spread, None, NormRank))
