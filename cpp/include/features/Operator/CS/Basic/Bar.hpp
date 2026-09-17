#pragma once

// =============================================================================
// Bar (CS) - 分钟价格的截面因子 (源 = Bar 节点 L1 列)
// =============================================================================
//   close_cs = pct_rank(z(winsor_mad(P_t)))   股价因子 (WinsorRank, 方法见 Method/CS.hpp)
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_CsBar(X, CAT1) \
  X(close_cs, CAT1, AUTO, "Price Factor", "股价因子", "pct_rank(z(winsor_mad(close)))", R"(\mathrm{pctl}(z(\mathrm{w}(P^{\mathrm{close}}_t))))", CS(1, close, None, WinsorRank))
