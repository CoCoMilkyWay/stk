#pragma once

// =============================================================================
// Ohlc (CS) - 分钟价格的截面因子 (源 = Ohlc 节点 L1 列)
// =============================================================================
//   close_cs = pct_rank(z(winsor_mad(P_t)))   股价因子 (WinsorRank, 方法见 Method/CS.hpp)
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_CsOhlc(X, CAT1) \
  X(close_cs, CAT1, RANK, NONE, "Price Factor", "股价因子", "pct_rank(z(winsor_mad(P_t)))", R"(\mathrm{pctl}(z(\mathrm{w}(P_t))))", CS(1, _ohlc_close, None, WinsorRank))
