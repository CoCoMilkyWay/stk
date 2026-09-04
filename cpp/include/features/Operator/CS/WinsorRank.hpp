#pragma once

// =============================================================================
// WinsorRank - 截面 winsor_mad(k=3) → z → pct_rank → 均值填充, 输出 ∈ [0,1]
// =============================================================================
//   忠实性契约: 与 qmt/cpp/src/feature/cs.cpp factor_pipeline 逐步一致
//   (中位数取法 / z 的 double 累加 / pct_rank 并列均秩 / 均值填充), 保证 qmt 因子效果可复现.
// =============================================================================

#include <cstddef>

namespace cs {

struct WinsorRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n); // CSKernels.cpp
};

} // namespace cs

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_WinsorRank(X)                                                                                                                                                                                      \
  X(mcap_cs, BASIC, RANK, NONE, "Market Cap Factor", "总市值因子", "pct_rank(z(winsor_mad(mcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mcap}))))", CS(1, mcap, None, WinsorRank))                                 \
  X(fmcap_cs, BASIC, RANK, NONE, "Float Market Cap Factor", "流通市值因子", "pct_rank(z(winsor_mad(fmcap)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{fmcap}))))", CS(1, fmcap, None, WinsorRank))                     \
  X(close_cs, BASIC, RANK, NONE, "Price Factor", "股价因子", "pct_rank(z(winsor_mad(P_t)))", R"(\mathrm{pctl}(z(\mathrm{w}(P_t))))", CS(1, _ohlc_close, None, WinsorRank))                                           \
  X(cffoa_ttm12, BASIC, RANK, NONE, "CFFOA Factor", "现金流改善因子", "pct_rank(z(winsor_mad(cffoa_raw)))(qmt同款,无中性化)", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{cffoa}))))", CS(1, cffoa_raw, None, WinsorRank)) \
  X(mr_bal_cs, BASIC, RANK, NONE, "Margin Buy Factor", "融资余额因子", "pct_rank(z(winsor_mad(mr_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{mr\_bal}))))", CS(1, mr_bal, None, WinsorRank))                      \
  X(ms_bal_cs, BASIC, RANK, NONE, "Margin Sell Factor", "融券余额因子", "pct_rank(z(winsor_mad(ms_bal)))", R"(\mathrm{pctl}(z(\mathrm{w}(\mathrm{ms\_bal}))))", CS(1, ms_bal, None, WinsorRank))
