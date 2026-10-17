#pragma once

// =============================================================================
// Pool (CS) - 票池列: 每行 = "收盘按 PoolKey 调仓"的成员位 (1 = 入池, 0 = 不入; 截面 = cs_valid 在市集)
// =============================================================================
//   pool_<pool> = Select{Low,High}400(key_*): 键有限值 (母集内) 中取低端 / 高端前 400, 并列按资产轴序先到先得 (方法见 Method/CS.hpp).
//   列名 pool_<pool> 的 <pool> = Lob 档库名 (本类每列即一个库, 由 feature_pools 展开, 见 shared/FeatureLib.hpp),
//   L_day (收盘行): Lob 档交易日 D 的票池 = 本库 D−1 的 L_day 行的该列 (shared/Pool.hpp). 全部日频调仓. 加一列即多一个库.
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_day_CsPool(X, CAT1)                                                                                                                                                                               \
  X(pool_mega, CAT1, AUTO, "Pool Mega", "巨盘池", "母集内实时总市值最大400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 400])", CS(bar, L_day, key_size, None, SelectHigh400))                     \
  X(pool_micro, CAT1, AUTO, "Pool Micro", "微盘池", "母集∧微盘行业白名单内实时总市值最小400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_day, key_size_micro, None, SelectLow400)) \
  X(pool_pe_low, CAT1, AUTO, "Pool PE Low", "低PE池", "母集内 −EP 最小400只 (EP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{EP}) \le 400])", CS(bar, L_day, key_pe, None, SelectLow400))                  \
  X(pool_pe_high, CAT1, AUTO, "Pool PE High", "高PE池", "母集内 −EP 最大400只 (亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{EP}) \le 400])", CS(bar, L_day, key_pe, None, SelectHigh400))        \
  X(pool_pb_low, CAT1, AUTO, "Pool PB Low", "低PB池", "母集内 −BP 最小400只 (BP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{BP}) \le 400])", CS(bar, L_day, key_pb, None, SelectLow400))                  \
  X(pool_pb_high, CAT1, AUTO, "Pool PB High", "高PB池", "母集内 −BP 最大400只 (负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{BP}) \le 400])", CS(bar, L_day, key_pb, None, SelectHigh400))    \
  X(pool_dy_high, CAT1, AUTO, "Pool DY High", "高股息池", "母集内股息率TTM最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 400])", CS(bar, L_day, key_dy, None, SelectHigh400))
