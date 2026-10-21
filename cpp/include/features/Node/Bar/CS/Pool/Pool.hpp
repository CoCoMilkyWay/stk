#pragma once

// =============================================================================
// Pool (CS) - 池列: 每行 = "收盘按 PoolKey 调仓"的成员位 (1 = 入池, 0 = 不入; 截面 = cs_valid 在市集)
// =============================================================================
//   pool_<pool><N> = Select{Low,High}<N>(key_*), N ∈ {400, 200}: 键有限值 (母集内) 中取低端 / 高端前 N, 并列按资产轴序先到先得 (方法见 Method/CS.hpp);
//     同键同序 ⇒ <pool>200 ⊂ <pool>400.
//   pool_st / pool_sst = SelectAll(key_st / key_sst): ST / *ST 全选 (不截断, 互斥).
//   列名 pool_<pool> 的 <pool> = Lob 档池名 (本类每列即一个池, 由 feature_pools 展开, 见 shared/FeatureLib.hpp),
//   L_1day (收盘行): Lob 档交易日 D 的池 = 本池 D−1 的 L_1day 行的该列 (shared/Pool.hpp). 全部日频调仓. 加一列即多一个池.
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_1day_CsPool(X, CAT1)                                                                                                                                                                                         \
  X(pool_mega400, CAT1, AUTO, "Pool Mega 400", "大盘池400", "母集内实时总市值最大400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size, None, SelectHigh400))                     \
  X(pool_mega200, CAT1, AUTO, "Pool Mega 200", "大盘池200", "母集内实时总市值最大200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size, None, SelectHigh200))                     \
  X(pool_micro400, CAT1, AUTO, "Pool Micro 400", "微盘池400", "母集∧微盘行业白名单内实时总市值最小400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size_micro, None, SelectLow400)) \
  X(pool_micro200, CAT1, AUTO, "Pool Micro 200", "微盘池200", "母集∧微盘行业白名单内实时总市值最小200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size_micro, None, SelectLow200)) \
  X(pool_pe_low400, CAT1, AUTO, "Pool PE Low 400", "低PE池400", "母集内 −EP 最小400只 (EP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{EP}) \le 400])", CS(bar, L_1day, key_pe, None, SelectLow400))                  \
  X(pool_pe_low200, CAT1, AUTO, "Pool PE Low 200", "低PE池200", "母集内 −EP 最小200只 (EP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{EP}) \le 200])", CS(bar, L_1day, key_pe, None, SelectLow200))                  \
  X(pool_pe_high400, CAT1, AUTO, "Pool PE High 400", "高PE池400", "母集内 −EP 最大400只 (亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{EP}) \le 400])", CS(bar, L_1day, key_pe, None, SelectHigh400))        \
  X(pool_pe_high200, CAT1, AUTO, "Pool PE High 200", "高PE池200", "母集内 −EP 最大200只 (亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{EP}) \le 200])", CS(bar, L_1day, key_pe, None, SelectHigh200))        \
  X(pool_pb_low400, CAT1, AUTO, "Pool PB Low 400", "低PB池400", "母集内 −BP 最小400只 (BP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{BP}) \le 400])", CS(bar, L_1day, key_pb, None, SelectLow400))                  \
  X(pool_pb_low200, CAT1, AUTO, "Pool PB Low 200", "低PB池200", "母集内 −BP 最小200只 (BP 最高)", R"(\mathbf{1}[\mathrm{rank}\uparrow(-\mathrm{BP}) \le 200])", CS(bar, L_1day, key_pb, None, SelectLow200))                  \
  X(pool_pb_high400, CAT1, AUTO, "Pool PB High 400", "高PB池400", "母集内 −BP 最大400只 (负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{BP}) \le 400])", CS(bar, L_1day, key_pb, None, SelectHigh400))    \
  X(pool_pb_high200, CAT1, AUTO, "Pool PB High 200", "高PB池200", "母集内 −BP 最大200只 (负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\downarrow(-\mathrm{BP}) \le 200])", CS(bar, L_1day, key_pb, None, SelectHigh200))    \
  X(pool_dy_high400, CAT1, AUTO, "Pool DY High 400", "高股息池400", "母集内股息率TTM最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 400])", CS(bar, L_1day, key_dy, None, SelectHigh400))                  \
  X(pool_dy_high200, CAT1, AUTO, "Pool DY High 200", "高股息池200", "母集内股息率TTM最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 200])", CS(bar, L_1day, key_dy, None, SelectHigh200))                  \
  X(pool_price_high400, CAT1, AUTO, "Pool Price High 400", "高价股池400", "母集内收盘价最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectHigh400))                      \
  X(pool_price_high200, CAT1, AUTO, "Pool Price High 200", "高价股池200", "母集内收盘价最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectHigh200))                      \
  X(pool_price_low400, CAT1, AUTO, "Pool Price Low 400", "低价股池400", "母集内收盘价最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectLow400))                           \
  X(pool_price_low200, CAT1, AUTO, "Pool Price Low 200", "低价股池200", "母集内收盘价最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectLow200))                           \
  X(pool_mom_pos400, CAT1, AUTO, "Pool Momentum Pos 400", "正动量池400", "母集内20日收益率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 400])", CS(bar, L_1day, key_mom, None, SelectHigh400))                \
  X(pool_mom_pos200, CAT1, AUTO, "Pool Momentum Pos 200", "正动量池200", "母集内20日收益率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 200])", CS(bar, L_1day, key_mom, None, SelectHigh200))                \
  X(pool_mom_neg400, CAT1, AUTO, "Pool Momentum Neg 400", "负动量池400", "母集内20日收益率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 400])", CS(bar, L_1day, key_mom, None, SelectLow400))                   \
  X(pool_mom_neg200, CAT1, AUTO, "Pool Momentum Neg 200", "负动量池200", "母集内20日收益率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 200])", CS(bar, L_1day, key_mom, None, SelectLow200))                   \
  X(pool_st, CAT1, AUTO, "Pool ST", "ST池", "活跃∧次日ST(不含*ST)全部", R"(\mathbf{1}[\mathrm{st}_{D} = 1])", CS(bar, L_1day, key_st, None, SelectAll))                                                                       \
  X(pool_sst, CAT1, AUTO, "Pool *ST", "*ST池", "活跃∧次日*ST全部", R"(\mathbf{1}[\mathrm{st}_{D} = 2])", CS(bar, L_1day, key_sst, None, SelectAll))
