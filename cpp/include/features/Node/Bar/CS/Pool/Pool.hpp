#pragma once

// =============================================================================
// Pool (CS) - 池列: 每行 = "收盘按 PoolKey 调仓"的成员位 (1 = 入池, 0 = 不入; 截面 = 全轴, 成员范围由键掩码定, 见 Method/CS.hpp FullAxis)
// =============================================================================
//   pool_<pool><N> = Select{Low,High}<N>(key_*), N ∈ {400, 200}: 键有限值 (母集内) 中取低端 / 高端前 N, 并列按资产轴序先到先得 (方法见 Method/CS.hpp);
//     同键同序 ⇒ <pool>200 ⊂ <pool>400. 键在母集内覆盖不足 (数据起点后财务项未到齐) 时全选母集, 指数不留空段.
//   pool_st / pool_sst = SelectAll(key_st / key_sst): ST / *ST 全选 (不截断, 互斥).
//   列名 pool_<pool> 的 <pool> = Lob 档池名 (本类每列即一个池, 由 feature_pools 展开, 见 shared/FeatureLib.hpp),
//   L_1day (收盘行): Lob 档交易日 D 的池 = 本池 D−1 的 L_1day 行的该列 (shared/Pool.hpp). 全部日频调仓. 加一列即多一个池.
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_1day_CsPool(X, CAT1)                                                                                                                                                                                               \
  X(pool_size_small400, CAT1, AUTO, "Pool Size Small 400", "小盘池400", "母集内收盘总市值最小400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size, None, SelectLow400))                  \
  X(pool_size_small200, CAT1, AUTO, "Pool Size Small 200", "小盘池200", "母集内收盘总市值最小200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size, None, SelectLow200))                  \
  X(pool_size_big400, CAT1, AUTO, "Pool Size Big 400", "大盘池400", "母集内收盘总市值最大400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size, None, SelectHigh400))                   \
  X(pool_size_big200, CAT1, AUTO, "Pool Size Big 200", "大盘池200", "母集内收盘总市值最大200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size, None, SelectHigh200))                   \
  X(pool_ep_high400, CAT1, AUTO, "Pool EP High 400", "高EP池400", "母集内EP最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{EP}) \le 400])", CS(bar, L_1day, key_ep, None, SelectHigh400))                                 \
  X(pool_ep_high200, CAT1, AUTO, "Pool EP High 200", "高EP池200", "母集内EP最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{EP}) \le 200])", CS(bar, L_1day, key_ep, None, SelectHigh200))                                 \
  X(pool_ep_low400, CAT1, AUTO, "Pool EP Low 400", "低EP池400", "母集内EP最低400只(亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{EP}) \le 400])", CS(bar, L_1day, key_ep, None, SelectLow400))                        \
  X(pool_ep_low200, CAT1, AUTO, "Pool EP Low 200", "低EP池200", "母集内EP最低200只(亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{EP}) \le 200])", CS(bar, L_1day, key_ep, None, SelectLow200))                        \
  X(pool_bp_high400, CAT1, AUTO, "Pool BP High 400", "高BP池400", "母集内BP最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{BP}) \le 400])", CS(bar, L_1day, key_bp, None, SelectHigh400))                                 \
  X(pool_bp_high200, CAT1, AUTO, "Pool BP High 200", "高BP池200", "母集内BP最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{BP}) \le 200])", CS(bar, L_1day, key_bp, None, SelectHigh200))                                 \
  X(pool_bp_low400, CAT1, AUTO, "Pool BP Low 400", "低BP池400", "母集内BP最低400只(负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{BP}) \le 400])", CS(bar, L_1day, key_bp, None, SelectLow400))                    \
  X(pool_bp_low200, CAT1, AUTO, "Pool BP Low 200", "低BP池200", "母集内BP最低200只(负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{BP}) \le 200])", CS(bar, L_1day, key_bp, None, SelectLow200))                    \
  X(pool_dy_high400, CAT1, AUTO, "Pool DY High 400", "高股息池400", "母集内股息率TTM最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 400])", CS(bar, L_1day, key_dy, None, SelectHigh400))                        \
  X(pool_dy_high200, CAT1, AUTO, "Pool DY High 200", "高股息池200", "母集内股息率TTM最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 200])", CS(bar, L_1day, key_dy, None, SelectHigh200))                        \
  X(pool_roe_high400, CAT1, AUTO, "Pool ROE High 400", "高ROE池400", "母集内单季ROE最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ROE}) \le 400])", CS(bar, L_1day, key_roe, None, SelectHigh400))                       \
  X(pool_roe_high200, CAT1, AUTO, "Pool ROE High 200", "高ROE池200", "母集内单季ROE最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ROE}) \le 200])", CS(bar, L_1day, key_roe, None, SelectHigh200))                       \
  X(pool_roe_low400, CAT1, AUTO, "Pool ROE Low 400", "低ROE池400", "母集内单季ROE最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ROE}) \le 400])", CS(bar, L_1day, key_roe, None, SelectLow400))                            \
  X(pool_roe_low200, CAT1, AUTO, "Pool ROE Low 200", "低ROE池200", "母集内单季ROE最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ROE}) \le 200])", CS(bar, L_1day, key_roe, None, SelectLow200))                            \
  X(pool_inv_low400, CAT1, AUTO, "Pool Investment Low 400", "低投资池400", "母集内总资产同比最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{inv}) \le 400])", CS(bar, L_1day, key_inv, None, SelectLow400))                 \
  X(pool_inv_low200, CAT1, AUTO, "Pool Investment Low 200", "低投资池200", "母集内总资产同比最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{inv}) \le 200])", CS(bar, L_1day, key_inv, None, SelectLow200))                 \
  X(pool_inv_high400, CAT1, AUTO, "Pool Investment High 400", "高投资池400", "母集内总资产同比最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{inv}) \le 400])", CS(bar, L_1day, key_inv, None, SelectHigh400))            \
  X(pool_inv_high200, CAT1, AUTO, "Pool Investment High 200", "高投资池200", "母集内总资产同比最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{inv}) \le 200])", CS(bar, L_1day, key_inv, None, SelectHigh200))            \
  X(pool_mom_high400, CAT1, AUTO, "Pool Momentum High 400", "高动量池400", "母集内12-1月动量最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{12-1}) \le 400])", CS(bar, L_1day, key_mom, None, SelectHigh400))                  \
  X(pool_mom_high200, CAT1, AUTO, "Pool Momentum High 200", "高动量池200", "母集内12-1月动量最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{12-1}) \le 200])", CS(bar, L_1day, key_mom, None, SelectHigh200))                  \
  X(pool_mom_low400, CAT1, AUTO, "Pool Momentum Low 400", "低动量池400", "母集内12-1月动量最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{12-1}) \le 400])", CS(bar, L_1day, key_mom, None, SelectLow400))                       \
  X(pool_mom_low200, CAT1, AUTO, "Pool Momentum Low 200", "低动量池200", "母集内12-1月动量最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{12-1}) \le 200])", CS(bar, L_1day, key_mom, None, SelectLow200))                       \
  X(pool_rev_low400, CAT1, AUTO, "Pool Reversal Low 400", "上月输家池400", "母集内20日收益率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 400])", CS(bar, L_1day, key_rev, None, SelectLow400))                       \
  X(pool_rev_low200, CAT1, AUTO, "Pool Reversal Low 200", "上月输家池200", "母集内20日收益率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 200])", CS(bar, L_1day, key_rev, None, SelectLow200))                       \
  X(pool_rev_high400, CAT1, AUTO, "Pool Reversal High 400", "上月赢家池400", "母集内20日收益率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 400])", CS(bar, L_1day, key_rev, None, SelectHigh400))                  \
  X(pool_rev_high200, CAT1, AUTO, "Pool Reversal High 200", "上月赢家池200", "母集内20日收益率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 200])", CS(bar, L_1day, key_rev, None, SelectHigh200))                  \
  X(pool_turn_low400, CAT1, AUTO, "Pool Turnover Ratio Low 400", "低换手池400", "母集内20/250日换手比最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{turn}) \le 400])", CS(bar, L_1day, key_turn, None, SelectLow400))      \
  X(pool_turn_low200, CAT1, AUTO, "Pool Turnover Ratio Low 200", "低换手池200", "母集内20/250日换手比最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{turn}) \le 200])", CS(bar, L_1day, key_turn, None, SelectLow200))      \
  X(pool_turn_high400, CAT1, AUTO, "Pool Turnover Ratio High 400", "高换手池400", "母集内20/250日换手比最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{turn}) \le 400])", CS(bar, L_1day, key_turn, None, SelectHigh400)) \
  X(pool_turn_high200, CAT1, AUTO, "Pool Turnover Ratio High 200", "高换手池200", "母集内20/250日换手比最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{turn}) \le 200])", CS(bar, L_1day, key_turn, None, SelectHigh200)) \
  X(pool_ivol_low400, CAT1, AUTO, "Pool IVOL Low 400", "低特质波动池400", "母集内特质波动率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ivol}) \le 400])", CS(bar, L_1day, key_ivol, None, SelectLow400))                \
  X(pool_ivol_low200, CAT1, AUTO, "Pool IVOL Low 200", "低特质波动池200", "母集内特质波动率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ivol}) \le 200])", CS(bar, L_1day, key_ivol, None, SelectLow200))                \
  X(pool_ivol_high400, CAT1, AUTO, "Pool IVOL High 400", "高特质波动池400", "母集内特质波动率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ivol}) \le 400])", CS(bar, L_1day, key_ivol, None, SelectHigh400))           \
  X(pool_ivol_high200, CAT1, AUTO, "Pool IVOL High 200", "高特质波动池200", "母集内特质波动率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ivol}) \le 200])", CS(bar, L_1day, key_ivol, None, SelectHigh200))           \
  X(pool_resvol_low400, CAT1, AUTO, "Pool ResVol Low 400", "低残差波动池400", "母集内残差波动率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{resvol}) \le 400])", CS(bar, L_1day, key_resvol, None, SelectLow400))        \
  X(pool_resvol_low200, CAT1, AUTO, "Pool ResVol Low 200", "低残差波动池200", "母集内残差波动率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{resvol}) \le 200])", CS(bar, L_1day, key_resvol, None, SelectLow200))        \
  X(pool_resvol_high400, CAT1, AUTO, "Pool ResVol High 400", "高残差波动池400", "母集内残差波动率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{resvol}) \le 400])", CS(bar, L_1day, key_resvol, None, SelectHigh400))   \
  X(pool_resvol_high200, CAT1, AUTO, "Pool ResVol High 200", "高残差波动池200", "母集内残差波动率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{resvol}) \le 200])", CS(bar, L_1day, key_resvol, None, SelectHigh200))   \
  X(pool_beta_low400, CAT1, AUTO, "Pool Beta Low 400", "低贝塔池400", "母集内市场β最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\beta) \le 400])", CS(bar, L_1day, key_beta, None, SelectLow400))                                 \
  X(pool_beta_low200, CAT1, AUTO, "Pool Beta Low 200", "低贝塔池200", "母集内市场β最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\beta) \le 200])", CS(bar, L_1day, key_beta, None, SelectLow200))                                 \
  X(pool_beta_high400, CAT1, AUTO, "Pool Beta High 400", "高贝塔池400", "母集内市场β最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\beta) \le 400])", CS(bar, L_1day, key_beta, None, SelectHigh400))                            \
  X(pool_beta_high200, CAT1, AUTO, "Pool Beta High 200", "高贝塔池200", "母集内市场β最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\beta) \le 200])", CS(bar, L_1day, key_beta, None, SelectHigh200))                            \
  X(pool_trend_high400, CAT1, AUTO, "Pool Trend High 400", "高趋势池400", "母集内趋势预期收益最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{trend}) \le 400])", CS(bar, L_1day, key_trend, None, SelectHigh400))         \
  X(pool_trend_high200, CAT1, AUTO, "Pool Trend High 200", "高趋势池200", "母集内趋势预期收益最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{trend}) \le 200])", CS(bar, L_1day, key_trend, None, SelectHigh200))         \
  X(pool_trend_low400, CAT1, AUTO, "Pool Trend Low 400", "低趋势池400", "母集内趋势预期收益最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{trend}) \le 400])", CS(bar, L_1day, key_trend, None, SelectLow400))              \
  X(pool_trend_low200, CAT1, AUTO, "Pool Trend Low 200", "低趋势池200", "母集内趋势预期收益最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{trend}) \le 200])", CS(bar, L_1day, key_trend, None, SelectLow200))              \
  X(pool_price_high400, CAT1, AUTO, "Pool Price High 400", "高价股池400", "母集内收盘价最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectHigh400))                            \
  X(pool_price_high200, CAT1, AUTO, "Pool Price High 200", "高价股池200", "母集内收盘价最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectHigh200))                            \
  X(pool_price_low400, CAT1, AUTO, "Pool Price Low 400", "低价股池400", "母集内收盘价最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectLow400))                                 \
  X(pool_price_low200, CAT1, AUTO, "Pool Price Low 200", "低价股池200", "母集内收盘价最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectLow200))                                 \
  X(pool_st, CAT1, AUTO, "Pool ST", "ST池", "活跃∧次日ST(不含*ST)全部", R"(\mathbf{1}[\mathrm{st}_{D} = 1])", CS(bar, L_1day, key_st, None, SelectAll))                                                                             \
  X(pool_sst, CAT1, AUTO, "Pool *ST", "*ST池", "活跃∧次日*ST全部", R"(\mathbf{1}[\mathrm{st}_{D} = 2])", CS(bar, L_1day, key_sst, None, SelectAll))
