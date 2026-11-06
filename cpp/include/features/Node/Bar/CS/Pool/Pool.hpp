#pragma once

// =============================================================================
// Pool (CS) - 池列: 每行 = "收盘按 PoolKey 调仓"的成员位 (1 = 入池, 0 = 不入; 截面 = 全轴, 成员范围由键掩码定, 见 Method/CS.hpp FullAxis)
// =============================================================================
//   池表 = all (全市场, 成员 = 活跃, 含 ST / *ST / 退市整理期) + 本类每列一个子池 (列名 pool_<pool> 的 <pool> = 子池名, 由 feature_pools 展开,
//     见 shared/FeatureLib.hpp). 加一列即多一个池.
//
//   范围口径 (键自带掩码, 判据见 Method/Fundamental.cpp univ_mask; 各口按 D 日盘前态判):
//     活跃 = 非北交所 ∧ 上市满 365 日历日 ∧ 未退市 ∧ 非停牌 ∧ 申万一级行业已知
//     母集 = 活跃 ∧ st_level == 0 (剔除 ST / *ST / 退市整理期) ∧ 下列风险警示预判皆 0:
//       st_trading   连续 15 日 (收盘价 < 1 元 ∨ 总市值 < 阈值; 主板 5 亿, 其他 3 亿)
//       st_profit    年报预告首亏 / 续亏 ∧ 上年归母净利 < 0
//       st_revenue   主板 ∧ 年报预亏 ∧ 营收 TTM < 1 亿 (报告期 2021–2023) / < 3 亿 (2024 起)
//       st_dividend  主板 ∧ 上市满 3 年 ∧ 近两年年报归母净利均值 > 0 ∧ 近 3 年累计分红 < 30% 净利 ∧ < 5000 万
//     (主板 = cn_stock_basic_info.list_sector == 1); 子池键另要求收盘市值 > 0. 退市整理期只在 all, 不进任何子池.
//
//   板块 (落盘列 board = fund::Board, 代码判定见 Method/Fundamental.hpp board_of; 前缀外 (B 股等) = 0 未知):
//     1 沪市主板 600 / 601 / 603 / 605        涨跌幅 10%
//     2 深市主板 000 / 001 / 002 / 003 / 004  涨跌幅 10%
//     3 科创板   688 / 689 (CDR)              涨跌幅 20%
//     4 创业板   300 / 301 / 302 / 309 (CDR)  涨跌幅 20%
//     5 北交所   代码 .BJ                     涨跌幅 30%, 不活跃 ⇒ 不进任何池 (含 all)
//
//   池列:
//     pool_<pool><N> = Select{Low,High}<N>(key_*), N ∈ {400, 200}: 母集内键有限值取低端 / 高端前 N, 并列按资产轴序先到先得 (方法见 Method/CS.hpp);
//       同键同序 ⇒ <pool>200 ⊂ <pool>400. 键在母集内覆盖不足 (数据起点后财务项未到齐) 时全选母集, 指数不留空段.
//       size_main_small<N>: 范围 = 母集 ∧ 主板 (board = 沪市主板 / 深市主板); size_margin_small<N>: 范围 = 母集 ∧ 融资融券标的 (is_margin).
//     pool_st / pool_sst = SelectAll(key_st / key_sst): 活跃内 ST / *ST 全选 (不截断, 互斥).
//     pool_<板块> = SelectAll(key_<板块>): 活跃内该板块全选 (含 ST / *ST, 互斥).
//     pool_margin / pool_nonmargin = SelectAll(key_margin / key_nonmargin): 活跃内融资融券标的 / 非标的全选 (含 ST / *ST, 互补).
//   权重: 子池一律等权 (组合页池线 / 估值); all 加权 / 等权两条.
//     中文名: 只名切片, 不带 "池" / 权重 (篮子行名自带 .等 / .加); 两端对仗 (高 / 低, 赢家 / 输家), 风格池尾标 N; 波动两池缩写 特波 = 特质波动, 残波 = 残差波动.
//
//   时点: L_1day (收盘行), 全部日频调仓. 子池库交易日 D 的成员 = all Bar 库 D−1 的 L_1day 行的该列 (shared/Pool.hpp), 无前视.
//   只有 all Bar 库的池列有意义 (子池库里同列照算, 截面是池内, 不读).
// =============================================================================

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_1day_CsPool(X, CAT1)                                                                                                                                                                                                                           \
  X(pool_size_small400, CAT1, AUTO, "Pool Size Small 400", "小盘400", "母集内收盘总市值最小400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size, None, SelectLow400))                                                \
  X(pool_size_small200, CAT1, AUTO, "Pool Size Small 200", "小盘200", "母集内收盘总市值最小200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size, None, SelectLow200))                                                \
  X(pool_size_big400, CAT1, AUTO, "Pool Size Big 400", "大盘400", "母集内收盘总市值最大400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size, None, SelectHigh400))                                                 \
  X(pool_size_big200, CAT1, AUTO, "Pool Size Big 200", "大盘200", "母集内收盘总市值最大200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size, None, SelectHigh200))                                                 \
  X(pool_size_main_small400, CAT1, AUTO, "Pool Size Main Small 400", "主板小盘400", "母集∧主板内收盘总市值最小400只", R"(\mathbf{1}[\mathrm{rank}^{main}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size_main, None, SelectLow400))                 \
  X(pool_size_main_small200, CAT1, AUTO, "Pool Size Main Small 200", "主板小盘200", "母集∧主板内收盘总市值最小200只", R"(\mathbf{1}[\mathrm{rank}^{main}\uparrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size_main, None, SelectLow200))                 \
  X(pool_size_margin_small400, CAT1, AUTO, "Pool Size Margin Small 400", "融资小盘400", "母集∧融资融券标的内收盘总市值最小400只", R"(\mathbf{1}[\mathrm{rank}^{margin}\uparrow(\mathrm{mcap}) \le 400])", CS(bar, L_1day, key_size_margin, None, SelectLow400)) \
  X(pool_size_margin_small200, CAT1, AUTO, "Pool Size Margin Small 200", "融资小盘200", "母集∧融资融券标的内收盘总市值最小200只", R"(\mathbf{1}[\mathrm{rank}^{margin}\uparrow(\mathrm{mcap}) \le 200])", CS(bar, L_1day, key_size_margin, None, SelectLow200)) \
  X(pool_ep_high400, CAT1, AUTO, "Pool EP High 400", "高EP400", "母集内EP最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{EP}) \le 400])", CS(bar, L_1day, key_ep, None, SelectHigh400))                                                               \
  X(pool_ep_high200, CAT1, AUTO, "Pool EP High 200", "高EP200", "母集内EP最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{EP}) \le 200])", CS(bar, L_1day, key_ep, None, SelectHigh200))                                                               \
  X(pool_ep_low400, CAT1, AUTO, "Pool EP Low 400", "低EP400", "母集内EP最低400只(亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{EP}) \le 400])", CS(bar, L_1day, key_ep, None, SelectLow400))                                                      \
  X(pool_ep_low200, CAT1, AUTO, "Pool EP Low 200", "低EP200", "母集内EP最低200只(亏损最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{EP}) \le 200])", CS(bar, L_1day, key_ep, None, SelectLow200))                                                      \
  X(pool_bp_high400, CAT1, AUTO, "Pool BP High 400", "高BP400", "母集内BP最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{BP}) \le 400])", CS(bar, L_1day, key_bp, None, SelectHigh400))                                                               \
  X(pool_bp_high200, CAT1, AUTO, "Pool BP High 200", "高BP200", "母集内BP最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{BP}) \le 200])", CS(bar, L_1day, key_bp, None, SelectHigh200))                                                               \
  X(pool_bp_low400, CAT1, AUTO, "Pool BP Low 400", "低BP400", "母集内BP最低400只(负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{BP}) \le 400])", CS(bar, L_1day, key_bp, None, SelectLow400))                                                  \
  X(pool_bp_low200, CAT1, AUTO, "Pool BP Low 200", "低BP200", "母集内BP最低200只(负净资产最深在前)", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{BP}) \le 200])", CS(bar, L_1day, key_bp, None, SelectLow200))                                                  \
  X(pool_dy_high400, CAT1, AUTO, "Pool DY High 400", "高息400", "母集内股息率TTM最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 400])", CS(bar, L_1day, key_dy, None, SelectHigh400))                                                        \
  X(pool_dy_high200, CAT1, AUTO, "Pool DY High 200", "高息200", "母集内股息率TTM最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{dy}) \le 200])", CS(bar, L_1day, key_dy, None, SelectHigh200))                                                        \
  X(pool_roe_high400, CAT1, AUTO, "Pool ROE High 400", "高ROE400", "母集内单季ROE最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ROE}) \le 400])", CS(bar, L_1day, key_roe, None, SelectHigh400))                                                     \
  X(pool_roe_high200, CAT1, AUTO, "Pool ROE High 200", "高ROE200", "母集内单季ROE最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ROE}) \le 200])", CS(bar, L_1day, key_roe, None, SelectHigh200))                                                     \
  X(pool_roe_low400, CAT1, AUTO, "Pool ROE Low 400", "低ROE400", "母集内单季ROE最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ROE}) \le 400])", CS(bar, L_1day, key_roe, None, SelectLow400))                                                          \
  X(pool_roe_low200, CAT1, AUTO, "Pool ROE Low 200", "低ROE200", "母集内单季ROE最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ROE}) \le 200])", CS(bar, L_1day, key_roe, None, SelectLow200))                                                          \
  X(pool_inv_low400, CAT1, AUTO, "Pool Investment Low 400", "低投资400", "母集内总资产同比最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{inv}) \le 400])", CS(bar, L_1day, key_inv, None, SelectLow400))                                               \
  X(pool_inv_low200, CAT1, AUTO, "Pool Investment Low 200", "低投资200", "母集内总资产同比最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{inv}) \le 200])", CS(bar, L_1day, key_inv, None, SelectLow200))                                               \
  X(pool_inv_high400, CAT1, AUTO, "Pool Investment High 400", "高投资400", "母集内总资产同比最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{inv}) \le 400])", CS(bar, L_1day, key_inv, None, SelectHigh400))                                          \
  X(pool_inv_high200, CAT1, AUTO, "Pool Investment High 200", "高投资200", "母集内总资产同比最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{inv}) \le 200])", CS(bar, L_1day, key_inv, None, SelectHigh200))                                          \
  X(pool_mom_high400, CAT1, AUTO, "Pool Momentum High 400", "高动量400", "母集内12-1月动量最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{12-1}) \le 400])", CS(bar, L_1day, key_mom, None, SelectHigh400))                                                \
  X(pool_mom_high200, CAT1, AUTO, "Pool Momentum High 200", "高动量200", "母集内12-1月动量最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{12-1}) \le 200])", CS(bar, L_1day, key_mom, None, SelectHigh200))                                                \
  X(pool_mom_low400, CAT1, AUTO, "Pool Momentum Low 400", "低动量400", "母集内12-1月动量最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{12-1}) \le 400])", CS(bar, L_1day, key_mom, None, SelectLow400))                                                     \
  X(pool_mom_low200, CAT1, AUTO, "Pool Momentum Low 200", "低动量200", "母集内12-1月动量最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{12-1}) \le 200])", CS(bar, L_1day, key_mom, None, SelectLow200))                                                     \
  X(pool_rev_low400, CAT1, AUTO, "Pool Reversal Low 400", "输家400", "母集内20日收益率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 400])", CS(bar, L_1day, key_rev, None, SelectLow400))                                                         \
  X(pool_rev_low200, CAT1, AUTO, "Pool Reversal Low 200", "输家200", "母集内20日收益率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(r_{20}) \le 200])", CS(bar, L_1day, key_rev, None, SelectLow200))                                                         \
  X(pool_rev_high400, CAT1, AUTO, "Pool Reversal High 400", "赢家400", "母集内20日收益率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 400])", CS(bar, L_1day, key_rev, None, SelectHigh400))                                                    \
  X(pool_rev_high200, CAT1, AUTO, "Pool Reversal High 200", "赢家200", "母集内20日收益率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(r_{20}) \le 200])", CS(bar, L_1day, key_rev, None, SelectHigh200))                                                    \
  X(pool_turn_low400, CAT1, AUTO, "Pool Turnover Ratio Low 400", "低换手400", "母集内20/250日换手比最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{turn}) \le 400])", CS(bar, L_1day, key_turn, None, SelectLow400))                                    \
  X(pool_turn_low200, CAT1, AUTO, "Pool Turnover Ratio Low 200", "低换手200", "母集内20/250日换手比最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{turn}) \le 200])", CS(bar, L_1day, key_turn, None, SelectLow200))                                    \
  X(pool_turn_high400, CAT1, AUTO, "Pool Turnover Ratio High 400", "高换手400", "母集内20/250日换手比最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{turn}) \le 400])", CS(bar, L_1day, key_turn, None, SelectHigh400))                               \
  X(pool_turn_high200, CAT1, AUTO, "Pool Turnover Ratio High 200", "高换手200", "母集内20/250日换手比最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{turn}) \le 200])", CS(bar, L_1day, key_turn, None, SelectHigh200))                               \
  X(pool_ivol_low400, CAT1, AUTO, "Pool IVOL Low 400", "低特波400", "母集内特质波动率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ivol}) \le 400])", CS(bar, L_1day, key_ivol, None, SelectLow400))                                                  \
  X(pool_ivol_low200, CAT1, AUTO, "Pool IVOL Low 200", "低特波200", "母集内特质波动率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{ivol}) \le 200])", CS(bar, L_1day, key_ivol, None, SelectLow200))                                                  \
  X(pool_ivol_high400, CAT1, AUTO, "Pool IVOL High 400", "高特波400", "母集内特质波动率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ivol}) \le 400])", CS(bar, L_1day, key_ivol, None, SelectHigh400))                                             \
  X(pool_ivol_high200, CAT1, AUTO, "Pool IVOL High 200", "高特波200", "母集内特质波动率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{ivol}) \le 200])", CS(bar, L_1day, key_ivol, None, SelectHigh200))                                             \
  X(pool_resvol_low400, CAT1, AUTO, "Pool ResVol Low 400", "低残波400", "母集内残差波动率最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{resvol}) \le 400])", CS(bar, L_1day, key_resvol, None, SelectLow400))                                          \
  X(pool_resvol_low200, CAT1, AUTO, "Pool ResVol Low 200", "低残波200", "母集内残差波动率最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{resvol}) \le 200])", CS(bar, L_1day, key_resvol, None, SelectLow200))                                          \
  X(pool_resvol_high400, CAT1, AUTO, "Pool ResVol High 400", "高残波400", "母集内残差波动率最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{resvol}) \le 400])", CS(bar, L_1day, key_resvol, None, SelectHigh400))                                     \
  X(pool_resvol_high200, CAT1, AUTO, "Pool ResVol High 200", "高残波200", "母集内残差波动率最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{resvol}) \le 200])", CS(bar, L_1day, key_resvol, None, SelectHigh200))                                     \
  X(pool_beta_low400, CAT1, AUTO, "Pool Beta Low 400", "低β400", "母集内市场β最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\beta) \le 400])", CS(bar, L_1day, key_beta, None, SelectLow400))                                                                  \
  X(pool_beta_low200, CAT1, AUTO, "Pool Beta Low 200", "低β200", "母集内市场β最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\beta) \le 200])", CS(bar, L_1day, key_beta, None, SelectLow200))                                                                  \
  X(pool_beta_high400, CAT1, AUTO, "Pool Beta High 400", "高β400", "母集内市场β最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\beta) \le 400])", CS(bar, L_1day, key_beta, None, SelectHigh400))                                                             \
  X(pool_beta_high200, CAT1, AUTO, "Pool Beta High 200", "高β200", "母集内市场β最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\beta) \le 200])", CS(bar, L_1day, key_beta, None, SelectHigh200))                                                             \
  X(pool_trend_high400, CAT1, AUTO, "Pool Trend High 400", "高趋势400", "母集内趋势预期收益最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{trend}) \le 400])", CS(bar, L_1day, key_trend, None, SelectHigh400))                                       \
  X(pool_trend_high200, CAT1, AUTO, "Pool Trend High 200", "高趋势200", "母集内趋势预期收益最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(\mathrm{trend}) \le 200])", CS(bar, L_1day, key_trend, None, SelectHigh200))                                       \
  X(pool_trend_low400, CAT1, AUTO, "Pool Trend Low 400", "低趋势400", "母集内趋势预期收益最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{trend}) \le 400])", CS(bar, L_1day, key_trend, None, SelectLow400))                                            \
  X(pool_trend_low200, CAT1, AUTO, "Pool Trend Low 200", "低趋势200", "母集内趋势预期收益最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(\mathrm{trend}) \le 200])", CS(bar, L_1day, key_trend, None, SelectLow200))                                            \
  X(pool_price_high400, CAT1, AUTO, "Pool Price High 400", "高价400", "母集内收盘价最高400只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectHigh400))                                                            \
  X(pool_price_high200, CAT1, AUTO, "Pool Price High 200", "高价200", "母集内收盘价最高200只", R"(\mathbf{1}[\mathrm{rank}\downarrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectHigh200))                                                            \
  X(pool_price_low400, CAT1, AUTO, "Pool Price Low 400", "低价400", "母集内收盘价最低400只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 400])", CS(bar, L_1day, key_price, None, SelectLow400))                                                                 \
  X(pool_price_low200, CAT1, AUTO, "Pool Price Low 200", "低价200", "母集内收盘价最低200只", R"(\mathbf{1}[\mathrm{rank}\uparrow(P) \le 200])", CS(bar, L_1day, key_price, None, SelectLow200))                                                                 \
  X(pool_st, CAT1, AUTO, "Pool ST", "ST", "活跃∧次日ST(不含*ST)全部", R"(\mathbf{1}[\mathrm{st}_{D} = 1])", CS(bar, L_1day, key_st, None, SelectAll))                                                                                                           \
  X(pool_sst, CAT1, AUTO, "Pool *ST", "*ST", "活跃∧次日*ST全部", R"(\mathbf{1}[\mathrm{st}_{D} = 2])", CS(bar, L_1day, key_sst, None, SelectAll))                                                                                                               \
  X(pool_sh_main, CAT1, AUTO, "Pool SH Main", "沪主板", "活跃∧沪市主板全部(含ST)", R"(\mathbf{1}[\mathrm{board}_{D} = \mathrm{sh}])", CS(bar, L_1day, key_sh_main, None, SelectAll))                                                                            \
  X(pool_sz_main, CAT1, AUTO, "Pool SZ Main", "深主板", "活跃∧深市主板全部(含ST)", R"(\mathbf{1}[\mathrm{board}_{D} = \mathrm{sz}])", CS(bar, L_1day, key_sz_main, None, SelectAll))                                                                            \
  X(pool_star, CAT1, AUTO, "Pool STAR", "科创板", "活跃∧科创板全部(含ST)", R"(\mathbf{1}[\mathrm{board}_{D} = \mathrm{star}])", CS(bar, L_1day, key_star, None, SelectAll))                                                                                     \
  X(pool_chinext, CAT1, AUTO, "Pool ChiNext", "创业板", "活跃∧创业板全部(含ST)", R"(\mathbf{1}[\mathrm{board}_{D} = \mathrm{gem}])", CS(bar, L_1day, key_chinext, None, SelectAll))                                                                             \
  X(pool_margin, CAT1, AUTO, "Pool Margin", "融资", "活跃∧融资融券标的全部(含ST)", R"(\mathbf{1}[\mathrm{margin}_{D}])", CS(bar, L_1day, key_margin, None, SelectAll))                                                                                          \
  X(pool_nonmargin, CAT1, AUTO, "Pool Non-Margin", "非融资", "活跃∧非融资融券标的全部(含ST)", R"(\mathbf{1}[\neg\mathrm{margin}_{D}])", CS(bar, L_1day, key_nonmargin, None, SelectAll))
