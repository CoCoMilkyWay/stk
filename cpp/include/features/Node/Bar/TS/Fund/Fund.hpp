#pragma once

// Bar 档 Fund 节点 + 日频基本面落盘列 (L_1day; 类见 Node/Common/Fund/Fund.hpp)
#include "features/Node/Common/Fund/Fund.hpp"

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 日频节点: L_1day 行 D = 次日盘前态 (逐表对齐见 Node/Common/Fund/Fund.hpp 文件头)
#define NODE_Fund(N) N(Fund, (Fund), (fund_panel, asset_id_, date_), onSettle)

// 三表流量项三口径行: item → item_q / item_ttm / item_lyr (Q 单季 / 滚动四季 / 上年年报)
#define FUND_ROWS3(X, CAT1, item, EN, CN, DESC, UNIT, SYM)                                                                           \
  X(item##_q, CAT1, AUTO, EN " Q", CN "单季", DESC "(单季, " UNIT ")", SYM R"(^{Q}_{D})", TS(Fund, item##_q, None, None))            \
  X(item##_ttm, CAT1, AUTO, EN " TTM", CN "TTM", DESC "(滚动四季, " UNIT ")", SYM R"(^{TTM}_{D})", TS(Fund, item##_ttm, None, None)) \
  X(item##_lyr, CAT1, AUTO, EN " LYR", CN "上年", DESC "(上年年报, " UNIT ")", SYM R"(^{LYR}_{D})", TS(Fund, item##_lyr, None, None))
// 资产负债表 MRQ 单行
#define FUND_ROW_BS(X, CAT1, item, EN, CN, DESC, SYM) \
  X(item, CAT1, AUTO, EN " MRQ", CN "MRQ", DESC "(最近报告期, 亿元)", SYM R"(^{MRQ}_{D})", TS(Fund, item, None, None))

// 日频列只在 L_1day (行业亦然: 日内截面中性化按 as-of 读前一交易日行 = 当日适用, 见 Method/CS.hpp NEUTRAL_RANK_*)
#define FIELDS_L_1day_Fund(X, CAT1)                                                                                                                                                                           \
  X(ind_l1, CAT1, AUTO, "Industry L1", "一级行业", "SW2021一级行业ID(0=未知,1..31)", R"(\mathrm{ind}_{D})", TS(Fund, ind_l1, None, None))                                                                    \
  X(list_age, CAT1, AUTO, "List Age", "上市龄", "次日上市日历日数(未上市→NaN)", R"(D - D_{list})", TS(Fund, list_age, None, None))                                                                           \
  X(delist_age, CAT1, AUTO, "Delist Age", "退市龄", "次日退市日历日数(未退市→NaN)", R"(D - D_{delist})", TS(Fund, delist_age, None, None))                                                                   \
  X(is_margin, CAT1, AUTO, "Is Margin", "两融标记", "是否融资融券资产(截至本日盘后明细)", R"(\mathbf{1}_{\mathrm{margin}})", TS(Fund, is_margin, None, None))                                                \
  X(is_susp, CAT1, AUTO, "Is Suspended", "停牌标记", "次日是否停牌(次日盘前可知)", R"(\mathbf{1}_{\mathrm{susp}})", TS(Fund, is_susp, None, None))                                                           \
  X(roe_ttm, CAT1, AUTO, "ROE TTM", "净资产收益率TTM", "归母净利TTM/归母权益TTM窗口5点均值×100", R"(\frac{NP^{TTM}}{\overline{EQ}_5} \times 100)", TS(Fund, roe_ttm, None, None))                            \
  X(roa_ttm, CAT1, AUTO, "ROA TTM", "总资产收益率TTM", "净利TTM(含少数)/总资产TTM窗口5点均值×100", R"(\frac{NP^{TTM}_{all}}{\overline{TA}_5} \times 100)", TS(Fund, roa_ttm, None, None))                    \
  X(dy_ttm, CAT1, AUTO, "Dividend Yield TTM", "股息率TTM", "近365日税前分红总额/总市值(公告日锚)", R"(\frac{\sum_{365d} Div}{MC_{D}})", TS(Fund, dy_ttm, None, None))                                        \
  X(cfo_chg_ttm, CAT1, AUTO, "CFO Change TTM", "现金流改善率TTM", "经营现金流TTM同比增量/市值(tanh封顶[-1,1])", R"(\tanh(\frac{CFO^{TTM}_0 - CFO^{TTM}_{-4Q}}{MC_{D}}))", TS(Fund, cfo_chg_ttm, None, None)) \
  X(rz_bal, CAT1, AUTO, "Margin Buy Balance", "融资余额", "融资余额(亿元,非资产→NaN)", R"(\frac{Bal^{rz}_{D}}{10^{8}})", TS(Fund, rz_bal, None, None))                                                       \
  X(rq_bal, CAT1, AUTO, "Margin Sell Balance", "融券余额", "融券余额(亿元,非资产→NaN)", R"(\frac{Bal^{rq}_{D}}{10^{8}})", TS(Fund, rq_bal, None, None))                                                      \
  X(st_profit, CAT1, AUTO, "ST Profit Warning", "预亏预警", "年报预亏状态机(首亏/续亏∧上年归母净利<0)", R"(\mathbf{1}_{\mathrm{st\_profit}})", TS(Fund, st_profit, None, None))                              \
  X(st_revenue, CAT1, AUTO, "ST Revenue Warning", "营收预警", "主板营收退市预警(预亏∧营收TTM<年度阈值)", R"(\mathbf{1}_{\mathrm{st\_revenue}})", TS(Fund, st_revenue, None, None))                           \
  X(st_dividend, CAT1, AUTO, "ST Dividend Warning", "分红预警", "主板分红不足预警(3年累计分红双阈值)", R"(\mathbf{1}_{\mathrm{st\_dividend}})", TS(Fund, st_dividend, None, None))                           \
  X(st_trading, CAT1, AUTO, "ST Trading Warning", "交易预警", "连续15日(日频低价∨低市值)", R"(\mathbf{1}_{\mathrm{st\_trading}})", TS(Fund, st_trading, None, None))                                         \
  X(st_level, CAT1, AUTO, "ST Level", "风险等级", "次日 0=正常/1=ST/2=*ST/3=退市整理期", R"(\mathrm{st}_{D} \in \{0,1,2,3\})", TS(Fund, st_level, None, None))                                               \
  X(is_new, CAT1, AUTO, "Is New Listing", "次新股", "上市龄 < 60 日历日", R"(\mathbf{1}[0 \leq D - D_{list} < 60])", TS(Fund, is_new, None, None))                                                           \
  X(total_shares, CAT1, AUTO, "Total Shares", "总股本", "次日总股本(亿股, PIT)", R"(S^{total}_{D})", TS(Fund, total_shares, None, None))                                                                     \
  X(float_shares, CAT1, AUTO, "Float Shares", "流通股本", "次日A股流通股本(亿股, PIT); 次日换手率=次日vol/本行float_shares", R"(S^{float}_{D})", TS(Fund, float_shares, None, None))                         \
  X(lim_up, CAT1, AUTO, "Upper Limit Price", "涨停价", "次日适用涨停价(元, 无限制→NaN)", R"(P^{up}_{D})", TS(Fund, lim_up, None, None))                                                                      \
  X(lim_dn, CAT1, AUTO, "Lower Limit Price", "跌停价", "次日适用跌停价(元, 无限制→NaN)", R"(P^{dn}_{D})", TS(Fund, lim_dn, None, None))                                                                      \
  X(pre_close, CAT1, AUTO, "Pre Close", "前收", "次日除权后前收盘价(元); 次日隔夜收益=次日open_day/本行pre_close", R"(P^{pre}_{D})", TS(Fund, pre_close, None, None))                                        \
  X(adj_factor, CAT1, AUTO, "Adjust Factor", "复权因子", "次日复权因子; 跨日价格拼接", R"(\mathrm{adj}_{D})", TS(Fund, adj_factor, None, None))                                                              \
  X(rz_buy_amt, CAT1, AUTO, "Margin Buy Amount", "融资买入额", "融资买入额(亿元, 非资产→NaN)", R"(\frac{Buy^{rz}_{D}}{10^{8}})", TS(Fund, rz_buy_amt, None, None))                                           \
  X(rz_repay_amt, CAT1, AUTO, "Margin Repay Amount", "融资偿还额", "融资偿还额(亿元, 非资产→NaN)", R"(\frac{Repay^{rz}_{D}}{10^{8}})", TS(Fund, rz_repay_amt, None, None))                                   \
  X(rq_sell_vol, CAT1, AUTO, "Short Sell Volume", "融券卖出量", "融券卖出量(万股, 非资产→NaN)", R"(\frac{Sell^{rq}_{D}}{10^{4}})", TS(Fund, rq_sell_vol, None, None))                                        \
  X(rq_repay_vol, CAT1, AUTO, "Short Repay Volume", "融券偿还量", "融券偿还量(万股, 非资产→NaN)", R"(\frac{Repay^{rq}_{D}}{10^{4}})", TS(Fund, rq_repay_vol, None, None))                                    \
  X(days_since_report, CAT1, AUTO, "Days Since Report", "财报距今", "距最近财报公告日历日数(无→NaN)", R"(D - D_{report})", TS(Fund, days_since_report, None, None))                                          \
  X(shares_yoy, CAT1, AUTO, "Shares YoY", "股本同比", "总股本/365日历日前总股本-1", R"(\frac{S^{total}_{D}}{S^{total}_{D-1y}} - 1)", TS(Fund, shares_yoy, None, None))                                       \
  FUND_ROW_BS(X, CAT1, bs_ta, "Total Assets", "总资产", "总资产", R"(TA)")                                                                                                                                   \
  FUND_ROW_BS(X, CAT1, bs_tl, "Total Liabilities", "总负债", "总负债", R"(TL)")                                                                                                                              \
  FUND_ROW_BS(X, CAT1, bs_ncl, "Noncurrent Liabilities", "非流动负债", "非流动负债", R"(NCL)")                                                                                                               \
  FUND_ROW_BS(X, CAT1, bs_ibd, "Interest Bearing Debt", "有息负债", "短期借款+一年内到期非流动负债+长期借款+应付债券", R"(IBD)")                                                                             \
  FUND_ROW_BS(X, CAT1, bs_ca, "Current Assets", "流动资产", "流动资产", R"(CA)")                                                                                                                             \
  FUND_ROW_BS(X, CAT1, bs_cl, "Current Liabilities", "流动负债", "流动负债", R"(CL)")                                                                                                                        \
  FUND_ROW_BS(X, CAT1, bs_cash, "Cash", "货币资金", "货币资金", R"(Cash)")                                                                                                                                   \
  FUND_ROW_BS(X, CAT1, bs_inv, "Inventories", "存货", "存货", R"(Inv)")                                                                                                                                      \
  FUND_ROW_BS(X, CAT1, bs_ar, "Accounts Receivable", "应收账款", "应收账款", R"(AR)")                                                                                                                        \
  FUND_ROW_BS(X, CAT1, bs_ap, "Accounts Payable", "应付账款", "应付账款", R"(AP)")                                                                                                                           \
  FUND_ROW_BS(X, CAT1, bs_fa_cip, "Fixed Assets + CIP", "固定资产及在建", "固定资产+在建工程", R"(FA{+}CIP)")                                                                                                \
  FUND_ROW_BS(X, CAT1, bs_intang_gw, "Intangibles + Goodwill", "无形及商誉", "无形资产+商誉", R"(IA{+}GW)")                                                                                                  \
  FUND_ROW_BS(X, CAT1, bs_eq, "Parent Equity", "归母权益", "归母股东权益", R"(EQ)")                                                                                                                          \
  FUND_ROW_BS(X, CAT1, bs_minority, "Minority Interests", "少数股东权益", "少数股东权益", R"(MI)")                                                                                                           \
  FUND_ROWS3(X, CAT1, pl_rev, "Revenue", "营业总收入", "营业总收入", "亿元", R"(Rev)")                                                                                                                       \
  FUND_ROWS3(X, CAT1, pl_cogs, "COGS", "营业成本", "营业成本", "亿元", R"(COGS)")                                                                                                                            \
  FUND_ROWS3(X, CAT1, pl_cost, "Total Cost", "营业总成本", "营业总成本", "亿元", R"(Cost)")                                                                                                                  \
  FUND_ROWS3(X, CAT1, pl_op, "Operating Profit", "营业利润", "营业利润", "亿元", R"(OP)")                                                                                                                    \
  FUND_ROWS3(X, CAT1, pl_ebit, "EBIT", "息税前利润", "利润总额+利息费用", "亿元", R"(EBIT)")                                                                                                                 \
  FUND_ROWS3(X, CAT1, pl_int, "Interest Expense", "利息费用", "利息费用(缺→财务费用)", "亿元", R"(Int)")                                                                                                     \
  FUND_ROWS3(X, CAT1, pl_tax, "Income Tax", "所得税", "所得税费用", "亿元", R"(Tax)")                                                                                                                        \
  FUND_ROWS3(X, CAT1, pl_np, "Net Profit", "净利润", "净利润(含少数)", "亿元", R"(NP)")                                                                                                                      \
  FUND_ROWS3(X, CAT1, pl_npp, "Net Profit Parent", "归母净利润", "归母净利润", "亿元", R"(NP^{p})")                                                                                                          \
  FUND_ROWS3(X, CAT1, pl_eps, "EPS Basic", "基本每股收益", "基本每股收益", "元", R"(EPS)")                                                                                                                   \
  X(pl_npd_ttm, CAT1, AUTO, "Deducted Net Profit TTM", "扣非归母TTM", "归母净利TTM-非经常性损益TTM(亿元)", R"(NP^{p,ded,TTM}_{D})", TS(Fund, pl_npd_ttm, None, None))                                        \
  FUND_ROWS3(X, CAT1, cf_ocf, "Operating Cash Flow", "经营现金流", "经营活动现金流净额", "亿元", R"(CFO)")                                                                                                   \
  FUND_ROWS3(X, CAT1, cf_icf, "Investing Cash Flow", "投资现金流", "投资活动现金流净额", "亿元", R"(CFI)")                                                                                                   \
  FUND_ROWS3(X, CAT1, cf_fin, "Financing Cash Flow", "筹资现金流", "筹资活动现金流净额", "亿元", R"(CFF)")                                                                                                   \
  FUND_ROWS3(X, CAT1, cf_net, "Net Cash Change", "现金净增加", "现金及等价物净增加额", "亿元", R"(\Delta Cash)")                                                                                             \
  FUND_ROWS3(X, CAT1, cf_capex, "Capex", "资本开支", "购建固定/无形/长期资产支付现金", "亿元", R"(Capex)")                                                                                                   \
  FUND_ROWS3(X, CAT1, cf_div, "Dividends Paid", "分红付息", "分配股利利润或偿付利息支付现金", "亿元", R"(Div)")                                                                                              \
  FUND_ROWS3(X, CAT1, cf_da, "D&A", "折旧摊销", "固定资产折旧+无形资产摊销+长期待摊摊销(间接法附表)", "亿元", R"(DA)")
