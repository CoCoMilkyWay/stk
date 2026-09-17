#pragma once

// =============================================================================
// Fund - 日频 PIT 基本面 (广播型算子: compute=onDay 每天一次, flush=onMinute 每分钟原样推出)
//        持一个 fund::Stream (per-资产日频状态机), 盘前沿交易日历推进到当日算出全部口 (估值分母 / 因子 raw / filter),
//        盘中每分钟广播; 下游 (Valuation) 按口取 Series, 落盘列直接 OP(Fund, 口). 方法见 Method/Fundamental.hpp
// =============================================================================
//   Out = 输出行布局 (一处定义, fund::Stream 按 Fund::<口> 写). 缺失 = NaN.
//   单位: 股本 [亿股], 金额 [亿元], 价格 [元] — 与 L1 特征输出单位直接对齐 (mcap = close × total_shares 即为亿元).
//   盘中 tick 域算子 (Depth / Flow) 读 Fund.y[Fund.lim_up] 而非 Fund.out(): 盘口 / 委托 9:15 就开始,
//   Fund 的 Series 要到首个分钟 flush 才有当日值; 涨跌停比较容差统一用 kPxEps (DataDefine.hpp).
//
// 【fast-math 契约】本文件不做 isnan/isfinite (状态机在 precise-math TU 里), y 只被下游算术消费, NaN 硬件透传.
// =============================================================================

#include "features/Method/Fundamental.hpp"
#include <cassert>
#include <cstddef>
#include <string>

class Fund {
public:
  enum Out : size_t {
    // ---- Valuation 输入 (分钟实时价 × 这些) ----
    total_shares,   // [亿股]
    float_shares,   // [亿股] A 股流通
    net_profit_ttm, // [亿元] 归母净利 TTM (可负)
    equity_mrq,     // [亿元] 归母权益 MRQ (可负)
    revenue_ttm,    // [亿元] 营业总收入 TTM (>0; ≤0 为源脏值 → NaN)
    cffoa_ttm,      // [亿元] 经营现金流 TTM (可负)
    lim_up,         // [元] T 当日适用涨停价 (无限制 → NaN)
    lim_dn,         // [元] T 当日适用跌停价
    low_mc_thr,     // [亿元] 低市值阈值 (主板 5 / 其他 3)
    // ---- 日频常量列 (直接落盘, 与 FIELDS_L1_Fund 一一对应) ----
    ind_l1,      // SW2021 一级行业 ID (0=未知, 1..31)
    list_age,    // [日历日] 未上市 → NaN
    delist_age,  // [日历日] 未退市 → NaN
    is_margin,   // 0/1
    is_susp,     // 0/1
    roe_ttm,     // [%]
    roa_ttm,     // [%]
    dy_ttm,      // [ratio]
    cfo_chg_ttm, // [-1,1]
    rz_bal,      // [亿元] 融资余额 (非标的 → NaN)
    rq_bal,      // [亿元] 融券余额
    st_profit,   // 0/1
    st_revenue,  // 0/1
    st_dividend, // 0/1
    st_trading,  // 0/1
    st_level,    // 0=正常/1=ST/2=*ST/3=退市整理期
    is_new,      // 0/1
    // ---- 行情日线 / 两融明细 / 公告日 ----
    pre_close,         // [元] 除权后前收 (盘前可知)
    adj_factor,        // 复权因子 (盘前可知)
    rz_buy,            // [亿元] 融资买入额
    rz_repay,          // [亿元] 融资偿还额
    rq_sell_vol,       // [万股] 融券卖出量
    rq_repay_vol,      // [万股] 融券偿还量
    days_since_report, // [日历日] 距最近财报公告日 (无财报 → NaN)
    shares_yoy,        // [ratio] 总股本同比 (回看 365 日历日)
    // ---- 资产负债表 MRQ [亿元] ----
    bs_ta,        // 总资产
    bs_tl,        // 总负债
    bs_ncl,       // 非流动负债
    bs_ibd,       // 有息负债 (短借 + 一年内到期非流动 + 长借 + 应付债券)
    bs_ca,        // 流动资产
    bs_cl,        // 流动负债
    bs_cash,      // 货币资金
    bs_inv,       // 存货
    bs_ar,        // 应收账款
    bs_ap,        // 应付账款
    bs_fa_cip,    // 固定资产 + 在建工程
    bs_intang_gw, // 无形资产 + 商誉
    bs_eq,        // 归母权益
    bs_minority,  // 少数股东权益
    // ---- 利润表 × {Q 单季, TTM, LYR} [亿元; eps 元] ----
    pl_rev_q,
    pl_rev_ttm,
    pl_rev_lyr, // 营业总收入
    pl_cogs_q,
    pl_cogs_ttm,
    pl_cogs_lyr, // 营业成本
    pl_cost_q,
    pl_cost_ttm,
    pl_cost_lyr, // 营业总成本
    pl_op_q,
    pl_op_ttm,
    pl_op_lyr, // 营业利润
    pl_ebit_q,
    pl_ebit_ttm,
    pl_ebit_lyr, // 利润总额 + 利息费用
    pl_int_q,
    pl_int_ttm,
    pl_int_lyr, // 利息费用 (缺 → 财务费用)
    pl_tax_q,
    pl_tax_ttm,
    pl_tax_lyr, // 所得税
    pl_np_q,
    pl_np_ttm,
    pl_np_lyr, // 净利润 (含少数)
    pl_npp_q,
    pl_npp_ttm,
    pl_npp_lyr, // 归母净利润
    pl_eps_q,
    pl_eps_ttm,
    pl_eps_lyr, // 基本 EPS
    pl_npd_ttm, // 扣非归母 TTM
    // ---- 现金流量表 × {Q 单季, TTM, LYR} [亿元] ----
    cf_ocf_q,
    cf_ocf_ttm,
    cf_ocf_lyr, // 经营
    cf_icf_q,
    cf_icf_ttm,
    cf_icf_lyr, // 投资
    cf_fcf_q,
    cf_fcf_ttm,
    cf_fcf_lyr, // 筹资
    cf_net_q,
    cf_net_ttm,
    cf_net_lyr, // 现金净增加
    cf_capex_q,
    cf_capex_ttm,
    cf_capex_lyr, // 购建长期资产支付
    cf_div_q,
    cf_div_ttm,
    cf_div_lyr, // 分配股利利息支付
    cf_da_q,
    cf_da_ttm,
    cf_da_lyr, // 折旧摊销
    kCount
  };
  float y[kCount] = {};

  Fund(const fund::Pool &pool, size_t asset_id, const std::string &date)
      : pool_(pool), date_(date), stream_(pool, asset_id) {}

  // onDay (盘前): 状态机推进到当日, 写 y; 之后每分钟 flush 原样广播
  void compute() {
    const int d = pool_.date_index(date_);
    assert(d >= 0 && "回测日不在基本面交易日历里");
    stream_.advance_to(d, y);
  }

private:
  const fund::Pool &pool_;  // DAG_Root::fund_pool (只读共享数据源)
  const std::string &date_; // DAG_Root::date_ (begin_day 设置)
  fund::Stream stream_;     // 本资产的日频状态机
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Fund(N) N(Fund, (Fund), (fund_pool, asset_id_, date_), onDay, onMinute)

// 三表流量项三口径行: item → item_q / item_ttm / item_lyr (Q 单季 / 滚动四季 / 上年年报)
#define FUND_ROWS3(X, CAT1, item, EN, CN, DESC, UNIT, SYM)                                                                          \
  X(item##_q, CAT1, RAW, EN " Q", CN "单季", DESC "(单季, " UNIT ")", SYM R"(^{Q}_{D})", OP(Fund, item##_q, None, None))            \
  X(item##_ttm, CAT1, RAW, EN " TTM", CN "TTM", DESC "(滚动四季, " UNIT ")", SYM R"(^{TTM}_{D})", OP(Fund, item##_ttm, None, None)) \
  X(item##_lyr, CAT1, RAW, EN " LYR", CN "上年", DESC "(上年年报, " UNIT ")", SYM R"(^{LYR}_{D})", OP(Fund, item##_lyr, None, None))
// 资产负债表 MRQ 单行
#define FUND_ROW_BS(X, CAT1, item, EN, CN, DESC, SYM) \
  X(item, CAT1, RAW, EN " MRQ", CN "MRQ", DESC "(最近报告期, 亿元)", SYM R"(^{MRQ}_{D})", OP(Fund, item, None, None))

#define FIELDS_L1_Fund(X, CAT1)                                                                                                                                                                             \
  X(ind_l1, CAT1, RAW, "Industry L1", "一级行业", "SW2021一级行业ID(0=未知,1..31)", R"(\mathrm{ind}_{D})", OP(Fund, ind_l1, None, None))                                                                    \
  X(list_age, CAT1, RAW, "List Age", "上市龄", "上市日历日数(未上市→NaN)", R"(D - D_{list})", OP(Fund, list_age, None, None))                                                                               \
  X(delist_age, CAT1, RAW, "Delist Age", "退市龄", "退市日历日数(未退市→NaN)", R"(D - D_{delist})", OP(Fund, delist_age, None, None))                                                                       \
  X(is_margin, CAT1, RAW, "Is Margin", "两融标记", "当日是否融资融券标的", R"(\mathbf{1}_{\mathrm{margin}})", OP(Fund, is_margin, None, None))                                                              \
  X(is_susp, CAT1, RAW, "Is Suspended", "停牌标记", "当日是否停牌", R"(\mathbf{1}_{\mathrm{susp}})", OP(Fund, is_susp, None, None))                                                                         \
  X(roe_ttm, CAT1, RATIO, "ROE TTM", "净资产收益率TTM", "归母净利TTM/归母权益TTM窗口5点均值×100", R"(\frac{NP^{TTM}}{\overline{EQ}_5} \times 100)", OP(Fund, roe_ttm, None, None))                          \
  X(roa_ttm, CAT1, RATIO, "ROA TTM", "总资产收益率TTM", "净利TTM(含少数)/总资产TTM窗口5点均值×100", R"(\frac{NP^{TTM}_{all}}{\overline{TA}_5} \times 100)", OP(Fund, roa_ttm, None, None))                  \
  X(dy_ttm, CAT1, RATIO, "Dividend Yield TTM", "股息率TTM", "近365日税前分红总额/总市值(公告日锚)", R"(\frac{\sum_{365d} Div}{MC_{D}})", OP(Fund, dy_ttm, None, None))                                      \
  X(cfo_chg_ttm, CAT1, RATIO, "CFO Change TTM", "现金流改善率TTM", "经营现金流TTM同比增量/市值(tanh封顶[-1,1])", R"(\tanh(\frac{CF^{TTM}_0 - CF^{TTM}_{-4Q}}{MC_{D}}))", OP(Fund, cfo_chg_ttm, None, None)) \
  X(rz_bal, CAT1, RAW, "Margin Buy Balance", "融资余额", "融资余额(亿元,非标的→NaN)", R"(\frac{Bal^{rz}_{D}}{10^{8}})", OP(Fund, rz_bal, None, None))                                                       \
  X(rq_bal, CAT1, RAW, "Margin Sell Balance", "融券余额", "融券余额(亿元,非标的→NaN)", R"(\frac{Bal^{rq}_{D}}{10^{8}})", OP(Fund, rq_bal, None, None))                                                      \
  X(st_profit, CAT1, RAW, "ST Profit Warning", "预亏预警", "年报预亏状态机(首亏/续亏∧上年归母净利<0)", R"(\mathbf{1}_{\mathrm{st\_profit}})", OP(Fund, st_profit, None, None))                              \
  X(st_revenue, CAT1, RAW, "ST Revenue Warning", "营收预警", "主板营收退市预警(预亏∧营收TTM<年度阈值)", R"(\mathbf{1}_{\mathrm{st\_revenue}})", OP(Fund, st_revenue, None, None))                           \
  X(st_dividend, CAT1, RAW, "ST Dividend Warning", "分红预警", "主板分红不足预警(3年累计分红双阈值)", R"(\mathbf{1}_{\mathrm{st\_dividend}})", OP(Fund, st_dividend, None, None))                           \
  X(st_trading, CAT1, RAW, "ST Trading Warning", "交易预警", "连续15日(日频低价∨低市值)", R"(\mathbf{1}_{\mathrm{st\_trading}})", OP(Fund, st_trading, None, None))                                         \
  X(st_level, CAT1, RAW, "ST Level", "风险等级", "0=正常/1=ST/2=*ST/3=退市整理期", R"(\mathrm{st}_{D} \in \{0,1,2,3\})", OP(Fund, st_level, None, None))                                                    \
  X(is_new, CAT1, RAW, "Is New Listing", "次新股", "上市龄 < 60 日历日", R"(\mathbf{1}[0 \leq D - D_{list} < 60])", OP(Fund, is_new, None, None))                                                           \
  X(total_shares, CAT1, RAW, "Total Shares", "总股本", "总股本(亿股, PIT)", R"(S^{total}_{D})", OP(Fund, total_shares, None, None))                                                                         \
  X(float_shares, CAT1, RAW, "Float Shares", "流通股本", "A股流通股本(亿股, PIT); 换手率=vol/float_shares", R"(S^{float}_{D})", OP(Fund, float_shares, None, None))                                         \
  X(lim_up, CAT1, RAW, "Upper Limit Price", "涨停价", "当日适用涨停价(元, 无限制→NaN)", R"(P^{up}_{D})", OP(Fund, lim_up, None, None))                                                                      \
  X(lim_dn, CAT1, RAW, "Lower Limit Price", "跌停价", "当日适用跌停价(元, 无限制→NaN)", R"(P^{dn}_{D})", OP(Fund, lim_dn, None, None))                                                                      \
  X(pre_close, CAT1, RAW, "Pre Close", "前收", "除权后前收盘价(元, 盘前可知); 隔夜收益=open/pre_close", R"(P^{pre}_{D})", OP(Fund, pre_close, None, None))                                                  \
  X(adj_factor, CAT1, RAW, "Adjust Factor", "复权因子", "复权因子(盘前可知); 跨日价格拼接", R"(\mathrm{adj}_{D})", OP(Fund, adj_factor, None, None))                                                        \
  X(rz_buy, CAT1, RAW, "Margin Buy Amount", "融资买入额", "融资买入额(亿元, 非标的→NaN)", R"(\frac{Buy^{rz}_{D}}{10^{8}})", OP(Fund, rz_buy, None, None))                                                   \
  X(rz_repay, CAT1, RAW, "Margin Repay Amount", "融资偿还额", "融资偿还额(亿元, 非标的→NaN)", R"(\frac{Repay^{rz}_{D}}{10^{8}})", OP(Fund, rz_repay, None, None))                                           \
  X(rq_sell_vol, CAT1, RAW, "Short Sell Volume", "融券卖出量", "融券卖出量(万股, 非标的→NaN)", R"(\frac{Sell^{rq}_{D}}{10^{4}})", OP(Fund, rq_sell_vol, None, None))                                        \
  X(rq_repay_vol, CAT1, RAW, "Short Repay Volume", "融券偿还量", "融券偿还量(万股, 非标的→NaN)", R"(\frac{Repay^{rq}_{D}}{10^{4}})", OP(Fund, rq_repay_vol, None, None))                                    \
  X(days_since_report, CAT1, RAW, "Days Since Report", "财报距今", "距最近财报公告日历日数(无→NaN)", R"(D - D_{report})", OP(Fund, days_since_report, None, None))                                          \
  X(shares_yoy, CAT1, RATIO, "Shares YoY", "股本同比", "总股本/365日历日前总股本-1", R"(\frac{S^{total}_{D}}{S^{total}_{D-1y}} - 1)", OP(Fund, shares_yoy, None, None))                                     \
  FUND_ROW_BS(X, CAT1, bs_ta, "Total Assets", "总资产", "总资产", R"(TA)")                                                                                                                                  \
  FUND_ROW_BS(X, CAT1, bs_tl, "Total Liabilities", "总负债", "总负债", R"(TL)")                                                                                                                             \
  FUND_ROW_BS(X, CAT1, bs_ncl, "Noncurrent Liabilities", "非流动负债", "非流动负债", R"(NCL)")                                                                                                              \
  FUND_ROW_BS(X, CAT1, bs_ibd, "Interest Bearing Debt", "有息负债", "短期借款+一年内到期非流动负债+长期借款+应付债券", R"(IBD)")                                                                            \
  FUND_ROW_BS(X, CAT1, bs_ca, "Current Assets", "流动资产", "流动资产", R"(CA)")                                                                                                                            \
  FUND_ROW_BS(X, CAT1, bs_cl, "Current Liabilities", "流动负债", "流动负债", R"(CL)")                                                                                                                       \
  FUND_ROW_BS(X, CAT1, bs_cash, "Cash", "货币资金", "货币资金", R"(Cash)")                                                                                                                                  \
  FUND_ROW_BS(X, CAT1, bs_inv, "Inventories", "存货", "存货", R"(Inv)")                                                                                                                                     \
  FUND_ROW_BS(X, CAT1, bs_ar, "Accounts Receivable", "应收账款", "应收账款", R"(AR)")                                                                                                                       \
  FUND_ROW_BS(X, CAT1, bs_ap, "Accounts Payable", "应付账款", "应付账款", R"(AP)")                                                                                                                          \
  FUND_ROW_BS(X, CAT1, bs_fa_cip, "Fixed Assets + CIP", "固定资产及在建", "固定资产+在建工程", R"(FA{+}CIP)")                                                                                               \
  FUND_ROW_BS(X, CAT1, bs_intang_gw, "Intangibles + Goodwill", "无形及商誉", "无形资产+商誉", R"(IA{+}GW)")                                                                                                 \
  FUND_ROW_BS(X, CAT1, bs_eq, "Parent Equity", "归母权益", "归母股东权益", R"(EQ)")                                                                                                                         \
  FUND_ROW_BS(X, CAT1, bs_minority, "Minority Interests", "少数股东权益", "少数股东权益", R"(MI)")                                                                                                          \
  FUND_ROWS3(X, CAT1, pl_rev, "Revenue", "营业总收入", "营业总收入", "亿元", R"(Rev)")                                                                                                                      \
  FUND_ROWS3(X, CAT1, pl_cogs, "COGS", "营业成本", "营业成本", "亿元", R"(COGS)")                                                                                                                           \
  FUND_ROWS3(X, CAT1, pl_cost, "Total Cost", "营业总成本", "营业总成本", "亿元", R"(Cost)")                                                                                                                 \
  FUND_ROWS3(X, CAT1, pl_op, "Operating Profit", "营业利润", "营业利润", "亿元", R"(OP)")                                                                                                                   \
  FUND_ROWS3(X, CAT1, pl_ebit, "EBIT", "息税前利润", "利润总额+利息费用", "亿元", R"(EBIT)")                                                                                                                \
  FUND_ROWS3(X, CAT1, pl_int, "Interest Expense", "利息费用", "利息费用(缺→财务费用)", "亿元", R"(Int)")                                                                                                    \
  FUND_ROWS3(X, CAT1, pl_tax, "Income Tax", "所得税", "所得税费用", "亿元", R"(Tax)")                                                                                                                       \
  FUND_ROWS3(X, CAT1, pl_np, "Net Profit", "净利润", "净利润(含少数)", "亿元", R"(NP)")                                                                                                                     \
  FUND_ROWS3(X, CAT1, pl_npp, "Net Profit Parent", "归母净利润", "归母净利润", "亿元", R"(NP^{p})")                                                                                                         \
  FUND_ROWS3(X, CAT1, pl_eps, "EPS Basic", "基本每股收益", "基本每股收益", "元", R"(EPS)")                                                                                                                  \
  X(pl_npd_ttm, CAT1, RAW, "Deducted Net Profit TTM", "扣非归母TTM", "归母净利TTM-非经常性损益TTM(亿元)", R"(NP^{p,ded,TTM}_{D})", OP(Fund, pl_npd_ttm, None, None))                                        \
  FUND_ROWS3(X, CAT1, cf_ocf, "Operating Cash Flow", "经营现金流", "经营活动现金流净额", "亿元", R"(OCF)")                                                                                                  \
  FUND_ROWS3(X, CAT1, cf_icf, "Investing Cash Flow", "投资现金流", "投资活动现金流净额", "亿元", R"(ICF)")                                                                                                  \
  FUND_ROWS3(X, CAT1, cf_fcf, "Financing Cash Flow", "筹资现金流", "筹资活动现金流净额", "亿元", R"(FCF)")                                                                                                  \
  FUND_ROWS3(X, CAT1, cf_net, "Net Cash Change", "现金净增加", "现金及等价物净增加额", "亿元", R"(\Delta Cash)")                                                                                            \
  FUND_ROWS3(X, CAT1, cf_capex, "Capex", "资本开支", "购建固定/无形/长期资产支付现金", "亿元", R"(Capex)")                                                                                                  \
  FUND_ROWS3(X, CAT1, cf_div, "Dividends Paid", "分红付息", "分配股利利润或偿付利息支付现金", "亿元", R"(Div)")                                                                                             \
  FUND_ROWS3(X, CAT1, cf_da, "D&A", "折旧摊销", "固定资产折旧+无形资产摊销+长期待摊摊销(间接法附表)", "亿元", R"(DA)")
