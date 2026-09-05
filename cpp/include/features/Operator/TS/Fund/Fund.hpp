#pragma once

// =============================================================================
// Fund - 日频 PIT 基本面 (广播型算子: compute=onDay 每天一次, flush=onMinute 每分钟原样推出)
//        持一个 fund::Stream (per-资产日频状态机), 盘前沿交易日历推进到当日算出全部口 (估值分母 / 因子 raw / filter),
//        盘中每分钟广播; 下游 (Valuation) 按口取 Series, 落盘列直接 OP(Fund, 口). 方法见 Method/Fundamental.hpp
// =============================================================================
//   Out = 输出行布局 (一处定义, fund::Stream 按 Fund::<口> 写). 缺失 = NaN.
//   单位: 股本 [亿股], 金额 [亿元], 价格 [元] — 与 L1 特征输出单位直接对齐 (mcap = close × total_shares 即为亿元).
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
    up_lim,         // [元] T 当日适用涨停价 (无限制 → NaN)
    dn_lim,         // [元] T 当日适用跌停价
    low_mc_thr,     // [亿元] 低市值阈值 (主板 5 / 其他 3)
    // ---- 日频常量列 (直接落盘, 与 FIELDS_L1_Fund 一一对应) ----
    industry_l1, // SW2021 一级行业 ID (0=未知, 1..31)
    list_age,    // [日历日] 未上市 → NaN
    delist_age,  // [日历日] 未退市 → NaN
    is_margin,   // 0/1
    susp,        // 0/1
    roe_raw,     // [%]
    roa_raw,     // [%]
    dy_raw,      // [ratio]
    cffoa_raw,   // [-1,1]
    mr_bal,      // [亿元] 融资余额 (非标的 → NaN)
    ms_bal,      // [亿元] 融券余额
    profit_st,   // 0/1
    revenue_st,  // 0/1
    dividend_st, // 0/1
    trading_st,  // 0/1
    risk_warn,   // 0=正常/1=ST/2=*ST/3=退市整理期
    new_list,    // 0/1
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

#define FIELDS_L1_Fund(X, CAT1)                                                                                                                                                                   \
  X(industry_l1, CAT1, RAW, NONE, "Industry L1", "一级行业", "SW2021一级行业ID(0=未知,1..31)", R"(\mathrm{ind}_{D})", OP(Fund, industry_l1))                                                      \
  X(list_age, CAT1, RAW, NONE, "List Age", "上市龄", "上市日历日数(未上市→NaN)", R"(D - D_{list})", OP(Fund, list_age))                                                                           \
  X(delist_age, CAT1, RAW, NONE, "Delist Age", "退市龄", "退市日历日数(未退市→NaN)", R"(D - D_{delist})", OP(Fund, delist_age))                                                                   \
  X(is_margin, CAT1, RAW, NONE, "Is Margin", "两融标记", "当日是否融资融券标的", R"(\mathbf{1}_{\mathrm{margin}})", OP(Fund, is_margin))                                                          \
  X(susp, CAT1, RAW, NONE, "Suspended", "停牌标记", "当日是否停牌", R"(\mathbf{1}_{\mathrm{susp}})", OP(Fund, susp))                                                                              \
  X(roe_raw, CAT1, RATIO, NONE, "ROE TTM", "净资产收益率TTM", "归母净利TTM/归母权益TTM窗口5点均值×100", R"(\frac{NP^{TTM}}{\overline{EQ}_5} \times 100)", OP(Fund, roe_raw))                      \
  X(roa_raw, CAT1, RATIO, NONE, "ROA TTM", "总资产收益率TTM", "净利TTM(含少数)/总资产TTM窗口5点均值×100", R"(\frac{NP^{TTM}_{all}}{\overline{TA}_5} \times 100)", OP(Fund, roa_raw))              \
  X(dy_raw, CAT1, RATIO, NONE, "Dividend Yield TTM", "股息率TTM", "近365日税前分红总额/总市值(公告日锚)", R"(\frac{\sum_{365d} Div}{MC_{D}})", OP(Fund, dy_raw))                                  \
  X(cffoa_raw, CAT1, RATIO, NONE, "CFFOA Improvement", "现金流改善率", "经营现金流TTM同比增量/市值(tanh封顶[-1,1])", R"(\tanh(\frac{CF^{TTM}_0 - CF^{TTM}_{-4Q}}{MC_{D}}))", OP(Fund, cffoa_raw)) \
  X(mr_bal, CAT1, RAW, LOG_ZSCORE, "Margin Buy Balance", "融资余额", "融资余额(亿元,非标的→NaN)", R"(\frac{Bal^{mr}_{D}}{10^{8}})", OP(Fund, mr_bal))                                             \
  X(ms_bal, CAT1, RAW, LOG_ZSCORE, "Margin Sell Balance", "融券余额", "融券余额(亿元,非标的→NaN)", R"(\frac{Bal^{ms}_{D}}{10^{8}})", OP(Fund, ms_bal))                                            \
  X(profit_st, CAT1, RAW, NONE, "Profit ST Warning", "预亏预警", "年报预亏状态机(首亏/续亏∧上年归母净利<0)", R"(\mathbf{1}_{\mathrm{profit\_st}})", OP(Fund, profit_st))                          \
  X(revenue_st, CAT1, RAW, NONE, "Revenue ST Warning", "营收预警", "主板营收退市预警(预亏∧营收TTM<年度阈值)", R"(\mathbf{1}_{\mathrm{revenue\_st}})", OP(Fund, revenue_st))                       \
  X(dividend_st, CAT1, RAW, NONE, "Dividend ST Warning", "分红预警", "主板分红不足预警(3年累计分红双阈值)", R"(\mathbf{1}_{\mathrm{dividend\_st}})", OP(Fund, dividend_st))                       \
  X(trading_st, CAT1, RAW, NONE, "Trading ST Warning", "交易预警", "连续15日(日频低价∨低市值)", R"(\mathbf{1}_{\mathrm{trading\_st}})", OP(Fund, trading_st))                                     \
  X(risk_warn, CAT1, RAW, NONE, "Risk Warning", "风险预警", "0=正常/1=ST/2=*ST/3=退市整理期", R"(\mathrm{st}_{D} \in \{0,1,2,3\})", OP(Fund, risk_warn))                                          \
  X(new_list, CAT1, RAW, NONE, "New Listing", "次新股", "上市龄 < 60 日历日", R"(\mathbf{1}[0 \leq D - D_{list} < 60])", OP(Fund, new_list))
