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

#define FIELDS_L1_Fund(X, CAT1)                                                                                                                                                                       \
  X(ind_l1, CAT1, RAW, NONE, "Industry L1", "一级行业", "SW2021一级行业ID(0=未知,1..31)", R"(\mathrm{ind}_{D})", OP(Fund, ind_l1))                                                                    \
  X(list_age, CAT1, RAW, NONE, "List Age", "上市龄", "上市日历日数(未上市→NaN)", R"(D - D_{list})", OP(Fund, list_age))                                                                               \
  X(delist_age, CAT1, RAW, NONE, "Delist Age", "退市龄", "退市日历日数(未退市→NaN)", R"(D - D_{delist})", OP(Fund, delist_age))                                                                       \
  X(is_margin, CAT1, RAW, NONE, "Is Margin", "两融标记", "当日是否融资融券标的", R"(\mathbf{1}_{\mathrm{margin}})", OP(Fund, is_margin))                                                              \
  X(is_susp, CAT1, RAW, NONE, "Is Suspended", "停牌标记", "当日是否停牌", R"(\mathbf{1}_{\mathrm{susp}})", OP(Fund, is_susp))                                                                         \
  X(roe_ttm, CAT1, RATIO, NONE, "ROE TTM", "净资产收益率TTM", "归母净利TTM/归母权益TTM窗口5点均值×100", R"(\frac{NP^{TTM}}{\overline{EQ}_5} \times 100)", OP(Fund, roe_ttm))                          \
  X(roa_ttm, CAT1, RATIO, NONE, "ROA TTM", "总资产收益率TTM", "净利TTM(含少数)/总资产TTM窗口5点均值×100", R"(\frac{NP^{TTM}_{all}}{\overline{TA}_5} \times 100)", OP(Fund, roa_ttm))                  \
  X(dy_ttm, CAT1, RATIO, NONE, "Dividend Yield TTM", "股息率TTM", "近365日税前分红总额/总市值(公告日锚)", R"(\frac{\sum_{365d} Div}{MC_{D}})", OP(Fund, dy_ttm))                                      \
  X(cfo_chg_ttm, CAT1, RATIO, NONE, "CFO Change TTM", "现金流改善率TTM", "经营现金流TTM同比增量/市值(tanh封顶[-1,1])", R"(\tanh(\frac{CF^{TTM}_0 - CF^{TTM}_{-4Q}}{MC_{D}}))", OP(Fund, cfo_chg_ttm)) \
  X(rz_bal, CAT1, RAW, LOG_ZSCORE, "Margin Buy Balance", "融资余额", "融资余额(亿元,非标的→NaN)", R"(\frac{Bal^{rz}_{D}}{10^{8}})", OP(Fund, rz_bal))                                                 \
  X(rq_bal, CAT1, RAW, LOG_ZSCORE, "Margin Sell Balance", "融券余额", "融券余额(亿元,非标的→NaN)", R"(\frac{Bal^{rq}_{D}}{10^{8}})", OP(Fund, rq_bal))                                                \
  X(st_profit, CAT1, RAW, NONE, "ST Profit Warning", "预亏预警", "年报预亏状态机(首亏/续亏∧上年归母净利<0)", R"(\mathbf{1}_{\mathrm{st\_profit}})", OP(Fund, st_profit))                              \
  X(st_revenue, CAT1, RAW, NONE, "ST Revenue Warning", "营收预警", "主板营收退市预警(预亏∧营收TTM<年度阈值)", R"(\mathbf{1}_{\mathrm{st\_revenue}})", OP(Fund, st_revenue))                           \
  X(st_dividend, CAT1, RAW, NONE, "ST Dividend Warning", "分红预警", "主板分红不足预警(3年累计分红双阈值)", R"(\mathbf{1}_{\mathrm{st\_dividend}})", OP(Fund, st_dividend))                           \
  X(st_trading, CAT1, RAW, NONE, "ST Trading Warning", "交易预警", "连续15日(日频低价∨低市值)", R"(\mathbf{1}_{\mathrm{st\_trading}})", OP(Fund, st_trading))                                         \
  X(st_level, CAT1, RAW, NONE, "ST Level", "风险等级", "0=正常/1=ST/2=*ST/3=退市整理期", R"(\mathrm{st}_{D} \in \{0,1,2,3\})", OP(Fund, st_level))                                                    \
  X(is_new, CAT1, RAW, NONE, "Is New Listing", "次新股", "上市龄 < 60 日历日", R"(\mathbf{1}[0 \leq D - D_{list} < 60])", OP(Fund, is_new))
