#pragma once

// =============================================================================
// Fund - 日频 PIT 基本面 (广播型算子: compute=onDay 每天一次, flush=onMinute 每分钟原样推出)
//        持一个 fund::Stream (per-资产日频状态机), 盘前沿交易日历推进到当日算出全部口 (估值分母 / 因子 raw / filter),
//        盘中每分钟广播; 下游 (Valuation) 按口取 Series, 落盘列直接 OP(Fund, 口). 方法见 Method/Fundamental.hpp
//        两档共用类; 节点实例 / 落盘列见 Operator/{Pool,All}/TS/Fund/Fund.hpp
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
    // ---- 日频常量列 (Bar 档直接落盘, 与 FIELDS_L1_Fund 一一对应) ----
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
    rz_buy_amt,        // [亿元] 融资买入额
    rz_repay_amt,      // [亿元] 融资偿还额
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
    cf_fin_q,
    cf_fin_ttm,
    cf_fin_lyr, // 筹资 (fin = financing; 不叫 fcf, 免与 free cash flow 混淆)
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

  Fund(const fund::Pool &pool, size_t asset_id, const misc::Ymd &date)
      : pool_(pool), date_(date), stream_(pool, asset_id) {}

  // 当日在市可交易: 非北交所 ∧ 已上市 (list_age ≥ 0) ∧ 未退市 (delist_age 未定义) ∧ 当日非停牌 (盘前可知).
  // 只许 precise-math TU 调 (NaN 比较语义; 违反本文件 fast-math 契约的唯一入口): Bar 档 cs_valid / PoolKey 母集
  static bool in_market(const float (&y)[kCount], bool bj) {
    return !bj && y[list_age] >= 0.0f && !(y[delist_age] >= 0.0f) && y[is_susp] == 0.0f;
  }

  // onDay (盘前): 状态机推进到当日, 写 y; 之后每分钟 flush 原样广播
  void compute() {
    const int d = pool_.date_index(date_);
    assert(d >= 0 && "回测日不在基本面交易日历里");
    stream_.advance_to(d, y);
  }

private:
  const fund::Pool &pool_; // DAG_Root::fund_pool (只读共享数据源)
  const misc::Ymd &date_;  // DAG_Root::date_ (begin_day 设置)
  fund::Stream stream_;    // 本资产的日频状态机
};
