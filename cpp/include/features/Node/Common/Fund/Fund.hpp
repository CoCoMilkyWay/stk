#pragma once

// =============================================================================
// Fund - 日频 PIT 基本面 (日频节点: compute=flush=onSettle, 每个交易日收盘后发布一次; 输出口跨天保留)
//        持一个 fund::Stream (per-资产日频状态机), 收盘后沿 D 轴推进到次日 (d+1) 算出全部口 (估值分母 / 因子 raw / filter):
//        L_1day 行 D = 次日盘前态; 下游 (Valuation / Depth / Flow / Book / Auction / Kline / LabelLob) 按口引用,
//        盘中读到的 = 前一交易日 L_1day 行 = 当日适用值; 落盘列直接 TS(Fund, 口). 方法见 Method/Fundamental.hpp
//        两档共用类; 节点实例 / 落盘列见 Node/{Bar,Lob}/TS/Fund/Fund.hpp
// =============================================================================
//   Out = 输出行布局 (一处定义, fund::Stream 按 Fund::<口> 写). 缺失 = NaN.
//   单位: 股本 [亿股], 金额 [亿元], 价格 [元] — 与 L_1min 特征输出单位直接对齐 (mcap = close × total_shares 即为亿元).
//   涨跌停比较容差统一用 kPxEps (DataDefine.hpp).
//
//   逐表对齐 (源表 date = T 的行; Stream 网格行 row = idx − cutoff, 见 Fundamental.cpp RowOf; L_1day 行 D = Stream 状态 d+1):
//     表 (字段)                                              更新时点               cutoff   L_1day 行 D 含
//     real_bar1d.close                                       T 盘后                 −1       T ≤ D
//     real_bar1d.pre_close / adjust_factor                   T 盘前 (交易所公布)     0       T ≤ D+1 (次日前收 / 除权)
//     shares (total / a_float)                               T 盘前 (生效日记账)     0       T ≤ D+1
//     limit_price (upper / lower)                            T 盘前 (由前收推出)     0       T ≤ D+1 (次日适用涨跌停)
//     status (st / risk_warning / suspended)                 T 盘前                  0       T ≤ D+1 (次日 ST / 停牌)
//     margin_trading_detail (余额 / 流量 / 两融标的)         T 盘后, T+1 盘前公布   −1       T ≤ D
//     financial ttm / balance / income / cashflow / notes    公告日 T 盘后          −1       公告日 ≤ D
//     dividend / forecast                                    公告日 T 盘后          −1       公告日 ≤ D
//     industry component / change                            T 盘后                 −1       T ≤ D
//     日历派生 (list_age / delist_age / is_new / days_since_report / shares_yoy 回看)   按 D+1 的日历日计
//     行情派生 (ret_12_1 / ret_1m / turn / ivol / resvol / beta / trend; 全市场预计算, 见 Method/Fundamental.hpp)   T 收盘后  −1   T ≤ D
//   盘前态全部在 D 收盘后至 D+1 开盘前可知 → L_1day 行 D 在 D+1 全天可 act on (实盘: 盘前表 D+1 早盘到齐后补算末行).
//   库末日的 D+1 须在 D 轴内 (D 轴截到 today ⇒ 末日早于日历末个交易日; ComputeService 规划, fund::Panel::build 断言).
//
// 【fast-math 契约】本文件不做 isnan/isfinite (状态机在 precise-math TU 里), y 只被下游算术消费, NaN 硬件透传.
// =============================================================================

#include "features/DataDefine.hpp"
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
    lim_up,         // [元] 次日适用涨停价 (无限制 → NaN; 盘中 back() = 当日适用)
    lim_dn,         // [元] 次日适用跌停价
    low_mc_thr,     // [亿元] 低市值阈值 (主板 5 / 其他 3)
    // ---- 日频常量列 (Bar 档直接落盘, 与 FIELDS_L_1day_Fund 一一对应) ----
    ind_l1,      // SW2021 一级行业 ID (0=未知, 1..31)
    list_age,    // [日历日] 未上市 → NaN
    delist_age,  // [日历日] 未退市 → NaN
    is_margin,   // 0/1
    is_susp,     // 0/1
    roe_ttm,     // [%]
    roa_ttm,     // [%]
    dy_ttm,      // [ratio]
    cfo_chg_ttm, // [-1,1]
    rz_bal,      // [亿元] 融资余额 (非资产 → NaN)
    rq_bal,      // [亿元] 融券余额
    st_profit,   // 0/1
    st_revenue,  // 0/1
    st_dividend, // 0/1
    st_trading,  // 0/1
    st_level,    // 0=正常/1=ST/2=*ST/3=退市整理期
    is_new,      // 0/1
    // ---- 行情日线 / 两融明细 / 公告日 ----
    pre_close,         // [元] 次日除权后前收 (盘中 back() = 当日前收)
    adj_factor,        // 次日复权因子
    adj_ratio,         // 次日复权比 = adj_factor / 本资产网格切片 (首回测日 −15 起) 内首个 adj_factor (后复权首日归一, fp16 存得下; 两档标签 / Bar 日层 CloseAdjDay 用)
    rz_buy_amt,        // [亿元] 融资买入额
    rz_repay_amt,      // [亿元] 融资偿还额
    rq_sell_vol,       // [万股] 融券卖出量
    rq_repay_vol,      // [万股] 融券偿还量
    days_since_report, // [日历日] 距最近财报公告日 (无财报 → NaN)
    shares_yoy,        // [ratio] 总股本同比 (回看 365 日历日)
    ta_yoy,            // [ratio] 总资产 MRQ 同比 (对去年同报告期)
    // ---- 资产态位 (fund::Panel 全史资产态掩码; 不落盘) ----
    live, // 0/1 活跃
    base, // 0/1 母集 (⊂ 活跃)
    // ---- 行情派生 (全市场预计算, 窗按可得史扩张; 池轴 Panel 无全市场 → NaN) ----
    ret_12_1, // [ratio] 12−1 月动量 P^adj_{D−20} / P^adj_{D−250} − 1
    ret_1m,   // [ratio] 1 月收益 P^adj_D / P^adj_{D−20} − 1
    turn,     // [ratio] 20 日均换手 / 250 日均换手 (换手 = 成交量 / A 股流通股本)
    ivol,     // [日收益] 近 250 日日收益对等权市场收益回归的残差标准差
    resvol,   // [z] 残差波动: dastd / cmra / hsigma 三项 z 按相关阵第一主成分合成, 对 β / log 市值截面正交 (母集外 NaN)
    beta,     // 近 156 周周收益对等权市场周收益的 β (周收益 = 5 日滚动, 逐日采样)
    trend,    // [20 日收益] 均线截面回归的预期收益 (系数取近 250 日均值)
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

  Fund(const fund::Panel &panel, size_t asset_id, const misc::Ymd &date)
      : panel_(panel), date_(date), stream_(panel, asset_id) {}

  // 资产态 (fund::Panel 全史判定一次, 判据见 Fundamental.cpp univ_mask; 行情派生截面 / Bar 档 cs_valid / PoolKey 全读这一份)
  //   活跃 = 在市 (非北交所 ∧ 已上市 ∧ 未退市 ∧ 非停牌) ∧ 非次新 (上市满 fund::kNewListDays 日历日) ∧ 申万一级行业已知
  //   母集 = 活跃 ∧ 非 ST / *ST / 退市整理期 ∧ st_trading / st_profit / st_revenue / st_dividend 皆 0
  // 节点侧读输出口 (Bar 档 cs_valid 盘中 = 当日, PoolKey onSettle = 次日)
  static bool is_live(const Ports<kCount> &f) { return f[live] != 0.0f; }
  static bool is_base(const Ports<kCount> &f) { return f[base] != 0.0f; }

  // onSettle (收盘后): 状态机推进到次日 d+1, 写 y → 发布 = L_1day 行 D
  void compute() {
    const int d = today();
    stream_.advance_to(d + 1, y);
    at_ = d + 1;
  }

  // 盘前承接: 输出口须是当日盘前态 (状态 d). 前一回测日已推到 d → 无事; 区间首日 / 日期轴跳日 → 推进到 d 补发布一次
  bool carry() {
    const int d = today();
    if (at_ == d)
      return false;
    stream_.advance_to(d, y);
    at_ = d;
    return true;
  }

private:
  int today() const {
    const int d = panel_.date_index(date_);
    assert(d >= 0 && "回测日不在基本面交易日历里");
    return d;
  }

  const fund::Panel &panel_; // DAG_Root::fund_panel (只读共享数据源)
  const misc::Ymd &date_;    // DAG_Root::date_ (begin_day 设置)
  fund::Stream stream_;      // 本资产的日频状态机
  int at_ = -1;              // y / 输出口对应的 D 轴日
};
