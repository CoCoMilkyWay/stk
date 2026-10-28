#pragma once

// =============================================================================
// Valuation - 实时估值: 5 分钟箱末最新价 (Kline5min.close) × 当日基本面 (Fund 节点输出口, PIT 预计算, 缺失=NaN)
// =============================================================================
//   落 L_5min (不落 L_1min: 逐分钟代价太大); 更粗层截面 / 日频节点经 as-of / 输出口读它 (L_1day 读到收盘箱值)
//   mcap    = P_t × S_total   (亿元)       fmcap   = P_t × S_float   (亿元)
//   pe_ttm  = mcap / NP_ttm                pb_mrq  = mcap / EQ_mrq
//   ps_ttm  = mcap / REV_ttm               pcf_ttm = mcap / CF_ttm
//   is_limit_up = 1[P_t ≥ P_up - kPxEps]  is_limit_dn = 1[P_t ≤ P_dn + kPxEps]   (容差见 DataDefine.hpp)
//   is_low_px   = 1[P_t < 1]               is_low_mcap = 1[mcap < thr]
//
// 【fast-math 契约】
//   本 TU 走 -ffast-math, 不做 isnan/isfinite: 缺失 (NaN) 靠硬件算术透传
//   (NaN 参与 ± × ÷ 仍是 NaN); 比较遇 NaN 恒 false → 标记列自然落 0
//   (无涨跌停限制 → 未触板, 股本缺失 → 非低市值), 与 qmt 口径一致.
//   sat: 比率极值 (分母趋零 → 超 fp16 上限) 与 NaN 一样比较 false → NaN.
// =============================================================================

#include "features/DataDefine.hpp"

#include <limits>

class Valuation {
public:
  enum Out : size_t { mcap,
                      fmcap,
                      pe_ttm,
                      pb_mrq,
                      ps_ttm,
                      pcf_ttm,
                      is_limit_up,
                      is_limit_dn,
                      is_low_px,
                      is_low_mcap,
                      kCount };
  float y[kCount] = {};

  // 只接用到的 Fund 口 (不接 Fund.outs()): 依赖表按口解析, 这些口都不落盘 → 估值列无字段级依赖
  Valuation(const Port &close,
            const Port &total_shares, const Port &float_shares,
            const Port &net_profit_ttm, const Port &equity_mrq,
            const Port &revenue_ttm, const Port &cffoa_ttm,
            const Port &lim_up, const Port &lim_dn, const Port &low_mc_thr)
      : close_(close),
        total_shares_(total_shares), float_shares_(float_shares),
        net_profit_ttm_(net_profit_ttm), equity_mrq_(equity_mrq),
        revenue_ttm_(revenue_ttm), cffoa_ttm_(cffoa_ttm),
        lim_up_(lim_up), lim_dn_(lim_dn), low_mc_thr_(low_mc_thr) {}

  void compute() {
    const float close = close_; // [元] Kline5min.close (无成交箱的携带 / 前收兜底在 Kline)

    const float mc = close * total_shares_; // [亿元]
    y[mcap] = sat(mc);
    y[fmcap] = sat(close * float_shares_);
    y[pe_ttm] = sat(mc / net_profit_ttm_);
    y[pb_mrq] = sat(mc / equity_mrq_);
    y[ps_ttm] = sat(mc / revenue_ttm_);
    y[pcf_ttm] = sat(mc / cffoa_ttm_);
    y[is_limit_up] = (close >= lim_up_ - kPxEps) ? 1.0f : 0.0f;
    y[is_limit_dn] = (close <= lim_dn_ + kPxEps) ? 1.0f : 0.0f;
    y[is_low_px] = (close < 1.0f) ? 1.0f : 0.0f;
    y[is_low_mcap] = (mc < low_mc_thr_) ? 1.0f : 0.0f;
  }

private:
  // Float16 饱和: 存不下的极值 → NaN (NaN 输入同样落到 NaN 分支)
  static float sat(float v) {
    constexpr float kFp16Max = 65504.0f;
    return (v > -kFp16Max && v < kFp16Max)
               ? v
               : std::numeric_limits<float>::quiet_NaN();
  }

  const Port &close_; // Kline5min.close (同域 on5Min, 拓扑序在前, 即本箱值)
  // Fund 节点输出口 (同上)
  const Port &total_shares_, &float_shares_;
  const Port &net_profit_ttm_, &equity_mrq_, &revenue_ttm_, &cffoa_ttm_;
  const Port &lim_up_, &lim_dn_, &low_mc_thr_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Valuation(N) N(Valuation, (Valuation), (Kline5min.out(Kline5min.close), Fund.out(Fund.total_shares), Fund.out(Fund.float_shares), Fund.out(Fund.net_profit_ttm), Fund.out(Fund.equity_mrq), Fund.out(Fund.revenue_ttm), Fund.out(Fund.cffoa_ttm), Fund.out(Fund.lim_up), Fund.out(Fund.lim_dn), Fund.out(Fund.low_mc_thr)), on5Min)

#define FIELDS_L_5min_Valuation(X, CAT1)                                                                                                                                                      \
  X(mcap, CAT1, AUTO, "Market Cap RT", "实时总市值", "5分钟箱末价×总股本(亿元,不复权真市值)", R"(\frac{P_t \cdot S^{total}_{D}}{10^{8}})", TS(Valuation, mcap, None, None))                   \
  X(fmcap, CAT1, AUTO, "Float Market Cap RT", "实时流通市值", "5分钟箱末价×A股流通股本(亿元)", R"(\frac{P_t \cdot S^{float}_{D}}{10^{8}})", TS(Valuation, fmcap, None, None))                 \
  X(pe_ttm, CAT1, AUTO, "PE TTM RT", "实时市盈率TTM", "实时市值/归母净利TTM(亏损→负PE保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{NP^{TTM}_{D}})", TS(Valuation, pe_ttm, None, None))        \
  X(pb_mrq, CAT1, AUTO, "PB MRQ RT", "实时市净率MRQ", "实时市值/归母权益MRQ(负权益→负PB保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{EQ^{MRQ}_{D}})", TS(Valuation, pb_mrq, None, None))      \
  X(ps_ttm, CAT1, AUTO, "PS TTM RT", "实时市销率TTM", "实时市值/营业总收入TTM(营收≤0为脏值→NaN)", R"(\frac{P_t S^{total}_{D}}{REV^{TTM}_{D}})", TS(Valuation, ps_ttm, None, None))            \
  X(pcf_ttm, CAT1, AUTO, "PCF TTM RT", "实时市现率TTM", "实时市值/经营现金流TTM(烧钱→负PCF保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{CFO^{TTM}_{D}})", TS(Valuation, pcf_ttm, None, None)) \
  X(is_limit_up, CAT1, AUTO, "Limit Up RT", "实时涨停标记", "5分钟箱末价触及当日涨停价", R"(\mathbf{1}[P_t \geq P^{up}_{D} - \varepsilon])", TS(Valuation, is_limit_up, None, None))          \
  X(is_limit_dn, CAT1, AUTO, "Limit Down RT", "实时跌停标记", "5分钟箱末价触及当日跌停价", R"(\mathbf{1}[P_t \leq P^{dn}_{D} + \varepsilon])", TS(Valuation, is_limit_dn, None, None))        \
  X(is_low_px, CAT1, AUTO, "Low Price RT", "实时低价标记", "5分钟箱末价 < 1元(面值退市风险)", R"(\mathbf{1}[P_t < 1])", TS(Valuation, is_low_px, None, None))                                 \
  X(is_low_mcap, CAT1, AUTO, "Low Market Cap RT", "实时低市值标记", "实时市值 < 阈值(主板5亿/其他3亿)", R"(\mathbf{1}[P_t S^{total}_{D} < \theta])", TS(Valuation, is_low_mcap, None, None))
