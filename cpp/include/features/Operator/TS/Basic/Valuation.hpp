#pragma once

// =============================================================================
// Valuation - 实时估值: 分钟最新价 × 当日基本面 (Fund 节点输出口, PIT 预计算, 缺失=NaN)
// =============================================================================
//   mcap  = P_t × S_total   (亿元)       fmcap = P_t × S_float   (亿元)
//   pe    = mcap / NP_ttm                pb    = mcap / EQ_mrq
//   ps    = mcap / REV_ttm               pcf   = mcap / CF_ttm
//   limit_up = 1[P_t ≥ P_up - 1e-4]      limit_dn = 1[P_t ≤ P_dn + 1e-4]
//   low_p    = 1[P_t < 1]                low_mc   = 1[mcap < thr]
//
// 【fast-math 契约】
//   本 TU 走 -ffast-math, 不做 isnan/isfinite: 缺失 (NaN) 靠硬件算术透传
//   (NaN 参与 ± × ÷ 仍是 NaN); 比较遇 NaN 恒 false → 标记列自然落 0
//   (无涨跌停限制 → 未触板, 股本缺失 → 非低市值), 与 qmt 口径一致.
//   sat: 比率极值 (分母趋零 → 超 fp16 上限) 与 NaN 一样比较 false → NaN.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Operator/TS/Basic/Fund.hpp" // Fund::Out 口下标

#include <limits>

class Valuation {
public:
  enum Out : size_t { mcap,
                      fmcap,
                      pe,
                      pb,
                      ps,
                      pcf,
                      limit_up,
                      limit_dn,
                      low_p,
                      low_mc,
                      kCount };
  float y[kCount] = {};

  Valuation(const MinuteData &md, const Series (&fund)[Fund::kCount])
      : md_(md), f_(fund) {}

  void compute() {
    const float close = md_.close.back(); // [元]
    const auto f = [this](Fund::Out k) { return f_[k].back(); };

    const float mc = close * f(Fund::total_shares); // [亿元]
    y[mcap] = sat(mc);
    y[fmcap] = sat(close * f(Fund::float_shares));
    y[pe] = sat(mc / f(Fund::net_profit_ttm));
    y[pb] = sat(mc / f(Fund::equity_mrq));
    y[ps] = sat(mc / f(Fund::revenue_ttm));
    y[pcf] = sat(mc / f(Fund::cffoa_ttm));
    y[limit_up] = (close >= f(Fund::up_lim) - 1e-4f) ? 1.0f : 0.0f;
    y[limit_dn] = (close <= f(Fund::dn_lim) + 1e-4f) ? 1.0f : 0.0f;
    y[low_p] = (close < 1.0f) ? 1.0f : 0.0f;
    y[low_mc] = (mc < f(Fund::low_mc_thr)) ? 1.0f : 0.0f;
  }

private:
  // Float16 饱和: 存不下的极值 → NaN (NaN 输入同样落到 NaN 分支)
  static float sat(float v) {
    constexpr float kFp16Max = 65504.0f;
    return (v > -kFp16Max && v < kFp16Max)
               ? v
               : std::numeric_limits<float>::quiet_NaN();
  }

  const MinuteData &md_;
  const Series (&f_)[Fund::kCount]; // Fund 节点全部输出口 (同域 onMinute, 拓扑序在前, back() 即本分钟值)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Val(N) N(Val, (Valuation), (minute_data, Fund.outs()), onMinute)

#define FIELDS_L1_Val(X, CAT1)                                                                                                                                            \
  X(mcap, CAT1, RAW, LOG_ZSCORE, "Market Cap RT", "实时总市值", "分钟最新价×总股本(亿元,不复权真市值)", R"(\frac{P_t \cdot S^{total}_{D}}{10^{8}})", OP(Val, mcap))       \
  X(fmcap, CAT1, RAW, LOG_ZSCORE, "Float Market Cap RT", "实时流通市值", "分钟最新价×A股流通股本(亿元)", R"(\frac{P_t \cdot S^{float}_{D}}{10^{8}})", OP(Val, fmcap))     \
  X(pe, CAT1, RATIO, NONE, "PE TTM RT", "实时市盈率TTM", "实时市值/归母净利TTM(亏损→负PE保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{NP^{TTM}_{D}})", OP(Val, pe))       \
  X(pb, CAT1, RATIO, NONE, "PB MRQ RT", "实时市净率MRQ", "实时市值/归母权益MRQ(负权益→负PB保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{EQ^{MRQ}_{D}})", OP(Val, pb))     \
  X(ps, CAT1, RATIO, NONE, "PS TTM RT", "实时市销率TTM", "实时市值/营业总收入TTM(营收≤0为脏值→NaN)", R"(\frac{P_t S^{total}_{D}}{REV^{TTM}_{D}})", OP(Val, ps))           \
  X(pcf, CAT1, RATIO, NONE, "PCF TTM RT", "实时市现率TTM", "实时市值/经营现金流TTM(烧钱→负PCF保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{CF^{TTM}_{D}})", OP(Val, pcf)) \
  X(limit_up, CAT1, RAW, NONE, "Limit Up RT", "实时涨停标记", "分钟最新价触及当日涨停价", R"(\mathbf{1}[P_t \geq P^{up}_{D} - 10^{-4}])", OP(Val, limit_up))              \
  X(limit_dn, CAT1, RAW, NONE, "Limit Down RT", "实时跌停标记", "分钟最新价触及当日跌停价", R"(\mathbf{1}[P_t \leq P^{dn}_{D} + 10^{-4}])", OP(Val, limit_dn))            \
  X(low_p, CAT1, RAW, NONE, "Low Price RT", "实时低价标记", "分钟最新价 < 1元(面值退市风险)", R"(\mathbf{1}[P_t < 1])", OP(Val, low_p))                                   \
  X(low_mc, CAT1, RAW, NONE, "Low Market Cap RT", "实时低市值标记", "实时市值 < 阈值(主板5亿/其他3亿)", R"(\mathbf{1}[P_t S^{total}_{D} < \theta])", OP(Val, low_mc))
