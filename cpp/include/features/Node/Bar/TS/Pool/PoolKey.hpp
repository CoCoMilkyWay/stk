#pragma once

// =============================================================================
// PoolKey - 池排序键 (compute=onSettle, 日频节点): 母集外 → NaN, 母集内 = 键值 (键缺 → +inf); CS 池列 (CS/Pool/Pool.hpp) 在其上取两端前 N
// =============================================================================
//   范围 (全部只看本资产 Fund 次日盘前态 (onSettle 之后 back()) + 收盘市值 / 收盘价 → 资产局部纯函数):
//     活跃 = Fund live 口 (在市: 非北交所 ∧ 已上市 ∧ 未退市 ∧ 非停牌; 非次新: 上市满 365 日历日; 申万一级行业已知) ∧ 收盘市值 > 0
//     母集 = Fund base 口 (活跃 ∧ st_level == 0 (含退市整理期) ∧ st_trading / st_profit / st_revenue / st_dividend 皆 0) ∧ 收盘市值 > 0
//     两口都由 fund::Panel 全史判定一次, 与 Bar 档 cs_valid / 行情派生截面同一份 (那边按当日盘前态)
//   键 (升序 = 指标由低到高, 池取低端 / 高端; 行情派生项全市场预计算, 截至本日收盘, 见 Method/Fundamental.hpp):
//     size   mcap (亿元, 收盘)
//     ep     NP_ttm / mcap          (亏损为负, 低端 = 亏损最深)
//     bp     EQ_mrq / mcap          (负净资产为负)
//     dy     dy_ttm                 (近 365 日税前分红 / 总市值; 分母 = 当日收盘 × 次日股本, 见 Fundamental.cpp)
//     roe    NP^p_Q / EQ_mrq        (单季归母净利 / 归母权益 MRQ; 权益 ≤ 0 记缺)
//     inv    ta_yoy                 (总资产 MRQ 同比)
//     mom    ret_12_1               (后复权 P_{D−20} / P_{D−250} − 1)
//     rev    ret_1m                 (后复权 P_D / P_{D−20} − 1)
//     turn   turn                   (20 日均换手 / 250 日均换手)
//     ivol   ivol                   (近 250 日对等权市场收益回归的残差标准差)
//     resvol resvol                 (残差波动: 三项波动 z 主成分合成, 对 β / log 市值截面正交; 截面同本母集)
//     beta   beta                   (近 156 周周收益对等权市场的 β)
//     trend  trend                  (均线截面回归的预期 20 日收益)
//     price  收盘价 (元, 不复权)
//   ST 键 (不走母集; 活跃 ∧ 次日 st_level 恰为该级 → mcap, 否则 NaN; 池列全选):
//     st     st_level == 1 (ST, 不含 *ST)
//     sst    st_level == 2 (*ST)
//   池日频: 键落 L_1day (收盘后, 母集按次日在市 / ST / 停牌判), 交易日 D 的池 = D−1 的 L_1day 行池列 (shared/Pool.hpp 读 Bar 池).
//   delist 只看已发生 (不含未来信息).
//   边界: 行情项窗按可得史扩张, 数据起点次日起即有值; 财务项随首批公告渐次到齐. 母集内键缺记 +inf (与母集外 NaN 分开),
//         池列据此算覆盖率, 不足则全选母集 (CS::Select*), 指数在暖机段等于母集等权而不停摆.
//
// 本 TU (bar_pipeline.cpp) precise-math: 依赖 NaN 语义 (mcap 缺失 = NaN 比较为假; 键 isfinite 分缺 / 有).
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Fund/Fund.hpp"
#include <cmath>
#include <limits>

class PoolKey {
public:
  enum Out : size_t { size,
                      ep,
                      bp,
                      dy,
                      roe,
                      inv,
                      mom,
                      rev,
                      turn,
                      ivol,
                      resvol,
                      beta,
                      trend,
                      price,
                      st,
                      sst,
                      kCount };
  float y[kCount] = {};

  PoolKey(const Series (&fund)[Fund::kCount], const Series &mcap, const Series &close)
      : fund_(fund), mcap_(mcap), close_(close) {}

  inline void compute() {
    constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
    const float mc = mcap_.back();
    const float lvl = f(Fund::st_level);
    const bool live = Fund::is_live(fund_) && mc > 0.0f;
    y[st] = (live && lvl == 1.0f) ? mc : kNaN;
    y[sst] = (live && lvl == 2.0f) ? mc : kNaN;
    if (!(mc > 0.0f && Fund::is_base(fund_))) {
      for (size_t k = 0; k < st; ++k)
        y[k] = kNaN;
      return;
    }
    const float eq = f(Fund::bs_eq);
    y[size] = mc;
    y[ep] = f(Fund::net_profit_ttm) / mc;
    y[bp] = f(Fund::equity_mrq) / mc;
    y[dy] = f(Fund::dy_ttm);
    y[roe] = eq > 0.0f ? f(Fund::pl_npp_q) / eq : kNaN;
    y[inv] = f(Fund::ta_yoy);
    y[mom] = f(Fund::ret_12_1);
    y[rev] = f(Fund::ret_1m);
    y[turn] = f(Fund::turn);
    y[ivol] = f(Fund::ivol);
    y[resvol] = f(Fund::resvol);
    y[beta] = f(Fund::beta);
    y[trend] = f(Fund::trend);
    y[price] = close_.back();
    for (size_t k = 0; k < st; ++k)
      if (!std::isfinite(y[k]))
        y[k] = std::numeric_limits<float>::infinity();
  }

private:
  float f(size_t port) const { return fund_[port].back(); }

  const Series (&fund_)[Fund::kCount]; // Fund 全部口 (同域 onSettle, 拓扑序在前 → back() = 次日盘前态)
  const Series &mcap_;                 // Valuation.mcap (back() = 收盘箱值)
  const Series &close_;                // KlineDay.close (日频节点, back() = 本日收盘价, 不复权)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_PoolKey(N) N(PoolKey, (PoolKey), (Fund.outs(), Valuation.out(Valuation.mcap), KlineDay.out(KlineDay.close)), onSettle)

#define FIELDS_L_1day_PoolKey(X, CAT1)                                                                                                                                                                \
  X(key_size, CAT1, AUTO, "Pool Key Size", "池键市值", "母集内收盘总市值(亿元), 缺+inf, 外NaN", R"(\mathrm{mcap}_D)", TS(PoolKey, size, None, None))                                                  \
  X(key_ep, CAT1, AUTO, "Pool Key EP", "池键EP", "母集内归母净利TTM/收盘市值, 缺+inf, 外NaN", R"(\frac{NP^{TTM}}{\mathrm{mcap}_D})", TS(PoolKey, ep, None, None))                                     \
  X(key_bp, CAT1, AUTO, "Pool Key BP", "池键BP", "母集内归母权益MRQ/收盘市值, 缺+inf, 外NaN", R"(\frac{EQ^{MRQ}}{\mathrm{mcap}_D})", TS(PoolKey, bp, None, None))                                     \
  X(key_dy, CAT1, AUTO, "Pool Key DY", "池键股息率", "母集内股息率TTM, 缺+inf, 外NaN", R"(\mathrm{dy}^{TTM})", TS(PoolKey, dy, None, None))                                                           \
  X(key_roe, CAT1, AUTO, "Pool Key ROE", "池键ROE", "母集内单季归母净利/归母权益MRQ(权益≤0记缺), 缺+inf, 外NaN", R"(\frac{NP^{p,Q}}{EQ^{MRQ}})", TS(PoolKey, roe, None, None))                        \
  X(key_inv, CAT1, AUTO, "Pool Key Investment", "池键投资", "母集内总资产MRQ同比, 缺+inf, 外NaN", R"(\frac{TA_{R}}{TA_{R-1y}} - 1)", TS(PoolKey, inv, None, None))                                    \
  X(key_mom, CAT1, AUTO, "Pool Key Momentum", "池键动量", "母集内12-1月动量(后复权), 缺+inf, 外NaN", R"(\frac{P^{adj}_{D-20}}{P^{adj}_{D-250}} - 1)", TS(PoolKey, mom, None, None))                   \
  X(key_rev, CAT1, AUTO, "Pool Key Reversal", "池键反转", "母集内20日收益率(后复权), 缺+inf, 外NaN", R"(\frac{P^{adj}_D}{P^{adj}_{D-20}} - 1)", TS(PoolKey, rev, None, None))                         \
  X(key_turn, CAT1, AUTO, "Pool Key Turnover Ratio", "池键换手比", "母集内20日均换手/250日均换手, 缺+inf, 外NaN", R"(\frac{\overline{TO}_{20}}{\overline{TO}_{250}})", TS(PoolKey, turn, None, None)) \
  X(key_ivol, CAT1, AUTO, "Pool Key IVOL", "池键特质波动", "母集内近250日特质波动率, 缺+inf, 外NaN", R"(\sigma(\varepsilon))", TS(PoolKey, ivol, None, None))                                         \
  X(key_resvol, CAT1, AUTO, "Pool Key ResVol", "池键残差波动", "母集内残差波动率(对β与市值正交), 缺+inf, 外NaN", R"(\mathrm{resvol})", TS(PoolKey, resvol, None, None))                               \
  X(key_beta, CAT1, AUTO, "Pool Key Beta", "池键贝塔", "母集内近156周市场β, 缺+inf, 外NaN", R"(\beta^{w}_{156})", TS(PoolKey, beta, None, None))                                                      \
  X(key_trend, CAT1, AUTO, "Pool Key Trend", "池键趋势", "母集内均线趋势预期收益, 缺+inf, 外NaN", R"(\sum_L \bar{b}_L \frac{MA_{L,D}}{P_D})", TS(PoolKey, trend, None, None))                         \
  X(key_price, CAT1, AUTO, "Pool Key Price", "池键股价", "母集内收盘价(元,不复权), 缺+inf, 外NaN", R"(P^{\mathrm{close}}_D)", TS(PoolKey, price, None, None))                                         \
  X(key_st, CAT1, AUTO, "Pool Key ST", "池键ST", "活跃∧次日ST(不含*ST)的收盘总市值(亿元), 外NaN", R"(\mathrm{mcap}_D \cdot \mathbf{1}_{live,st=1})", TS(PoolKey, st, None, None))                     \
  X(key_sst, CAT1, AUTO, "Pool Key *ST", "池键*ST", "活跃∧次日*ST的收盘总市值(亿元), 外NaN", R"(\mathrm{mcap}_D \cdot \mathbf{1}_{live,st=2})", TS(PoolKey, sst, None, None))
