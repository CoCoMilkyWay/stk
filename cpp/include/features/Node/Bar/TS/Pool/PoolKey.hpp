#pragma once

// =============================================================================
// PoolKey - 池排序键 (compute=onSettle, 日频节点): 母集外 → NaN, 母集内 = 键值; CS 池列 (CS/Pool/Pool.hpp) 在其上取前 N
// =============================================================================
//   母集 (全部只看本资产代码 + 本资产 Fund 次日盘前态 (onSettle 之后 back()) + 收盘市值 / 收盘价 / 20 日收益 → 资产局部纯函数):
//     活跃 = 在市 (Fund::in_market: 非北交所 ∧ 已上市 ∧ 未退市 ∧ 非停牌) ∧ 上市满 60 日历日 (list_age ≥ 60)
//            ∧ 收盘市值 > 0 ∧ 申万一级行业已知 (∈ 白名单)
//     母集 = 活跃 ∧ st_level == 0 (含退市整理期) ∧ st_trading / st_profit / st_revenue / st_dividend 皆 0
//   键 (升序 = 指标由低到高, 池取低端 / 高端):
//     size        mcap (亿元, 收盘)
//     size_micro  mcap, 行业白名单另去 农林牧渔 / 房地产 / 环保 / 钢铁 / 银行 (qmt strat_0)
//     pe          −NP_ttm / mcap   (= −EP; 亏损接在正 EP 的高端之后: PE 2 < 1000 < −1000 < −2)
//     pb          −EQ_mrq / mcap   (= −BP)
//     dy          dy_ttm           (近 365 日税前分红 / 总市值; 分母 = 当日收盘 × 次日股本, 见 Fundamental.cpp)
//     price       收盘价 (元, 不复权)
//     mom         ret_20d          (Momentum: 后复权 20 个交易日涨跌幅; 历史不足 → NaN)
//   ST 键 (不走母集; 活跃 ∧ 次日 st_level 恰为该级 → mcap, 否则 NaN; 池列全选):
//     st          st_level == 1 (ST, 不含 *ST)
//     sst         st_level == 2 (*ST)
//   池日频: 键落 L_1day (收盘后, 母集按次日在市 / ST / 停牌判), 交易日 D 的池 = D−1 的 L_1day 行池列 (shared/Pool.hpp 读 Bar 池).
//   delist 只看已发生 (不含未来信息); 键无暖机 (数据起点后一年内 PE/PB/DY 不全).
//
// 本 TU (bar_pipeline.cpp) precise-math: 母集判定依赖 NaN 比较语义 (list_age / delist_age / mcap 缺失 = NaN).
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Method/Fundamental.hpp" // fund::SW2021_L1_NAMES
#include "features/Node/Common/Fund/Fund.hpp"
#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

class PoolKey {
public:
  enum Out : size_t { size,
                      size_micro,
                      pe,
                      pb,
                      dy,
                      price,
                      mom,
                      st,
                      sst,
                      kCount };
  float y[kCount] = {};

  PoolKey(const Series (&fund)[Fund::kCount], const Series &mcap, const Series &close, const Series &ret_20d, const std::string &code)
      : fund_(fund), mcap_(mcap), close_(close), ret_20d_(ret_20d), bj_(code.ends_with(".BJ")) {}

  inline void compute() {
    constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
    const float mc = mcap_.back();
    const float ind = f(Fund::ind_l1);
    const float lvl = f(Fund::st_level);
    const bool live = Fund::in_market(fund_, bj_) && f(Fund::list_age) >= 60.0f && mc > 0.0f && ind > 0.0f;
    y[st] = (live && lvl == 1.0f) ? mc : kNaN;
    y[sst] = (live && lvl == 2.0f) ? mc : kNaN;
    const bool base = live && lvl == 0.0f &&
                      f(Fund::st_trading) == 0.0f && f(Fund::st_profit) == 0.0f &&
                      f(Fund::st_revenue) == 0.0f && f(Fund::st_dividend) == 0.0f;
    if (!base) {
      y[size] = y[size_micro] = y[pe] = y[pb] = y[dy] = y[price] = y[mom] = kNaN;
      return;
    }
    assert(ind < static_cast<float>(fund::SW2021_L1_COUNT));
    y[size] = mc;
    y[size_micro] = (kMicroOut & (1u << static_cast<uint32_t>(ind))) ? kNaN : mc;
    y[pe] = -f(Fund::net_profit_ttm) / mc;
    y[pb] = -f(Fund::equity_mrq) / mc;
    y[dy] = f(Fund::dy_ttm);
    y[price] = close_.back();
    y[mom] = ret_20d_.back();
  }

private:
  float f(size_t port) const { return fund_[port].back(); }

  // 微盘行业白名单 = 全部已知行业 − {农林牧渔, 房地产, 环保, 钢铁, 银行} (位 = SW2021 一级 ID)
  static_assert(fund::SW2021_L1_COUNT <= 32, "行业位图用 uint32_t");
  static constexpr uint32_t kMicroOut = [] {
    uint32_t m = 0;
    for (std::string_view name : {"农林牧渔", "房地产", "环保", "钢铁", "银行"})
      for (size_t i = 1; i < fund::SW2021_L1_COUNT; ++i)
        if (fund::SW2021_L1_NAMES[i] == name)
          m |= 1u << i;
    return m;
  }();
  static_assert(std::popcount(kMicroOut) == 5, "微盘行业黑名单拼写不在 SW2021 一级表内");

  const Series (&fund_)[Fund::kCount]; // Fund 全部口 (同域 onSettle, 拓扑序在前 → back() = 次日盘前态)
  const Series &mcap_;                 // Valuation.mcap (back() = 收盘箱值)
  const Series &close_;                // KlineDay.close (日频节点, back() = 本日收盘价, 不复权)
  const Series &ret_20d_;              // Momentum.ret_20d (日频节点, back() = 本日)
  const bool bj_;                      // 北交所 (交易制度 / 流动性不同, 不入池)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_PoolKey(N) N(PoolKey, (PoolKey), (Fund.outs(), Valuation.out(Valuation.mcap), KlineDay.out(KlineDay.close), Momentum.out(Momentum.ret_20d), asset_code_), onSettle)

#define FIELDS_L_1day_PoolKey(X, CAT1)                                                                                                                                                                       \
  X(key_size, CAT1, AUTO, "Pool Key Size", "池键市值", "母集内收盘总市值(亿元), 母集外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{base})", TS(PoolKey, size, None, None))                                     \
  X(key_size_micro, CAT1, AUTO, "Pool Key Size Micro", "池键微盘市值", "母集∧微盘行业白名单内收盘总市值(亿元), 外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{base,wl})", TS(PoolKey, size_micro, None, None)) \
  X(key_pe, CAT1, AUTO, "Pool Key PE", "池键PE", "母集内 −归母净利TTM/收盘市值 (= −EP), 外NaN", R"(-\frac{NP^{TTM}}{\mathrm{mcap}_t})", TS(PoolKey, pe, None, None))                                         \
  X(key_pb, CAT1, AUTO, "Pool Key PB", "池键PB", "母集内 −归母权益MRQ/收盘市值 (= −BP), 外NaN", R"(-\frac{EQ^{MRQ}}{\mathrm{mcap}_t})", TS(PoolKey, pb, None, None))                                         \
  X(key_dy, CAT1, AUTO, "Pool Key DY", "池键股息率", "母集内股息率TTM, 外NaN", R"(\mathrm{dy}^{TTM})", TS(PoolKey, dy, None, None))                                                                          \
  X(key_price, CAT1, AUTO, "Pool Key Price", "池键股价", "母集内收盘价(元,不复权), 外NaN", R"(P^{\mathrm{close}}_D)", TS(PoolKey, price, None, None))                                                        \
  X(key_mom, CAT1, AUTO, "Pool Key Momentum", "池键动量", "母集内20日收益率(后复权), 外NaN", R"(\frac{P^{adj}_D}{P^{adj}_{D-20}} - 1)", TS(PoolKey, mom, None, None))                                        \
  X(key_st, CAT1, AUTO, "Pool Key ST", "池键ST", "活跃∧次日ST(不含*ST)的收盘总市值(亿元), 外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{live,st=1})", TS(PoolKey, st, None, None))                            \
  X(key_sst, CAT1, AUTO, "Pool Key *ST", "池键*ST", "活跃∧次日*ST的收盘总市值(亿元), 外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{live,st=2})", TS(PoolKey, sst, None, None))
