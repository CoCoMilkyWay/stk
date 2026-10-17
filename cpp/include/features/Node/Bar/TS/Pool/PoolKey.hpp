#pragma once

// =============================================================================
// PoolKey - 选池排序键 (compute=onMinute): 母集外 → NaN, 母集内 = 键值; CS 选池列 (CS/Universe/Pool.hpp) 在其上取前 N
// =============================================================================
//   母集 (全部只看本资产代码 + 本资产 Fund 当日 PIT 口 + 本分钟价 → 资产局部纯函数):
//     在市 (Fund::in_market: 非北交所 ∧ 已上市 ∧ 未退市 ∧ 非停牌) ∧ 上市满 60 日历日 (list_age ≥ 60) ∧ st_level == 0 (含退市整理期)
//     ∧ st_trading / st_profit / st_revenue / st_dividend 皆 0 ∧ 实时市值 > 0 ∧ 申万一级行业已知 (∈ 白名单)
//   键 (升序 = 指标由低到高, 选池取低端 / 高端):
//     size        mcap (亿元, 实时)
//     size_micro  mcap, 行业白名单另去 农林牧渔 / 房地产 / 环保 / 钢铁 / 银行 (qmt strat_0)
//     pe          −NP_ttm / mcap   (= −EP; 亏损接在正 EP 的高端之后: PE 2 < 1000 < −1000 < −2)
//     pb          −EQ_mrq / mcap   (= −BP)
//     dy          dy_ttm           (近 365 日税前分红 / 总市值; 分母 = 昨收 × 当日股本, 见 Fundamental.cpp)
//   选池日频: 交易日 D 的池 = D−1 收盘行 (L1 254) 的选池列 (shared/Pool.hpp 读 all 库), 盘中各行 = "此刻调仓"的池.
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
                      kCount };
  float y[kCount] = {};

  PoolKey(const float (&fund)[Fund::kCount], const Series &mcap, const std::string &code)
      : fund_(fund), mcap_(mcap), bj_(code.ends_with(".BJ")) {}

  inline void compute() {
    constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
    const float mc = mcap_.back();
    const float ind = fund_[Fund::ind_l1];
    const bool base = Fund::in_market(fund_, bj_) && fund_[Fund::list_age] >= 60.0f && fund_[Fund::st_level] == 0.0f &&
                      fund_[Fund::st_trading] == 0.0f && fund_[Fund::st_profit] == 0.0f &&
                      fund_[Fund::st_revenue] == 0.0f && fund_[Fund::st_dividend] == 0.0f &&
                      mc > 0.0f && ind > 0.0f;
    if (!base) {
      y[size] = y[size_micro] = y[pe] = y[pb] = y[dy] = kNaN;
      return;
    }
    assert(ind < static_cast<float>(fund::SW2021_L1_COUNT));
    y[size] = mc;
    y[size_micro] = (kMicroOut & (1u << static_cast<uint32_t>(ind))) ? kNaN : mc;
    y[pe] = -fund_[Fund::net_profit_ttm] / mc;
    y[pb] = -fund_[Fund::equity_mrq] / mc;
    y[dy] = fund_[Fund::dy_ttm];
  }

private:
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

  const float (&fund_)[Fund::kCount]; // Fund 当日口 (onDay 已算, 日内不变)
  const Series &mcap_;                // Valuation.mcap (同域 onMinute, 拓扑序在前)
  const bool bj_;                     // 北交所 (交易制度 / 流动性不同, 不入池)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_PoolKey(N) N(PoolKey, (PoolKey), (Fund.y, Valuation.out(Valuation.mcap), asset_code_), onMinute)

#define FIELDS_L1_PoolKey(X, CAT1)                                                                                                                                                                             \
  X(key_size, CAT1, AUTO, "Pool Key Size", "选池键市值", "母集内实时总市值(亿元), 母集外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{base})", TS(PoolKey, size, None, None))                                     \
  X(key_size_micro, CAT1, AUTO, "Pool Key Size Micro", "选池键微盘市值", "母集∧微盘行业白名单内实时总市值(亿元), 外NaN", R"(\mathrm{mcap}_t \cdot \mathbf{1}_{base,wl})", TS(PoolKey, size_micro, None, None)) \
  X(key_pe, CAT1, AUTO, "Pool Key PE", "选池键PE", "母集内 −归母净利TTM/实时市值 (= −EP), 外NaN", R"(-\frac{NP^{TTM}}{\mathrm{mcap}_t})", TS(PoolKey, pe, None, None))                                         \
  X(key_pb, CAT1, AUTO, "Pool Key PB", "选池键PB", "母集内 −归母权益MRQ/实时市值 (= −BP), 外NaN", R"(-\frac{EQ^{MRQ}}{\mathrm{mcap}_t})", TS(PoolKey, pb, None, None))                                         \
  X(key_dy, CAT1, AUTO, "Pool Key DY", "选池键股息率", "母集内股息率TTM, 外NaN", R"(\mathrm{dy}^{TTM})", TS(PoolKey, dy, None, None))
