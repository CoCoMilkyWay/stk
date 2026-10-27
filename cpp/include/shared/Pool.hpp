#pragma once
// pool — 池日频面板 (PIT 逐日成员), Lob 档 Meta 的 cs_valid 数据源; 与 fund::Panel (基本面日频面板) 对仗.
//
//   池本身是 Bar 档的截面列 (Node/Bar/CS/Pool/Pool.hpp, 每池一列 pool_<pool>), 落在 Bar 池;
//   Lob 池 <pool> 规划时 (ComputeService) 从 Bar 池读出:
//     第 D 日成员 = Bar 池 D 的前一交易日 (Bar 池日期轴上的前一日) L_1day 行 (收盘) 的 pool_<pool> 列 > 0.5
//                   (池列是盘后列: 盘中 as-of 恒 -1, 经读端 Source 同一投影读出)
//     axis()        日期轴内进过池的并集 (全局 id 升序) = 池轴 (Lob 池静态池轴 PoolAxis: 张量列 / 落盘 / 读端).
//                   TS 特征对池轴上所有资产在区间内全程计算, 区间内日后进池不用补历史
//     in_pool(sub, D) 第 D 日成员位 (sub = 池轴下标) → CoreLob::member → MetaTracker → cs_valid 整日常量
//   PIT: 第 D 日池由 D−1 收盘定, 对 Lob 档只是 (资产, 日) 的 bool.
//   各网格层同一成员 (整日常量, 含 L_1day): 因子在子池里按 Bar 网格评估时的截面门控 (FactorsLoad). L_1day 也不读 D 收盘行:
//   池键母集按次日 (D+1) 停牌 / ST 判 (盘后列), D 收盘建仓时尚不可知 (前视).
//   全市场不是池: 那是 Bar 池 (全轴, 见 FeatureLib.hpp).
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // PoolAxis

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct FeatureLib;

namespace pool {

class Panel {
public:
  // Bar 池的池列 col → dates 逐日成员 (盘中 as-of). 非盘后列 / D 不在 Bar 池日期轴 / D−1 不在轴上或未落盘 / 并集为空 → assert
  static Panel build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates);

  const PoolAxis &axis() const { return axis_; }
  const std::vector<misc::Ymd> &dates() const { return dates_; } // = build 的 dates
  // 第 D 日成员位 [axis().size()] / 单格. D 不在 dates() → assert
  const uint8_t *row(misc::Ymd d) const;
  bool in_pool(size_t sub, misc::Ymd d) const;

private:
  std::vector<misc::Ymd> dates_;
  PoolAxis axis_;
  std::vector<uint8_t> member_; // [date_i * axis_.size() + sub]
};

} // namespace pool
