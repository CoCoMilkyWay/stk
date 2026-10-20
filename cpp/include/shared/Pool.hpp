#pragma once
// pool — 池日频面板 (PIT 逐日成员), Lob 档 Meta 的 cs_valid 数据源; 与 fund::Panel (基本面日频面板) 对仗.
//
//   池本身是 Bar 档的截面列 (Node/Bar/CS/Pool/Pool.hpp, 每池一列 pool_<pool>), 落在 Bar 池;
//   Lob 池 <pool> 规划时 (ComputeService) 从 Bar 池读出:
//     第 D 日成员 = Bar 池 D 的前一交易日 (Bar 池日期轴上的前一日) L_1day 行 (收盘) 的 pool_<pool> 列 > 0.5
//                   (= 盘前 as-of: level_asof(L_1day, L_sec, 0) = -1, 经读端 Source 同一投影读出)
//     axis()        日期轴内进过池的并集 (全局 id 升序) = 池轴 (Lob 池静态池轴 PoolAxis: 张量列 / 落盘 / 读端).
//                   TS 特征对池轴上所有资产在区间内全程计算, 区间内日后进池不用补历史
//     in_pool(sub, D) 第 D 日成员位 (sub = 池轴下标) → CoreLob::member → MetaTracker → cs_valid 整日常量
//   PIT: 第 D 日池由 D−1 收盘定, 对 Lob 档只是 (资产, 日) 的 bool.
//   网格层 grid (默认 L_sec = 上述盘前口径): 成员 = level_asof(L_1day, grid, ·) 投影, 整日常量; 日内层 = D−1 收盘行,
//   L_1day = D 收盘行 (D 收盘定的池, 次日持有) —— 因子在子池里按 Bar 网格评估时的截面门控 (FactorsLoad).
//   全市场不是池: 那是 Bar 池 (全轴, 见 FeatureLib.hpp).
#include "features/FeaturesDefine.hpp" // L_sec
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
  // Bar 池的池列 col → dates 逐日成员 (grid 层 as-of). D 不在 Bar 池日期轴 / 所读行 (D−1 或 D) 不在轴上或未落盘 / 并集为空 → assert
  static Panel build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates, size_t grid = L_sec);

  const PoolAxis &axis() const { return axis_; }
  const std::vector<misc::Ymd> &dates() const { return dates_; } // = build 的 dates
  // 第 D 日成员位. D 不在 dates() → assert
  bool in_pool(size_t sub, misc::Ymd d) const;

private:
  std::vector<misc::Ymd> dates_;
  PoolAxis axis_;
  std::vector<uint8_t> member_; // [date_i * axis_.size() + sub]
};

} // namespace pool
