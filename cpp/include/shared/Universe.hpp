#pragma once
// universe — 选池逐日成员 (日频 PIT), Lob 档 Meta 的 cs_valid 数据源; 与 Fund 吃 fund::Pool 同形.
//
//   选池本身是 Bar 档的 CS 算子 (Operator/Bar/CS/Universe/Pool.hpp, 每池一列 pool_<name>), 落在 all 库;
//   Lob 档库 <name> 规划时 (ComputeService) 从 all 库读出:
//     第 D 日成员 = all 库 D 的前一交易日 (all 库日期轴上的前一日) L1 末有效行 (254, 收盘) 的 pool_<name> 列 > 0.5
//     axis()        日期轴内进过池的并集 (全局 id 升序) = Lob 库静态 A 轴 (UniverseAxis: 张量列 / 落盘 / 读端).
//                   TS 特征对轴上所有标的在区间内全程计算, 区间内日后进池不用补历史
//     in_pool(sub, D) 第 D 日成员位 (sub = axis 子轴下标) → CoreLob::member → MetaTracker → cs_valid 整日常量
//   PIT: 第 D 日池由 D−1 收盘定, 对 Lob 档只是 (资产, 日) 的 bool.
//   全市场不是票池: 那是 Bar 档 (all 库, 全 A 轴, 见 FeatureLib.hpp).
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // UniverseAxis

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct FeatureLib;

namespace universe {

class Pool {
public:
  // all 库 (bar) 的选池列 col → dates 逐日成员. D 不在 all 库日期轴 / 是其首日 / 前一日未落盘 / 并集为空 → assert
  static Pool build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates);

  const UniverseAxis &axis() const { return axis_; }
  const std::vector<misc::Ymd> &dates() const { return dates_; } // = build 的 dates
  // 第 D 日成员位. D 不在 dates() → assert
  bool in_pool(size_t sub, misc::Ymd d) const;

private:
  std::vector<misc::Ymd> dates_;
  UniverseAxis axis_;
  std::vector<uint8_t> member_; // [date_i * axis_.size() + sub]
};

} // namespace universe
