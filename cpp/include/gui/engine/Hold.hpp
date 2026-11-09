// Hold — 持有期的开 / 清仓描述
#pragma once

#include "gui/engine/Table.hpp" // FactorLevelId

#include <string>

namespace GUI::Engine {

// 持有期在因子层 lv 上的开 / 清仓 = Stat/Contract.hpp【协议表 1】的一行: 各一个 时点 @ 价 (UI 凡显示开关仓处同一份; 由层表 + Geo 推, 不按层分支)
struct HoldPoint {
  std::string at, px;
};
struct HoldPoints {
  HoldPoint open, close;
};
HoldPoints hold_points(int hold, FactorLevelId lv);
// hold_points 的完整写法: "开仓: C(层) @ 价, 清仓: 点 @ 价"
std::string hold_desc(int hold, FactorLevelId lv);

} // namespace GUI::Engine
