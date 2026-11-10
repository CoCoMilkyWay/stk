// Tab Rebal - 一键调仓页: 名单 / 配置 / 两融条款, 试算 (账户 现 / 调仓后、持仓行卖出、名单行买入), 执行与日志
//   试算与执行的口径见 live/Rebal.hpp, 事务的步骤见 LiveHub.hpp; 委托价与持仓页共用一个
//   卖出滑杆按股数线性走 (0 … 全部可卖), 拖动时就地 snap 到能成单的股数: 0 不卖、
//     first 起按步长递增 (first 够起买股数且够最小调仓单元)、全部可卖 (够最小调仓单元才有), 取最近的
//   隐私 (LiveHub::priv, 与持仓页共用): 购买力等金额、账户对照各格、持仓与名单行的代码 / 名称 / 金额、事务日志遮成 kMask
#pragma once

#include "gui/task_live/LiveHub.hpp"

#include <optional>

namespace GUI::Live {

struct RebalState {
  bool ask = false;           // 执行调仓待确认
  std::optional<double> unit; // 最小调仓单元输入框编辑中的值 (无 = 显示当前配置)
};

void RenderTabRebal(LiveHub &hub, RebalState &st);

} // namespace GUI::Live
