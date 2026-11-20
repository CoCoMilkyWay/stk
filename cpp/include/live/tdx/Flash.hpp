// 通达信适配: 下单 = 闪电下单窗 UI 自动化, 五步一表 (切换 / 开窗 / 填单 / 确认 / 回报)
//   每步统一经 trace::Span 报进 Track 与日志, 填单后经 trace::live 报概要, 零双写
#pragma once

#include "live/Broker.hpp"

#include <span>
#include <string_view>

namespace live::tdx::flash {

std::span<const std::string_view> steps();
Placed place(const OrderReq &o); // 当前线程跑完 (须持界面锁)

} // namespace live::tdx::flash
