// 通达信适配: 资金流水 — 经请求层直发按窗 (客户端一次至多查 30 天) 覆盖起止日入参, 各窗应答按日接起;
//   须插件已发过资金流水的功能 (tables::seen; Driver 先经界面引导)
#pragma once

#include "live/Broker.hpp"

#include <vector>

namespace live::tdx::flow {

// from 日起 (含) 至昨日的资金流水 (客户端只给到上一日); from = 0 即从昨日往前逐窗查到连续几窗无流水 (开户前)
std::vector<CashFlow> pull(misc::Ymd from, const Report &r);

} // namespace live::tdx::flow
