// 通达信 (TdxW.exe, Wine 下) 的 Broker: 读经注入的 shim 在客户端里直发请求 (照搬交易插件的入参, 不动界面), 写经 shim 驱动闪电下单窗
//   刷新线程: 有人在看才转; 一轮直发 成交、持仓 (两问之间又有成交即对不上, 连查)、挂单、信用账户, 每读完一样即发布, 不等整轮.
//     起因: 有人要 (request / sync)、有人在看而还没刷过或距上轮满 every 秒、下单成功后的定点. 失败不自动重试, 等下一个起因.
//   插件在本客户端进程里还没发过的表先经界面查一次 (引导). 动界面的 (引导 / 下单 / 独占) 一次一路 (界面锁 Gate);
//     下单 / 独占同形: 段 + 界面锁 + 接上客户端 (txn); 两融条款 / 资金流水同形: 段 + 接上客户端 + 引导 (pull).
//   摸客户端 / 在线用例另有独立程序 shim (src/live/tdx/shim/).
#pragma once

#include "live/Broker.hpp"

#include <memory>

namespace live::tdx {

std::unique_ptr<Broker> make();

} // namespace live::tdx
