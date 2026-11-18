// 通达信 (TdxW.exe, Wine 下) 的 Driver: 读 / 拉经注入的 shim 在客户端里直发请求 (照搬交易插件的入参, 不动界面), 写经 shim 驱动闪电下单窗
//   接: 进程换了即重开堆、shim 不在线即注入、请求层钩上; 再引导刷新四样的表 (接上即可读)
//   读: 四样各直发一表 (信用账户 = 信用资产 + 融资负债); 拉: 先引导其表, 再直发翻页 (两融条款) / 逐窗 (资金流水)
//   引导: 插件在本客户端进程里还没发过的表, 各经界面查一次 (其引导页), 末了回持仓页
//   动界面的 (引导 / 下单) 一次一路 (界面锁); 直发不碰界面, 不占
//   摸客户端 / 在线用例另有独立程序 shim (src/live/tdx/shim/).
#pragma once

#include "live/Broker.hpp"

#include <memory>

namespace live::tdx {

std::unique_ptr<Driver> make();

} // namespace live::tdx
