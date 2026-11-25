// 通达信 (TdxW.exe, Wine 下) 的 Driver: 读 / 拉经注入的 shim 在客户端里直发请求 (照搬交易插件的入参, 不动界面), 写经 shim 驱动闪电下单窗
//   接: 进程换了即重连 shim 通道、shim 不在线即注入、请求层钩上 (进程内找虚表与字典, 不读进程堆、不要 sudo); 再引导刷新五样的表 (接上即可读)
//   读: 五表一趟直发、同时在途 (一表一样); 拉: 先引导其表, 再直发翻页 (两融条款两表一并) / 逐窗 (账本: 资金流水与历史成交同窗)
//   引导: 插件在本客户端进程里还没发过的表, 各经界面查一次 (其引导页), 末了回持仓页
//   动界面的 (引导 / 下单) 一次一路 (界面锁); 直发不碰界面, 不占
//   画面: mirror 给 GUI 的 Client 页复现窗口树 / 绘制层 / 控件子项, 并可透传鼠键; 其接入与 connect 同一路
//   摸客户端 / 在线用例另有独立程序 shim (src/live/tdx/shim/).
#pragma once

#include "live/Broker.hpp"

#include <memory>

namespace live::tdx {

std::unique_ptr<Driver> make();

} // namespace live::tdx
