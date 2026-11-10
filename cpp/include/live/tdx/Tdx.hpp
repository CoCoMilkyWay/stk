// 通达信 (TdxW.exe, Wine 下) 的 Broker: 读走进程堆里的明文表, 写与重查走 shim 驱动界面
//   盯持仓线程: 有人在看才转; 每 0.1s follow 持仓 (约 0.03ms; 客户端自己重查过, 如成交后, 即换代),
//     每 60s 一遍全量扫出持仓与成交 (补全「活动块唯一」校验、跟上客户端换进程); 出过错即下一轮先接回客户端
//     (shim 不在线即重注入)、全量重扫 (重找进程).
//   刷新线程: 一轮让客户端依次重查 成交、持仓 (两表对不上再连查)、挂单、信用账户, 每查完一样即发布, 不等整轮.
//     起因: 有人要 (request / sync)、有人在看而还没刷过或距上轮满 every 秒、下单后的 kick 定点、
//     客户端自己重查了持仓却与成交对不上 (交易端成交了). 失败不自动重试, 等下一个起因.
//   界面自动化 (刷新 / 下单 / 两融条款 / 调仓) 一次一路 (ui::Gate).
#pragma once

#include "live/Broker.hpp"

#include <memory>

namespace live::tdx {

std::unique_ptr<Broker> make();

} // namespace live::tdx
