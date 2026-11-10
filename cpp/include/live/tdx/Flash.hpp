// 通达信适配: 下单 = 闪电下单窗 UI 自动化, 一步一函数 (Broker::order_steps 即其名称):
//   切换  图表切到该只 (闪电窗只认图表当前证券): 向主窗投通达信外部联动消息, 等标题换过来
//   开窗  主窗 WM_COMMAND 打开闪电窗
//   填单  窗内证券代码再核一遍, 填价格 / 数量 (数量单位为元即按额: 只收整数元, 客户端按价折股、向下取整步长, 以预估股数核对)
//   确认  点提交,「…交易确认」框核对正文 (代码 / 价格 / 数量)
//   回报  点确认,「提示」框读回报 (成功含合同号) 点掉, 等各框与闪电窗都关了
// 开窗 / 确认 / 回报同形: 做一个动作, 等它弹出的那个对话框. 任一步不符或超时即 Fail 停在那一步, 已弹出的窗口留给人工处理.
// 闪电窗按 (方向, 信用方式) 取; 卖券还款未录到, 没有融资卖出.
#pragma once

#include "live/Broker.hpp"

#include <string>
#include <vector>

namespace live::tdx::flash {

const std::vector<std::string> &steps();
bool supports(int side, Credit credit);
// 当前线程跑完 (须持界面锁); step(i, 概要) 报进度
Placed place(const OrderReq &o, const Step &step);

} // namespace live::tdx::flash
