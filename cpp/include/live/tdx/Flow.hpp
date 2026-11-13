// 通达信适配: 资金流水 — 功能树开「资金流水」页, 按窗 (客户端一次至多查 30 天) 设起止日、点查询, 各窗应答按日接起
//   起止日控件 (SysDateTimePick32) 只认键盘: 点年份段聚焦, 逐段敲数字, 回读核对.
//   空应答与堆里留着的旧空表逐字节相同, 内存里认不出: 查前清空条数标签, 等客户端写回「共N条」定条数, N > 0 才等内存里冒出恰 N 行的新块
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Tables.hpp"

#include <vector>

namespace live::tdx::flow {

// 客户端重查 from 日起 (含) 至昨日的资金流水 (须持界面锁; 客户端只给到上一日); from = 0 即从昨日往前逐窗查到连续几窗无流水 (开户前)
std::vector<CashFlow> pull(mem::Table<tables::CashFlows> &t, const mem::HeapP &heap, misc::Ymd from, const Log &log, const Live &live);

} // namespace live::tdx::flow
