// 通达信适配: 信用账户 — 两融条款 (标的证券 × 担保品) 与账户 (信用资产 + 融资负债, 随查随读)
//   条款: 功能树开「标的证券查询」「担保品查询」页各点「查询全部」, 等内存里拼出行数恰为页上条数的整表
//     (担保品近万只, 客户端要卡一两分钟). 两表以 (市场, 代码) 为键, 合成时按代码归一 (跨市场同号的只留 market_of 那只).
//   账户: 刷新一轮里接在持仓之后, 开「信用资产」「融资负债汇总」页, ask 读本次应答 (须持界面锁)
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Tables.hpp"

namespace live::tdx::credit {

struct Tables {
  mem::Table<tables::Margin> margin;
  mem::Table<tables::Collat> collat;
  mem::Table<tables::Credit> credit;
  mem::Table<tables::Debts> debts;
};

// 客户端重查两融条款 (须持界面锁)
Terms pull(Tables &t, int pid, const Log &log, const Live &live);
// 客户端重查信用资产与融资负债 (须持界面锁, 停在融资负债汇总页); pos 为同一轮刚读的持仓
Account account(Tables &t, int pid, const Keyed<Position> &pos);

} // namespace live::tdx::credit
