// 实盘: 两融条款的本地缓存, 券商无关 (与资金流水账本 account/Ledger.hpp 同形)
//   output/live/terms.csv (原子替换): 首行 pulled,<拉于 YYYYMMDD>; 次行表头; 其后一行一只
#pragma once

#include "live/Broker.hpp"
#include "misc/date.hpp"

namespace live::terms {

struct Cache {
  misc::Ymd pulled = 0; // 拉于 (0 = 还没有缓存)
  Terms terms;
};

Cache load(); // 没有缓存 → 空
void save(const Cache &c);

} // namespace live::terms
