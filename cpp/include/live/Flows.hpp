// 实盘: 资金流水账本, 券商无关 (与两融条款 live/Terms.hpp 同形: 拉 → 存 → 用)
//   缓存 output/live/flows.csv (live/Pulled.hpp): 首行另带 rate,<融资年利率>; 流水原序 (截至拉的前一日); 存一次账本代 +1
//   拉: 重查账本末日起的资金流水并入账本 (账本末日起的各行须恰为重查的开头几行, 名称除外; 余额链断开 / 有不认识的带股数业务 → Fail),
//     流水累计的持仓 + 今日成交对上券商持仓才落盘
//   用: 账户净值逐日回放 (account/Nav.hpp)
#pragma once

#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <cstdint>
#include <vector>

namespace live::flows {

inline constexpr double kShare = 0.5; // 股数相等的容差 [股]

struct Cache {
  misc::Ymd pulled = 0; // 拉于 (0 = 还没有账本)
  double rate = 0;      // 融资年利率
  std::vector<CashFlow> rows;
};

Cache load();          // 没有账本 → 空
Cache pull(Broker &b); // 客户端重查 → 并 → 核 → 存; 失败 → Fail
uint64_t generation(); // 账本代: 本进程起于 1, 存一次 +1 (任意线程)

// 计入持仓的行: 买卖 / 融资买入 / 担保品划入 / 新股入账 (申购途中的配号、中签与回冲不计; 其余带股数的业务不认识 → Fail)
bool holds(const CashFlow &r);

} // namespace live::flows
