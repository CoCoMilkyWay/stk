// 实盘: 资金流水账本, 券商无关 (与两融条款 live/Terms.hpp 同形: 拉 → 存 → 用); 账本 = 资金流水 + 历史成交 (同拉同存)
//   缓存 output/live/flows.csv (live/Pulled.hpp): 首行另带 rate,<融资年利率>; 流水原序 (截至拉的前一日); 存一次账本代 +1
//     output/live/deals.csv: 历史成交 (日, 时刻) 序, 起于流水首日, 止于同一日
//   拉: 重查账本末日起的资金流水并入账本 (账本末日起的各行须恰为重查的开头几行, 名称除外; 余额链断开 / 有不认识的带股数业务 → Fail);
//     历史成交重查其末日起 (没有即流水首日起), 末日起的整段换成重查的; 逐 (日, 代码) 的成交股数 (买正卖负) 须等于流水里买卖行的股数;
//     流水累计的持仓 + 今日成交对上券商持仓才落盘
//   用: 账户净值逐日回放 (account/Nav.hpp; 历史成交给日内按时刻回推) + 历史成交点 (Market NAV 账户线)
//   资金流水没有时刻, 历史成交没有现金 / 负债 / 利息 / 转账, 两者缺一不可
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
  std::vector<Deal> deals; // (日, 时刻) 序
};

Cache load();          // 没有账本 → 空
Cache pull(Broker &b); // 客户端重查 → 并 → 核 → 存; 失败 → Fail
bool partial();        // 有资金流水而缺历史成交 (deals.csv 还没落过): 该补拉 (pull 即自流水首日起拉全)
uint64_t generation(); // 账本代: 本进程起于 1, 存一次 +1 (任意线程)

// 计入持仓的行: 买卖 / 融资买入 / 担保品划入 / 新股入账 (申购途中的配号、中签与回冲不计; 其余带股数的业务不认识 → Fail)
bool holds(const CashFlow &r);
// 其中的成交行 (买卖 / 融资买入: 与历史成交对应)
bool traded(const CashFlow &r);
// 历史成交对资金流水: 自 from 日起逐 (日, 代码) 的成交股数 (买正卖负) 等于流水里成交行的股数; 不等 → Fail
void match(const std::vector<CashFlow> &rows, const std::vector<Deal> &deals, misc::Ymd from);

} // namespace live::flows
