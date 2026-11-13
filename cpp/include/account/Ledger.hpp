// 账户净值: 券商资金流水 (live::CashFlow) 的本地账本与逐日回放. 券商无关, 只认 live/Broker.hpp 的类型
//   账本 output/live/flows.csv (原子替换): 首行 pulled,<拉于 YYYYMMDD>,rate,<融资年利率>; 次行表头; 其后流水原序 (截至拉的前一日)
//   回放 (逐交易日, 当日的流水全记完后计):
//     现金     = 末个发生金额非 0 行的剩余金额
//     持仓     = 买卖 / 融资买入 / 担保品划入 / 新股入账 的股数累计 (申购途中的配号、中签与回冲不计; 其余带股数的业务不认识 → Fail);
//                市值按当日收盘, 无收盘沿用上一收盘 (未上市新股 = 入账价)
//     融资负债 = 融资借款 + 偿还融资负债 / 费用 (发生金额带号相加)
//     应计利息 = 逐自然日 上一交易日末负债 × 年利率 / 360, 偿还融资利息抵减
//     净值     = 现金 + 市值 − 融资负债 − 应计利息
//     外部进出 = 银证转账 + 担保品划入 (按当日收盘计价)
//     指数     = 时间加权: idx_t = idx_{t−1} × (净值_t − 进出_t) / 净值_{t−1} (前一日净值 ≤ 0 → 不变)
#pragma once

#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace account {

using live::CashFlow;

struct Ledger {
  misc::Ymd pulled = 0; // 拉于 (0 = 还没有账本)
  double rate = 0;      // 融资年利率
  std::vector<CashFlow> rows;
};

Ledger load(); // 没有账本 → 空
void save(const Ledger &l);
uint64_t generation(); // 账本代: 本进程起于 1, save 一次 +1 (任意线程)

// 账本并入一次重查 (重查覆盖 from 日起): 账本 from 日起的各行须恰为重查的开头几行; 并后余额链断开 / 有不认识的带股数业务 → Fail
std::vector<CashFlow> merge(const std::vector<CashFlow> &old, misc::Ymd from, std::vector<CashFlow> fresh);

// 流水累计的持仓: 代码 → 股 (不含 0)
std::map<std::string, double> holdings(const std::vector<CashFlow> &rows);
// 流水累计的持仓 + 今日成交 对 券商持仓: 对不上的各只 "代码 流水/券商" (对上 → 空)
std::string reconcile(const std::vector<CashFlow> &rows, const live::Keyed<live::Position> &pos, const live::Fills &fills);

// 回放要计价的证券 (计入持仓的各行的代码, 升序)
std::vector<std::string> securities(const std::vector<CashFlow> &rows);
using Closes = std::unordered_map<std::string, std::map<misc::Ymd, double>>; // 代码 → 日 → 收盘

struct Day {
  misc::Ymd date = 0;
  double nav = 0, flow = 0, index = 1;              // 净值 / 外部进出 / 指数
  std::vector<std::pair<std::string, double>> hold; // 当日收后持仓 (代码, 股; 代码升序)
};
// 在交易日 days (升序) 上回放, 自首条流水所在日 (之后首个交易日) 起; 晚于 days 末日的流水不记
std::vector<Day> replay(const std::vector<CashFlow> &rows, double rate, const std::vector<misc::Ymd> &days, const Closes &px);
// 晚于 d 的外部进出 (担保品按入账价; 回放末日之后、今日之前的那段)
double flow_after(const std::vector<CashFlow> &rows, misc::Ymd d);

} // namespace account
