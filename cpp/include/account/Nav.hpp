// 账户净值: 资金流水账本 (live/Flows.hpp) 的逐日回放 → 时间加权指数. 券商无关, 只认 live::CashFlow (live/Broker.hpp)
//   回放 (逐交易日, 当日的流水全记完后计):
//     现金     = 末个发生金额非 0 行的剩余金额
//     持仓     = 计入持仓的各行 (live::flows::holds) 的股数累计;
//                市值按当日收盘, 无收盘沿用上一收盘 (未上市新股 = 入账价);
//                库里全无收盘的 (ETF / LOF 等) 以各日成交均价 + 券商现价 (Spot) 为观测点, 相邻两点间按自然日线性插值, 末点之后持平
//     融资负债 = 融资借款 + 偿还融资负债 / 费用 (发生金额带号相加)
//     应计利息 = 逐自然日 上一交易日末负债 × 年利率 / 360, 偿还融资利息抵减
//     净值     = 现金 + 市值 − 融资负债 − 应计利息
//     外部进出 = 银证转账 + 担保品划入 (按当日收盘计价)
//     指数     = 时间加权: idx_t = idx_{t−1} × growth(净值_{t−1}, 净值_t, 进出_t)
#pragma once

#include "misc/date.hpp"

#include <functional>
#include <map>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace live {
struct CashFlow;
}

namespace account {

using live::CashFlow;

// 回放要计价的证券 (计入持仓的各行的代码, 升序)
std::vector<std::string> securities(const std::vector<CashFlow> &rows);
using Closes = std::unordered_map<std::string, std::map<misc::Ymd, double>>; // 代码 → 日 → 收盘

// 一只持仓: 代码 / 股 / 计价 (回放 = 当日收盘, 无收盘沿用上一收盘)
struct Hold {
  std::string code;
  double qty = 0, px = 0;
  bool operator==(const Hold &) const = default;
};
struct Day {
  misc::Ymd date = 0;
  double nav = 0, flow = 0, index = 1; // 净值 / 外部进出 / 指数
  std::vector<Hold> hold;              // 当日收后持仓 (回放 = 代码升序)
};
// 一段的净值增长倍数: 盈亏 (nav − nav0 − flow) / 基数; 段内进出时点不知, 入金计段初、出金计段末 (段内在场的资金都进基数); 基数 ≤ 0 → 1
double growth(double nav0, double nav, double flow);
// 当日一笔成交 (日内回推用): 所在槽 / 股 (买正卖负) / 成交价 / 现金腿 (买 −额 − 费, 卖 +额 − 费; 融资买入同记: 现金与负债合看)
struct Trade {
  int slot = 0;
  std::string code;
  double qty = 0, px = 0, cash = 0;
};
// 一日的分钟指数 (历史回放 / 当日同一份): 持仓 d.hold 按各只分钟收计价, 末槽对齐 d.nav; 槽 s 尚未发生的成交 (槽晚于 s) 退回:
//   nav_s = d.nav + Σ 股 × (收_s − 末收) − Σ_{槽 > s 的成交} (现金腿 + 股 × 收_s) (该只无分钟按成交价), out[s] = idx0 × growth(nav0, nav_s, d.flow);
//   path(代码) = 该只 [out.size()] 逐槽收 (无根沿用上一根, 首根前 = 前收; 空 = 无分钟 → 不动); trades 空 = 不回推 (资金流水无成交时刻, 时刻取历史成交 / 券商今日成交)
void intraday(const Day &d, double nav0, double idx0, std::span<const Trade> trades, const std::function<std::span<const float>(const std::string &)> &path,
              std::span<double> out);
// 券商现价: date 日的观测点 (只用于 px 里全无收盘的证券; date 不晚于其末笔成交 → 不用)
struct Spot {
  misc::Ymd date = 0;
  std::map<std::string, double> px; // 代码 → 现价
  bool operator==(const Spot &) const = default;
};
// 在交易日 days (升序) 上回放, 自首条流水所在日 (之后首个交易日) 起; 晚于 days 末日的流水不记
std::vector<Day> replay(const std::vector<CashFlow> &rows, double rate, const std::vector<misc::Ymd> &days, const Closes &px, const Spot &spot);
// 晚于 d 的外部进出 (担保品按入账价; 回放末日之后、今日之前的那段)
double flow_after(const std::vector<CashFlow> &rows, misc::Ymd d);

} // namespace account
