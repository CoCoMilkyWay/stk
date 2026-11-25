// 行情的账户一侧 (Quote::kAccount): 历史 (价格库 worker 上回放资金流水账本) / 当日 (AccountHub) 同一核 day_of
//   一日 = 收后持仓按各只分钟收计价 (无根沿用上一根, 首根前 = 前收; 末槽对齐当日净值; 按成交时刻回推: 历史 = 账本的历史成交, 当日 = 券商今日成交)
//     → 时间加权指数的逐槽点位 → 根根首尾相接的一日 (开 = 上一槽, 首槽 = 前收; 收 = 本槽; 高 / 低 = 两者之极; 额 0; 日根包住全路径)
//   全史 = 各日按 Series 的紧凑形打包 (篮子口径步长), 恒带分钟, 复权 1
//   成交 (历史成交 / 今日成交同形) 两用: 日内回推的成交 (trade_of) / 成交点 (mark_of, 一笔一点; 聚合见 Quote::rebin)
//   持仓段 (positions_of → Quote::Positions): 逐日扫流水, 买入按额加权摊成本, 股数归零分段; 成本比 (成本 × adj(定日) ÷ 收盘 × adj(当日)) 逐日存,
//     渲染处 成本线位 = 成本比 × 线在该日收盘处的值 —— 与图的归一化 / 复权口径无关
#pragma once

#include "account/Nav.hpp"
#include "gui/quote/Quote.hpp"
#include "live/Flows.hpp"
#include "misc/date.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace GUI::Account {

// 流水代码的一只持仓 (全局 id 在资产轴上查, 不在 = Ref::kNone)
Quote::Holding holding(const std::string &code, double qty, double px);

// 一笔成交 → 日内回推的成交: 槽 = 所在分钟根 (竞价并入首根), 至多到 last (券商时钟领先新浪也算已发生); 费用按费率估 (live::rebal::fee)
account::Trade trade_of(const live::Fill &x, int last);
// d 日的一笔成交 → 成交点 (一分钟一档)
Quote::TradeMark mark_of(misc::Ymd d, const live::Fill &x);

// 一只当日的根 (日根 ref = 前收 + 分钟根; 指针用到下次调用为止); nullptr = 无 (该只不动)
using DayOf = std::function<const Quote::Day *(const std::string &code)>;

// 账户一日 d (前一日净值 nav0 / 指数 idx0 = 本日前收) 的槽 [0, n): 持仓 d.hold 按 of 的分钟收计价, 槽晚于 s 的成交 trades 退回,
//   点位 = idx0 × growth(d.base, nav0, nav_s, d.flow) (account::intraday)
Quote::Day day_of(const account::Day &d, double nav0, double idx0, std::span<const account::Trade> trades, size_t n, const DayOf &of);

// 账本的回放: 账户全史 (日脊上自首条流水所在日起; 首日平于 1; 各日按该日的历史成交回推) + 摘要 (含末日收后持仓、回放各日的历史成交点);
//   无账本 / 日脊上无日 → se 空
struct Replay {
  std::shared_ptr<const Quote::Series> se;
  Quote::Equity eq;
};
// minutes(全局 id, 自日脊第 from 日, out) = 个股连分钟的全史 (价格库给; 无 → false)
using Minutes = std::function<bool(uint32_t id, size_t from, Quote::Series &out)>;
Replay replay(const live::flows::Cache &l, const std::vector<misc::Ymd> &dates, const account::Spot &spot, const Minutes &minutes);

} // namespace GUI::Account
