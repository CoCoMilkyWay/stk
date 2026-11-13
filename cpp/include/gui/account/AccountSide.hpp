// 行情的账户一侧 (Quote::kAccount): 历史 (价格库 worker 上回放资金流水账本) / 当日 (AccountHub) 同一核 day_of
//   一日 = 收后持仓按各只分钟收计价 (无根沿用上一根, 首根前 = 前收; 末槽对齐当日净值) → 时间加权指数的逐槽点位
//     → 根根首尾相接的一日 (开 = 上一槽, 首槽 = 前收; 收 = 本槽; 高 / 低 = 两者之极; 额 0; 日根包住全路径)
//   全史 = 各日按 Series 的紧凑形打包 (篮子口径步长), 恒带分钟, 复权 1
#pragma once

#include "account/Ledger.hpp"
#include "gui/quote/Quote.hpp"
#include "misc/date.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Account {

// 流水代码的一只持仓 (全局 id 在资产轴上查, 不在 = Ref::kNone)
Quote::Holding holding(const std::string &code, double qty, double px);

// 一只当日的根 (日根 ref = 前收 + 分钟根; 指针用到下次调用为止); nullptr = 无 (该只不动)
using DayOf = std::function<const Quote::Day *(const std::string &code)>;

// 账户一日 d (前一日净值 nav0 / 指数 idx0 = 本日前收) 的槽 [0, n): 持仓 d.hold 按 of 的分钟收计价, 点位 = idx0 × growth(nav0, nav_s, d.flow)
Quote::Day day_of(const account::Day &d, double nav0, double idx0, size_t n, const DayOf &of);

// 账本的回放: 账户全史 (日脊上自首条流水所在日起; 首日平于 1) + 摘要 (含末日收后持仓); 无账本 / 日脊上无日 → se 空
struct Replay {
  std::shared_ptr<const Quote::Series> se;
  Quote::Equity eq;
};
// minutes(全局 id, 自日脊第 from 日, out) = 个股连分钟的全史 (价格库给; 无 → false)
using Minutes = std::function<bool(uint32_t id, size_t from, Quote::Series &out)>;
Replay replay(const account::Ledger &l, const std::vector<misc::Ymd> &dates, const account::Spot &spot, const Minutes &minutes);

} // namespace GUI::Account
