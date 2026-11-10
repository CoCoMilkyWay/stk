// 实盘: 持仓页一帧 — 持仓 × 当日成交 × 行情, 按代码对齐 (纯函数, 只出数字, 格式化归 UI)
//   三张表同以代码为键: 持仓 pos、成交合计 fills.by_code、行情 quotes; 对齐成 (h, f):
//     h 空 = 今日清仓 (持仓表已无此行或余额为 0, 只有当日列), f 空 = 今日该只没有成交
//   一行由几组列拼成, 缺了某组要的输入即整组空:
//     标识: 代码 / 名称 / 是否今日清仓 / 行情 (现价、盘口, 行情图也用);
//     持仓: 量、成本、券商盈亏取自持仓表, 随价变的按 Δ价 × 量 平移 (保留券商口径);
//     当日: 按成交价自算 (毛, 不扣费) = 现市值 − 昨收 × 昨仓 − Σ 方向 × 额, 昨仓 = 现量 − Σ 方向 × 量,
//           % 的分母为 昨收 × 昨仓 + 今买额; 与持仓的今买 / 今卖对不上即空;
//     盘口: 各列滑点 (竞价时空)、价位, 无参考价皆空
//   占比两列同构 = 市值 / 分母: 仓位% 的分母是券商口径的资产 (持仓表的 市值 / 仓位 反推, 取仓位最大那只、
//   舍入误差最小, 再随总市值平移), 占比的分母是总市值
#pragma once

#include "live/Book.hpp"
#include "live/Broker.hpp"

#include <array>
#include <optional>
#include <string>
#include <vector>

namespace live::board {

struct Pair {
  const Position *h = nullptr; // 空 = 今日清仓
  const Flow *f = nullptr;     // 空 = 今日没有成交
  const Sec &sec() const { return h ? static_cast<const Sec &>(*h) : static_cast<const Sec &>(*f); }
};
// 持仓 (余额 > 0) 在前、今日清仓在后; 两表市场须一致 (否则 Fail)
std::vector<Pair> join(const Keyed<Position> &pos, const Fills &fills);
bool synced(const Pair &p);                                                         // 今买 / 今卖与成交合计相符 (不符 = 两表之间又有成交, 其一旧了)
bool synced(const Keyed<Position> &pos, const Fills &fills);                        // 每只都对得上
bool covered(const Keyed<Position> &pos, const Fills &fills, const Quotes &quotes); // 每只都有行情

struct Row {
  std::string code, name;
  bool closed = false;
  const Quote *quote = nullptr;
  std::optional<double> market_value, pnl, pnl_pct, margin_value, sellable_value, collateral, cost;
  std::optional<double> day_pnl, day_pnl_pct;
  bool auction = false;
  std::optional<std::array<Slip, kSlipCols.size()>> slips;
  std::optional<std::array<double, kLevelCols.size()>> levels;
  std::optional<double> position_pct, weight;
};

struct Frame {
  std::vector<Row> rows;
  double market_value = 0, pnl = 0, day_pnl = 0;             // 合计 (分)
  std::array<std::optional<double>, kSlipCols.size()> slips; // 各列按占比加权
  bool synced = false;
  std::string time; // 行情最新时刻
};
// quotes 须 covered; 返回的 Row::quote 指向 quotes
Frame view(const Keyed<Position> &pos, const Fills &fills, const Quotes &quotes);

} // namespace live::board
