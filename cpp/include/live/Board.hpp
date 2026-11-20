// 实盘: 持仓页一帧 — 快照的持仓 × 当日成交 × 行情, 按代码对齐 (纯函数, 只出数字, 格式化归 UI)
//   三张表同以代码为键: 持仓 pos、成交合计 fills.by_code、行情 quotes; 对齐成 (h, f):
//     h 空 = 今日清仓 (持仓表已无此行或余额为 0, 只有当日列), f 空 = 今日该只没有成交
//   一行由几组列拼成, 缺了某组要的输入即整组空:
//     标识: 代码 / 名称 / 是否今日清仓 / 行情 (现价、盘口, 行情图也用);
//     持仓: 量、成本、券商盈亏取自持仓表, 随价变的按 Δ价 × 量 平移 (保留券商口径);
//     当日: 按成交价自算 (毛, 不扣费) = 现市值 − 昨收 × 昨仓 − Σ 方向 × 额, 昨仓 = 现量 − Σ 方向 × 量,
//           % 的分母为 昨收 × 昨仓 + 今买额; 与持仓的今买 / 今卖对不上即空;
//     盘口: 买卖各几额的滑点 (Slips, 镜像同形), 竞价或无参考价时空
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

struct Row {
  std::string code, name;
  bool closed = false;
  const Quote *quote = nullptr;
  std::optional<double> market_value, pnl, pnl_pct, margin_value, sellable_value, collateral, cost;
  std::optional<double> day_pnl, day_pnl_pct;
  bool auction = false;
  std::optional<Slips> slips;
  std::optional<double> position_pct, weight;
};

struct Frame {
  std::vector<Row> rows;
  double market_value = 0, pnl = 0, day_pnl = 0;                     // 合计 (分)
  Sides<std::array<std::optional<double>, kNotionals.size()>> slips; // [方向][额] 按占比加权
  bool synced = false;
  int sec = -1; // 行情最新时刻 (当日秒)
};

// 持仓 (余额 > 0) 在前、今日清仓在后; 有一只没行情 → 无; 两表市场不一致 → Fail. Row::quote 指向 quotes
std::optional<Frame> view(const Snapshot &s, const Quotes &quotes);
// 名单里未持有的一只: 标识 + 盘口 + 当日涨跌 (day_pnl_pct = 现价 / 昨收 − 1); 持仓组与当日盈亏额皆空. Row::quote 指向 q
Row quoted(const std::string &code, const std::string &name, const Quote &q);

} // namespace live::board
