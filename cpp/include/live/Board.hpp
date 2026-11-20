// 实盘: 持仓页一帧 — Snapshot (持仓 × 当日成交) × 行情, 按代码对齐 (纯函数, 只出数字)
//   每行: 标识 + 持仓 (随 Δ价 × 量平移, 保留券商口径) + 当日 (毛盈亏自算, 持仓与成交对不上即空) + 盘口 (买卖镜像三档滑点) + 占比
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
  double market_value = 0, pnl = 0, day_pnl = 0; // 合计 (分)
  Sides<std::array<std::optional<double>, kNotionals.size()>> slips;
  bool synced = false;
  int sec = -1; // 行情最新当日秒
};

// 持仓 (余额 > 0) 在前、今日清仓在后; 有一只没行情 → 无; 两表市场不一致 → Fail. Row::quote 指向 quotes
std::optional<Frame> view(const Snapshot &s, const Quotes &quotes);
// 名单里未持有的一只: 标识 + 盘口 + 当日涨跌
Row quoted(const std::string &code, const std::string &name, const Quote &q);

} // namespace live::board
