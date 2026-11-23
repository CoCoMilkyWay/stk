// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 (account/Ledger.hpp) → 行情的账户一侧 (Quote::kAccount)
//   拉: 归 Live 的事务道 (LiveHub, account::pull), 与两融条款同形; 落盘即账本代 +1, 这里随代重放
//   历史: 价格库按账本在日脊上回放 (PriceView::tape[kAccount]; 时间加权指数, 分钟频); 库轴外的持仓以券商现价为今日的观测点 (spot → QuoteHub)
//   持仓 (Quote::Holdings; Market 账户行的成分 / 当日四列): 券商实时持仓 (快照的信用账户有净资产) 优先, 否则回放末日收后持仓; 今日成交额取券商成交
//   成交点 (Quote::Trades; NAV 日内栏的账户线): 券商今日成交按分钟聚合 (一分钟一点, 点内同只合并)
//   当日: 接在回放末日之后才有 (QuoteHub 每帧取, 并进 Live::days[kAccount]); 与历史同一核 (AccountSide.hpp day_of): 当日收后持仓 = 上面的持仓,
//     末槽 = 此刻净资产 (Holdings::now), 外部进出 = 券商持仓时取末日之后账本里的 (回放持仓 = 0); 全市场外 / 当日无成交的持仓不动; 当日的外部进出不计;
//     券商持仓时今日成交 (同一份快照) 按时刻回推: 槽 s 之后的成交退回 (股与现金腿, 费用按费率估), 卖光的票卖出前、新买的票买入前都按实际持有计
#pragma once

#include "account/Ledger.hpp" // Spot
#include "gui/quote/Quote.hpp"
#include "live/Broker.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Account {

class AccountHub {
public:
  // ---------- Live 侧 (GUI 线程每帧) ----------
  void feed(const std::shared_ptr<const live::Snapshot> &snap); // 快照 (持仓 / 今日成交 / 信用账户同一轮; 空 = 还没成功刷过)
  account::Spot spot() const;                                   // 库轴外持仓 (ETF / LOF) 的券商现价, 记于今日 (无快照 → 空)

  // ---------- 行情侧 (GUI 线程每帧) ----------
  // 账户的当日 (空 = 无; QuoteHub 取); view / live 可空
  std::shared_ptr<const Quote::Day> today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live);
  const std::string &note() const { return note_; }                                // 账户行的悬停 (口径 + 持仓 + 累计盈亏; 随 today)
  const std::shared_ptr<const Quote::Holdings> &holdings() const { return hold_; } // 持仓 (空 = 无账户全史; 随 today, 内容不变不换)
  const std::shared_ptr<const Quote::Trades> &trades() const { return trades_; }   // 今日成交点 (NAV 日内栏; 空 = 未读到成交; 随 feed, 快照变才换)

private:
  void rehold(const Quote::PriceView *view); // 回放 / 快照变了: 持仓 + 悬停 (有账户全史 ⇒ view 非空)

  std::shared_ptr<const live::Snapshot> snap_;
  // 持仓由回放与快照拼成, 当日再加新浪的一份; 变了才重拼
  std::shared_ptr<const Quote::Tape> tape_;
  std::shared_ptr<const live::Snapshot> shown_snap_; // 持仓拼自哪份快照
  uint64_t ver_ = 0;                                 // Live::ver
  std::shared_ptr<const Quote::Holdings> hold_;
  std::shared_ptr<const Quote::Trades> trades_;
  std::shared_ptr<const Quote::Day> day_;
  std::string note_;
};

} // namespace GUI::Account
