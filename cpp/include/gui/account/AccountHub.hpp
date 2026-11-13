// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 (account/Ledger.hpp) → 行情的账户一侧 (Quote::kAccount)
//   拉: Live 连上客户端、读到信用账户后每天自动一次, 走 Live 的事务道 (LiveHub::job): 重查账本末日起的资金流水并入账本,
//     流水累计的持仓 + 今日成交对上券商持仓才落盘 (账本代 +1)
//   历史: 价格库按账本在日脊上回放 (PriceView::tape[kAccount]; 时间加权指数, 分钟频); 库轴外的持仓以券商现价为今日的观测点 (spot → QuoteHub)
//   持仓 (Quote::Holdings; Market 账户行的成分 / 当日四列): 券商实时持仓 (信用账户有净资产) 优先, 否则回放末日收后持仓; 今日成交额取券商成交
//   当日: 接在回放末日之后才有 (QuoteHub 每帧取, 并进 Live::days[kAccount]); 与历史同一核 (AccountSide.hpp day_of): 当日收后持仓 = 上面的持仓,
//     末槽 = 此刻净资产 (Holdings::now), 外部进出 = 券商持仓时取末日之后账本里的 (回放持仓 = 0); 全市场外 / 当日无成交的持仓不动; 当日的外部进出不计
#pragma once

#include "account/Ledger.hpp" // Spot
#include "gui/quote/Quote.hpp"
#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Account {

class AccountHub {
public:
  AccountHub();

  // ---------- Live 侧 ----------
  bool due() const;                                                              // GUI 线程: 今天还没拉过
  void tried();                                                                  // GUI 线程: 今天的已交给事务道
  void pull(live::Broker &b, const live::Log &log, const live::Live &live);      // 事务线程: 拉 → 并 → 核 → 落盘; 失败 → Fail
  void feed(const live::Got<live::Account> &a, const live::Got<live::Fills> &f); // GUI 线程每帧: 信用账户 + 今日成交 (持仓的源)
  account::Spot spot() const;                                                    // 库轴外持仓 (ETF / LOF) 的券商现价, 记于今日 (无账户 → 空)

  // ---------- 行情侧 (GUI 线程每帧) ----------
  // 账户的当日 (空 = 无; QuoteHub 取); view / live 可空
  std::shared_ptr<const Quote::Day> today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live);
  const std::string &note() const { return note_; }                                // 账户行的悬停 (口径 + 持仓 + 累计盈亏; 随 today)
  const std::shared_ptr<const Quote::Holdings> &holdings() const { return hold_; } // 持仓 (空 = 无账户全史; 随 today, 内容不变不换)

private:
  void rehold(const Quote::PriceView *view); // 回放 / 信用账户 / 成交变了: 持仓 + 悬停 (有账户全史 ⇒ view 非空)

  misc::Ymd pulled_ = 0; // 账本拉于 (含今天已交出的)
  live::Got<live::Account> acct_;
  live::Got<live::Fills> fills_;
  // 持仓由前三样拼成, 当日再加新浪的一份; 变了才重拼
  std::shared_ptr<const Quote::Tape> tape_;
  std::shared_ptr<const live::Account> at_;
  std::shared_ptr<const live::Fills> fl_;
  uint64_t ver_ = 0; // Live::ver
  std::shared_ptr<const Quote::Holdings> hold_;
  std::shared_ptr<const Quote::Day> day_;
  std::string note_;
};

} // namespace GUI::Account
