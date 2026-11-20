// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 → 行情的账户一侧 (Quote::kAccount)
#pragma once

#include "account/Ledger.hpp"
#include "gui/quote/Quote.hpp"
#include "live/Broker.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Account {

class AccountHub {
public:
  // ---------- Live 侧 (GUI 线程每帧) ----------
  void feed(const std::shared_ptr<const live::Snapshot> &snap); // 整轮原子快照 (空 = 未刷过)
  account::Spot spot() const;                                   // 库轴外持仓的券商现价

  // ---------- 行情侧 (GUI 线程每帧) ----------
  std::shared_ptr<const Quote::Day> today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live);
  const std::string &note() const { return note_; }
  const std::shared_ptr<const Quote::Holdings> &holdings() const { return hold_; }
  const std::shared_ptr<const Quote::Trades> &trades() const { return trades_; }

private:
  void rehold(const Quote::PriceView *view);

  std::shared_ptr<const live::Snapshot> snap_;
  std::shared_ptr<const Quote::Tape> tape_;
  std::shared_ptr<const live::Snapshot> shown_snap_;
  uint64_t ver_ = 0;
  std::shared_ptr<const Quote::Holdings> hold_;
  std::shared_ptr<const Quote::Trades> trades_;
  std::shared_ptr<const Quote::Day> day_;
  std::string note_;
};

} // namespace GUI::Account
