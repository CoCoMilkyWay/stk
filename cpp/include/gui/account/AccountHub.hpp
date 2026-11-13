// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 (account/Ledger.hpp) → Market 自选里的「账户」行
//   拉: Live 连上客户端、读到信用账户后每天自动一次, 走 Live 的事务道 (LiveHub::job): 重查账本末日起的资金流水并入账本,
//     流水累计的持仓 + 今日成交对上券商持仓才落盘 (账本代 +1)
//   历史: 价格库按账本在日脊上回放 (PriceView::book; 时间加权指数, 分钟频)
//   当日: 接在回放末日之后才有; 一根分钟线 = 末日指数 × (净值_s − 末日之后账本里的外部进出) / 末日净值,
//     净值_s = 信用账户净资产 + Σ 持仓 × (当日分钟收_s − 现价) (全市场外的持仓不动; 当日的外部进出不计)
#pragma once

#include "gui/quote/Quote.hpp"
#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Account {

// 账户行的源: 全史 (空 = 无账本 / 回放为空) + 当日 (空 = 无)
struct Line {
  std::shared_ptr<const Quote::Series> se;
  std::shared_ptr<const Quote::Day> today;
  bool operator==(const Line &) const = default;
};

class AccountHub {
public:
  AccountHub();

  // ---------- Live 侧 ----------
  bool due() const;                                                         // GUI 线程: 今天还没拉过
  void tried();                                                             // GUI 线程: 今天的已交给事务道
  void pull(live::Broker &b, const live::Log &log, const live::Live &live); // 事务线程: 拉 → 并 → 核 → 落盘; 失败 → Fail
  void feed(const live::Got<live::Account> &a);                             // GUI 线程每帧: 信用账户 (当日的源)

  // ---------- Market 侧 (GUI 线程每帧) ----------
  Line line(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live);
  const std::string &note() const { return note_; } // 线的悬停 (不含金额)

private:
  misc::Ymd pulled_ = 0; // 账本拉于 (含今天已交出的)
  live::Got<live::Account> acct_;
  // 线由这三样拼成, 变了才重拼
  std::shared_ptr<const Quote::Book> book_;
  std::shared_ptr<const live::Account> at_;
  uint64_t ver_ = 0; // Live::ver
  Line line_;
  std::string note_;
};

} // namespace GUI::Account
