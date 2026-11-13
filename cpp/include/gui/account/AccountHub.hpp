// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 (account/Ledger.hpp) → 时间加权指数 → Market 自选里的「账户」行
//   拉: Live 连上客户端、读到信用账户后每天自动一次, 走 Live 的事务道 (LiveHub::job): 重查账本末日起的资金流水并入账本,
//     流水累计的持仓 + 今日成交对上券商持仓才落盘
//   算: 后台线程按 账本 × 收盘 (本地 bar1d 不复权; 本地库没有的 ETF / LOF 取新浪日 K) 回放, 账本或日线水位变了即重算;
//     回放止于 min(日线水位, 拉于前一日)
//   线: Quote::Kline 日根 (前收 = 前一日指数, 收 = 指数), 与价格历史同形; 行情当日在回放末日之后即补一根
//     = 末日指数 × (信用账户净资产 − 末日之后账本里的外部进出) / 末日净值 (当日的外部进出不计)
#pragma once

#include "account/Ledger.hpp"
#include "gui/quote/Quote.hpp"
#include "gui/util/Service.hpp"
#include "live/Broker.hpp"
#include "misc/date.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Account {

struct AccountReq {
  uint64_t gen = 0;
  uint64_t ledger = 0; // 账本代 (落盘一次 +1)
  misc::Ymd fund = 0;  // 日线水位 (AssetInfo::quote_end)
  bool operator==(const AccountReq &) const = default;
};

// 一次回放
struct History {
  uint64_t gen = 0;
  misc::Ymd pulled = 0;
  std::vector<account::Day> days;
  double after = 0; // 回放末日之后账本里的外部进出
};

class AccountHub : public Service<AccountHub, AccountReq>, public Published {
public:
  static constexpr const char *kName = "account";
  static constexpr Phase kStart = Phase::Loading;

  AccountHub();
  ~AccountHub() { Stop(); }
  Progress &board() { return *this; }
  bool run(AccountReq &req);
  std::shared_ptr<const History> result; // 持 mutex 读写

  // ---------- Live 侧 ----------
  bool due() const;                                                         // GUI 线程: 今天还没拉过
  void tried();                                                             // GUI 线程: 今天的已发出
  void pull(live::Broker &b, const live::Log &log, const live::Live &live); // 事务线程: 拉 → 并 → 核 → 落盘; 失败 → Fail
  void feed(const live::Got<live::Account> &a);                             // GUI 线程每帧: 信用账户 (当日一根的源)

  // ---------- Market 侧 (GUI 线程每帧) ----------
  // fund = 日线水位 (0 = 未同步, 不算), live = 行情当日 (0 = 无) → 线; 空 = 还没有账本 / 还没算完 / 回放为空
  std::shared_ptr<const Quote::Kline> line(misc::Ymd fund, misc::Ymd live);
  const std::string &note() const { return note_; } // 线的悬停

private:
  std::atomic<uint64_t> ledger_{1};
  misc::Ymd pulled_ = 0; // 账本拉于 (含今天发出了的)
  Sent<AccountReq> sent_;
  live::Got<live::Account> acct_;
  std::shared_ptr<const History> hist_; // line_ 由这几样拼成
  std::shared_ptr<const live::Account> acct_used_;
  misc::Ymd live_used_ = 0;
  std::shared_ptr<const Quote::Kline> line_;
  std::string note_;
};

} // namespace GUI::Account
