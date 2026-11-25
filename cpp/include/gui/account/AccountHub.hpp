// 账户净值的中枢 (gui/Tasks.cpp 建, Live 与 Market 共用): 资金流水账本 (live/Flows.hpp) 的回放 (account/Nav.hpp) → 行情的账户一侧 (Quote::kAccount)
//   进: Live 每帧交快照 (feed); 出: 行情经 QuoteHub::AccountFeed 取当日与券商现价, Market 取持仓 / 悬停 / 成交点
//   拉: 归 Live 的事务道 (LiveHub, live::flows::pull), 与两融条款同形; 落盘即账本代 +1, 价格库随代重放
//   历史: 价格库按账本在日脊上回放 (PriceView::tape[kAccount]; 时间加权指数, 分钟频); 库轴外的持仓以券商现价为今日的观测点 (spot, 随快照换)
//   持仓 (Quote::Holdings; Market 账户行的成分 / 当日四列): 券商实时持仓 (快照的信用账户有净资产) 优先, 否则回放末日收后持仓; 今日成交额取券商成交
//   成交点 (Quote::Trades; NAV 日内栏 / 主图的账户线): 回放的历史成交点 (今日前) + 券商今日成交, 按两档 (Quote::TradeBin: 5 / 30 分钟) 各聚合一份
//   持仓成本 (Quote::Costs; NAV 账户线的成本线 / 仓位色带): 上面持仓的各只 × 本段 (回放的开仓日起, 回放里没有的 = 今日新开) 的买入 (历史成交 + 今日成交), 仓位取现在的
//   当日: 接在回放末日之后才有 (QuoteHub 每帧取, 并进 Live::days[kAccount]); 与历史同一核 (AccountSide.hpp day_of): 当日收后持仓 = 上面的持仓,
//     末槽 = 此刻净资产 (Holdings::now), 外部进出 = 券商持仓时取末日之后账本里的 (回放持仓 = 0); 全市场外 / 当日无成交的持仓不动; 当日的外部进出不计;
//     券商持仓时今日成交 (同一份快照) 按时刻回推: 槽 s 之后的成交退回 (股与现金腿, 费用按费率估), 卖光的票卖出前、新买的票买入前都按实际持有计
//   三级键控缓存 (GUI::Stage, 下级键含上级 gen): 快照 → 现价; (回放, 快照) → 持仓 / 悬停 / 成交点 / 持仓成本; (持仓, 新浪的当日) → 账户的当日
#pragma once

#include "account/Nav.hpp" // Spot
#include "gui/quote/Quote.hpp"
#include "gui/util/Stage.hpp"
#include "live/Broker.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace GUI::Account {

class AccountHub {
public:
  // ---------- Live 侧 (GUI 线程每帧) ----------
  void feed(const std::shared_ptr<const live::Snapshot> &snap); // 快照 (持仓 / 今日成交 / 信用账户同一轮; 空 = 还没成功刷过)

  // ---------- 行情侧 (GUI 线程每帧, QuoteHub::frame 取) ----------
  const account::Spot &spot() const { return fed_.val.spot; } // 库轴外持仓 (ETF / LOF) 的券商现价, 记于今日 (无快照 → 空; 随 feed)
  // 账户的当日 (空 = 无); view / live 可空
  std::shared_ptr<const Quote::Day> today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live);

  // ---------- Market 侧 ----------
  const std::string &note() const { return held_.val.note; }                                // 账户行的悬停 (口径 + 持仓 + 累计盈亏; 随 today)
  const std::shared_ptr<const Quote::Holdings> &holdings() const { return held_.val.hold; } // 持仓 (空 = 无账户全史; 随 today, 内容不变不换)
  // 成交点两档 (日内栏 / 主图; 各空 = 无历史成交也未读到今日成交; 随 today)
  const Quote::TradeBins &trades() const { return held_.val.trades; }
  // 持仓成本 (NAV 账户线的成本线 / 仓位色带; 空 = 无持仓; 随 today)
  const std::shared_ptr<const Quote::Costs> &costs() const { return held_.val.costs; }

private:
  struct Fed {
    std::shared_ptr<const live::Snapshot> snap;
    account::Spot spot;
  };
  struct Held {
    std::shared_ptr<const Quote::Tape> tape; // 账户全史 (空 = 无)
    std::shared_ptr<const Quote::Holdings> hold;
    std::string note;
    Quote::TradeBins trades;
    std::shared_ptr<const Quote::Costs> costs;
  };
  Stage<const void *, Fed> fed_;                                                // 快照 →
  Stage<std::pair<const void *, uint64_t>, Held> held_;                         // (回放, fed_ 的 gen) →
  Stage<std::pair<uint64_t, uint64_t>, std::shared_ptr<const Quote::Day>> day_; // (held_ 的 gen, Live::ver) →
};

} // namespace GUI::Account
