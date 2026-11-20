#include "gui/account/AccountHub.hpp"

#include "account/Ledger.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/account/AccountSide.hpp"

#include <algorithm>
#include <cassert>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace GUI::Account {

namespace {

using live::strf;

std::shared_ptr<const Quote::Day> today_of(const Quote::Equity &e, float idx0, const Quote::Holdings &h, const Quote::Live &y) {
  const size_t n = static_cast<size_t>(std::clamp(Quote::sec_slot(y.sec), 0, Quote::kSlots - 1)) + 1;
  account::Day d{y.date, h.now(&y), h.broker ? e.after : 0, 1, {}};
  std::unordered_map<std::string, uint32_t> id;
  for (const Quote::Holding &x : h.list) {
    d.hold.push_back({x.code, x.qty, x.px});
    if (x.id != Quote::Ref::kNone)
      id.emplace(x.code, x.id);
  }
  return std::make_shared<const Quote::Day>(day_of(d, e.nav, idx0, n, [&](const std::string &code) -> const Quote::Day * {
    const auto it = id.find(code);
    return it != id.end() ? y.at({Quote::kStock, it->second}) : nullptr;
  }));
}

std::shared_ptr<const Quote::Holdings> holdings_of(const Quote::Equity &e, const live::Snapshot *s) {
  auto h = std::make_shared<Quote::Holdings>();
  if (s && s->acct.net > 0) {
    h->broker = true, h->net = s->acct.net;
    for (const live::Position &p : s->pos.list)
      if (p.balance != 0)
        h->list.push_back(holding(p.code, static_cast<double>(p.balance), p.price));
    for (const live::Traded &x : s->fills.by_code.list)
      h->amount += x.amount[live::kBuy] + x.amount[live::kSell], h->buy += x.amount[live::kBuy];
  } else if (e.nav > 0) {
    h->net = e.nav, h->list = e.hold;
  } else {
    return nullptr;
  }
  std::ranges::stable_sort(h->list, std::greater{}, [](const Quote::Holding &x) { return x.qty * x.px; });
  return h;
}

std::shared_ptr<const Quote::Trades> trades_of(const live::Fills &f) {
  std::map<int, Quote::TradeMark> by;
  for (const live::Fill &x : f.rows) {
    assert(x.sec >= 0);
    Quote::TradeMark &m = by[std::clamp(Quote::sec_slot(x.sec), 0, Quote::kSlots - 1)];
    auto it = std::ranges::find(m.secs, x.code, &Quote::TradeSec::code);
    if (it == m.secs.end())
      it = m.secs.insert(it, Quote::TradeSec{.code = x.code, .name = x.name});
    const bool buy = x.side == live::kBuy;
    (buy ? m.buy : m.sell) += x.amount;
    (buy ? it->buy : it->sell) += x.amount;
    (buy ? it->buy_qty : it->sell_qty) += static_cast<double>(x.qty);
  }
  auto t = std::make_shared<Quote::Trades>();
  t->date = misc::today_ymd();
  for (auto &[s, m] : by) {
    m.slot = s;
    t->marks.push_back(std::move(m));
  }
  return t;
}

} // namespace

void AccountHub::feed(const std::shared_ptr<const live::Snapshot> &snap) {
  if (snap == snap_)
    return;
  snap_ = snap;
  trades_ = snap_ ? trades_of(snap_->fills) : nullptr;
}

account::Spot AccountHub::spot() const {
  account::Spot s;
  if (!snap_)
    return s;
  s.date = misc::today_ymd();
  const AssetAxis &ax = asset_axis();
  for (const live::Position &p : snap_->pos.list)
    if (ax.find(account::asset_code(p.code)) >= ax.size())
      s.px[p.code] = p.price;
  return s;
}

std::shared_ptr<const Quote::Day> AccountHub::today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live) {
  const std::shared_ptr<const Quote::Tape> t = view && view->lack.empty() ? view->tape[Quote::kAccount] : nullptr;
  const uint64_t ver = live ? live->ver : 0;
  if (t == tape_ && snap_ == shown_snap_ && ver == ver_)
    return day_;
  if (t != tape_ || snap_ != shown_snap_) {
    tape_ = t, shown_snap_ = snap_;
    rehold(view.get());
  }
  ver_ = ver;
  day_ = nullptr;
  if (hold_ && live && view->equity.nav > 0 && view->joins(live->prev))
    day_ = today_of(view->equity, t->line(0)->daily.back().close, *hold_, *live);
  return day_;
}

void AccountHub::rehold(const Quote::PriceView *view) {
  note_.clear();
  const Quote::Series *se = tape_ ? tape_->line(0) : nullptr;
  if (!se) {
    hold_ = nullptr;
    return;
  }
  const std::vector<Quote::Candle> &d = se->daily;
  const Quote::Equity &e = view->equity;
  note_ = strf("账户\n资金流水逐日回放 %s ~ %s, 时间加权 (银证转账 / 担保品划入不计收益; 入金计日初, 出金计日末)", misc::dash(d.front().date).c_str(),
               misc::dash(d.back().date).c_str());
  std::shared_ptr<const Quote::Holdings> h = holdings_of(e, snap_.get());
  if (!(h && hold_ && *h == *hold_))
    hold_ = std::move(h);
  if (!hold_)
    return;
  const Quote::Holdings &x = *hold_;
  note_ += strf("\n持仓 = %s %zu 只 (▸ 展开 = 其中个股, 市值降序; 仓位 = 市值 / 净资产)", x.broker ? "券商实时持仓" : misc::dash(d.back().date).append(" 收后持仓").c_str(),
                x.list.size());
  for (const Quote::Holding &k : x.list)
    if (k.id == Quote::Ref::kNone)
      note_ += strf("\n• 库轴外 %s 仓位 %.1f%% (不列成分)", k.code.c_str(), 100 * x.weight(k));
  note_ += "\n当日 (接得上日脊) = 持仓 × 新浪分钟收, 末槽 = 净资产 + 各只自计价起的涨跌";
  const double in = e.in + (x.broker ? e.after : 0);
  note_ += strf("\n累计盈亏 %+.0f = 净资产 %.0f − 累计净转入 %.0f (净资产计于%s)", x.net - in, x.net, in, x.broker ? "券商现价" : "末日收盘");
}

} // namespace GUI::Account
