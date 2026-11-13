#include "gui/account/AccountHub.hpp"

#include "account/Ledger.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/account/AccountSide.hpp"

#include <algorithm>
#include <cassert>
#include <functional>
#include <unordered_map>
#include <vector>

namespace GUI::Account {

namespace {

using live::strf;

// 当日: 回放末日 (指数 idx0, 回放摘要 e) 之后, 持仓 h 按 y 的分钟收盘, 末槽 = h.now (与历史回放同一核 day_of)
std::shared_ptr<const Quote::Day> today_of(const Quote::Equity &e, float idx0, const Quote::Holdings &h, const Quote::Live &y) {
  const size_t n = static_cast<size_t>(std::clamp(Quote::sec_slot(y.sec), 0, Quote::kSlots - 1)) + 1;
  account::Day d{y.date, h.now(&y), h.broker ? e.after : 0, 1, {}};
  std::unordered_map<std::string, uint32_t> id; // 代码 → 全局 id (资产轴上的)
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

// 持仓 (见 Quote::Holdings): 信用账户 a 有净资产 → 券商持仓 (计价 = 券商现价) + 今日成交 f; 否则回放末日 (末日净值 ≤ 0 → 空)
std::shared_ptr<const Quote::Holdings> holdings_of(const Quote::Equity &e, const live::Account *a, const live::Fills *f) {
  auto h = std::make_shared<Quote::Holdings>();
  if (a && a->net > 0) {
    h->broker = true, h->net = a->net;
    for (const auto &[code, q] : a->bal)
      if (q != 0) {
        const auto p = a->px.find(code);
        assert(p != a->px.end() && "信用账户: 持仓缺现价");
        h->list.push_back(holding(code, q, p->second));
      }
    if (f)
      for (const live::Flow &x : f->by_code.list)
        h->amount += x.amount[live::kBuy] + x.amount[live::kSell], h->buy += x.amount[live::kBuy];
  } else if (e.nav > 0) {
    h->net = e.nav, h->list = e.hold;
  } else {
    return nullptr;
  }
  std::ranges::stable_sort(h->list, std::greater{}, [](const Quote::Holding &x) { return x.qty * x.px; });
  return h;
}

} // namespace

AccountHub::AccountHub() : pulled_(account::load().pulled) {}

bool AccountHub::due() const { return pulled_ < misc::today_ymd(); }

void AccountHub::tried() { pulled_ = misc::today_ymd(); }

void AccountHub::pull(live::Broker &b, const live::Log &log, const live::Live &live) {
  account::Ledger l = account::load();
  const misc::Ymd from = l.rows.empty() ? 0 : l.rows.back().date;
  std::vector<live::CashFlow> fresh = b.pull_flows(from, log, live);
  const size_t n = fresh.size();
  l.rows = account::merge(l.rows, from, std::move(fresh));
  live("刷新持仓, 核对资金流水…");
  b.sync();
  const std::shared_ptr<const live::State> s = b.state();
  live::need(s->pos && s->fills && s->acct, "持仓 / 成交 / 信用账户没读到");
  const std::string off = account::reconcile(l.rows, *s->pos, *s->fills);
  live::need(off.empty(), "资金流水累计的持仓 + 今日成交对不上券商持仓 (代码 流水/券商): " + off);
  l.pulled = misc::today_ymd();
  l.rate = s->acct->rate;
  account::save(l);
  log("flows", {{"rows", l.rows.size()}, {"from", from}, {"fresh", n}});
}

void AccountHub::feed(const live::Got<live::Account> &a, const live::Got<live::Fills> &f) { acct_ = a, fills_ = f; }

account::Spot AccountHub::spot() const {
  account::Spot s;
  if (!acct_)
    return s;
  s.date = misc::today_ymd();
  const AssetAxis &ax = asset_axis();
  for (const auto &[code, p] : acct_->px)
    if (ax.find(account::asset_code(code)) >= ax.size())
      s.px[code] = p;
  return s;
}

std::shared_ptr<const Quote::Day> AccountHub::today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live) {
  const std::shared_ptr<const Quote::Tape> t = view && view->lack.empty() ? view->tape[Quote::kAccount] : nullptr;
  const uint64_t ver = live ? live->ver : 0;
  if (t == tape_ && acct_.v == at_ && fills_.v == fl_ && ver == ver_)
    return day_;
  if (t != tape_ || acct_.v != at_ || fills_.v != fl_) {
    tape_ = t, at_ = acct_.v, fl_ = fills_.v;
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
  std::shared_ptr<const Quote::Holdings> h = holdings_of(e, acct_.v.get(), fills_.v.get());
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
