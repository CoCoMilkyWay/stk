#include "gui/account/AccountHub.hpp"

#include "account/Ledger.hpp"
#include "fundamental/AssetAxis.hpp"

#include <algorithm>
#include <array>

namespace GUI::Account {

namespace {

using live::strf;

// 当日一根: 回放末日 (指数 idx / 净值 nav, 其后外部进出 after) 之后, 净资产 a.net × 持仓 a.bal 按 y 的分钟收盘
std::shared_ptr<const Quote::Day> today_of(const Quote::Book &bk, const live::Account &a, const Quote::Live &y) {
  const Quote::Candle &e = bk.se->daily.back();
  const int cur = std::clamp(Quote::sec_slot(y.sec), 0, Quote::kSlots - 1);
  std::array<double, Quote::kSlots> nav;
  nav.fill(a.net);
  const AssetAxis &ax = asset_axis();
  for (const auto &[code, q] : a.bal) {
    const size_t id = ax.find(code + live::kMktSuffix[static_cast<int>(live::market_of(code))]);
    const Quote::Day *d = id < ax.size() ? y.at({Quote::kStock, static_cast<uint32_t>(id)}) : nullptr;
    if (!d)
      continue;
    float c = d->candle.ref;
    for (size_t s = 0; s <= static_cast<size_t>(cur); ++s) {
      if (d->bars.has[s])
        c = d->bars.cell[s].close;
      nav[s] += q * (c - d->candle.close);
    }
  }
  auto o = std::make_shared<Quote::Day>();
  o->prev = y.prev;
  o->bars.date = y.date;
  float hi = e.close, lo = e.close, x = e.close;
  for (int s = 0; s <= cur; ++s) {
    x = static_cast<float>(e.close * account::growth(bk.nav, nav[static_cast<size_t>(s)], bk.after));
    o->bars.add(s, {x, x, x, x, 0, 0});
    hi = std::max(hi, x), lo = std::min(lo, x);
  }
  o->candle = {Quote::cell(e.close, hi, lo, x, 0, 0), y.date, e.close};
  return o;
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

void AccountHub::feed(const live::Got<live::Account> &a) { acct_ = a; }

account::Spot AccountHub::spot() const {
  account::Spot s;
  if (!acct_)
    return s;
  s.date = misc::today_ymd();
  const AssetAxis &ax = asset_axis();
  for (const auto &[code, p] : acct_->px)
    if (ax.find(code + live::kMktSuffix[static_cast<int>(live::market_of(code))]) >= ax.size())
      s.px[code] = p;
  return s;
}

Line AccountHub::line(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live) {
  const std::shared_ptr<const Quote::Book> bk = view ? view->book : nullptr;
  const uint64_t ver = live ? live->ver : 0;
  if (bk == book_ && acct_.v == at_ && ver == ver_)
    return line_;
  book_ = bk, at_ = acct_.v, ver_ = ver;
  line_ = {};
  note_.clear();
  if (!bk || !bk->se)
    return line_;
  line_.se = bk->se;
  const std::vector<Quote::Candle> &d = bk->se->daily;
  note_ = strf("账户\n资金流水逐日回放 %s ~ %s, 时间加权 (银证转账 / 担保品划入不计收益; 入金计日初, 出金计日末)", misc::dash(d.front().date).c_str(),
               misc::dash(d.back().date).c_str());
  double net = bk->nav, in = bk->in;
  if (live && acct_ && acct_->net > 0 && bk->nav > 0 && view->joins(live->prev)) {
    line_.today = today_of(*bk, *acct_, *live);
    note_ += "\n当日 = 信用账户净资产 × 持仓分钟收盘";
    net = acct_->net, in += bk->after;
  }
  note_ += strf("\n累计盈亏 %+.0f = 净资产 %.0f − 累计净转入 %.0f", net - in, net, in);
  return line_;
}

} // namespace GUI::Account
