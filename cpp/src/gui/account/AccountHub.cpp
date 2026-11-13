#include "gui/account/AccountHub.hpp"

#include "api/sina/client.hpp"
#include "fundamental/Scan.hpp"
#include "fundamental/Store.hpp"

#include <algorithm>
#include <mutex>

namespace GUI::Account {

namespace {

using live::strf;

constexpr int kSinaPad = 10; // 新浪日 K 多取几根 (节假日估不准)

// 各证券在 [lo, hi] 的收盘: 本地 bar1d 有的取本地, 一行都没有的 (ETF / LOF / 本地库外) 取新浪日 K; 取消 → false
bool closes(const std::vector<std::string> &secs, misc::Ymd lo, misc::Ymd hi, const fundamental::Catalog &cat, const std::atomic<bool> &cancel,
            account::Closes &px) {
  fundamental::Ids ids(fundamental::Ids::Cover::Part);
  for (const std::string &s : secs)
    ids.add(s + live::kMktSuffix[static_cast<int>(live::market_of(s))]);
  fundamental::Scratch xs;
  for (const fundamental::Shard &sh : cat.shards(Quote::kBar1d)) {
    if (sh.ym < misc::ym_of(lo) || sh.ym > misc::ym_of(hi))
      continue;
    if (cancel.load(std::memory_order_relaxed))
      return false;
    Quote::each_daily(xs, sh, ids, [&](uint32_t a, const Quote::Candle &c) {
      if (c.close > 0 && c.date >= lo && c.date <= hi)
        px[secs[a]][c.date] = c.close;
    });
  }
  sina::Client cl(sina::kBarHost);
  const int n = static_cast<int>((misc::to_days(misc::today_ymd()) - misc::to_days(lo)).count() * 5 / 7) + kSinaPad;
  for (const std::string &s : secs) {
    if (px.contains(s))
      continue;
    if (cancel.load(std::memory_order_relaxed))
      return false;
    for (const sina::Bar &b : cl.daily(live::sina_sym(live::market_of(s), s), n))
      if (b.close > 0 && b.date >= lo && b.date <= hi)
        px[s][b.date] = b.close;
  }
  return true;
}

} // namespace

AccountHub::AccountHub() : pulled_(account::load().pulled) { Start(); }

bool AccountHub::run(AccountReq &req) {
  const account::Ledger l = account::load();
  auto h = std::make_shared<History>();
  h->gen = req.gen;
  if (!l.rows.empty()) {
    const misc::Ymd lo = l.rows.front().date, hi = std::min(req.fund, misc::add_days(l.pulled, -1));
    const fundamental::Catalog cat;
    std::vector<misc::Ymd> days;
    for (const misc::Ymd d : fundamental::calendar(cat, hi))
      if (d >= lo)
        days.push_back(d);
    account::Closes px;
    if (!closes(account::securities(l.rows), lo, hi, cat, cancel_, px))
      return false;
    h->days = account::replay(l.rows, l.rate, days, px);
    if (!h->days.empty())
      h->after = account::flow_after(l.rows, h->days.back().date);
  }
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(h);
  publish();
  return true;
}

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
  ledger_.fetch_add(1, std::memory_order_release);
}

void AccountHub::feed(const live::Got<live::Account> &a) { acct_ = a; }

std::shared_ptr<const Quote::Kline> AccountHub::line(misc::Ymd fund, misc::Ymd live) {
  if (!fund)
    return nullptr;
  sent_.send(*this, {0, ledger_.load(std::memory_order_acquire), fund});
  const std::shared_ptr<const History> h = latest(*this);
  const double net = acct_ ? acct_->net : 0;
  if (h == hist_ && net == net_ && live == live_)
    return line_;
  hist_ = h, net_ = net, live_ = live;
  line_ = nullptr;
  note_.clear();
  if (!h || h->days.empty())
    return line_;
  auto k = std::make_shared<Quote::Kline>();
  float ref = 1;
  const auto put = [&](misc::Ymd d, double idx) {
    const float x = static_cast<float>(idx);
    k->daily.push_back({Quote::cell(ref, std::max(ref, x), std::min(ref, x), x, 0, 0), d, ref});
    ref = x;
  };
  for (const account::Day &d : h->days)
    put(d.date, d.index);
  const account::Day &e = h->days.back();
  note_ = strf("账户\n资金流水逐日回放 %s ~ %s, 时间加权 (银证转账 / 担保品划入不计收益)", misc::dash(h->days.front().date).c_str(),
               misc::dash(e.date).c_str());
  if (live > e.date && net > 0 && e.nav > 0) {
    put(live, e.index * (net - h->after) / e.nav);
    note_ += "\n当日一根 = 信用账户净资产";
  }
  line_ = std::move(k);
  return line_;
}

} // namespace GUI::Account
