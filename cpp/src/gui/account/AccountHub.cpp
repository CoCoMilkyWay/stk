#include "gui/account/AccountHub.hpp"

#include "account/Ledger.hpp"
#include "fundamental/AssetAxis.hpp"

#include <algorithm>
#include <span>
#include <vector>

namespace GUI::Account {

namespace {

using live::strf;

// 当日: 回放末日 (指数 idx0, 账户回放 t) 之后, 净资产 a.net × 持仓 a.bal 按 y 的分钟收盘 (与历史回放同一份 account::intraday)
std::shared_ptr<const Quote::Day> today_of(const Quote::Tape &t, float idx0, const live::Account &a, const Quote::Live &y) {
  const size_t n = static_cast<size_t>(std::clamp(Quote::sec_slot(y.sec), 0, Quote::kSlots - 1)) + 1;
  const account::Day d{y.date, a.net, t.after, 1, {a.bal.begin(), a.bal.end()}};
  const AssetAxis &ax = asset_axis();
  std::vector<float> p(n);
  const auto path = [&](const std::string &code) -> std::span<const float> {
    const size_t id = ax.find(account::asset_code(code));
    const Quote::Day *s = id < ax.size() ? y.at({Quote::kStock, static_cast<uint32_t>(id)}) : nullptr;
    if (!s)
      return {};
    float c = s->candle.ref;
    for (size_t k = 0; k < n; ++k)
      p[k] = c = s->bars.has[k] ? s->bars.cell[k].close : c;
    return p;
  };
  std::vector<double> idx(n);
  account::intraday(d, t.nav, idx0, path, idx);
  auto o = std::make_shared<Quote::Day>();
  float hi = idx0, lo = idx0, prev = idx0;
  for (size_t s = 0; s < n; ++s) {
    const float x = static_cast<float>(idx[s]);
    o->bars.add(static_cast<int>(s), Quote::cell(prev, std::max(prev, x), std::min(prev, x), x, 0));
    hi = std::max(hi, x), lo = std::min(lo, x), prev = x;
  }
  o->candle = {Quote::cell(idx0, hi, lo, prev, 0), y.date, idx0};
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
    if (ax.find(account::asset_code(code)) >= ax.size())
      s.px[code] = p;
  return s;
}

std::shared_ptr<const Quote::Day> AccountHub::today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live) {
  const std::shared_ptr<const Quote::Tape> t = view && view->lack.empty() ? view->tape[Quote::kAccount] : nullptr;
  const uint64_t ver = live ? live->ver : 0;
  if (t == tape_ && acct_.v == at_ && ver == ver_)
    return day_;
  tape_ = t, at_ = acct_.v, ver_ = ver;
  day_ = nullptr;
  note_.clear();
  const Quote::Series *se = t ? t->line(0) : nullptr;
  if (!se)
    return day_;
  const std::vector<Quote::Candle> &d = se->daily;
  note_ = strf("账户\n资金流水逐日回放 %s ~ %s, 时间加权 (银证转账 / 担保品划入不计收益; 入金计日初, 出金计日末)", misc::dash(d.front().date).c_str(),
               misc::dash(d.back().date).c_str());
  double net = t->nav, in = t->in;
  if (live && acct_ && acct_->net > 0 && t->nav > 0 && view->joins(live->prev)) {
    day_ = today_of(*t, d.back().close, *acct_, *live);
    note_ += "\n当日 = 信用账户净资产 × 持仓分钟收盘";
    net = acct_->net, in += t->after;
  }
  note_ += strf("\n累计盈亏 %+.0f = 净资产 %.0f − 累计净转入 %.0f", net - in, net, in);
  return day_;
}

} // namespace GUI::Account
