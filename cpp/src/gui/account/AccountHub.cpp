#include "gui/account/AccountHub.hpp"

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

// 今日成交 → 日内回推的成交 (槽至多到此刻的槽 last)
std::vector<account::Trade> trades_in(const live::Fills &f, int last) {
  std::vector<account::Trade> out;
  out.reserve(f.rows.size());
  for (const live::Fill &x : f.rows)
    out.push_back(trade_of(x, last));
  return out;
}

// 当日: 回放末日 (指数 idx0, 回放摘要 e) 之后, 持仓 h 按 y 的分钟收盘, 末槽 = h.now; 券商持仓时今日成交 f 按时刻回推 (与历史回放同一核 day_of)
std::shared_ptr<const Quote::Day> today_of(const Quote::Equity &e, float idx0, const Quote::Holdings &h, const live::Fills *f, const Quote::Live &y) {
  const size_t n = static_cast<size_t>(std::clamp(Quote::sec_slot(y.sec), 0, Quote::kSlots - 1)) + 1;
  account::Day d{y.date, h.now(&y), h.broker ? e.after : 0, 1, {}};
  std::unordered_map<std::string, uint32_t> id; // 代码 → 全局 id (资产轴上的)
  for (const Quote::Holding &x : h.list) {
    d.hold.push_back({x.code, x.qty, x.px});
    if (x.id != Quote::Ref::kNone)
      id.emplace(x.code, x.id);
  }
  const std::vector<account::Trade> trades = h.broker && f ? trades_in(*f, static_cast<int>(n) - 1) : std::vector<account::Trade>{};
  for (const account::Trade &t : trades)
    if (const Quote::Holding x = holding(t.code, 0, 0); x.id != Quote::Ref::kNone)
      id.emplace(t.code, x.id);
  return std::make_shared<const Quote::Day>(day_of(d, e.nav, idx0, trades, n, [&](const std::string &code) -> const Quote::Day * {
    const auto it = id.find(code);
    return it != id.end() ? y.at({Quote::kStock, it->second}) : nullptr;
  }));
}

// 持仓 (见 Quote::Holdings): 快照 s 的信用账户有净资产 → 券商持仓 (计价 = 券商现价) + 今日成交; 否则回放末日 (末日净值 ≤ 0 → 空)
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

// 账户行的悬停: 口径 (回放 se 的首末日) + 持仓 h (可空) + 累计盈亏
std::string note_of(const Quote::Series &se, const Quote::Equity &e, const Quote::Holdings *h) {
  const std::vector<Quote::Candle> &d = se.daily;
  std::string s = strf("账户\n资金流水逐日回放 %s ~ %s, 时间加权 (银证转账 / 担保品划入不计收益; 入金计日初, 出金计日末)", misc::dash(d.front().date).c_str(),
                       misc::dash(d.back().date).c_str());
  if (!h)
    return s;
  s += strf("\n持仓 = %s %zu 只 (▸ 展开 = 其中个股, 市值降序; 仓位 = 市值 / 净资产)", h->broker ? "券商实时持仓" : misc::dash(d.back().date).append(" 收后持仓").c_str(),
            h->list.size());
  for (const Quote::Holding &k : h->list)
    if (k.id == Quote::Ref::kNone)
      s += strf("\n• 库轴外 %s 仓位 %.1f%% (不列成分)", k.code.c_str(), 100 * h->weight(k));
  s += h->broker ? "\n当日 (接得上日脊) = 持仓 × 新浪分钟收, 今日成交按时刻回推 (费用按费率估), 末槽 = 净资产 + 各只自计价起的涨跌"
                 : "\n当日 (接得上日脊) = 持仓 × 新浪分钟收, 末槽 = 净资产 + 各只自计价起的涨跌";
  const double in = e.in + (h->broker ? e.after : 0);
  s += strf("\n累计盈亏 %+.0f = 净资产 %.0f − 累计净转入 %.0f (净资产计于%s)", h->net - in, h->net, in, h->broker ? "券商现价" : "末日收盘");
  return s;
}

// 快照里库轴外持仓 (ETF / LOF) 的券商现价, 记于今日
account::Spot spot_of(const live::Snapshot &s) {
  account::Spot o{.date = misc::today_ymd()};
  const AssetAxis &ax = asset_axis();
  for (const live::Position &p : s.pos.list)
    if (ax.find(live::asset_sym(p.mkt, p.code)) >= ax.size())
      o.px[p.code] = p.price;
  return o;
}

// 全部成交点 (一分钟一档): 历史 h (回放的, 今日不取; 可空) + 今日成交 f (可空)
Quote::Trades marks_of(const Quote::Trades *h, const live::Fills *f) {
  const misc::Ymd today = misc::today_ymd();
  Quote::Trades all;
  if (h)
    for (const Quote::TradeMark &m : h->marks)
      if (m.date < today)
        all.marks.push_back(m);
  if (f)
    for (const live::Fill &x : f->rows) {
      assert(x.sec >= 0);
      all.marks.push_back(mark_of(today, x));
    }
  return all;
}

// 成交点两档: 全部成交点各按档聚合; 无 → 空
Quote::TradeBins trades_of(const Quote::Trades &all) {
  Quote::TradeBins out;
  if (!all.marks.empty())
    for (int b = 0; b < Quote::kTradeBins; ++b)
      out[b] = std::make_shared<const Quote::Trades>(Quote::rebin(all, Quote::kBinSlots[b]));
  return out;
}

// 持仓成本: 持仓 h 的各只 × 本段买入 (开仓日 = 回放的 opens, 回放末日后才开的 = 今日; all = 一分钟一档的成交点); 本段无买入可算的只计进总仓位
std::shared_ptr<const Quote::Costs> costs_of(const Quote::Holdings &h, const Quote::Opens *opens, const Quote::Trades &all) {
  auto out = std::make_shared<Quote::Costs>();
  const misc::Ymd today = misc::today_ymd();
  for (const Quote::Holding &x : h.list) {
    out->total += h.weight(x);
    misc::Ymd open = today;
    if (opens)
      if (const auto o = opens->find(x.code); o != opens->end())
        open = o->second;
    Quote::Cost c{.code = x.code, .weight = h.weight(x)};
    for (const Quote::TradeMark &m : all.marks)
      if (m.date >= open)
        for (const Quote::TradeSec &s : m.secs)
          if (s.code == x.code && s.amount[live::kBuy] > 0)
            c.buys.push_back({m.date, m.slot, s.amount[live::kBuy]});
    if (!c.buys.empty())
      out->list.push_back(std::move(c));
  }
  return out;
}

} // namespace

void AccountHub::feed(const std::shared_ptr<const live::Snapshot> &snap) {
  fed_.update(snap.get(), [&](Fed &f) { f = {snap, snap ? spot_of(*snap) : account::Spot{}}; });
}

std::shared_ptr<const Quote::Day> AccountHub::today(const std::shared_ptr<const Quote::PriceView> &view, const std::shared_ptr<const Quote::Live> &live) {
  const std::shared_ptr<const Quote::Tape> t = view && view->lack.empty() ? view->tape[Quote::kAccount] : nullptr;
  const Fed &f = fed_.val;
  held_.update({t.get(), fed_.gen}, [&](Held &x) {
    x.tape = t;
    const Quote::Series *se = t ? t->line(0) : nullptr;
    std::shared_ptr<const Quote::Holdings> h = se ? holdings_of(view->equity, f.snap.get()) : nullptr;
    if (!(h && x.hold && *h == *x.hold))
      x.hold = std::move(h);
    x.note = se ? note_of(*se, view->equity, x.hold.get()) : "";
    const Quote::Trades all = marks_of(t ? view->equity.trades.get() : nullptr, f.snap ? &f.snap->fills : nullptr);
    x.trades = trades_of(all);
    x.costs = x.hold ? costs_of(*x.hold, view->equity.opens.get(), all) : nullptr;
  });
  const Held &x = held_.val;
  day_.update({held_.gen, live ? live->ver : 0}, [&](std::shared_ptr<const Quote::Day> &d) {
    d = x.hold && live && view->equity.nav > 0 && view->joins(live->prev)
            ? today_of(view->equity, x.tape->line(0)->daily.back().close, *x.hold, f.snap ? &f.snap->fills : nullptr, *live)
            : nullptr;
  });
  return day_.val;
}

} // namespace GUI::Account
