#include "live/Board.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>

namespace live::board {

namespace {

const char *mkt_name(Mkt m) { return kMktPrefix[static_cast<int>(m)]; }

void ident(Row &r, const Pair &p, const Quote &q) {
  r.code = p.sec().code;
  r.name = p.sec().name;
  r.closed = !p.h;
  r.quote = &q;
}

void hold(Row &r, const Pair &p, const Quote &q) {
  if (!p.h)
    return;
  const Position &h = *p.h;
  const double px = q.price, d = (px - h.price) * h.balance;
  r.market_value = h.market_value + d;
  r.pnl = h.pnl + d;
  if (h.cost > 0)
    r.pnl_pct = (px / h.cost - 1) * 100;
  if (h.financed)
    r.margin_value = *h.financed * px;
  r.sellable_value = h.available * px;
  r.collateral = h.collateral;
  r.cost = h.cost;
}

void today(Row &r, const Pair &p, const Quote &q) {
  if (!synced(p))
    return;
  const double bal = p.h ? p.h->balance : 0;
  double traded = 0, amount = 0, bought = 0;
  if (p.f) {
    for (const int s : kSides) {
      traded += s * p.f->traded[s];
      amount += s * p.f->amount[s];
    }
    bought = p.f->amount[kBuy];
  }
  const double base = (bal - traded) * q.last_close;
  const double day = bal * q.price - base - amount;
  const double den = base + bought;
  r.day_pnl = day;
  if (den)
    r.day_pnl_pct = day / den * 100;
}

void depth(Row &r, const Pair &p, const Quote &q) {
  if (!p.h)
    return;
  const auto x = ref(q);
  r.auction = auction(q);
  if (x && !r.auction)
    r.slips = slips(q, *x);
  if (x)
    r.levels = levels(*x);
}

// 券商口径的资产: 持仓表的 市值 / 仓位 反推, 再随总市值 mv 平移; 持仓表无仓位 → 无
std::optional<double> asset(const Keyed<Position> &pos, double mv) {
  const Position *top = nullptr;
  for (const Position &h : pos.list)
    if (!top || h.position_pct.value_or(0) > top->position_pct.value_or(0))
      top = &h;
  if (!top || !top->position_pct.value_or(0))
    return std::nullopt;
  double held = 0;
  for (const Position &h : pos.list)
    held += h.market_value;
  return top->market_value / *top->position_pct * 100 + mv - held;
}

std::optional<double> pct(const std::optional<double> &x, const std::optional<double> &den) {
  if (!x || !den || !*den)
    return std::nullopt;
  return *x / *den * 100;
}

// 第 i 列滑点按占比加权
std::optional<double> agg(const std::vector<Row> &rows, size_t i) {
  double num = 0, den = 0;
  for (const Row &r : rows)
    if (r.slips && (*r.slips)[i].pct && r.weight.value_or(0)) {
      num += *(*r.slips)[i].pct * *r.weight;
      den += *r.weight;
    }
  return den ? std::optional<double>(num / den) : std::nullopt;
}

double cents(double x) { return std::round(x * 100) / 100; }

} // namespace

std::vector<Pair> join(const Keyed<Position> &pos, const Fills &fills) {
  std::vector<Pair> out;
  for (const Position &h : pos.list)
    if (h.balance > 0)
      out.push_back({&h, fills.by_code.find(h.code)});
  for (const Flow &f : fills.by_code.list) {
    const Position *h = pos.find(f.code);
    if (!h || !(h->balance > 0))
      out.push_back({nullptr, &f});
  }
  std::string bad;
  for (const Pair &p : out)
    if (p.h && p.f && p.h->mkt != p.f->mkt)
      bad += strf(" %s %s (持仓 %s, 成交 %s)", p.h->code.c_str(), p.h->name.c_str(), mkt_name(p.h->mkt), mkt_name(p.f->mkt));
  if (!bad.empty())
    fail("成交与持仓的市场不一致:" + bad);
  return out;
}

bool synced(const Pair &p) {
  if (!p.h || !p.h->traded)
    return true;
  return *p.h->traded == (p.f ? p.f->traded : BySide{});
}

bool synced(const Keyed<Position> &pos, const Fills &fills) {
  for (const Pair &p : join(pos, fills))
    if (!synced(p))
      return false;
  return true;
}

bool covered(const Keyed<Position> &pos, const Fills &fills, const Quotes &quotes) {
  for (const Pair &p : join(pos, fills))
    if (!quotes.contains(p.sec().code))
      return false;
  return true;
}

Frame view(const Keyed<Position> &pos, const Fills &fills, const Quotes &quotes) {
  Frame fr;
  fr.synced = true;
  double mv = 0;
  for (const Pair &p : join(pos, fills)) {
    const auto it = quotes.find(p.sec().code);
    assert(it != quotes.end());
    const Quote &q = it->second;
    Row r;
    ident(r, p, q);
    hold(r, p, q);
    today(r, p, q);
    depth(r, p, q);
    mv += r.market_value.value_or(0);
    fr.synced = fr.synced && synced(p);
    fr.rows.push_back(std::move(r));
  }
  const std::optional<double> base = asset(pos, mv);
  for (Row &r : fr.rows) {
    r.position_pct = pct(r.market_value, base);
    r.weight = pct(r.market_value, mv);
  }
  for (const Row &r : fr.rows) {
    fr.market_value += r.market_value.value_or(0);
    fr.pnl += r.pnl.value_or(0);
    fr.day_pnl += r.day_pnl.value_or(0);
  }
  fr.market_value = cents(fr.market_value);
  fr.pnl = cents(fr.pnl);
  fr.day_pnl = cents(fr.day_pnl);
  for (size_t i = 0; i < kSlipCols.size(); ++i)
    fr.slips[i] = agg(fr.rows, i);
  for (const auto &[code, q] : quotes)
    fr.time = std::max(fr.time, quote_time(q));
  return fr;
}

} // namespace live::board
