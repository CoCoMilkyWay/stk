#include "live/Board.hpp"

#include <algorithm>
#include <cmath>

namespace live::board {

namespace {

struct Pair {
  const Position *h = nullptr; // 空 = 今日清仓
  const Traded *f = nullptr;   // 空 = 今日没有成交
  const Sec &sec() const { return h ? static_cast<const Sec &>(*h) : static_cast<const Sec &>(*f); }
  bool synced() const { return !h || live::synced(*h, f); }
};

// 持仓 (余额 > 0) 在前、今日清仓在后; 两表市场须一致 (否则 Fail)
std::vector<Pair> join(const Keyed<Position> &pos, const Fills &fills) {
  std::vector<Pair> out;
  for (const Position &h : pos.list)
    if (h.balance > 0)
      out.push_back({&h, fills.by_code.find(h.code)});
  for (const Traded &f : fills.by_code.list) {
    const Position *h = pos.find(f.code);
    if (!h || h->balance <= 0)
      out.push_back({nullptr, &f});
  }
  std::string bad;
  for (const Pair &p : out)
    if (p.h && p.f && p.h->mkt != p.f->mkt)
      bad += strf(" %s %s (持仓 %s, 成交 %s)", p.h->code.c_str(), p.h->name.c_str(), mkt_name(p.h->mkt), mkt_name(p.f->mkt));
  need(bad.empty(), "成交与持仓的市场不一致:" + bad);
  return out;
}

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
  const double px = q.price, d = (px - h.price) * static_cast<double>(h.balance);
  r.market_value = h.market_value + d;
  r.pnl = h.pnl + d;
  if (h.cost > 0)
    r.pnl_pct = (px / h.cost - 1) * 100;
  if (h.financed)
    r.margin_value = static_cast<double>(*h.financed) * px;
  r.sellable_value = static_cast<double>(h.available) * px;
  r.collateral = h.collateral;
  r.cost = h.cost;
}

void today(Row &r, const Pair &p, const Quote &q) {
  if (!p.synced())
    return;
  const double bal = p.h ? static_cast<double>(p.h->balance) : 0;
  double traded = 0, amount = 0, bought = 0;
  if (p.f) {
    for (const int s : kSides) {
      traded += s * static_cast<double>(p.f->qty[s]);
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

void depth(Row &r, const Quote &q) {
  const auto x = ref(q);
  r.auction = auction(q);
  if (x && !r.auction)
    r.slips = slips(q, *x);
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

// 方向 s 第 i 额的滑点按占比加权
std::optional<double> agg(const std::vector<Row> &rows, int s, size_t i) {
  double num = 0, den = 0;
  for (const Row &r : rows)
    if (r.slips && (*r.slips)[s][i].pct && r.weight.value_or(0)) {
      num += *(*r.slips)[s][i].pct * *r.weight;
      den += *r.weight;
    }
  return den ? std::optional<double>(num / den) : std::nullopt;
}

double cents(double x) { return std::round(x * 100) / 100; }

} // namespace

std::optional<Frame> view(const Snapshot &s, const Quotes &quotes) {
  const std::vector<Pair> ps = join(s.pos, s.fills);
  if (!std::all_of(ps.begin(), ps.end(), [&](const Pair &p) { return quotes.contains(p.sec().code); }))
    return std::nullopt;
  Frame fr;
  fr.synced = true;
  double mv = 0;
  for (const Pair &p : ps) {
    const Quote &q = quotes.at(p.sec().code);
    Row r;
    ident(r, p, q);
    hold(r, p, q);
    today(r, p, q);
    if (p.h)
      depth(r, q);
    mv += r.market_value.value_or(0);
    fr.synced = fr.synced && p.synced();
    fr.rows.push_back(std::move(r));
  }
  const std::optional<double> base = asset(s.pos, mv);
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
  for (const int side : kSides)
    for (size_t i = 0; i < kNotionals.size(); ++i)
      fr.slips[side][i] = agg(fr.rows, side, i);
  for (const auto &[code, q] : quotes)
    fr.sec = std::max(fr.sec, q.sec);
  return fr;
}

Row quoted(const std::string &code, const std::string &name, const Quote &q) {
  Row r{.code = code, .name = name, .quote = &q};
  if (q.last_close > 0)
    r.day_pnl_pct = (q.price / q.last_close - 1) * 100;
  depth(r, q);
  return r;
}

} // namespace live::board
