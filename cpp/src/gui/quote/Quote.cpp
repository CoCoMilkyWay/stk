#include "gui/quote/Quote.hpp"

#include "api/sina/client.hpp"
#include "features/Bar1mDay.hpp"
#include "fundamental/Scan.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace GUI::Quote {

void each_daily(fundamental::Scratch &xs, const fundamental::Shard &sh, const fundamental::Ids &ids,
                const std::function<void(uint32_t, const Candle &)> &f) {
  static const fundamental::Spec &kDaily = fundamental::spec(kBar1d);
  static const std::vector<std::string> kCols = {"open", "high", "low", "close", "volume", "amount", "pre_close"};
  const fundamental::Rows r = xs.open(sh, kDaily, kCols, ids);
  misc::pq::Col c[7];
  for (size_t j = 0; j < kCols.size(); ++j)
    c[j] = r.col(kCols[j]);
  r.each([&](std::int64_t i, std::uint32_t id, misc::Ymd d) {
    f(id, {cell(c[0].f32(i), c[1].f32(i), c[2].f32(i), c[3].f32(i), c[4].f32(i), c[5].f32(i)), d, c[6].f32(i)});
  });
}

void bars_of(const Bar1m *m, misc::Ymd d, Bars &b) {
  b.date = d;
  b.has.reset();
  if (!m)
    return;
  for (size_t t = 0; t < TRADE_MINUTES_PER_DAY; ++t)
    if (m[t].volume > 0 && m[t].close > 0)
      b.add(row_slot(t), cell(m[t].open, m[t].high, m[t].low, m[t].close, m[t].volume, m[t].amount));
}

double net_flow(Bars &b, float base) {
  b.net.fill(0);
  if (!(base > 0))
    return 0;
  double px = base, sum = 0;
  int dir = 0;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b.has[s]) {
      const Cell &c = b.cell[s];
      dir = tick_dir(c.close, px, dir);
      b.net[s] = dir * c.amount, sum += b.net[s], px = c.close;
    }
  return sum;
}

const Candle *Source::last() const {
  return joined ? &today->candle : !daily.empty() ? &daily.back()
                                                  : nullptr;
}

const Candle *Source::candle(misc::Ymd d) const {
  if (joined && today->candle.date == d)
    return &today->candle;
  const auto it = std::ranges::lower_bound(daily, d, {}, &Candle::date);
  return it != daily.end() && it->date == d ? &*it : nullptr;
}

const Bars *Source::bars(misc::Ymd d) const {
  if (joined && today->bars.has.any() && today->bars.date == d)
    return &today->bars;
  if (!kl)
    return nullptr;
  const auto it = std::ranges::lower_bound(kl->minutes, d, {}, &Bars::date);
  return it != kl->minutes.end() && it->date == d ? &*it : nullptr;
}

std::span<const Candle> Tails::of(uint32_t id) const {
  const auto it = std::ranges::lower_bound(ids, id);
  return it != ids.end() && *it == id ? daily[static_cast<size_t>(it - ids.begin())] : std::span<const Candle>{};
}

const Kline *Tape::line(uint32_t key) const {
  const size_t k = find(key);
  return k < keys.size() ? list[k].get() : nullptr;
}

const Day *Live::at(Ref r) const {
  assert(r.ok());
  if (r.side == kBasket)
    return r.key < baskets.size() ? baskets[r.key].get() : nullptr;
  const size_t s = axis->sub_of(r.key);
  return s < stocks.size() ? stocks[s].get() : nullptr;
}

const sina::Quote *Live::quote(uint32_t id) const {
  const size_t s = axis->sub_of(id);
  return quotes && s < quotes->size() ? &(*quotes)[s] : nullptr;
}

const float *Minutes::on(misc::Ymd d) const {
  const auto it = std::ranges::lower_bound(dates, d);
  return it != dates.end() && *it == d ? px.data() + static_cast<size_t>(it - dates.begin()) * kSlots * kFields : nullptr;
}

std::shared_ptr<const Minutes> PriceView::minute(Ref r) const {
  const auto it = std::ranges::find(refs, r);
  return it == refs.end() ? nullptr : minutes[static_cast<size_t>(it - refs.begin())];
}

bool PriceView::covers(Ref r) const { return std::ranges::find(refs, r) != refs.end(); }

const Tape *Joint::tape() const { return view ? view->tape[side].get() : nullptr; }

misc::Ymd Joint::today() const {
  const bool has = live && (side == kStock || std::ranges::any_of(live->baskets, [](const auto &y) { return y != nullptr; }));
  return has && view && view->joins(live->prev) ? live->date : 0;
}

Source Joint::of(uint32_t key) const {
  Source o;
  o.today = live ? live->at({side, key}) : nullptr;
  if (const Tape *t = tape()) {
    o.kl = t->line(key);
    o.daily = o.kl ? std::span<const Candle>(o.kl->daily) : t->tails ? t->tails->of(key)
                                                                     : std::span<const Candle>{};
  }
  o.joined = o.today && (o.daily.empty() || view->joins(o.today->prev));
  return o;
}

namespace {

// 一根 → 一格的比 (价 / 基准) + 额, 净流入 / 缺口待填 0; 无根 → 平于 at, 额 0
void ratio(const Cell *c, double inv, double at, double *q) {
  if (c)
    q[0] = c->open * inv, q[1] = c->high * inv, q[2] = c->low * inv, q[3] = c->close * inv, q[4] = c->amount;
  else
    q[0] = q[1] = q[2] = q[3] = at, q[4] = 0;
  q[5] = q[6] = 0;
}

// 一格 (比 × 权之和) → 一根 (× f = 1 / Σ w)
Cell scaled(const double *a, double f) {
  return {static_cast<float>(a[0] * f), static_cast<float>(a[1] * f), static_cast<float>(a[2] * f), static_cast<float>(a[3] * f), 0, a[4]};
}

void scale(Cell &c, double f) {
  c.open = static_cast<float>(c.open * f), c.high = static_cast<float>(c.high * f);
  c.low = static_cast<float>(c.low * f), c.close = static_cast<float>(c.close * f);
}

} // namespace

void rebase(Day &y, double level) {
  assert(y.candle.date != 0 && level > 0);
  scale(y.candle, level);
  y.candle.ref = static_cast<float>(level);
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (y.bars.has[s])
      scale(y.bars.cell[s], level);
}

Fold::Fold(size_t baskets) : sw_(baskets), acc_(baskets * kCells * kQ) {}

void Fold::open(int from, int freeze, int slots) {
  assert(0 <= from && from <= freeze && freeze <= slots && slots <= kSlots);
  assert((from == 0 || from == freeze_) && "Fold: 增量只从上一轮的冻结线接着");
  if (from == 0)
    fend_ = -1;
  from_ = from, freeze_ = freeze, slots_ = slots;
  end_ = fend_;
  std::fill(sw_.begin(), sw_.end(), 0.0);
  for (size_t b = 0; b < sw_.size(); ++b) {
    double *a = acc_.data() + b * kCells * kQ;
    std::fill_n(a, kQ, 0.0);
    std::fill(a + (1 + static_cast<size_t>(from)) * kQ, a + cells() * kQ, 0.0);
  }
}

bool Fold::add(const Leg &g, std::span<const Members::Weight> ws, Carry &k) {
  Row r;
  const int x = row(g, ws.empty() || !(g.base > 0) ? 0.f : g.base, k, r, end_, fend_);
  if (x > 0)
    put(r, ws);
  return x >= 0;
}

int Fold::row(const Leg &g, float base, Carry &k, Row &r, int &end, int &fend) const {
  assert((g.bars != nullptr) == (slots_ > 0) && "Fold: 合成分钟的日每只都给分钟根");
  if (from_ == 0)
    k = {base, 0, 1, base, 0};
  else if (k.base != base)
    return -1;
  if (!(base > 0))
    return 0;
  const double inv = 1.0 / base;
  ratio(g.day, inv, 1, r.data());
  double c = k.c, px = k.px, net = k.net;
  int dir = k.dir;
  for (int s = from_; s < slots_; ++s) {
    if (s == freeze_)
      k = {base, dir, c, px, net};
    const Cell *b = g.bars->has[static_cast<size_t>(s)] ? &g.bars->cell[static_cast<size_t>(s)] : nullptr;
    double *q = r.data() + (1 + static_cast<size_t>(s)) * kQ;
    ratio(b, inv, c, q);
    q[6] = g.bars->hole[static_cast<size_t>(s)];
    if (b) {
      c = q[3], end = std::max(end, s);
      if (s < freeze_)
        fend = std::max(fend, s);
      dir = tick_dir(b->close, px, dir);
      q[5] = dir * b->amount, net += q[5], px = b->close;
    }
  }
  if (freeze_ == slots_)
    k = {base, dir, c, px, net};
  r[5] = net;
  return 1;
}

void Fold::put(const Row &r, std::span<const Members::Weight> ws) {
  const size_t j0 = (1 + static_cast<size_t>(from_)) * kQ, n = cells() * kQ;
  for (const Members::Weight &w : ws) {
    sw_[w.key] += w.w;
    double *a = acc_.data() + static_cast<size_t>(w.key) * kCells * kQ;
    const auto at = [&](size_t j) {
      for (size_t i = 0; i < 4; ++i)
        a[j + i] += w.w * r[j + i];
      a[j + 4] += r[j + 4], a[j + 5] += r[j + 5], a[j + 6] += r[j + 6];
    };
    at(0);
    for (size_t j = j0; j < n; j += kQ)
      at(j);
  }
}

void Fold::finish(misc::Ymd d, misc::Ymd prev, std::span<Day> out) {
  assert(out.size() == sw_.size());
  for (size_t b = 0; b < sw_.size(); ++b) {
    Day &o = out[b];
    const double *a = acc_.data() + b * kCells * kQ;
    if (!(sw_[b] > 0 && a[4] > 0)) {
      o.candle.date = 0;
      continue;
    }
    const double f = 1.0 / sw_[b];
    o.prev = prev;
    o.candle = {scaled(a, f), d, 1.f};
    o.flow = a[5];
    o.bars.date = d;
    o.bars.has.reset(), o.bars.hole.reset();
    for (int s = 0; s <= end_; ++s) {
      const double *q = a + (1 + static_cast<size_t>(s)) * kQ;
      if (q[6] > 0) {
        o.bars.hole.set(static_cast<size_t>(s));
        continue;
      }
      o.bars.add(s, scaled(q, f));
      o.bars.net[static_cast<size_t>(s)] = q[5];
    }
  }
}

} // namespace GUI::Quote