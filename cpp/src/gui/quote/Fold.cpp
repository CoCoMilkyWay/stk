#include "gui/quote/Fold.hpp"

#include <cassert>

namespace GUI::Quote {

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
Cell weighed(const double *a, double f) {
  return {static_cast<float>(a[0] * f), static_cast<float>(a[1] * f), static_cast<float>(a[2] * f), static_cast<float>(a[3] * f), 0, a[4]};
}

} // namespace

void rebase(Day &y, double level) {
  assert(y.candle.date != 0 && level > 0);
  static_cast<Cell &>(y.candle) = scaled(y.candle, level);
  y.candle.ref = static_cast<float>(level);
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (y.bars.has[s])
      y.bars.cell[s] = scaled(y.bars.cell[s], level);
}

Fold::Fold(size_t baskets) : sw_(baskets), froze_(baskets * kCells * kQ), live_(baskets * kCells * kQ) {}

void Fold::open(int slots) {
  assert(0 <= slots && slots <= kSlots);
  const size_t n = (1 + static_cast<size_t>(std::max(slots, slots_))) * kQ;
  slots_ = slots;
  end_ = fend_;
  std::fill(sw_.begin(), sw_.end(), 0.0);
  for (size_t b = 0; b < sw_.size(); ++b)
    std::fill_n(live_.data() + b * kCells * kQ, n, 0.0);
}

void Fold::reset() {
  std::fill(froze_.begin(), froze_.end(), 0.0);
  end_ = fend_ = -1;
}

bool Fold::add(const Leg &g, std::span<const Members::Weight> ws, int freeze, Carry &k) {
  Row r;
  const int from = k.at;
  const int x = row(g, ws.empty() || !(g.base > 0) ? 0.f : g.base, freeze, k, r, end_, fend_);
  if (x > 0)
    put(r, from, freeze, ws);
  return x >= 0;
}

int Fold::row(const Leg &g, float base, int freeze, Carry &k, Row &r, int &end, int &fend) const {
  assert((g.bars != nullptr) == (slots_ > 0) && "Fold: 合成分钟的日每只都给分钟根");
  assert(k.at <= freeze && freeze <= slots_);
  if (k.at == 0)
    k = {base, 0, 0, 1, base, 0};
  else if (k.base != base)
    return -1;
  if (!(base > 0)) {
    k.at = freeze;
    return 0;
  }
  const double inv = 1.0 / base;
  ratio(g.day, inv, 1, r.data());
  double c = k.c, px = k.px, net = k.net;
  int dir = k.dir;
  for (int s = k.at; s < slots_; ++s) {
    if (s == freeze)
      k = {base, freeze, dir, c, px, net};
    const Cell *b = g.bars->has[static_cast<size_t>(s)] ? &g.bars->cell[static_cast<size_t>(s)] : nullptr;
    double *q = r.data() + (1 + static_cast<size_t>(s)) * kQ;
    ratio(b, inv, c, q);
    q[6] = g.bars->hole[static_cast<size_t>(s)];
    if (b) {
      c = q[3], end = std::max(end, s);
      if (s < freeze)
        fend = std::max(fend, s);
      dir = tick_dir(b->close, px, dir);
      q[5] = dir * b->amount, net += q[5], px = b->close;
    }
  }
  if (freeze == slots_)
    k = {base, freeze, dir, c, px, net};
  r[5] = net;
  return 1;
}

void Fold::put(const Row &r, int from, int freeze, std::span<const Members::Weight> ws) {
  for (const Members::Weight &w : ws) {
    sw_[w.key] += w.w;
    double *f = froze_.data() + static_cast<size_t>(w.key) * kCells * kQ, *l = live_.data() + static_cast<size_t>(w.key) * kCells * kQ;
    const auto at = [&](double *a, size_t j) {
      for (size_t i = 0; i < 4; ++i)
        a[j + i] += w.w * r[j + i];
      a[j + 4] += r[j + 4], a[j + 5] += r[j + 5], a[j + 6] += r[j + 6];
    };
    at(l, 0);
    for (int s = from; s < slots_; ++s)
      at(s < freeze ? f : l, (1 + static_cast<size_t>(s)) * kQ);
  }
}

void Fold::finish(misc::Ymd d, misc::Ymd prev, std::span<Day> out) {
  assert(out.size() == sw_.size());
  for (size_t b = 0; b < sw_.size(); ++b) {
    Day &o = out[b];
    const double *f = froze_.data() + b * kCells * kQ, *l = live_.data() + b * kCells * kQ;
    if (!(sw_[b] > 0 && l[4] > 0)) {
      o.candle.date = 0;
      continue;
    }
    const double w = 1.0 / sw_[b];
    o.prev = prev;
    o.candle = {weighed(l, w), d, 1.f};
    o.flow = l[5];
    o.bars.date = d;
    o.bars.has.reset(), o.bars.hole.reset();
    o.bars.net.fill(0);
    for (int s = 0; s <= end_; ++s) {
      const size_t j = (1 + static_cast<size_t>(s)) * kQ;
      double q[kQ];
      for (size_t i = 0; i < kQ; ++i)
        q[i] = f[j + i] + l[j + i];
      if (q[6] > 0) {
        o.bars.hole.set(static_cast<size_t>(s));
        continue;
      }
      o.bars.add(s, weighed(q, w));
      o.bars.net[static_cast<size_t>(s)] = q[5];
    }
  }
}

} // namespace GUI::Quote
