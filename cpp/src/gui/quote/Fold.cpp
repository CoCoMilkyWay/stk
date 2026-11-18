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
  return {static_cast<float>(a[0] * f), static_cast<float>(a[1] * f), static_cast<float>(a[2] * f), static_cast<float>(a[3] * f), a[4]};
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
  const size_t lo = (1 + static_cast<size_t>(std::min(touch_, slots_))) * kQ, hi = (1 + static_cast<size_t>(slots_)) * kQ;
  for (size_t b = 0; b < sw_.size(); ++b) {
    double *l = live_.data() + b * kCells * kQ;
    std::fill_n(l, kQ, 0.0);
    std::fill(l + lo, l + hi, 0.0);
  }
  slots_ = touch_ = slots;
  end_ = fend_;
  std::fill(sw_.begin(), sw_.end(), 0.0);
}

void Fold::reset() {
  std::fill(froze_.begin(), froze_.end(), 0.0);
  end_ = fend_ = -1;
}

bool Fold::add(const Leg &g, std::span<const Members::Weight> ws, int freeze, Carry &k) {
  Row r;
  const int from = k.at;
  touch_ = std::min(touch_, freeze);
  const int x = row(g, ws.empty() || !(g.base > 0) ? 0.f : g.base, freeze, k, r, end_, fend_);
  if (x > 0)
    put(r, from, freeze, ws);
  return x >= 0;
}

int Fold::row(const Leg &g, float base, int freeze, Carry &k, Row &r, int &end, int &fend) const {
  assert((g.bars != nullptr) == (slots_ > 0) && "Fold: 合成分钟的日每只都给分钟根");
  assert(k.at <= freeze && freeze <= slots_);
  if (k.at == 0)
    k = {base, 0, 0, 1, base};
  else if (k.base != base)
    return -1;
  if (!(base > 0)) {
    k.at = freeze;
    return 0;
  }
  const double inv = 1.0 / base;
  ratio(g.day, inv, 1, r.data());
  double c = k.c, px = k.px;
  int dir = k.dir;
  for (int s = k.at; s < slots_; ++s) {
    if (s == freeze)
      k = {base, freeze, dir, c, px};
    const Cell *b = g.bars->has[static_cast<size_t>(s)] ? &g.bars->cell[static_cast<size_t>(s)] : nullptr;
    double *q = r.data() + (1 + static_cast<size_t>(s)) * kQ;
    ratio(b, inv, c, q);
    q[6] = g.bars->hole[static_cast<size_t>(s)];
    if (b) {
      c = q[3], end = std::max(end, s);
      if (s < freeze)
        fend = std::max(fend, s);
      dir = tick_dir(b->close, px, dir);
      q[5] = dir * b->amount, px = b->close;
    }
  }
  if (freeze == slots_)
    k = {base, freeze, dir, c, px};
  return 1;
}

void Fold::put(const Row &r, int from, int freeze, std::span<const Members::Weight> ws) {
  // 格内前 4 量 (开 高 低 收) 乘权, 其余直加; 一段连续格一趟 (可向量化)
  static constexpr std::array<bool, kCells * kQ> kPriced = [] {
    std::array<bool, kCells * kQ> p{};
    for (size_t i = 0; i < p.size(); ++i)
      p[i] = i % kQ < 4;
    return p;
  }();
  const size_t a = (1 + static_cast<size_t>(from)) * kQ, z = (1 + static_cast<size_t>(freeze)) * kQ, e = (1 + static_cast<size_t>(slots_)) * kQ;
  for (const Members::Weight &w : ws) {
    sw_[w.key] += w.w;
    const double ww = w.w;
    const size_t o = static_cast<size_t>(w.key) * kCells * kQ;
    const auto at = [&](std::vector<double> &acc, size_t i0, size_t i1) {
      double *x = acc.data() + o;
      for (size_t i = i0; i < i1; ++i)
        x[i] += r[i] * (kPriced[i] ? ww : 1.0);
    };
    at(live_, 0, kQ);
    at(froze_, a, z);
    at(live_, z, e);
  }
}

void Fold::finish(misc::Ymd d, std::span<Day> out) {
  assert(out.size() == sw_.size());
  for (size_t b = 0; b < sw_.size(); ++b) {
    Day &o = out[b];
    const double *f = froze_.data() + b * kCells * kQ, *l = live_.data() + b * kCells * kQ;
    if (!(sw_[b] > 0 && l[4] > 0)) {
      o.candle.date = 0;
      continue;
    }
    const double w = 1.0 / sw_[b];
    o.candle = {weighed(l, w), d, 1.f};
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
    o.candle.flow = day_flow(o.bars);
  }
}

} // namespace GUI::Quote
