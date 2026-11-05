#include "gui/task_database/services/Kline.hpp"

#include <cassert>

namespace GUI::Database {

namespace {

// 一根 → 一格的比 (价 / 基准) + 额; 无根 → 平于 at, 额 0
void ratio(const Cell *c, double inv, double at, double *q) {
  if (c)
    q[0] = c->open * inv, q[1] = c->high * inv, q[2] = c->low * inv, q[3] = c->close * inv, q[4] = c->amount;
  else
    q[0] = q[1] = q[2] = q[3] = at, q[4] = 0;
}

// 一格 (比 × 权之和) → 一根 (× f = 前日点位 / Σ w)
Cell scaled(const double *a, double f) {
  return {static_cast<float>(a[0] * f), static_cast<float>(a[1] * f), static_cast<float>(a[2] * f), static_cast<float>(a[3] * f), 0, a[4]};
}

} // namespace

Fold::Fold(size_t baskets) : sw_(baskets), acc_(baskets * kCells * kQ) {}

void Fold::open(int slots) {
  assert(slots >= 0 && slots <= kSlots);
  slots_ = slots;
  end_ = -1;
  std::fill(sw_.begin(), sw_.end(), 0.0);
  for (size_t b = 0; b < sw_.size(); ++b)
    std::fill_n(acc_.begin() + static_cast<ptrdiff_t>(b * kCells * kQ), cells() * kQ, 0.0);
}

void Fold::add(const Leg &g, std::span<const Members::Weight> ws) {
  assert((g.bars != nullptr) == (slots_ > 0) && "Fold: 合成分钟的日每只都给分钟根");
  if (ws.empty() || !(g.base > 0))
    return;
  const double inv = 1.0 / g.base;
  std::array<double, kCells * kQ> r;
  ratio(g.day, inv, 1, r.data());
  double c = 1;
  for (int s = 0; s < slots_; ++s) {
    const Cell *b = g.bars->has[static_cast<size_t>(s)] ? &g.bars->cell[static_cast<size_t>(s)] : nullptr;
    double *q = r.data() + (1 + static_cast<size_t>(s)) * kQ;
    ratio(b, inv, c, q);
    if (b)
      c = q[3], end_ = std::max(end_, s);
  }
  const size_t n = cells() * kQ;
  for (const Members::Weight &w : ws) {
    sw_[w.basket] += w.w;
    double *a = acc_.data() + static_cast<size_t>(w.basket) * kCells * kQ;
    for (size_t j = 0; j < n; j += kQ) {
      for (size_t k = 0; k < 4; ++k)
        a[j + k] += w.w * r[j + k];
      a[j + 4] += r[j + 4];
    }
  }
}

void Fold::finish(misc::Ymd d, misc::Ymd prev, std::span<const double> level, std::span<Day> out) {
  assert(level.size() == sw_.size() && out.size() == sw_.size());
  for (size_t b = 0; b < sw_.size(); ++b) {
    Day &o = out[b];
    const double *a = acc_.data() + b * kCells * kQ;
    if (!(sw_[b] > 0 && a[4] > 0 && level[b] > 0)) {
      o.candle.date = 0;
      continue;
    }
    const double f = level[b] / sw_[b];
    o.prev = prev;
    o.candle = {scaled(a, f), d, static_cast<float>(level[b])};
    o.bars.date = d;
    o.bars.has.reset();
    for (int s = 0; s <= end_; ++s)
      o.bars.add(s, scaled(a + (1 + static_cast<size_t>(s)) * kQ, f));
  }
}

} // namespace GUI::Database
