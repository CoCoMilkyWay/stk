#include "gui/task_database/services/Kline.hpp"

#include "features/Bar1mDay.hpp"
#include "fundamental/Scan.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace GUI::Database {

misc::Ymd fund_last(const fundamental::Catalog &cat) {
  static const fundamental::Spec &kDaily = fundamental::spec(kBar1d);
  const std::vector<fundamental::Shard> &months = cat.shards(kBar1d), &days = cat.shards(kBar1m);
  assert(!months.empty() && !days.empty() && "基本面未同步日线 / 分钟");
  misc::Ymd mark = 0;
  for (size_t k = months.size(); k-- > 0 && mark == 0;)
    mark = fundamental::mark(kDaily, months[k]);
  return std::min(mark, days.back().day);
}

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

Fold::Fold(size_t baskets) : sw_(baskets), net_(baskets), acc_(baskets * kCells * kQ) {}

void Fold::open(int slots) {
  assert(slots >= 0 && slots <= kSlots);
  slots_ = slots;
  end_ = -1;
  std::fill(sw_.begin(), sw_.end(), 0.0);
  std::fill(net_.begin(), net_.end(), 0.0);
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
  double c = 1, px = g.base, net = 0;
  int dir = 0;
  for (int s = 0; s < slots_; ++s) {
    const Cell *b = g.bars->has[static_cast<size_t>(s)] ? &g.bars->cell[static_cast<size_t>(s)] : nullptr;
    double *q = r.data() + (1 + static_cast<size_t>(s)) * kQ;
    ratio(b, inv, c, q);
    if (b) {
      c = q[3], end_ = std::max(end_, s);
      dir = b->close > px ? 1 : b->close < px ? -1 : dir;
      net += dir * b->amount, px = b->close;
    }
  }
  const size_t n = cells() * kQ;
  for (const Members::Weight &w : ws) {
    sw_[w.basket] += w.w;
    net_[w.basket] += net;
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
    o.flow = net_[b];
    o.bars.date = d;
    o.bars.has.reset();
    for (int s = 0; s <= end_; ++s)
      o.bars.add(s, scaled(a + (1 + static_cast<size_t>(s)) * kQ, f));
  }
}

} // namespace GUI::Database
