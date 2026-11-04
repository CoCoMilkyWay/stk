#include "gui/task_database/services/Kline.hpp"

#include <cassert>

namespace GUI::Database {

Fold::Fold(size_t baskets) : sw_(baskets), day_(baskets), bars_(baskets * kSlots * 5) {}

void Fold::open(int slots) {
  assert(slots >= 0 && slots <= kSlots);
  std::fill(sw_.begin(), sw_.end(), 0.0);
  std::fill(day_.begin(), day_.end(), std::array<double, 5>{});
  if (slots > 0)
    std::fill(bars_.begin(), bars_.end(), 0.0);
  slots_ = slots;
  end_ = -1;
}

void Fold::add(const Leg &g, std::span<const Members::Weight> ws) {
  assert((g.bars != nullptr) == (slots_ > 0) && "Fold: 合成分钟的日每只都给分钟根");
  if (ws.empty() || !(g.base > 0))
    return;
  const double inv = 1.0 / g.base;
  const std::array<double, 5> k = g.day ? std::array<double, 5>{g.day->open * inv, g.day->high * inv, g.day->low * inv,
                                                                g.day->close * inv, g.day->amount}
                                        : std::array<double, 5>{1, 1, 1, 1, 0};
  const size_t n = static_cast<size_t>(slots_) * 5;
  std::array<double, kSlots * 5> r; // 各根 开 高 低 收 (比) / 额
  double c = 1;
  for (size_t s = 0; s < static_cast<size_t>(slots_); ++s) {
    double *q = r.data() + s * 5;
    if (g.bars->has[s]) {
      const Cell &b = g.bars->cell[s];
      q[0] = b.open * inv, q[1] = b.high * inv, q[2] = b.low * inv, q[3] = c = b.close * inv, q[4] = b.amount;
      end_ = std::max(end_, static_cast<int>(s));
    } else {
      q[0] = q[1] = q[2] = q[3] = c, q[4] = 0;
    }
  }
  for (const Members::Weight &w : ws) {
    sw_[w.basket] += w.w;
    std::array<double, 5> &a = day_[w.basket];
    for (size_t j = 0; j < 4; ++j)
      a[j] += w.w * k[j];
    a[4] += k[4];
    double *acc = bars_.data() + static_cast<size_t>(w.basket) * kSlots * 5;
    for (size_t s = 0; s < n; s += 5) {
      for (size_t j = 0; j < 4; ++j)
        acc[s + j] += w.w * r[s + j];
      acc[s + 4] += r[s + 4];
    }
  }
}

void Fold::finish(misc::Ymd d, misc::Ymd prev, std::span<const double> level, std::span<Day> out) {
  assert(level.size() == sw_.size() && out.size() == sw_.size());
  for (size_t b = 0; b < sw_.size(); ++b) {
    Day &o = out[b];
    const std::array<double, 5> &k = day_[b];
    if (!(sw_[b] > 0 && k[4] > 0 && level[b] > 0)) {
      o.date = 0;
      continue;
    }
    const double f = level[b] / sw_[b];
    o.date = d, o.prev = prev;
    o.candle = {{static_cast<float>(k[0] * f), static_cast<float>(k[1] * f), static_cast<float>(k[2] * f),
                 static_cast<float>(k[3] * f), 0, k[4]},
                d,
                0,
                static_cast<float>(level[b])};
    o.bars.date = d;
    o.bars.has.reset();
    const double *acc = bars_.data() + b * kSlots * 5;
    for (int s = 0; s <= end_; ++s) {
      const double *a = acc + static_cast<size_t>(s) * 5;
      o.bars.add(s, {static_cast<float>(a[0] * f), static_cast<float>(a[1] * f), static_cast<float>(a[2] * f),
                     static_cast<float>(a[3] * f), 0, a[4]});
    }
  }
}

} // namespace GUI::Database
