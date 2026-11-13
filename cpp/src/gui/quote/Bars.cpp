#include "gui/quote/Bars.hpp"

#include <cassert>

namespace GUI::Quote {

void closes(const Bars &b, float ref, std::span<float> out) {
  assert(out.size() <= static_cast<size_t>(kSlots));
  float c = ref;
  for (size_t s = 0; s < out.size(); ++s)
    out[s] = c = b.has[s] ? b.cell[s].close : c;
}

Day path_day(misc::Ymd d, float ref, std::span<const double> v) {
  assert(v.size() <= static_cast<size_t>(kSlots));
  Day y;
  float prev = ref, hi = ref, lo = ref;
  for (size_t s = 0; s < v.size(); ++s) {
    const float x = static_cast<float>(v[s]);
    y.bars.add(static_cast<int>(s), cell(prev, std::max(prev, x), std::min(prev, x), x, 0));
    hi = std::max(hi, x), lo = std::min(lo, x), prev = x;
  }
  y.candle = {cell(ref, hi, lo, prev, 0), d, ref, {}};
  return y;
}

void net_flow(const Bars &b, float base, std::span<double, kSlots> net) {
  std::ranges::fill(net, 0.0);
  if (!(base > 0))
    return;
  double px = base;
  int dir = 0;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b.has[s]) {
      const Cell &c = b.cell[s];
      dir = tick_dir(c.close, px, dir);
      net[s] = dir * c.amount, px = c.close;
    }
}

DayFlow day_flow(const Bars &b, std::span<const double, kSlots> net) {
  double n = 0, a = 0;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b.has[s])
      n += net[s], a += b.cell[s].amount;
  DayFlow o{static_cast<float>(n), static_cast<float>(a), 0, 0};
  for (const size_t s : {size_t{0}, static_cast<size_t>(kSlots) - 1})
    if (b.has[s])
      o.edge_net += static_cast<float>(net[s]), o.edge_amount += static_cast<float>(b.cell[s].amount);
  return o;
}

} // namespace GUI::Quote
