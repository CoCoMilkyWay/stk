#include "gui/quote/Bars.hpp"

namespace GUI::Quote {

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
