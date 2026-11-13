#include "gui/quote/Bars.hpp"

#include "features/Storage/FeatureLevels.hpp" // feature_storage_t

#include <cassert>
#include <cmath>
#include <vector>

namespace GUI::Quote {

namespace {

constexpr double kQMax = 65534, kAmt = 2048, kNet = 32767;

// 个股存价落成 f16 (库段本就是, 逐位不变) / f16 在 x 处的步长
float f16(float v) { return static_cast<float>(static_cast<feature_storage_t>(v)); }
float ulp16(float x) {
  int e = 0;
  std::frexp(x, &e);
  return std::ldexp(1.f, std::max(e - 11, -24));
}

// 一槽的额格 → 额 (查表)
double amount_of(uint16_t a) {
  static const std::vector<double> t = [] {
    std::vector<double> v(65536);
    for (size_t q = 0; q < v.size(); ++q)
      v[q] = std::expm1(static_cast<double>(q) / kAmt);
    return v;
  }();
  return t[a];
}

} // namespace

Scale pack(const Bars &b, std::span<const double, kSlots> net, bool half, Packed &m) {
  const auto px = [half](float v) { return half ? f16(v) : v; };
  float lo = INFINITY, hi = -INFINITY;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b.has[s])
      for (const float v : {b.cell[s].open, b.cell[s].high, b.cell[s].low, b.cell[s].close})
        lo = std::min(lo, px(v)), hi = std::max(hi, px(v));
  float step = half ? ulp16(lo) : static_cast<float>((hi - lo) / kQMax);
  while (half && (hi - lo) / step > kQMax)
    step *= 2;
  const auto q = [&](float v) { return static_cast<uint16_t>(step > 0 ? std::clamp(std::lround((px(v) - lo) / step), 0L, static_cast<long>(kQMax)) : 0L); };
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
    if (!b.has[s]) {
      m.s[s] = {0, 0, 0, kNoBar, 0, 0};
      continue;
    }
    const Cell &c = b.cell[s];
    const long a = std::lround(std::log1p(std::max(c.amount, 0.0)) * kAmt), n = c.amount > 0 ? std::lround(net[s] / c.amount * kNet) : 0L;
    m.s[s] = {q(c.open), q(c.high), q(c.low), q(c.close), static_cast<uint16_t>(std::clamp(a, 0L, 65535L)), static_cast<int16_t>(std::clamp(n, -32767L, 32767L))};
  }
  return {lo, step};
}

Cell unpack(const Slot &x, Scale k, double a) {
  assert(x.c != kNoBar);
  const auto px = [&](uint16_t q) { return static_cast<float>((k.lo + static_cast<float>(q) * k.step) * a); };
  return {px(x.o), px(x.h), px(x.l), px(x.c), amount_of(x.a)};
}

double unpack_net(const Slot &x) {
  assert(x.c != kNoBar);
  return x.n / kNet * amount_of(x.a);
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
