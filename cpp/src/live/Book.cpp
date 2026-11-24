#include "live/Book.hpp"

#include <algorithm>

namespace live {

namespace {

double reach(const std::vector<Level> &side, double notional) {
  double spent = 0;
  for (const Level &l : side) {
    spent += l.px * l.vol;
    if (spent >= notional - 1e-6)
      return l.px;
  }
  return side.back().px;
}

std::pair<std::optional<double>, bool> eat(const std::vector<Level> &side, double notional) {
  double spent = 0, shares = 0;
  for (const Level &l : side) {
    const double take = std::min(l.vol, (notional - spent) / l.px);
    spent += take * l.px;
    shares += take;
    if (spent >= notional - 1e-6)
      break;
  }
  return {shares ? std::optional<double>(spent / shares) : std::nullopt, spent >= notional - 1e-6};
}

} // namespace

Quote quote_of(const sina::Quote &q) {
  Quote o{.sym = q.sym,
          .date = q.date,
          .sec = q.sec,
          .price = q.price > 0 ? q.price : q.last_close,
          .last_close = q.last_close,
          .open = q.open,
          .high = q.high,
          .low = q.low,
          .volume = q.volume};
  for (int i = 0; i < sina::kLevels; ++i) {
    if (q.ask[i] > 0 && q.ask_vol[i] > 0)
      o.book[kBuy].push_back({q.ask[i], q.ask_vol[i]});
    if (q.bid[i] > 0 && q.bid_vol[i] > 0)
      o.book[kSell].push_back({q.bid[i], q.bid_vol[i]});
  }
  return o;
}

bool auction(const Quote &q) {
  const auto &ask = q.book[kBuy], &bid = q.book[kSell];
  return !ask.empty() && !bid.empty() && ask[0].px <= bid[0].px;
}

std::optional<double> ref(const Quote &q) { return q.price > 0 ? std::optional<double>(q.price) : std::nullopt; }

std::optional<double> order_px(const Quote &q, int s, PxMode mode, double notional) {
  const bool opp = mode != PxMode::Own, sweep = mode == PxMode::Sweep;
  const double pct = mode == PxMode::Opp1 ? 1 : 0;
  const auto &side = q.book[opp ? s : -s];
  if (side.empty())
    return std::nullopt;
  return (sweep && !auction(q) ? reach(side, notional) : side[0].px) * (1 + s * pct / 100);
}

std::pair<int64_t, int64_t> lot(std::string_view code) {
  const int64_t first = code.starts_with("688") ? 200 : 100;
  const bool odd = code.starts_with("688") || code.starts_with("4") || code.starts_with("8") || code.starts_with("92");
  return {first, odd ? 1 : 100};
}

int64_t shares_of(std::string_view code, int s, double yuan, double px) {
  const auto [first, step] = lot(code);
  const int64_t n = static_cast<int64_t>(yuan / px) / step * step;
  return s == kSell ? std::max(n, first) : n;
}

std::pair<std::optional<double>, bool> cost(const Quote &q, int s, double notional) {
  const auto r = ref(q);
  if (!r || auction(q))
    return {std::nullopt, false};
  const auto [avg, full] = eat(q.book[s], notional);
  return {avg ? std::optional<double>(s * (*avg - *r) / *r * notional) : std::nullopt, full};
}

Slips slips(const Quote &q, double r) {
  Slips out;
  for (const int s : kSides) {
    for (size_t i = 0; i < kNotionals.size(); ++i) {
      const double n = kNotionals[i];
      const auto [avg, full] = eat(q.book[s], n);
      Slip &x = out[s][i];
      x.full = full;
      if (!avg)
        continue;
      const double rel = s * (*avg - r) / r;
      x.px = avg;
      x.pct = rel * 1e4;
      x.cost = rel * n;
    }
  }
  return out;
}

} // namespace live
