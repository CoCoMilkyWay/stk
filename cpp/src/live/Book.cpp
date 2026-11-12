#include "live/Book.hpp"

#include <algorithm>

namespace live {

namespace {

// 按档累计吃 notional 元, 恰好吃满的那档价; 吃不满取最后一档
double reach(const std::vector<Level> &side, double notional) {
  double spent = 0;
  for (const Level &l : side) {
    spent += l.px * l.vol;
    if (spent >= notional - 1e-6)
      return l.px;
  }
  return side.back().px;
}

// 按档吃 notional 元 → (均价, 是否吃满)
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
  Quote o{.sym = q.sym, .date = q.date, .sec = q.sec, .price = q.price > 0 ? q.price : q.last_close, .last_close = q.last_close, .open = q.open, .high = q.high, .low = q.low, .volume = q.volume};
  for (int i = 0; i < sina::kLevels; ++i) {
    if (q.ask[i] > 0 && q.ask_vol[i] > 0)
      o.side[0].push_back({q.ask[i], q.ask_vol[i]});
    if (q.bid[i] > 0 && q.bid_vol[i] > 0)
      o.side[1].push_back({q.bid[i], q.bid_vol[i]});
  }
  return o;
}

std::string quote_time(const Quote &q) { return strf("%02d:%02d:%02d", q.sec / 3600, q.sec / 60 % 60, q.sec % 60); }

bool auction(const Quote &q) {
  const auto &ask = q.book(kBuy), &bid = q.book(kSell);
  return !ask.empty() && !bid.empty() && ask[0].px <= bid[0].px;
}

std::optional<double> ref(const Quote &q) { return q.price > 0 ? std::optional<double>(q.price) : std::nullopt; }

std::optional<double> order_px(const Quote &q, int s, PxMode mode, double notional) {
  const bool opp = mode != PxMode::Own, sweep = mode == PxMode::Sweep;
  const double pct = mode == PxMode::Opp1 ? 1 : 0;
  const auto &side = q.book(opp ? s : -s);
  if (auction(q) || side.empty())
    return std::nullopt;
  return (sweep ? reach(side, notional) : side[0].px) * (1 + s * pct / 100);
}

std::pair<int, int> lot(const std::string &code) {
  const int first = code.starts_with("688") ? 200 : 100;
  const bool odd = code.starts_with("688") || code.starts_with("4") || code.starts_with("8") || code.starts_with("92");
  return {first, odd ? 1 : 100};
}

std::pair<std::optional<double>, bool> cost(const Quote &q, int s, double notional) {
  const auto r = ref(q);
  if (!r || auction(q))
    return {std::nullopt, false};
  const auto [avg, full] = eat(q.book(s), notional);
  return {avg ? std::optional<double>(s * (*avg - *r) / *r * notional) : std::nullopt, full};
}

std::array<Slip, kSlipCols.size()> slips(const Quote &q, double r) {
  std::array<Slip, kSlipCols.size()> out;
  for (size_t i = 0; i < kSlipCols.size(); ++i) {
    const auto [n, s] = kSlipCols[i];
    const auto [avg, full] = eat(q.book(s), n);
    out[i] = {avg ? std::optional<double>(s * (*avg - r) / r * 1e4) : std::nullopt, full};
  }
  return out;
}

std::array<double, kLevelCols.size()> levels(double r) {
  std::array<double, kLevelCols.size()> out;
  for (size_t i = 0; i < kLevelCols.size(); ++i)
    out[i] = r * (1 + kLevelCols[i].s * kLevelCols[i].n / 100);
  return out;
}

} // namespace live
