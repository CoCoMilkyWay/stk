#include "account/Nav.hpp"

#include "live/Flows.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iterator>
#include <optional>
#include <set>

namespace account {

namespace {

namespace flows = live::flows;
using flows::kShare;
using flows::moves;

// 一行 (所动的账 m) 的外部进出: 银证转账 = 发生金额, 担保品划入 = 股 × 计价 px() (只在要时求); 其余 0
template <class Px>
double flow_of(const CashFlow &r, uint8_t m, Px &&px) {
  return (m & flows::kBank) ? r.amount : (m & flows::kPledge) ? r.qty * px()
                                                              : 0;
}

std::optional<double> close_on(const Closes &px, const std::string &code, misc::Ymd d) {
  const auto c = px.find(code);
  if (c == px.end())
    return std::nullopt;
  const auto it = c->second.find(d);
  return it == c->second.end() ? std::nullopt : std::optional<double>(it->second);
}

// px 里全无收盘的证券在 days 上的估价: 各日成交按股数加权的均价 + 券商现价为观测点, 相邻两点间按自然日线性插值, 末点之后持平, 首点之前无
Closes marks(const std::vector<CashFlow> &rows, const std::vector<misc::Ymd> &days, const Closes &px, const Spot &spot) {
  std::map<std::string, std::map<misc::Ymd, std::pair<double, double>>> obs; // 代码 → 日 → (Σ 股 × 价, Σ 股)
  for (const CashFlow &r : rows)
    if ((moves(r) & flows::kHold) && r.price > 0 && r.qty != 0 && !px.contains(r.code)) {
      auto &[v, q] = obs[r.code][r.date];
      v += std::fabs(r.qty) * r.price, q += std::fabs(r.qty);
    }
  for (auto &[code, o] : obs)
    if (const auto it = spot.px.find(code); it != spot.px.end() && it->second > 0 && spot.date > o.rbegin()->first)
      o[spot.date] = {it->second, 1};
  const auto avg = [](const auto &o) { return o.second.first / o.second.second; };
  Closes out;
  for (const auto &[code, o] : obs) {
    std::map<misc::Ymd, double> &m = out[code];
    auto at = o.begin(); // 不晚于 d 的末个观测点
    for (const misc::Ymd d : days) {
      if (d < o.begin()->first)
        continue;
      while (std::next(at) != o.end() && std::next(at)->first <= d)
        ++at;
      const auto nx = std::next(at);
      if (nx == o.end()) {
        m[d] = avg(*at);
        continue;
      }
      const double w = static_cast<double>((misc::to_days(d) - misc::to_days(at->first)).count()) /
                       static_cast<double>((misc::to_days(nx->first) - misc::to_days(at->first)).count());
      m[d] = avg(*at) + (avg(*nx) - avg(*at)) * w;
    }
  }
  return out;
}

} // namespace

std::vector<std::string> securities(const std::vector<CashFlow> &rows) {
  std::set<std::string> s;
  for (const CashFlow &r : rows)
    if (moves(r) & flows::kHold)
      s.insert(r.code);
  return {s.begin(), s.end()};
}

double growth(double nav0, double nav, double flow) {
  const double base = nav0 + std::max(flow, 0.0);
  return base > 0 ? 1 + (nav - nav0 - flow) / base : 1;
}

void intraday(const Day &d, double nav0, double idx0, std::span<const Trade> trades, const std::function<std::span<const float>(const std::string &)> &path,
              std::span<double> out) {
  assert(!out.empty());
  std::ranges::fill(out, d.nav);
  for (const Hold &h : d.hold)
    if (const std::span<const float> p = path(h.code); !p.empty()) {
      assert(p.size() == out.size());
      for (size_t s = 0; s < out.size(); ++s)
        out[s] += h.qty * (p[s] - p.back());
    }
  for (const Trade &t : trades) {
    assert(t.slot >= 0 && t.qty != 0 && t.px > 0);
    const std::span<const float> p = path(t.code);
    assert(p.empty() || p.size() == out.size());
    for (size_t s = 0; s < out.size() && static_cast<int>(s) < t.slot; ++s)
      out[s] -= t.cash + t.qty * (p.empty() ? t.px : p[s]);
  }
  for (double &x : out)
    x = idx0 * growth(nav0, x, d.flow);
}

std::vector<Day> replay(const std::vector<CashFlow> &rows, double rate, const std::vector<misc::Ymd> &days, const Closes &px, const Spot &spot) {
  std::vector<Day> out;
  if (rows.empty())
    return out;
  const Closes mk = marks(rows, days, px, spot);
  const auto price_on = [&](const std::string &code, misc::Ymd d) {
    const std::optional<double> c = close_on(px, code, d);
    return c ? c : close_on(mk, code, d);
  };
  std::map<std::string, double> hold;
  std::unordered_map<std::string, double> last; // 代码 → 上一收盘 (起于入账价)
  double cash = 0, owe = 0, acc = 0, idx = 1, nav0 = 0;
  misc::Ymd prev = 0;
  size_t i = 0;
  for (const misc::Ymd d : days) {
    if (d < rows.front().date)
      continue;
    if (prev)
      acc += owe * rate / 360 * static_cast<double>((misc::to_days(d) - misc::to_days(prev)).count());
    double flow = 0;
    for (; i < rows.size() && rows[i].date <= d; ++i) {
      const CashFlow &r = rows[i];
      const uint8_t m = moves(r);
      if (r.amount != 0)
        cash = r.balance;
      if (m & flows::kHold) {
        hold[r.code] += r.qty;
        last.emplace(r.code, r.price);
      }
      if (m & flows::kDebt)
        owe += r.amount;
      if (m & flows::kInterest)
        acc += r.amount;
      flow += flow_of(r, m, [&] { return price_on(r.code, d).value_or(r.price); });
    }
    double mv = 0;
    std::vector<Hold> held;
    for (const auto &[code, q] : hold) {
      if (std::fabs(q) < kShare)
        continue;
      if (const std::optional<double> c = price_on(code, d))
        last[code] = *c;
      mv += q * last.at(code);
      held.push_back({code, q, last.at(code)});
    }
    const double nav = cash + mv - owe - acc;
    if (prev)
      idx *= growth(nav0, nav, flow);
    out.push_back({d, nav, flow, idx, std::move(held)});
    prev = d;
    nav0 = nav;
  }
  return out;
}

double flow_after(const std::vector<CashFlow> &rows, misc::Ymd d) {
  double f = 0;
  for (const CashFlow &r : rows)
    if (r.date > d)
      f += flow_of(r, moves(r), [&] { return r.price; });
  return f;
}

} // namespace account
