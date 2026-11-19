#pragma once

// 指标: 收益序列统计 (策略 / 买入持有同一份) + 仓位 / 成本统计
//   平均仓位 = 逐根 |w| 的均值; 年换手 = Σ |Δw| / 年数
//   年成本 = 成本分项各自 Σ / 年数 (Engine.hpp kCosts), 及其和
//   一本账 Book = 一条逐根链的 Run → 全期指标 + 分层复利净值 (逐项叠回成本: 净值 → … → 毛, 画成压在净值之上的成本带; 策略 / 买入持有同一份)
#include "cta/Engine.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <span>
#include <utility>
#include <vector>

namespace cta {

struct Stat {
  double years = 0, cagr = 0, vol = 0, sharpe = 0, sortino = 0, mdd = 0; // 索提诺 = 均值 / 下行波动 √E[min(r, 0)²]
  double expo = 0, turn = 0;                                             // 平均仓位 / 年换手 (单边)
  double cost = 0;                                                       // 年成本 (分项之和)
  Costs costs{};                                                         // 年成本分项
};

inline Stat returns_stat(std::span<const double> r, double bpy) {
  Stat s;
  const double n = static_cast<double>(r.size());
  s.years = n / bpy;
  double sum = 0, sq = 0, dq = 0, nav = 1, peak = 1;
  for (const double x : r) {
    sum += x, sq += x * x, dq += std::min(x, 0.0) * std::min(x, 0.0);
    nav *= 1 + x, peak = std::max(peak, nav);
    s.mdd = std::min(s.mdd, nav / peak - 1);
  }
  const double mean = sum / n, sd = std::sqrt(std::max(0.0, sq / n - mean * mean)), dd = std::sqrt(dq / n);
  s.vol = sd * std::sqrt(bpy);
  s.sharpe = sd > 0 ? mean / sd * std::sqrt(bpy) : 0;
  s.sortino = dd > 0 ? mean / dd * std::sqrt(bpy) : 0;
  s.cagr = std::pow(nav, 1 / s.years) - 1;
  return s;
}

inline Stat stat_of(const Tape &t, const Run &run, double bpy) {
  Stat s = returns_stat(run.r, bpy);
  const size_t n = t.n;
  double expo = 0, turn = 0;
  for (size_t i = 0; i < n; ++i) {
    expo += std::fabs(run.w[i]), turn += std::fabs(run.w[i] - (i ? run.w[i - 1] : 0.0f));
    for (size_t j = 0; j < kNC; ++j)
      s.costs[j] += run.k[i][j];
  }
  s.expo = expo / static_cast<double>(n), s.turn = turn / s.years;
  for (double &c : s.costs)
    c /= s.years;
  s.cost = s.costs.sum();
  return s;
}

// 一本账: 回测 + 全期指标 + 分层净值
struct Book {
  Run run;
  Stat st;
  std::array<std::vector<double>, kNC + 1> nav; // [层][根] Π(1 + 净 + 前 j 项成本): 层 0 = 净值, 层 kNC = 毛
};

// run = 回测, bpy = 每年根数
inline Book book_of(const Tape &t, Run run, double bpy) {
  assert(run.w.size() == t.n && "回测长度");
  Book b{std::move(run), {}, {}};
  b.st = stat_of(t, b.run, bpy);
  std::array<double, kNC + 1> v;
  v.fill(1);
  for (std::vector<double> &o : b.nav)
    o.resize(t.n);
  for (size_t i = 0; i < t.n; ++i) {
    double r = b.run.r[i];
    for (size_t j = 0; j <= kNC; ++j) {
      b.nav[j][i] = v[j] *= 1 + r;
      if (j < kNC)
        r += b.run.k[i][j];
    }
  }
  return b;
}

} // namespace cta
