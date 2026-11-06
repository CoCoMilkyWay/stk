#pragma once

// 指标: 收益序列统计 (策略 / 买入持有同一份) + 择时 / 交易统计
//   择时 = 上涨捕获 up / 下跌捕获 dn (毛, 不含成本): 资产逐根收益 a 为正的根上 Σ 策略毛收益 / Σ a, 为负的同理
//     全程满仓 up = dn = 1; 随机择时 (平均仓位 e) up ≈ dn ≈ e; edge = up − dn = 择时本身挣的, dn = 磨损 (承受的下跌), 成本另计
//   在场段 = 仓位 > 0 的连续根 (建仓根到清仓根, 均为收盘), 段收益 = 段内 Π(1 + 净) − 1
//   一本账 Book = 目标仓位列 → 回测 + 全期指标 + 净值 (策略 / 买入持有同一份)
#include "cta/Engine.hpp"

#include <algorithm>
#include <cmath>
#include <span>
#include <vector>

namespace cta {

struct Stat {
  double years = 0, cagr = 0, vol = 0, sharpe = 0, sortino = 0, mdd = 0; // 索提诺 = 均值 / 下行波动 √E[min(r, 0)²]
  double expo = 0, turn = 0, cost = 0;                                   // 平均仓位 / 年换手 (单边) / 年成本
  double up = 0, dn = 0, edge = 0;                                       // 上涨 / 下跌捕获, 两者之差
  double trades = 0, win = 0, payoff = 0, hold = 0;                      // 年在场段数 / 胜率 / 盈亏比 (均盈 / 均亏) / 平均在场根数
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

inline Stat stat_of(const Tape &t, const Run &run, const Setup &u) {
  Stat s = returns_stat(run.r, u.bpy);
  const size_t n = t.n;
  double expo = 0, ua = 0, ug = 0, da = 0, dg = 0;
  for (size_t i = 0; i < n; ++i) {
    const double a = t.a[i];
    expo += run.w[i];
    (a > 0 ? ua : da) += a, (a > 0 ? ug : dg) += run.g[i];
  }
  s.expo = expo / static_cast<double>(n);
  s.turn = run.turn / s.years, s.cost = s.turn * u.cost;
  s.up = ua > 0 ? ug / ua : kNaN, s.dn = da < 0 ? dg / da : kNaN, s.edge = s.up - s.dn;
  double nw = 0, sw = 0, nl = 0, sl = 0, bars = 0, nav = 1;
  size_t i0 = n; // 在场段建仓根 (n = 不在场)
  const auto seal = [&](size_t i1) {
    const double x = nav - 1;
    (x > 0 ? nw : nl) += 1, (x > 0 ? sw : sl) += x;
    bars += static_cast<double>(i1 - i0);
  };
  for (size_t i = 0; i < n; ++i) {
    if (i0 == n && run.w[i] > 0)
      i0 = i, nav = 1;
    if (i0 < n)
      nav *= 1 + run.r[i];
    if (i0 < n && run.w[i] == 0)
      seal(i), i0 = n;
  }
  if (i0 < n)
    seal(n - 1);
  const double nt = nw + nl;
  s.trades = nt / s.years;
  s.win = nt > 0 ? nw / nt : kNaN;
  s.payoff = nw > 0 && nl > 0 && sl < 0 ? (sw / nw) / (-sl / nl) : kNaN;
  s.hold = nt > 0 ? bars / nt : kNaN;
  return s;
}

// 一本账: 回测 + 全期指标 + 净值
struct Book {
  Run run;
  Stat st;
  std::vector<double> nav; // [根] Π(1 + 净)
};

// w = 目标仓位列
inline Book book_of(const Tape &t, const Col &w, const Setup &u) {
  Book b{simulate(t, w, u), {}, std::vector<double>(t.n)};
  b.st = stat_of(t, b.run, u);
  double v = 1;
  for (size_t i = 0; i < t.n; ++i)
    b.nav[i] = v *= 1 + b.run.r[i];
  return b;
}

} // namespace cta
