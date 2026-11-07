#pragma once

// 指标: 收益序列统计 (策略 / 买入持有同一份) + 择时 / 交易统计
//   择时 = 上涨捕获 up / 下跌捕获 dn (毛 = 按层线价成交, 不含各项成本): 资产逐根收益 a 为正的根上 Σ 策略毛收益 / Σ a, 为负的同理
//     全程满多 up = dn = 1, 满空 = −1; 随机择时 (平均净仓位 e) up ≈ dn ≈ e; edge = up − dn = 择时本身挣的, dn = 磨损 (承受的下跌, 做空为负), 成本另计
//   在场段 = 仓位同号非零的连续根 (建仓根到清仓 / 翻向根, 均为收盘), 段收益 = 段内 Π(1 + 净) − 1; 开仓成本归本段 (翻向根的成本按平旧 |p| : 开新 |w| 分给两段)
//   平均仓位 = 逐根 |w| 的均值
//   年成本 = 佣金 + 印花税 + 冲击 + 震荡 + 跳空 (Engine.hpp 成本分项各自 Σ / 年数)
//   一本账 Book = 刀列 → 回测 + 全期指标 + 复利净值 + 成本叠层 (逐项叠回成本的复利净值, 画成压在净值之上的成本带; 策略 / 买入持有同一份)
#include "cta/Engine.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <span>
#include <vector>

namespace cta {

struct Stat {
  double years = 0, cagr = 0, vol = 0, sharpe = 0, sortino = 0, mdd = 0;     // 索提诺 = 均值 / 下行波动 √E[min(r, 0)²]
  double expo = 0, turn = 0;                                                 // 平均仓位 / 年换手 (单边)
  double cost = 0, commission = 0, stamp = 0, impact = 0, chop = 0, gap = 0; // 年成本 = 佣金 + 印花税 + 冲击 + 震荡 + 跳空
  double up = 0, dn = 0, edge = 0;                                           // 上涨 / 下跌捕获, 两者之差
  double trades = 0, win = 0, payoff = 0, hold = 0;                          // 年在场段数 / 胜率 / 盈亏比 (均盈 / 均亏) / 平均在场根数
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
    expo += std::fabs(run.w[i]);
    (a > 0 ? ua : da) += a, (a > 0 ? ug : dg) += run.g[i];
  }
  s.expo = expo / static_cast<double>(n);
  const Costs &k = run.cost;
  s.turn = run.turn / s.years, s.cost = k.sum() / s.years;
  s.commission = k.commission / s.years, s.stamp = k.stamp / s.years, s.impact = k.impact / s.years, s.chop = k.chop / s.years, s.gap = k.gap / s.years;
  s.up = ua > 0 ? ug / ua : kNaN, s.dn = da < 0 ? dg / da : kNaN, s.edge = s.up - s.dn;
  double nw = 0, sw = 0, nl = 0, sl = 0, bars = 0, nav = 1;
  size_t i0 = n; // 在场段建仓根 (n = 不在场)
  const auto seal = [&](size_t i1) {
    const double x = nav - 1;
    (x > 0 ? nw : nl) += 1, (x > 0 ? sw : sl) += x;
    bars += static_cast<double>(i1 - i0);
  };
  for (size_t i = 0; i < n; ++i) {
    const double r = run.r[i], w = run.w[i];
    if (i0 == n) { // 前根空仓
      if (w != 0)
        i0 = i, nav = 1 + r;
      continue;
    }
    if (w * run.w[i0] > 0) {
      nav *= 1 + r;
      continue;
    }
    const Costs &e = run.k[i];
    const double p = run.w[i - 1], open = (e.commission + e.stamp + e.impact) * std::fabs(w) / std::fabs(w - p); // 本根成本里开新仓的那份
    nav *= 1 + r + open;
    seal(i), i0 = n;
    if (w != 0)
      i0 = i, nav = 1 - open;
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
  std::vector<double> nav;                   // [根] Π(1 + 净)
  std::array<std::vector<double>, kNC> lift; // [项][根] 叠回前 j + 1 项成本的复利净值 Π(1 + 净 + Σ_{≤ j} 成本); 末项 = 毛
};

// cut = 刀列 (目标仓位 + 层线), m = 其分层
inline Book book_of(const Tape &t, const Cuts &cut, const Ladder &m, const Setup &u) {
  Book b{simulate(t, cut, m, u), {}, std::vector<double>(t.n), {}};
  b.st = stat_of(t, b.run, u);
  double v = 1;
  for (size_t i = 0; i < t.n; ++i)
    b.nav[i] = v *= 1 + b.run.r[i];
  for (size_t j = 0; j < kNC; ++j) {
    std::vector<double> &o = b.lift[j];
    o.resize(t.n);
    double x = 1;
    for (size_t i = 0; i < t.n; ++i) {
      double r = b.run.r[i];
      for (size_t q = 0; q <= j; ++q)
        r += b.run.k[i].*kCosts[q].f;
      o[i] = x *= 1 + r;
    }
  }
  return b;
}

} // namespace cta
