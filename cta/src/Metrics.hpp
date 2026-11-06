#pragma once

// 指标: 收益序列统计 (组合 / 单资产同一份) + 单资产的择时 / 交易统计 + 方差比
//   择时 = 上涨捕获 up / 下跌捕获 dn (毛, 不含成本): 资产逐根收益 a 为正的根上 Σ 策略毛收益 / Σ a, 为负的同理
//     全程满仓 up = dn = 1; 随机择时 (平均仓位 e) up ≈ dn ≈ e; edge = up − dn = 择时本身挣的, dn = 磨损 (承受的下跌), 成本另计
#include "Engine.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>
#include <vector>

struct Stat {
  double years = 0, cagr = 0, vol = 0, sharpe = 0, mdd = 0;
  double expo = 0, turn = 0, cost = 0;              // 平均仓位 / 年换手 (单边) / 年成本
  double up = 0, dn = 0, edge = 0;                  // 上涨 / 下跌捕获, 两者之差
  double trades = 0, win = 0, payoff = 0, hold = 0; // 年在场段数 / 胜率 / 盈亏比 (均盈 / 均亏) / 平均在场根数
};

inline constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

inline Stat returns_stat(std::span<const double> r, double bpy) {
  Stat s;
  const double n = static_cast<double>(r.size());
  s.years = n / bpy;
  double sum = 0, sq = 0, lg = 0, nav = 1, peak = 1;
  for (const double x : r) {
    sum += x, sq += x * x, lg += std::log1p(x);
    nav *= 1 + x, peak = std::max(peak, nav);
    s.mdd = std::min(s.mdd, nav / peak - 1);
  }
  const double mean = sum / n, sd = std::sqrt(std::max(0.0, sq / n - mean * mean));
  s.vol = sd * std::sqrt(bpy);
  s.sharpe = sd > 0 ? mean / sd * std::sqrt(bpy) : 0;
  s.cagr = std::expm1(lg / s.years);
  return s;
}

inline Stat stat_of(const Series &se, const Run &run, const Setup &u) {
  Stat s = returns_stat(run.r, u.bpy);
  const size_t n = run.r.size();
  double expo = 0, ua = 0, ug = 0, da = 0, dg = 0;
  for (size_t i = 0; i < n; ++i) {
    expo += run.w[i];
    if (i == 0)
      continue;
    const double a = se.bar[i].c / se.bar[i - 1].c - 1;
    (a > 0 ? ua : da) += a, (a > 0 ? ug : dg) += run.gross[i];
  }
  s.expo = expo / static_cast<double>(n);
  s.turn = run.turn / s.years, s.cost = s.turn * u.cost;
  s.up = ua > 0 ? ug / ua : kNaN, s.dn = da < 0 ? dg / da : kNaN, s.edge = s.up - s.dn;
  double nw = 0, sw = 0, nl = 0, sl = 0, bars = 0;
  for (const Trade &t : run.trades) {
    (t.ret > 0 ? nw : nl) += 1, (t.ret > 0 ? sw : sl) += t.ret;
    bars += static_cast<double>(t.i1 - t.i0 + 1);
  }
  const double nt = nw + nl;
  s.trades = nt / s.years;
  s.win = nt > 0 ? nw / nt : kNaN;
  s.payoff = nw > 0 && nl > 0 && sl < 0 ? (sw / nw) / (-sl / nl) : kNaN;
  s.hold = nt > 0 ? bars / nt : kNaN;
  return s;
}

// 方差比 VR(k) = Var(k 根对数收益和) / (k · Var(单根)), 重叠窗: > 1 = 该周期上有趋势 (正自相关), < 1 = 均值回归
inline double variance_ratio(const Series &se, size_t k) {
  const size_t n = se.bar.size();
  if (n <= 2 * k + 1)
    return kNaN;
  std::vector<double> lp(n); // 对数价
  for (size_t i = 0; i < n; ++i)
    lp[i] = std::log(se.bar[i].c);
  const auto var = [&](size_t h) {
    double s = 0, q = 0;
    for (size_t i = h; i < n; ++i) {
      const double x = lp[i] - lp[i - h];
      s += x, q += x * x;
    }
    const double m = static_cast<double>(n - h);
    return q / m - (s / m) * (s / m);
  };
  return var(k) / (static_cast<double>(k) * var(1));
}

// 中位数 (跳过 NaN; 全 NaN → NaN)
inline double median(std::vector<double> v) {
  std::erase_if(v, [](double x) { return std::isnan(x); });
  if (v.empty())
    return kNaN;
  const auto mid = v.begin() + static_cast<long>(v.size() / 2);
  std::nth_element(v.begin(), mid, v.end());
  return *mid;
}
