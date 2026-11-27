#pragma once

// 指标: 收益统计 (策略 / 买入持有同一份; 逐日 = 日内各根净收益复利, 年化 / 波动 / 夏普 / 索提诺 / 回撤全走 define/Annual.hpp Daily / Path) + 仓位 / 成本统计
//   平均仓位 = 逐根 |w| 的均值; 年换手 = Σ |Δw| / 年数 (年数 = 日数 / 每年交易日)
//   年成本 = 成本分项各自 Σ / 年数 (Engine.hpp kCosts), 及其和
//   一本账 Book = 一条逐根链的 Run → 全期指标 + 分层复利净值 (逐项叠回成本: 净值 → … → 毛, 画成压在净值之上的成本带; 策略 / 买入持有同一份)
#include "cta/Engine.hpp"
#include "define/Annual.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <span>
#include <utility>
#include <vector>

namespace cta {

struct Stat {
  double years = 0, cagr = 0, vol = 0, sharpe = 0, sortino = 0, mdd = 0; // 回撤 ≤ 0 (净值自前高的最大回落)
  double expo = 0, turn = 0;                                             // 平均仓位 / 年换手 (单边)
  double cost = 0;                                                       // 年成本 (分项之和)
  Costs costs{};                                                         // 年成本分项
};

// 根段 [i0, i1) 的逐日净收益 (日内各根复利; 段首 / 段末的日只计段内的根; 无根的日不计) → Path
inline Path daily_path(std::span<const double> r, const Days &days, size_t i0, size_t i1) {
  Path p;
  size_t d = static_cast<size_t>(std::upper_bound(days.off.begin(), days.off.end(), i0) - days.off.begin()) - 1;
  for (; d < days.size() && days.off[d] < i1; ++d) {
    const size_t lo = std::max(days.off[d], i0), hi = std::min(days.off[d + 1], i1);
    if (lo >= hi)
      continue;
    double g = 1;
    for (size_t i = lo; i < hi; ++i)
      g *= 1 + r[i];
    p.add(g - 1);
  }
  return p;
}

// 根段 [i0, i1) 的指标 (i0 < i1; 段首的调仓量按前一根的仓位算)
inline Stat stat_of(const Run &run, const Days &days, size_t i0, size_t i1) {
  assert(i0 < i1 && i1 <= run.w.size() && "指标根段");
  const Path p = daily_path(run.r, days, i0, i1);
  const Daily w = p.win(0, p.days());
  Stat s;
  s.years = static_cast<double>(p.days()) / kDaysPerYear;
  s.cagr = w.ann(), s.vol = w.vol(), s.sharpe = w.sharpe(), s.sortino = w.sortino(), s.mdd = -p.mdd;
  const size_t n = i1 - i0;
  double expo = 0, turn = 0;
  for (size_t i = i0; i < i1; ++i) {
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

// 一本账: 回测 + 全期指标 (任一根段另由 stat_of 算) + 分层净值
struct Book {
  Run run;
  Stat st;
  std::array<std::vector<double>, kNC + 1> nav; // [层][根] Π(1 + 净 + 前 j 项成本): 层 0 = 净值, 层 kNC = 毛
};

// run = 回测
inline Book book_of(const Tape &t, Run run) {
  assert(run.w.size() == t.n && "回测长度");
  Book b{std::move(run), {}, {}};
  b.st = stat_of(b.run, t.days, 0, t.n);
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
