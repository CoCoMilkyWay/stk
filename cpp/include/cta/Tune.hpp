#pragma once

// 调参: 优化目标 (模型表 kGoals; split = 逐根链的汇 Split, 累成日前缀取窗) + 参数网格 Grid (格点 + 中心可移的网格 + 其上的岭回归曲面)
//   参数轴 = 自 v0 向两侧铺的格点 (相邻之比 kStep; 间距不小于轴的基本单位), 一个配置 = 各轴格点号 Q, 以根计的参数由它查 (Grid::p)
//   网格 = 中心周围各轴 m 个相邻格点的全部组合 (配置数可配, 由各多值轴轮流分得 m); 一点在网格内 = 各轴下标 At
//   每日重拟: 窗 = 此前至多 W 日 (不足 = 自首日起, expanding); 最高点落在网格边上 → 以它为中心移网格再拟 (Trace.hpp)
//   岭回归曲面 (网格上): 特征 = 各多值参数轴的网格内下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值 → X [n][d];
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + X Z y (X 去均值 ⇒ Z · 1 = 0); Z [d][n] 只看网格, 预先算好; 拟合最高点 = 平原顶
#include "cta/Engine.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <vector>

namespace cta {

inline constexpr size_t kMinRoll = 250; // 滚动调参窗下限 (日) = 一年
inline constexpr double kRidge = 1;     // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// 优化目标: 选一个模型 (下标同 kGoals), 逐根核出两项 [a, b], 一段的分数 = Σa / Σb
enum : size_t { kSplit };

struct Goal {
  const char *name, *tip;
};

inline const Goal kGoals[] = {
    {"split", "行情分割质量: 带把行情切成带外的趋势段与带内的震荡段, 切得好 = 趋势吃满、震荡不挨打、换手少; "
              "分数 = (带外顺向 − 带内来回 − 成本) / 总振幅, 对数价单位, 1 ≈ 走过的路程全部顺向吃到; 带内来回在分层仓位坐标里量, 单边穿过不罚; 方向之外的一侧 (空仓) 不计; "
              "成本 = 佣金 + 印花税 + 冲击 + 震荡 + 跳空, 与回测同一份"},
};

// split = 行情分割质量: 逐根两项 [a, b] = [净顺向, 振幅] (见 Split)

// 一段的目标值
inline double score(double a, double b) { return b > 0 ? a / b : 0; }

// 两项的日前缀 [nd + 1] (前导 0): 日段 [d0, d1) 之和 = x[d1] − x[d0]
struct Terms {
  std::vector<double> a, b;
  double over(size_t d0, size_t d1) const { return score(a[d1] - a[d0], b[d1] - b[d0]); }
  bool live(size_t d0, size_t d1) const { return b[d1] > b[d0]; } // 段内有带就绪的根
};

// 目标汇 (逐根, 与损益汇 Pnl 对仗): 一根成交 Fill → 两项; seal 收一日进前缀
//   净顺向 = 带外顺向 − 带内来回 − 成本, 振幅 = |对数涨跌|; 都是对数价 (≈ 满仓收益) 单位, 带就绪 (0 < 下线 < 中线 < 上线, 本根与前根) 才计
//     带外顺向: 前根在上线之上 (满多) → +对数涨跌, 在下线之下 (满空) → −对数涨跌 (随机游走期望 0); 前根在带内、本根破线 → 越线那截记正; 方向之外的一侧 (空仓) 不计
//     带内来回: 带内位置 = 刀的分层坐标 Cut::s ∈ [lo, hi] (等差按价格 / 等比按对数价, 按方向截取; 每层仓位相同), 一步的带内路程 = |Δs| / 2 · ln(上线 / 下线),
//               s 含带本身的漂移; 一段 = s 离开边界 (lo / hi) 到再碰边界 (或带失效), 来回 = 段内路程 − |段内净位移|, 单边穿过不罚
//     成本: 佣金 + 印花税 + 冲击 + 震荡 + 跳空 (Fill::k, 与回测 Pnl 同一份), 窄带的代价
struct Split {
  const Tape &t;
  const float lo, hi;           // 方向 (分层坐标的边界)
  Terms acc;                    // 日前缀
  double net = 0, a = 0, b = 0; // 当前带内段净位移 / 本日两项

  Split(const Tape &t, const Ladder &lad, size_t nd) : t(t), lo(lad.lo), hi(lad.hi) {
    acc.a.reserve(nd + 1), acc.b.reserve(nd + 1);
    acc.a.push_back(0), acc.b.push_back(0);
  }

  void operator()(const Fill &f) {
    const Cut &z = f.z, &x = f.x;
    a -= f.k.sum();
    if (!ready(z) || !ready(x)) {
      a += std::fabs(net), net = 0;
      return;
    }
    const float c0 = t.c[f.i - 1], c = t.c[f.i];
    const double r = t.lr[f.i];
    b += std::fabs(r);
    if (c0 > z.up)
      a += hi * r;
    else if (c0 < z.dn)
      a += lo * r;
    else {
      if (c > x.up)
        a += hi * std::log(static_cast<double>(c) / x.up);
      else if (c < x.dn)
        a -= lo * std::log(static_cast<double>(x.dn) / c);
      const double ds = (static_cast<double>(x.s) - z.s) / 2 * std::log(static_cast<double>(x.up) / x.dn);
      a -= std::fabs(ds), net += ds;
    }
    if (x.s <= lo || x.s >= hi)
      a += std::fabs(net), net = 0;
  }
  void seal() {
    acc.a.push_back(acc.a.back() + a), acc.b.push_back(acc.b.back() + b);
    a = b = 0;
  }
};

// 优化目标 goal 的汇 (分层 lad 定坐标边界): split → Split
inline Split goal_of(size_t goal, const Tape &t, const Ladder &lad, size_t nd) {
  assert(goal == kSplit && "优化目标");
  return Split(t, lad, nd);
}

// 网格的一点: 各轴网格内下标, 按组序拼成一条
using At = std::vector<size_t>;

// 里程表进一格: at[i] ∈ [0, size(i)); 转满一圈 → false
template <class A, class F>
bool tick(A &at, F size) {
  for (size_t i = 0; i < at.size(); ++i) {
    if (++at[i] < size(i))
      return true;
    at[i] = 0;
  }
  return false;
}

// 解 A Z = B: A [d][d] 对称正定 (就地 Cholesky), B [d][m] 就地变成 Z
inline void solve(std::vector<double> &A, std::vector<double> &B, size_t d, size_t m) {
  for (size_t j = 0; j < d; ++j) {
    for (size_t k = 0; k < j; ++k)
      A[j * d + j] -= A[j * d + k] * A[j * d + k];
    assert(A[j * d + j] > 0 && "岭回归: 非正定");
    A[j * d + j] = std::sqrt(A[j * d + j]);
    for (size_t i = j + 1; i < d; ++i) {
      for (size_t k = 0; k < j; ++k)
        A[i * d + j] -= A[i * d + k] * A[j * d + k];
      A[i * d + j] /= A[j * d + j];
    }
  }
  for (size_t c = 0; c < m; ++c) {
    for (size_t i = 0; i < d; ++i) {
      double v = B[i * m + c];
      for (size_t k = 0; k < i; ++k)
        v -= A[i * d + k] * B[k * m + c];
      B[i * m + c] = v / A[i * d + i];
    }
    for (size_t i = d; i-- > 0;) {
      double v = B[i * m + c];
      for (size_t k = i + 1; k < d; ++k)
        v -= A[k * d + i] * B[k * m + c];
      B[i * m + c] = v / A[i * d + i];
    }
  }
}

// 岭回归曲面: ŷ − ȳ = X (Z y)
struct Ridge {
  size_t n = 0, d = 0;
  std::vector<double> X; // [n][d] 去均值特征
  std::vector<double> Z; // [d][n] (XᵀX + λ · n · I)⁻¹ Xᵀ

  // 拟合值 ŷ = ȳ + X (Z y) [n]
  std::vector<double> fit(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    double m = 0;
    for (const double v : y) {
      assert(std::isfinite(v) && "岭回归: 目标值");
      m += v;
    }
    m /= static_cast<double>(n);
    std::vector<double> z(d, 0), o(n, m);
    for (size_t k = 0; k < d; ++k)
      for (size_t j = 0; j < n; ++j)
        z[k] += Z[k * n + j] * y[j];
    for (size_t i = 0; i < n; ++i)
      for (size_t k = 0; k < d; ++k)
        o[i] += X[i * d + k] * z[k];
    return o;
  }
  // 拟合最高点 argmax ŷ
  size_t best(const std::vector<double> &y) const {
    const std::vector<double> f = fit(y);
    return static_cast<size_t>(std::max_element(f.begin(), f.end()) - f.begin());
  }
};

// m = 各轴网格格点数, at = 网格 (各轴网格内下标)
inline Ridge ridge_of(const std::vector<size_t> &m, const std::vector<At> &at) {
  assert(!at.empty() && "岭回归: 空网格");
  const size_t n = at.size();
  std::vector<std::vector<double>> X(n);
  for (size_t s = 0; s < n; ++s) {
    std::vector<double> u;
    std::vector<char> sq;
    for (size_t x = 0; x < m.size(); ++x)
      if (m[x] > 1)
        u.push_back(2.0 * static_cast<double>(at[s][x]) / static_cast<double>(m[x] - 1) - 1), sq.push_back(m[x] > 2);
    for (size_t i = 0; i < u.size(); ++i) {
      X[s].push_back(u[i]);
      if (sq[i])
        X[s].push_back(u[i] * u[i]);
      for (size_t j = 0; j < i; ++j)
        X[s].push_back(u[i] * u[j]);
    }
  }
  const size_t d = X[0].size();
  for (size_t c = 0; c < d; ++c) {
    double m = 0;
    for (const std::vector<double> &r : X)
      m += r[c];
    m /= static_cast<double>(n);
    for (std::vector<double> &r : X)
      r[c] -= m;
  }
  Ridge rg{n, d, std::vector<double>(n * d), std::vector<double>(d * n)};
  std::vector<double> A(d * d, 0);
  for (size_t i = 0; i < d; ++i) {
    for (size_t j = 0; j < d; ++j)
      for (const std::vector<double> &r : X)
        A[i * d + j] += r[i] * r[j];
    A[i * d + i] += kRidge * static_cast<double>(n);
    for (size_t s = 0; s < n; ++s)
      rg.X[s * d + i] = rg.Z[i * n + s] = X[s][i];
  }
  solve(A, rg.Z, d, n);
  return rg;
}

// 一个配置 = 各轴的格点号 q (按组序拼成一条; q = 0 = v0), 参数值查轴的格点表 (Grid::value)
using Q = std::vector<int>;

inline constexpr double kStep = 1.41421356; // 相邻格点之比 (大值处; 小值处退成等差 u)

// 一组模型的格点 + 中心可移的网格 + 网格上的岭回归曲面
//   网格 = 中心 c 周围各轴 m 个相邻格点的全部组合 (收进硬边界); 岭回归只看网格内下标, 与中心无关, 预先算好
struct Grid {
  Kinds kinds{};
  size_t dir = kLong;                   // 方向 (kDirs 下标)
  size_t S = 0;                         // 每日根数 (以日计的轴 × S)
  std::vector<const Axis *> ax;         // [轴] 按组序拼成一条
  std::vector<size_t> off;              // [组 + 1] 组 j 的轴 = [off[j], off[j + 1])
  std::vector<int> lo, hi;              // [轴] 格点号的硬边界 (固定轴 = 0, 0)
  std::vector<std::vector<double>> val; // [轴][q − lo] 格点值
  std::vector<size_t> m;                // [轴] 网格格点数 (固定轴 = 1; 各轴之积 ≤ 配置数)
  std::vector<At> at;                   // [网格配置] 各轴网格内下标 ∈ [0, m)
  Ridge ridge;

  // 轴 a 格点 q 的值 (轴的单位: 日 / 倍数 / 层数)
  double value(size_t a, int q) const { return val[a][static_cast<size_t>(q - lo[a])]; }
  // 初始中心
  Q home() const { return Q(ax.size(), 0); }
  // 中心 c 的网格在轴 a 上的首个格点号 (整格收进硬边界)
  int base(const Q &c, size_t a) const { return std::clamp(c[a] - static_cast<int>(m[a] - 1) / 2, lo[a], hi[a] - static_cast<int>(m[a]) + 1); }
  // 中心 c 的网格 (序同 at)
  std::vector<Q> around(const Q &c) const {
    std::vector<Q> o(at.size(), Q(ax.size()));
    for (size_t a = 0; a < ax.size(); ++a) {
      const int b = base(c, a);
      for (size_t x = 0; x < at.size(); ++x)
        o[x][a] = b + static_cast<int>(at[x][a]);
    }
    return o;
  }
  // q 落在中心 c 的网格边上, 且网格还能往那边移 (未到硬边界)
  bool edge(const Q &c, const Q &q) const {
    for (size_t a = 0; a < ax.size(); ++a) {
      const int b = base(c, a), e = b + static_cast<int>(m[a]) - 1;
      if ((q[a] == b && b > lo[a]) || (q[a] == e && e < hi[a]))
        return true;
    }
    return false;
  }
  // 配置 q 部件 j 的参数 (以根计)
  P p(const Q &q, size_t j) const {
    P v;
    for (size_t a = off[j]; a < off[j + 1]; ++a)
      v.push_back(static_cast<float>(value(a, q[a]) * (ax[a]->day ? static_cast<double>(S) : 1.0)));
    return v;
  }
  // 配置 q 所读的指标列 (取进 Tape, 串行)
  Src src(Tape &t, const Q &q) const { return src_of(t, kinds, p(q, kCenter), p(q, kBand)); }
  // 配置 q 的信号核 (只读 Tape, 可并行)
  Signal signal(const Tape &t, const Q &q, const Src &s) const {
    const float k = p(q, kBand)[1];
    assert(k > 0 && "带半宽倍数");
    return {t, *s.mid, *s.half, k, ladder(q)};
  }
  // 配置 q 的分层
  Ladder ladder(const Q &q) const { return ladder_of(kinds[kLadder], p(q, kLadder), dir); }
};

// S = 每日根数, k = 各组所选模型, dir = 方向, n = 网格配置数上限 (≥ 1)
inline Grid grid_of(size_t S, const Kinds &k, size_t dir, size_t n) {
  assert(n >= 1 && "网格配置数");
  Grid g{.kinds = k, .dir = dir, .S = S};
  g.off.push_back(0);
  for (size_t j = 0; j < kNP; ++j) {
    for (const Axis &a : kParts[j].kinds[k[j]].axes)
      g.ax.push_back(&a);
    g.off.push_back(g.ax.size());
  }
  for (const Axis *a : g.ax) {
    assert(a->lo <= a->v0 && a->v0 <= a->hi && a->u > 0 && "参数轴: 中心在边界内, 单位为正");
    std::vector<double> dn, up; // 自 v0 向下 / 向上
    for (double v = a->v0, w; (w = std::min(v / kStep, v - a->u)) >= a->lo * (1 - 1e-9);)
      dn.push_back(v = w);
    for (double v = a->v0, w; (w = std::max(v * kStep, v + a->u)) <= a->hi * (1 + 1e-9);)
      up.push_back(v = w);
    g.lo.push_back(-static_cast<int>(dn.size()));
    g.hi.push_back(static_cast<int>(up.size()));
    std::vector<double> &t = g.val.emplace_back(dn.rbegin(), dn.rend());
    t.push_back(a->v0);
    t.insert(t.end(), up.begin(), up.end());
  }
  // 配置数分到各轴: 每次给格点最少 (未铺满整轴) 的轴加一个, 积不超过 n
  g.m.assign(g.ax.size(), 1);
  for (size_t prod = 1;;) {
    size_t a = g.ax.size();
    for (size_t b = 0; b < g.ax.size(); ++b)
      if (g.m[b] < static_cast<size_t>(g.hi[b] - g.lo[b] + 1) && (a == g.ax.size() || g.m[b] < g.m[a]))
        a = b;
    if (a == g.ax.size() || prod / g.m[a] * (g.m[a] + 1) > n)
      break;
    prod = prod / g.m[a] * (g.m[a] + 1), ++g.m[a];
  }
  At at(g.ax.size(), 0);
  do
    g.at.push_back(at);
  while (tick(at, [&](size_t i) { return g.m[i]; }));
  g.ridge = ridge_of(g.m, g.at);
  return g;
}

} // namespace cta
