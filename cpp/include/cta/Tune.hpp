#pragma once

// 中线调参 (分层固定, 不参与): 优化目标 (模型表 kGoals; 逐根两项累成日前缀取窗) + 参数网格 Grid (格点 + 中心可移的网格 + 其上的岭回归曲面)
//   目标 = 一条逐根链 (Engine.hpp drive): 源 Switch (定参中线核当开关) → 仓位层不调 → 汇 Capture (日前缀); 成本与回测同一份 costs_of; 只读 Tape, 一个配置一个任务
//   参数轴 = 自 v0 向两侧铺的格点 (相邻之比 kStep; 间距不小于轴的基本单位), 一个配置 = 各轴格点号 Q, 以根计的参数由它查 (Grid::p); 各轴量定长 Ax (轴数之外不用)
//   网格 = 中心周围各轴 m 个相邻格点的全部组合 (配置数可配, 由各多值轴轮流分得 m); 一点在网格内 = 各轴下标 At
//   每日重拟: 窗 = 此前至多 W 日 (不足 = 自首日起, expanding); 最高点落在网格边上 → 以它为中心移网格再拟 (Trace.hpp)
//   岭回归曲面 (网格上): 特征 = 各多值参数轴的网格内下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值 → X [n][d];
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + X Z y (X 去均值 ⇒ Z · 1 = 0); Z [d][n] 只看网格, 预先算好
//   最高点 = 曲面在网格坐标上的连续最高点 (平原顶; Ridge::peak), 参数取小数格点号处的值 (相邻格点间对数插值; Grid::values)
#include "cta/Engine.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iterator>
#include <utility>
#include <vector>

namespace cta {

inline constexpr size_t kMinRoll = 20;   // 滚动调参窗下限 (日) = 一月
inline constexpr size_t kMaxRoll = 2500; // 滚动调参窗上限 (日) = 十年
inline constexpr size_t kRoll = 250;     // 滚动调参窗默认 (日) = 一年
inline constexpr double kRidge = 1;      // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// 优化目标: 选一个模型 (下标同 kGoals), 逐根出两项 [a, b], 一段的分数 = Σa / Σb
enum : size_t { kCapture };

struct Goal {
  const char *name, *tip;
};

inline const Goal kGoals[] = {
    {"capture", "有效多空: 中线当开关 (顺向侧满仓 / 逆向侧空仓), 切得好 = 顺向吃满、来回翻转少; "
                "分数 = (开关顺向 − 翻转成本) / 总振幅, 对数价单位, 1 ≈ 走过的路程全部顺向吃到; 成本 = 佣金 + 印花税 + 冲击, 与回测同一费率; 不含分层"},
};

// 一段的目标值
inline double score(double a, double b) { return b > 0 ? a / b : 0; }

// 两项的日前缀 [nd + 1] (前导 0): 日段 [d0, d1) 之和 = x[d1] − x[d0]
struct Terms {
  std::vector<double> a, b;
  double over(size_t d0, size_t d1) const { return score(a[d1] - a[d0], b[d1] - b[d0]); }
  bool live(size_t d0, size_t d1) const { return b[d1] > b[d0]; } // 段内有中线就绪的根
};

// 调参源: 中线当开关, 仓位 g = s (顺向侧: s · (收 − 中线) ≥ 0) / 0 (逆向侧 / 中线未就绪); 中线核 c 各日同参 p (以根计)
template <class K>
struct Switch {
  K &c;
  const Tape &t;
  P p;
  float s;
  void day(size_t d) { c.day(d, p); }
  Cut operator()(size_t i) {
    const float m = c.bar(i);
    return {.mid = m, .w = s * (t.c[i] - m) >= 0 ? s : 0};
  }
};

// capture 汇 = 有效多空: 逐根两项 [a, b] 累成日前缀 o, 对数价 (≈ 满仓收益) 单位
//   a = 开关顺向 − 翻转成本 = 前根 g · 对数涨跌 − 本根成本 (佣金 + 印花税 + 冲击; 开关无越线差)
//   b = 振幅 |对数涨跌| (前根与本根中线都就绪才计)
struct Capture {
  const Tape &t;
  Terms o;
  double a = 0, b = 0; // 当日累计
  float pm = kNaN;     // 前根中线
  void operator()(const Fill &f) {
    if (f.i) {
      const double r = t.lr[f.i];
      a += f.p * r;
      if (pm > 0 && f.x.mid > 0)
        b += std::fabs(r);
    }
    a -= f.k.sum(), pm = f.x.mid;
  }
  void close(size_t) { o.a.push_back(o.a.back() + a), o.b.push_back(o.b.back() + b), a = b = 0; }
};

// 优化目标 goal 的日前缀: 中线模型 kind 定参 p (以根计), 顺向 s, 费率 u
inline Terms goal_of(size_t goal, const Tape &t, size_t kind, const P &p, float s, const Rates &u) {
  assert(goal == kCapture && "优化目标");
  Capture sink{t};
  sink.o.a.reserve(t.days.size() + 1), sink.o.b.reserve(t.days.size() + 1);
  sink.o.a.push_back(0), sink.o.b.push_back(0);
  with_center(kind, t, [&]<class K>(K &k) { drive(t, u, Size{t}, Switch<K>{k, t, p, s}, sink); });
  return std::move(sink.o);
}

// 网格的一点: 各轴网格内下标
using At = Ax<size_t>;

// 里程表进一格: 前 n 轴 at[i] ∈ [0, size(i)); 转满一圈 → false
template <class A, class F>
bool tick(A &at, size_t n, F size) {
  for (size_t i = 0; i < n; ++i) {
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

inline constexpr size_t kPeak = 64; // 曲面最高点: 各多值轴 [−1, 1] 的细分份数

// 特征 (去均值前): 各多值轴坐标 u 的 u, u² (sq = ≥ 3 值的轴), 两两 u · u'
inline void feats(const std::vector<char> &sq, const double *u, std::vector<double> &f) {
  f.clear();
  for (size_t i = 0; i < sq.size(); ++i) {
    f.push_back(u[i]);
    if (sq[i])
      f.push_back(u[i] * u[i]);
    for (size_t j = 0; j < i; ++j)
      f.push_back(u[i] * u[j]);
  }
}

// 岭回归曲面: ŷ − ȳ = (特征 − 均值) · (Z y)
struct Ridge {
  size_t n = 0, d = 0;
  std::vector<char> sq;   // [多值轴] 有平方项
  std::vector<double> mu; // [d] 特征均值
  std::vector<double> X;  // [n][d] 去均值特征
  std::vector<double> Z;  // [d][n] (XᵀX + λ · n · I)⁻¹ Xᵀ

  // 曲面系数 z = Z y [d]
  std::vector<double> coef(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    std::vector<double> z(d, 0);
    for (size_t j = 0; j < n; ++j) {
      assert(std::isfinite(y[j]) && "岭回归: 目标值");
      for (size_t k = 0; k < d; ++k)
        z[k] += Z[k * n + j] * y[j];
    }
    return z;
  }
  // 曲面最高点 (连续): 各多值轴坐标 u ∈ [−1, 1] (前 sq.size() 个, 轴序), 每轴 kPeak 等分的细格上取最大, 同值取离网格中心近的; 无多值轴 = 全 0
  Ax<double> peak(const std::vector<double> &y) const {
    const std::vector<double> z = coef(y);
    const size_t a = sq.size();
    Ax<double> u{}, best{};
    std::vector<double> f;
    At at{};
    double top = -INFINITY, r2 = INFINITY;
    if (a)
      do {
        double r = 0, v = 0;
        for (size_t k = 0; k < a; ++k)
          u[k] = 2.0 * static_cast<double>(at[k]) / kPeak - 1, r += u[k] * u[k];
        feats(sq, u.data(), f);
        for (size_t k = 0; k < d; ++k)
          v += z[k] * (f[k] - mu[k]);
        if (v > top + 1e-12 || (v >= top - 1e-12 && r < r2))
          top = v, r2 = r, best = u;
      } while (tick(at, a, [](size_t) { return kPeak + 1; }));
    return best;
  }
};

// m = 各轴网格格点数 (轴数之外 = 1), at = 网格 (各轴网格内下标)
inline Ridge ridge_of(const At &m, const std::vector<At> &at) {
  assert(!at.empty() && "岭回归: 空网格");
  const size_t n = at.size();
  std::vector<char> sq;
  for (const size_t k : m)
    if (k > 1)
      sq.push_back(k > 2);
  std::vector<std::vector<double>> X(n);
  for (size_t s = 0; s < n; ++s) {
    std::vector<double> u;
    for (size_t x = 0; x < m.size(); ++x)
      if (m[x] > 1)
        u.push_back(2.0 * static_cast<double>(at[s][x]) / static_cast<double>(m[x] - 1) - 1);
    feats(sq, u.data(), X[s]);
  }
  const size_t d = X[0].size();
  std::vector<double> mu(d, 0);
  for (size_t c = 0; c < d; ++c) {
    for (const std::vector<double> &r : X)
      mu[c] += r[c];
    mu[c] /= static_cast<double>(n);
    for (std::vector<double> &r : X)
      r[c] -= mu[c];
  }
  Ridge rg{n, d, std::move(sq), std::move(mu), std::vector<double>(n * d), std::vector<double>(d * n)};
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

// 一个配置 = 各轴的格点号 q (q = 0 = v0), 参数值查轴的格点表 (Grid::value)
using Q = Ax<int>;
// 一组参数, 轴的单位 (以日计的轴 = 日)
using V = Ax<double>;

inline constexpr double kStep = 1.41421356; // 相邻格点之比 (大值处; 小值处退成等差 u)

// 一个中线模型的格点 + 中心可移的网格 + 网格上的岭回归曲面
//   网格 = 中心 c 周围各轴 m 个相邻格点的全部组合 (收进硬边界); 岭回归只看网格内下标, 与中心无关, 预先算好
struct Grid {
  size_t kind = kEma;          // 中线模型 (kCenters 下标)
  size_t S = 0;                // 每日根数 (以日计的轴 × S)
  size_t na = 0;               // 轴数 (≤ kAxes)
  Ax<const Axis *> ax{};       // [轴]
  Q lo{}, hi{};                // [轴] 格点号的硬边界
  Ax<std::vector<double>> val; // [轴][q − lo] 格点值
  At m{};                      // [轴] 网格格点数 (各轴之积 ≤ 配置数; 轴数之外 = 1)
  std::vector<At> at;          // [网格配置] 各轴网格内下标 ∈ [0, m)
  Ridge ridge;

  // 轴 a 格点 q 的值 (轴的单位; 以日计的轴 = 日)
  double value(size_t a, int q) const { return val[a][static_cast<size_t>(q - lo[a])]; }
  // 初始中心
  Q home() const { return {}; }
  // 中心 c 的网格在轴 a 上的首个格点号 (整格收进硬边界)
  int base(const Q &c, size_t a) const { return std::clamp(c[a] - static_cast<int>(m[a] - 1) / 2, lo[a], hi[a] - static_cast<int>(m[a]) + 1); }
  // 中心 c 的网格 (序同 at)
  std::vector<Q> around(const Q &c) const {
    std::vector<Q> o(at.size());
    for (size_t a = 0; a < na; ++a) {
      const int b = base(c, a);
      for (size_t x = 0; x < at.size(); ++x)
        o[x][a] = b + static_cast<int>(at[x][a]);
    }
    return o;
  }
  // q 落在中心 c 的网格边上, 且网格还能往那边移 (未到硬边界)
  bool edge(const Q &c, const Q &q) const {
    for (size_t a = 0; a < na; ++a) {
      const int b = base(c, a), e = b + static_cast<int>(m[a]) - 1;
      if ((q[a] == b && b > lo[a]) || (q[a] == e && e < hi[a]))
        return true;
    }
    return false;
  }
  // 轴 a 小数格点号 f 的值: 相邻两格点间按对数插值, 收进硬边界
  double value_at(size_t a, double f) const {
    f = std::clamp(f, static_cast<double>(lo[a]), static_cast<double>(hi[a]));
    const int q = std::min(static_cast<int>(std::floor(f)), std::max(lo[a], hi[a] - 1));
    const double w = f - q;
    return w > 0 ? std::exp(std::log(value(a, q)) * (1 - w) + std::log(value(a, q + 1)) * w) : value(a, q);
  }
  // 中心 c 的网格上的连续点 u (Ridge::peak) → 各轴参数 (轴的单位); q = 最近的格点
  V values(const Q &c, const Ax<double> &u, Q &q) const {
    V v{};
    q = {};
    for (size_t a = 0, j = 0; a < na; ++a) {
      const double f = base(c, a) + (m[a] > 1 ? (u[j++] + 1) / 2 * static_cast<double>(m[a] - 1) : 0);
      v[a] = value_at(a, f), q[a] = static_cast<int>(std::lround(f));
    }
    return v;
  }
  // 参数 v (轴的单位) → 以根计
  P bars(const V &v) const {
    P o{};
    for (size_t a = 0; a < na; ++a)
      o[a] = static_cast<float>(v[a] * (ax[a]->day ? static_cast<double>(S) : 1.0));
    return o;
  }
  // 配置 q 的参数 (以根计)
  P p(const Q &q) const {
    V v{};
    for (size_t a = 0; a < na; ++a)
      v[a] = value(a, q[a]);
    return bars(v);
  }
};

// S = 每日根数, kind = 中线模型, n = 网格配置数上限 (≥ 1)
inline Grid grid_of(size_t S, size_t kind, size_t n) {
  assert(n >= 1 && "网格配置数");
  assert(kind < std::size(kCenters) && "中线模型");
  const std::vector<Axis> &axes = kCenters[kind].axes;
  assert(axes.size() <= kAxes && "参数轴数");
  Grid g{.kind = kind, .S = S, .na = axes.size()};
  g.m.fill(1);
  for (size_t k = 0; k < g.na; ++k) {
    const Axis *a = g.ax[k] = &axes[k];
    assert(a->lo <= a->v0 && a->v0 <= a->hi && a->u > 0 && "参数轴: 中心在边界内, 单位为正");
    std::vector<double> dn, up; // 自 v0 向下 / 向上
    for (double v = a->v0, w; (w = std::min(v / kStep, v - a->u)) >= a->lo * (1 - 1e-9);)
      dn.push_back(v = w);
    for (double v = a->v0, w; (w = std::max(v * kStep, v + a->u)) <= a->hi * (1 + 1e-9);)
      up.push_back(v = w);
    g.lo[k] = -static_cast<int>(dn.size()), g.hi[k] = static_cast<int>(up.size());
    std::vector<double> &t = g.val[k];
    t.assign(dn.rbegin(), dn.rend());
    t.push_back(a->v0);
    t.insert(t.end(), up.begin(), up.end());
  }
  // 配置数分到各轴: 每次给格点最少 (未铺满整轴) 的轴加一个, 积不超过 n
  for (size_t prod = 1;;) {
    size_t a = g.na;
    for (size_t b = 0; b < g.na; ++b)
      if (g.m[b] < static_cast<size_t>(g.hi[b] - g.lo[b] + 1) && (a == g.na || g.m[b] < g.m[a]))
        a = b;
    if (a == g.na || prod / g.m[a] * (g.m[a] + 1) > n)
      break;
    prod = prod / g.m[a] * (g.m[a] + 1), ++g.m[a];
  }
  At at{};
  do
    g.at.push_back(at);
  while (tick(at, g.na, [&](size_t i) { return g.m[i]; }));
  g.ridge = ridge_of(g.m, g.at);
  return g;
}

} // namespace cta
