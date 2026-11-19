#pragma once

// 减仓线 / 加仓线调参 (band 与分层规则固定, 不参与): 两个目标 (逐根链出分, 取窗) + 参数网格 Grid (格点 + 中心可移的网格 + 其上的岭回归曲面)
//   减仓线目标 = 保敞口: 源 Steps (定参减仓线核当静态档线: 第 r 份的线 = 减仓线 − r · band, 跌破即出, 涨回即入; 与加仓线无关) → 汇 Keep (逐根两项累成日前缀);
//     分 = (顺向 − 翻转成本) / 总振幅, 对数价单位, 1 ≈ 走过的路程全部顺向吃到; 敞口留得多、来回翻得少则高
//   加仓线目标 = 每笔中位夏普: 源 Laddered (定参减仓线 + 定参加仓线的完整分层, Ladder 录逐笔) → 空汇; 窗内卖出的各笔 (Trip)
//     夏普 = (s · ln(卖价 / 买回价) − 该笔成本) / (卖出当日 band / 卖价 · √持续日), 窗末未买回 = 按窗末前一根收盘计; 分 = 各笔的中位
//   成本与回测同一份费率; 只读 Tape, 一个配置一个任务
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
#include <utility>
#include <vector>

namespace cta {

inline constexpr int kGrid = 20;    // 网格配置数默认 (Spec::grid)
inline constexpr double kRidge = 1; // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// ---- 减仓线目标 ----

// 两项的日前缀 [nd + 1] (前导 0): 日段 [d0, d1) 之和 = x[d1] − x[d0]; 分 = Σa / Σb
struct Terms {
  std::vector<double> a, b;
  double over(size_t d0, size_t d1) const { return b[d1] > b[d0] ? (a[d1] - a[d0]) / (b[d1] - b[d0]) : 0; }
  bool live(size_t d0, size_t d1) const { return b[d1] > b[d0]; } // 段内有减仓线就绪的根
};

// 减仓线调参源: 静态档线, 第 r 份 (r ∈ [0, U)) 持有 ⇔ s · 收 ≥ s · (减仓线 − s · r · g); 仓位 = s · 持有份数 / U, 未就绪 = 0; 减仓线核 c 各日同参 p (以根计)
template <class K>
struct Steps {
  K &c;
  const Tape &t;
  const Col &N; // [日] band
  P p;
  float s;
  int U;
  double g = kNaN;
  void day(size_t d) { c.day(d, p), g = N[d]; }
  Cut operator()(size_t i) {
    const float m = c.bar(i);
    if (!(m > 0 && g > 0))
      return {.mid = m};
    const int out = std::clamp(static_cast<int>(std::ceil(s * (m - t.c[i]) / g)), 0, U); // 跌破了几条
    return {.mid = m, .h = static_cast<float>(g), .w = s * static_cast<float>(U - out) / static_cast<float>(U)};
  }
};

// 保敞口汇: 逐根两项 [a, b] 累成日前缀 o, 对数价 (≈ 满仓收益) 单位
//   a = 顺向 − 翻转成本 = 前根仓位 · 对数涨跌 − 本根成本 (佣金 + 印花税 + 冲击; 静态档线无越线差)
//   b = 振幅 |对数涨跌| (前根与本根都就绪才计)
struct Keep {
  const Tape &t;
  Terms o;
  double a = 0, b = 0; // 当日累计
  float ph = kNaN;     // 前根间距 (未就绪 = NaN)
  void operator()(const Fill &f) {
    if (f.i) {
      const double r = t.lr[f.i];
      a += f.p * r;
      if (ph > 0 && f.x.h > 0)
        b += std::fabs(r);
    }
    a -= f.k.sum(), ph = f.x.h;
  }
  void close(size_t) { o.a.push_back(o.a.back() + a), o.b.push_back(o.b.back() + b), a = b = 0; }
};

// 减仓线目标的日前缀: 减仓线模型 kind 定参 p (以根计), band N, 顺向 s, 份数 U, 费率 u
inline Terms keep_of(const Tape &t, size_t kind, const P &p, const Col &N, float s, int U, const Rates &u) {
  Keep sink{t};
  sink.o.a.reserve(t.days.size() + 1), sink.o.b.reserve(t.days.size() + 1);
  sink.o.a.push_back(0), sink.o.b.push_back(0);
  with_reduce(kind, t, [&]<class K>(K &k) { drive(t, u, Steps<K>{k, t, N, p, s, U}, sink); });
  return std::move(sink.o);
}

// ---- 加仓线目标 ----

// 分层源: 减仓线核 c (日 d 用 pr[d]) + band N [日] → 分层 lad (日 d 新卖出的份的加仓线用 pa[d], 之后不变); sel 未选参日 = Hold (分层复位; nullptr = 各日都选),
//   刀未就绪 = Hold; 核逐根都走 (状态接续)
template <class K, class A>
struct Laddered {
  K &c;
  Ladder<A> &lad;
  const Col &N;
  const std::vector<P> &pr, &pa;
  const std::vector<uint8_t> *sel = nullptr;
  size_t d = 0;
  bool on(size_t x) const { return !sel || (*sel)[x]; }
  void day(size_t x) {
    c.day(d = x, pr[x]), lad.day(x, pa[x]);
    if (!on(x))
      lad.reset(lad.t.days.off[x]);
  }
  Cut operator()(size_t i) {
    const float m = c.bar(i);
    const Cut x = on(d) ? lad(i, m, N[d]) : Cut{};
    return x.h > 0 ? x : Hold{}(i);
  }
};

// 定参完整分层的逐笔 (按卖出根升序): 减仓线模型 reduce 定参 pr, 加仓线模型 add 定参 pa (以根计), band N, 顺向 s, 分层规则 z
inline std::vector<Trip> trips_of(const Tape &t, size_t reduce, size_t add, const P &pr, const P &pa, const Col &N, float s, const Rule &z) {
  std::vector<Trip> o;
  const std::vector<P> vr(t.days.size(), pr), va(t.days.size(), pa);
  with_reduce(reduce, t, [&]<class K>(K &k) {
    with_add(add, t, s, [&]<class A>(A &a) {
      Ladder<A> lad{t, s, z, a};
      lad.log = &o;
      drive(t, Rates{}, Laddered<K, A>{k, lad, N, vr, va}, Skip{});
    });
  });
  return o;
}

// 日段 [d0, d1) 内卖出的各笔的中位夏普 (无笔 = 0; 有笔则 live = true); 顺向 s, 费率 u; buf = 暂存
inline double median_of(const std::vector<Trip> &tr, const Tape &t, size_t d0, size_t d1, float s, const Rates &u, bool &live, std::vector<double> &buf) {
  const size_t b = t.days.off[d0], e = t.days.off[d1];
  const double one = u.commission + u.impact; // 单边佣金 + 冲击
  buf.clear();
  for (auto it = std::ranges::lower_bound(tr, b, {}, &Trip::i0); it != tr.end() && it->i0 < e; ++it) {
    const bool shut = it->i1 < e;
    const size_t j = shut ? it->i1 : e - 1;
    const double a = t.c[it->i0], cost = shut ? 2 * one + u.stamp : one + (s > 0 ? u.stamp : 0);
    const double tau = static_cast<double>(std::max<size_t>(j - it->i0, 1)) / static_cast<double>(t.S);
    buf.push_back((s * std::log(a / t.c[j]) - cost) / (it->g / a * std::sqrt(tau)));
  }
  if (buf.empty())
    return 0;
  live = true;
  const size_t h = buf.size() / 2;
  std::ranges::nth_element(buf, buf.begin() + static_cast<std::ptrdiff_t>(h));
  if (buf.size() % 2)
    return buf[h];
  return (buf[h] + *std::max_element(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(h))) / 2;
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

// 一个模型的格点 + 中心可移的网格 + 网格上的岭回归曲面
//   网格 = 中心 c 周围各轴 m 个相邻格点的全部组合 (收进硬边界); 岭回归只看网格内下标, 与中心无关, 预先算好
struct Grid {
  const Kind *model = nullptr; // 模型 (kReduceKinds / kAddKinds 的一项)
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

// 轴 a 的格点值 (轴的单位, 升序): 自 v0 向下 / 向上铺, 收进硬边界; v0 的下标 = 向下铺的个数
inline std::vector<double> ticks_of(const Axis &a, int &below) {
  assert(a.lo <= a.v0 && a.v0 <= a.hi && a.u > 0 && "参数轴: 中心在边界内, 单位为正");
  std::vector<double> dn, up;
  for (double v = a.v0, w; (w = std::min(v / kStep, v - a.u)) >= a.lo * (1 - 1e-9);)
    dn.push_back(v = w);
  for (double v = a.v0, w; (w = std::max(v * kStep, v + a.u)) <= a.hi * (1 + 1e-9);)
    up.push_back(v = w);
  below = static_cast<int>(dn.size());
  std::vector<double> t(dn.rbegin(), dn.rend());
  t.push_back(a.v0);
  t.insert(t.end(), up.begin(), up.end());
  return t;
}

// 网格配置数 n 分到模型各轴的格点数: 每次给格点最少 (未铺满整轴) 的轴加一个, 积不超过 n; 轴数之外 = 1
inline At split(const Kind &model, size_t n) {
  assert(n >= 1 && "网格配置数");
  const size_t na = model.axes.size();
  assert(na <= kAxes && "参数轴数");
  At m, all;
  m.fill(1), all.fill(1);
  for (size_t k = 0; k < na; ++k) {
    int below;
    all[k] = ticks_of(model.axes[k], below).size();
  }
  for (size_t prod = 1;;) {
    size_t a = na;
    for (size_t b = 0; b < na; ++b)
      if (m[b] < all[b] && (a == na || m[b] < m[a]))
        a = b;
    if (a == na || prod / m[a] * (m[a] + 1) > n)
      break;
    prod = prod / m[a] * (m[a] + 1), ++m[a];
  }
  return m;
}

// S = 每日根数, model = 模型, n = 网格配置数上限 (≥ 1)
inline Grid grid_of(size_t S, const Kind &model, size_t n) {
  Grid g{.model = &model, .S = S, .na = model.axes.size(), .m = split(model, n)};
  for (size_t k = 0; k < g.na; ++k) {
    int below;
    g.val[k] = ticks_of(*(g.ax[k] = &model.axes[k]), below);
    g.lo[k] = -below, g.hi[k] = static_cast<int>(g.val[k].size()) - 1 - below;
  }
  At at{};
  do
    g.at.push_back(at);
  while (tick(at, g.na, [&](size_t i) { return g.m[i]; }));
  g.ridge = ridge_of(g.m, g.at);
  return g;
}

} // namespace cta
