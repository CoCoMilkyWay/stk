#pragma once

// 减仓 / 跟随调参 (band 与分层规则固定, 不参与): 两个目标 (逐根链出记录, 取根段打分) + 参数网格 Grid (格点 + 中心可移的网格 + 其上的岭回归曲面)
//   两个目标同一形: 一个配置全期一遍出一份记录, 之后任一根段 [i0, i1) → 分 (NaN = 段内不可打分)
//   减仓目标 = 保敞口: 源 Steps (定参减仓核, 只有减仓线: 第 r 份 (r = 1 起) 的线 = 中线 − r · band, 跌破即出, 涨回即入; 与跟随无关) → 逐根两项的前缀 Terms;
//     分 = (顺向 − 翻转成本) / 总振幅, 对数价单位, 1 ≈ 走过的路程全部顺向吃到; 敞口留得多、来回翻得少则高
//   跟随目标 = 每笔中位夏普: 源 Laddered (定参减仓 + 定参跟随的完整分层, Ladder 录逐笔) → 逐笔 Trip; 段内卖出的各笔
//     夏普 = (s · ln(卖价 / 买回价) − 该笔成本) / (卖出当日 band / 卖价 · √持续日), 段末未买回 = 按段末前一根收盘计; 分 = 各笔的中位
//   打分一律跑日线 (日 K 合成的原料带, 每日 1 根): 训练每日一次, 不必逐分钟; 回测才跑原频
//   成本与回测同一份费率; 只读 Tape, 一个配置一个任务
//   参数轴 = 自 v0 向两侧铺的格点 (相邻之比 kStep; 间距不小于轴的基本单位), 一个配置 = 各轴格点号 Q, 参数 (轴的单位) 由它查 (Grid::p); 各轴量定长 Ax (轴数之外不用)
//   网格 = 中心周围各轴 m 个相邻格点的全部组合 (配置数可配, 由各多值轴轮流分得 m); 一点在网格内 = 各轴下标 At
//   每日重拟: 窗 = 此前至多 W 日 (不足 = 自首日起, expanding); 最高点落在网格边上 → 以它为中心移网格再拟 (Trace.hpp)
//   岭回归曲面 (网格上): 特征 = 各多值参数轴的网格内下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值 → X [n][d];
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + X Z y (X 去均值 ⇒ Z · 1 = 0); Z [d][n] 只看网格, 预先算好
//   最高点 = 曲面在网格坐标上的连续最高点 (平原顶; Ridge::peak, 细格预先铺好), 参数取小数格点号处的值 (相邻格点间对数插值; Grid::values)
#include "cta/Engine.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <vector>

namespace cta {

inline constexpr int kGrid = 20;    // 网格配置数默认 (Spec::grid)
inline constexpr double kRidge = 1; // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// ---- 减仓目标 ----

// 保敞口两项的逐根前缀 [n + 1] (前导 0): 根段 [i0, i1) 之和 = x[i1] − x[i0]
//   a = 顺向 − 翻转成本 = 前根仓位 · 对数涨跌 − 本根成本 (佣金 + 印花税 + 冲击; 静态档线无越线差)
//   b = 振幅 |对数涨跌| (前根与本根都就绪才计)
struct Terms {
  std::vector<double> a, b;
  // 分 = Σa / Σb (段内无减仓线就绪的根 = NaN)
  double over(size_t i0, size_t i1) const { return b[i1] > b[i0] ? (a[i1] - a[i0]) / (b[i1] - b[i0]) : kNaN; }
};

// 减仓目标: 减仓模型 kind 定参 p (轴的单位), 份数 U, 费率 u
inline Terms keep_of(const Ctx &x, size_t kind, const P &p, int U, const Rates &u) {
  const Tape &t = x.t;
  Terms o{{0}, {0}};
  o.a.reserve(t.n + 1), o.b.reserve(t.n + 1);
  float ph = kNaN; // 前根间距 (未就绪 = NaN)
  Reduce::with(kind, [&]<class K>() {
    drive(t, u, Steps<K>{K{x}, p, U}, [&](const Fill &f) {
      const double r = f.i ? std::log(static_cast<double>(t.c[f.i]) / t.c[f.i - 1]) : 0;
      o.a.push_back(o.a.back() + f.p * r - f.k.sum());
      o.b.push_back(o.b.back() + (ph > 0 && f.x.h > 0 ? std::fabs(r) : 0));
      ph = f.x.h;
    });
  });
  return o;
}

// ---- 跟随目标 ----

// 定参完整分层的逐笔 (按卖出根升序): 减仓模型 reduce 定参 pr, 跟随模型 follow 定参 pf (轴的单位), 分层规则 z
inline std::vector<Trip> trips_of(const Ctx &x, size_t reduce, size_t follow, const P &pr, const P &pf, const Rule &z) {
  std::vector<Trip> o;
  const size_t nd = x.t.days.size();
  const std::vector<P> vr(nd, pr), vf(nd, pf);
  const std::vector<uint8_t> on(nd, 1);
  Reduce::with(reduce, [&]<class K>() {
    Follow::with(follow, [&]<class F>() {
      Laddered<K, F> src{K{x}, Ladder<F>{x, z}, vr, vf, on};
      src.lad.log = &o;
      drive(x.t, Rates{}, src, [](const Fill &) {});
    });
  });
  return o;
}

// 根段 [i0, i1) 内卖出的各笔的中位夏普 (无笔 = NaN); 顺向 s, 费率 u; buf = 暂存
inline double median_of(const std::vector<Trip> &tr, const Tape &t, size_t i0, size_t i1, float s, const Rates &u, std::vector<double> &buf) {
  const double one = u.commission + u.impact; // 单边佣金 + 冲击
  buf.clear();
  for (auto it = std::ranges::lower_bound(tr, i0, {}, &Trip::i0); it != tr.end() && it->i0 < i1; ++it) {
    const bool shut = it->i1 < i1;
    const size_t j = shut ? it->i1 : i1 - 1;
    const double a = t.c[it->i0], cost = shut ? 2 * one + u.stamp : one + (s > 0 ? u.stamp : 0);
    const double tau = static_cast<double>(std::max<size_t>(j - it->i0, 1)) / static_cast<double>(t.S);
    buf.push_back((s * std::log(a / t.c[j]) - cost) / (it->g / a * std::sqrt(tau)));
  }
  if (buf.empty())
    return kNaN;
  const size_t h = buf.size() / 2;
  std::ranges::nth_element(buf, buf.begin() + static_cast<std::ptrdiff_t>(h));
  if (buf.size() % 2)
    return buf[h];
  return (buf[h] + *std::max_element(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(h))) / 2;
}

// ---- 岭回归曲面 ----

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

// 岭回归曲面: ŷ − ȳ = (特征 − 均值) · (Z y); 最高点在细格 (各多值轴 kPeak 等分) 上取
struct Ridge {
  size_t n = 0, d = 0;
  std::vector<double> Z;     // [d][n] (XᵀX + λ · n · I)⁻¹ Xᵀ
  std::vector<double> F;     // [细格点][d] 去均值特征
  std::vector<Ax<double>> u; // [细格点] 各多值轴坐标 ∈ [−1, 1] (轴序前几个; 无多值轴 = 一点全 0)
  std::vector<double> r2;    // [细格点] 离网格中心的距离²

  // 曲面最高点 (连续): 细格上取最大, 同值取离网格中心近的
  Ax<double> peak(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    std::vector<double> z(d, 0); // 曲面系数 Z y
    for (size_t j = 0; j < n; ++j) {
      assert(std::isfinite(y[j]) && "岭回归: 目标值");
      for (size_t k = 0; k < d; ++k)
        z[k] += Z[k * n + j] * y[j];
    }
    size_t best = 0;
    double top = -INFINITY;
    for (size_t p = 0; p < u.size(); ++p) {
      double v = 0;
      for (size_t k = 0; k < d; ++k)
        v += z[k] * F[p * d + k];
      if (v > top + 1e-12 || (v >= top - 1e-12 && r2[p] < r2[best]))
        top = v, best = p;
    }
    return u[best];
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
  const size_t na = sq.size();
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
  Ridge rg{n, d, std::vector<double>(d * n), {}, {}, {}};
  std::vector<double> A(d * d, 0);
  for (size_t i = 0; i < d; ++i) {
    for (size_t j = 0; j < d; ++j)
      for (const std::vector<double> &r : X)
        A[i * d + j] += r[i] * r[j];
    A[i * d + i] += kRidge * static_cast<double>(n);
    for (size_t s = 0; s < n; ++s)
      rg.Z[i * n + s] = X[s][i];
  }
  solve(A, rg.Z, d, n);
  std::vector<double> f;
  At g{};
  do {
    Ax<double> u{};
    double r = 0;
    for (size_t k = 0; k < na; ++k)
      u[k] = 2.0 * static_cast<double>(g[k]) / kPeak - 1, r += u[k] * u[k];
    feats(sq, u.data(), f);
    for (size_t k = 0; k < d; ++k)
      rg.F.push_back(f[k] - mu[k]);
    rg.u.push_back(u), rg.r2.push_back(r);
  } while (tick(g, na, [](size_t) { return kPeak + 1; }));
  return rg;
}

// ---- 参数网格 ----

// 一个配置 = 各轴的格点号 q (q = 0 = v0), 参数值查轴的格点表 (Grid::value)
using Q = Ax<int>;

inline constexpr double kStep = 1.41421356; // 相邻格点之比 (大值处; 小值处退成等差 u)

// 一个模型的格点 + 中心可移的网格 + 网格上的岭回归曲面
//   网格 = 中心 c 周围各轴 m 个相邻格点的全部组合 (收进硬边界); 岭回归只看网格内下标, 与中心无关, 预先算好; 初始中心 = Q{}
struct Grid {
  const Kind *kind = nullptr;  // 模型 (族表的一项)
  Q lo{}, hi{};                // [轴] 格点号的硬边界
  Ax<std::vector<double>> val; // [轴][q − lo] 格点值
  At m{};                      // [轴] 网格格点数 (各轴之积 ≤ 配置数; 轴数之外 = 1)
  std::vector<At> at;          // [网格配置] 各轴网格内下标 ∈ [0, m)
  Ridge ridge;

  // 轴 a 格点 q 的值 (轴的单位)
  double value(size_t a, int q) const { return val[a][static_cast<size_t>(q - lo[a])]; }
  // 中心 c 的网格在轴 a 上的首个格点号 (整格收进硬边界)
  int base(const Q &c, size_t a) const { return std::clamp(c[a] - static_cast<int>(m[a] - 1) / 2, lo[a], hi[a] - static_cast<int>(m[a]) + 1); }
  // 中心 c 的网格 (序同 at)
  std::vector<Q> around(const Q &c) const {
    std::vector<Q> o(at.size());
    for (size_t a = 0; a < kind->na; ++a) {
      const int b = base(c, a);
      for (size_t x = 0; x < at.size(); ++x)
        o[x][a] = b + static_cast<int>(at[x][a]);
    }
    return o;
  }
  // q 落在中心 c 的网格边上, 且网格还能往那边移 (未到硬边界)
  bool edge(const Q &c, const Q &q) const {
    for (size_t a = 0; a < kind->na; ++a) {
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
  // 中心 c 的网格上的连续点 u (Ridge::peak) → 参数; q = 最近的格点
  P values(const Q &c, const Ax<double> &u, Q &q) const {
    P v{};
    q = {};
    for (size_t a = 0, j = 0; a < kind->na; ++a) {
      const double f = base(c, a) + (m[a] > 1 ? (u[j++] + 1) / 2 * static_cast<double>(m[a] - 1) : 0);
      v[a] = value_at(a, f), q[a] = static_cast<int>(std::lround(f));
    }
    return v;
  }
  // 配置 q 的参数
  P p(const Q &q) const {
    P v{};
    for (size_t a = 0; a < kind->na; ++a)
      v[a] = value(a, q[a]);
    return v;
  }
};

// 轴 a 的格点值 (轴的单位, 升序): 自 v0 向下 / 向上铺, 收进硬边界; v0 的下标 = 向下铺的个数 below
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
inline At split(const Kind &kind, size_t n) {
  assert(n >= 1 && "网格配置数");
  const size_t na = kind.na;
  At m, all;
  m.fill(1), all.fill(1);
  for (size_t k = 0; k < na; ++k) {
    int below;
    all[k] = ticks_of(kind.ax[k], below).size();
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

// kind = 模型, n = 网格配置数上限 (≥ 1)
inline Grid grid_of(const Kind &kind, size_t n) {
  Grid g{.kind = &kind, .m = split(kind, n)};
  for (size_t k = 0; k < kind.na; ++k) {
    int below;
    g.val[k] = ticks_of(kind.ax[k], below);
    g.lo[k] = -below, g.hi[k] = static_cast<int>(g.val[k].size()) - 1 - below;
  }
  At at{};
  do
    g.at.push_back(at);
  while (tick(at, kind.na, [&](size_t i) { return g.m[i]; }));
  g.ridge = ridge_of(g.m, g.at);
  return g;
}

} // namespace cta
