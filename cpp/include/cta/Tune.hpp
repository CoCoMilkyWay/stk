#pragma once

// 调参: 优化目标 (模型表 kGoals; split = 逐根核 Split, 累成日前缀取窗) + 参数网格 Grid (网格 + 其上的岭回归曲面)
//   网格 = 各组所选模型参数轴的全部组合; 一点 = 各轴取值下标 At, 以根计的参数由它查 (Grid::p)
//   每日重拟: 窗 = 此前至多 W 日 (不足 = 自首日起, expanding), 参数逐日微调不跳变
//   岭回归曲面 (一组模型的网格上): 特征 = 各多值参数轴的下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值 → X [n][d];
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + X Z y (X 去均值 ⇒ Z · 1 = 0); Z [d][n] 只看网格, 预先算好; 拟合最高点 = 平原顶
#include "cta/Engine.hpp"

#include <cassert>
#include <cmath>
#include <limits>
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

// 目标核 (逐根, 与损益核 Pnl 对仗): 一刀 x + 实际仓位 w (仓位层之后) → 两项; seal 收一日进前缀
//   净顺向 = 带外顺向 − 带内来回 − 成本, 振幅 = |对数涨跌|; 都是对数价 (≈ 满仓收益) 单位, 带就绪 (0 < 下线 < 中线 < 上线, 本根与前根) 才计
//     带外顺向: 前根在上线之上 (满多) → +对数涨跌, 在下线之下 (满空) → −对数涨跌 (随机游走期望 0); 前根在带内、本根破线 → 越线那截记正; 方向之外的一侧 (空仓) 不计
//     带内来回: 带内位置 s = 分层的仓位坐标 Ladder::s ∈ [lo, hi] (等差按价格 / 等比按对数价, 按方向截取; 每层仓位相同), 一步的带内路程 = |Δs| / 2 · ln(上线 / 下线),
//               s 含带本身的漂移; 一段 = s 离开边界 (lo / hi) 到再碰边界 (或带失效), 来回 = 段内路程 − |段内净位移|, 单边穿过不罚
//     成本: 佣金 + 印花税 + 冲击 + 震荡 + 跳空 (costs_of, 与回测 Pnl 同一份), 窄带的代价
struct Split {
  const Tape &t;
  const Setup &u;
  const Ladder m;               // 分层: 仓位坐标 (等差 / 等比) + 层线
  Terms acc;                    // 日前缀
  Cut q;                        // 前根的刀 (起始 = 未就绪)
  float p = 0;                  // 前根实际仓位
  double net = 0, a = 0, b = 0; // 当前带内段净位移 / 本日两项

  Split(const Tape &t, const Setup &u, const Ladder &m, size_t nd) : t(t), u(u), m(m) {
    acc.a.reserve(nd + 1), acc.b.reserve(nd + 1);
    acc.a.push_back(0), acc.b.push_back(0);
  }

  void operator()(size_t i, const Cut &x, float w) {
    a -= costs_of(u, m, t.c[i ? i - 1 : 0], t.c[i], q.w, x, p, w).sum();
    p = w;
    const Cut z = q;
    q = x;
    if (!ready(z) || !ready(x)) {
      a += std::fabs(net), net = 0;
      return;
    }
    const float c0 = t.c[i - 1], c = t.c[i];
    const double r = t.lr[i];
    b += std::fabs(r);
    if (c0 > z.up)
      a += m.hi * r;
    else if (c0 < z.dn)
      a += m.lo * r;
    else {
      if (c > x.up)
        a += m.hi * std::log(static_cast<double>(c) / x.up);
      else if (c < x.dn)
        a -= m.lo * std::log(static_cast<double>(x.dn) / c);
      const double ds = (m.s(c, x) - m.s(c0, z)) / 2 * std::log(static_cast<double>(x.up) / x.dn);
      a -= std::fabs(ds), net += ds;
    }
    if (const double s = m.s(c, x); s <= m.lo || s >= m.hi)
      a += std::fabs(net), net = 0;
  }
  void seal() {
    acc.a.push_back(acc.a.back() + a), acc.b.push_back(acc.b.back() + b);
    a = b = 0;
  }
};

// 优化目标 goal 的核 (分层 m 定仓位坐标与层线): split → Split
inline Split goal_of(size_t goal, const Tape &t, const Setup &u, const Ladder &m, size_t nd) {
  assert(goal == kSplit && "优化目标");
  return Split(t, u, m, nd);
}

// 网格的一点: 各组所选模型的参数轴取值下标, 按组序拼成一条
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

// 一组模型 k 的各参数轴, 按组序拼成一条
inline std::vector<const Axis *> axes_of(const Kinds &k) {
  std::vector<const Axis *> ax;
  for (size_t j = 0; j < kNP; ++j)
    for (const Axis &a : kParts[j].kinds[k[j]].axes)
      ax.push_back(&a);
  return ax;
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

  // 拟合最高点 argmax X (Z y)
  size_t best(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    for ([[maybe_unused]] const double v : y)
      assert(std::isfinite(v) && "岭回归: 目标值");
    std::vector<double> z(d, 0);
    for (size_t k = 0; k < d; ++k)
      for (size_t j = 0; j < n; ++j)
        z[k] += Z[k * n + j] * y[j];
    size_t b = 0;
    double top = -std::numeric_limits<double>::infinity();
    for (size_t i = 0; i < n; ++i) {
      double f = 0;
      for (size_t k = 0; k < d; ++k)
        f += X[i * d + k] * z[k];
      if (f > top)
        top = f, b = i;
    }
    return b;
  }
};

// ax = 一组模型的参数轴, at = 其网格
inline Ridge ridge_of(const std::vector<const Axis *> &ax, const std::vector<At> &at) {
  assert(!at.empty() && "岭回归: 空网格");
  const size_t n = at.size();
  std::vector<std::vector<double>> X(n);
  for (size_t s = 0; s < n; ++s) {
    std::vector<double> u;
    std::vector<char> sq;
    for (size_t x = 0; x < ax.size(); ++x)
      if (const size_t m = ax[x]->v.size(); m > 1)
        u.push_back(2.0 * static_cast<double>(at[s][x]) / static_cast<double>(m - 1) - 1), sq.push_back(m > 2);
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

// 一组模型的参数网格 + 其上的岭回归曲面
struct Grid {
  Kinds kinds{};
  size_t dir = kLong; // 方向 (kDirs 下标)
  size_t S = 0;       // 每日根数 (以日计的轴 × S)
  std::vector<At> at; // [配置]
  Ridge ridge;

  // 配置 x 部件 j 的参数 (以根计)
  P p(size_t x, size_t j) const {
    size_t o = 0;
    for (size_t i = 0; i < j; ++i)
      o += kParts[i].kinds[kinds[i]].axes.size();
    const std::vector<Axis> &ax = kParts[j].kinds[kinds[j]].axes;
    P v(ax.size());
    for (size_t i = 0; i < ax.size(); ++i)
      v[i] = ax[i].v[at[x][o + i]] * (ax[i].day ? static_cast<float>(S) : 1.0f);
    return v;
  }
  // 配置 x 所读的指标列 (取进 Tape, 串行)
  Src src(Tape &t, size_t x) const { return src_of(t, kinds, p(x, kCenter), p(x, kBand)); }
  // 配置 x 的信号核 (只读 Tape, 可并行)
  Signal signal(const Tape &t, size_t x, const Src &s) const {
    const float k = p(x, kBand)[1];
    assert(k > 0 && "带半宽倍数");
    return {t, *s.m, *s.s, k, ladder(x)};
  }
  // 配置 x 的分层核
  Ladder ladder(size_t x) const { return ladder_of(kinds[kLayer], p(x, kLayer), dir); }
};

inline Grid grid_of(size_t S, const Kinds &k, size_t dir) {
  const std::vector<const Axis *> ax = axes_of(k);
  Grid g{k, dir, S, {}, {}};
  At at(ax.size(), 0);
  do
    g.at.push_back(at);
  while (tick(at, [&](size_t i) { return ax[i]->v.size(); }));
  g.ridge = ridge_of(ax, g.at);
  return g;
}

} // namespace cta
