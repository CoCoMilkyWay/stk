#pragma once

// 调参: 优化目标 (逐根两项, 累成日前缀取窗) + 参数网格 Grid (网格 + 其上的岭回归曲面)
//   网格 = 各组所选模型参数轴的组合 (经 admit 筛), 以根计
//   每日重拟: 窗 = 前 W 日, 参数逐日微调不跳变
//   岭回归曲面 (一组模型的网格上): 特征 = 各多值参数轴的下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值;
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + H y (X 去均值 ⇒ H · 1 = 0); H 只看网格, 预先算好; 拟合最高点 = 平原顶
#include "cta/Model.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>

namespace cta {

inline constexpr size_t kMinRoll = 250; // 滚动调参窗下限 (日) = 一年
inline constexpr double kRidge = 1;     // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// 优化目标 = 行情分割质量: 带把行情切成带外的趋势段与带内的震荡段, 切得好 = 趋势吃满、震荡不挨打、换手少
//   逐根两项 [a, b] = [净顺向, 振幅] (见 terms), 一段的分数 = Σa / Σb; 1 ≈ 走过的路程全部顺向吃到

// 一段的目标值
inline double score(double a, double b) { return b > 0 ? a / b : 0; }

// 两项的日前缀 [nd + 1] (前导 0): 日段 [d0, d1) 之和 = x[d1] − x[d0]
struct Terms {
  std::vector<double> a, b;
  double over(size_t d0, size_t d1) const { return score(a[d1] - a[d0], b[d1] - b[d0]); }
};

// 一个配置在一个资产上的目标项, 逐根算、按日累成前缀; b = 该配置的上线 / 下线, w = 其实际仓位 (仓位层之后)
//   净顺向 = 带外顺向 − 带内来回 − 费率 × 换手, 振幅 = |对数涨跌|; 都是对数价 (≈ 满仓收益) 单位, 带就绪 (0 < 下线 < 上线, 本根与前根) 才计
//     带外顺向: 前根在上线之上 → +对数涨跌, 在下线之下 → −对数涨跌 (随机游走期望 0); 前根在带内、本根破线 → 越线那截记正
//     带内来回: 带内位置 x = (收 − 下线) / (上线 − 下线) ∈ [0, 1] (分层的仓位坐标; 每层仓位相同), 一步的带内路程 = |Δx| · ln(上线 / 下线),
//               x 含带本身的漂移; 一段 = x 离开边界到再碰边界 (或带失效), 来回 = 段内路程 − |段内净位移|, 单边穿过带不罚
//     换手: |Δw| × 单边费率, 窄带的代价
inline Terms terms(const Tape &t, const Band &b, const Col &w, double cost, const Days &days) {
  const size_t nd = days.size();
  assert(b.up.size() == t.n && b.dn.size() == t.n && w.size() == t.n && days.off[nd] == t.n && "分割质量: 上线 / 下线 / 仓位 / 日轴");
  Terms o{std::vector<double>(nd + 1, 0), std::vector<double>(nd + 1, 0)};
  const Col &c = t.c, &up = b.up, &dn = b.dn;
  const auto ok = [&](size_t i) { return dn[i] > 0 && up[i] > dn[i]; };
  const auto pos = [&](size_t i) { return std::clamp((static_cast<double>(c[i]) - dn[i]) / (up[i] - dn[i]), 0.0, 1.0); };
  double net = 0; // 当前段带内净位移
  for (size_t d = 0; d < nd; ++d) {
    double &A = o.a[d + 1], &B = o.b[d + 1];
    for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
      A -= cost * std::fabs(w[i] - (i ? w[i - 1] : 0.f));
      if (i == 0 || !ok(i - 1) || !ok(i)) {
        A += std::fabs(net), net = 0;
        continue;
      }
      const double r = t.lr[i];
      B += std::fabs(r);
      if (c[i - 1] > up[i - 1])
        A += r;
      else if (c[i - 1] < dn[i - 1])
        A -= r;
      else {
        if (c[i] > up[i])
          A += std::log(static_cast<double>(c[i]) / up[i]);
        else if (c[i] < dn[i])
          A += std::log(static_cast<double>(dn[i]) / c[i]);
        const double dx = (pos(i) - pos(i - 1)) * std::log(static_cast<double>(up[i]) / dn[i]);
        A -= std::fabs(dx), net += dx;
      }
      if (const double x = pos(i); x <= 0 || x >= 1)
        A += std::fabs(net), net = 0;
    }
    A += o.a[d], B += o.b[d];
  }
  return o;
}

struct Spec {
  Cfg g;                  // 以根计
  std::vector<size_t> at; // 各组所选模型的参数轴取值下标, 按组序拼成一条
};

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
inline std::vector<const Axis *> axes_of(const std::array<size_t, kNP> &k) {
  std::vector<const Axis *> ax;
  for (size_t j = 0; j < kNP; ++j)
    for (const Axis &a : kParts[j].kinds[k[j]].axes)
      ax.push_back(&a);
  return ax;
}

// 一组模型 k 的参数网格
inline std::vector<Spec> specs_of(double S, const std::array<size_t, kNP> &k) {
  const std::vector<const Axis *> ax = axes_of(k);
  std::vector<Spec> out;
  std::vector<size_t> at(ax.size(), 0);
  do {
    Cfg g{k, {}};
    for (size_t j = 0, x = 0; j < kNP; ++j)
      for (size_t i = 0; i < kParts[j].kinds[k[j]].axes.size(); ++i, ++x)
        g.p[j].push_back(ax[x]->v[at[x]]);
    g = to_bars(std::move(g), S);
    if (admit(g))
      out.push_back({std::move(g), at});
  } while (tick(at, [&](size_t i) { return ax[i]->v.size(); }));
  return out;
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

struct Ridge {
  size_t n = 0;
  std::vector<double> H; // [n][n]

  // 拟合最高点 argmax ŷ = argmax H y
  size_t best(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    for ([[maybe_unused]] const double v : y)
      assert(std::isfinite(v) && "岭回归: 目标值");
    size_t k = 0;
    double top = -std::numeric_limits<double>::infinity();
    for (size_t i = 0; i < n; ++i) {
      double f = 0;
      for (size_t j = 0; j < n; ++j)
        f += H[i * n + j] * y[j];
      if (f > top)
        top = f, k = i;
    }
    return k;
  }
};

// specs = 一组模型的网格
inline Ridge ridge_of(const std::vector<Spec> &specs) {
  assert(!specs.empty() && "岭回归: 空网格");
  const std::vector<const Axis *> ax = axes_of(specs[0].g.k);
  const size_t n = specs.size();
  std::vector<std::vector<double>> X(n);
  for (size_t s = 0; s < n; ++s) {
    assert(specs[s].g.k == specs[0].g.k && "岭回归: 网格须同一组模型");
    std::vector<double> u;
    std::vector<char> sq;
    for (size_t x = 0; x < ax.size(); ++x)
      if (const size_t m = ax[x]->v.size(); m > 1)
        u.push_back(2.0 * static_cast<double>(specs[s].at[x]) / static_cast<double>(m - 1) - 1), sq.push_back(m > 2);
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
  std::vector<double> A(d * d, 0), Z(d * n);
  for (size_t i = 0; i < d; ++i) {
    for (size_t j = 0; j < d; ++j)
      for (const std::vector<double> &r : X)
        A[i * d + j] += r[i] * r[j];
    A[i * d + i] += kRidge * static_cast<double>(n);
    for (size_t s = 0; s < n; ++s)
      Z[i * n + s] = X[s][i];
  }
  solve(A, Z, d, n);
  Ridge rg{n, std::vector<double>(n * n, 0)};
  for (size_t i = 0; i < n; ++i)
    for (size_t j = 0; j < n; ++j)
      for (size_t k = 0; k < d; ++k)
        rg.H[i * n + j] += X[i][k] * Z[k * n + j];
  return rg;
}

// 一组模型的参数网格 + 其上的岭回归曲面
struct Grid {
  std::vector<Spec> specs;
  Ridge ridge;
};

inline Grid grid_of(double S, const std::array<size_t, kNP> &k) {
  std::vector<Spec> specs = specs_of(S, k);
  assert(!specs.empty() && "所选模型无可用网格");
  Ridge rg = ridge_of(specs);
  return {std::move(specs), std::move(rg)};
}

} // namespace cta
