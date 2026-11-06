#pragma once

// 调参 (测试台与轨迹共用): 参数网格 + 优化目标 (逐根两项, 按日前缀和取窗) + 岭回归曲面
//   网格 = 各组所选模型参数轴的组合 (经 admit 筛), 以根计; 全网格再遍历各组模型
//   每日重拟: 窗 = 前 W 日, 参数逐日微调不跳变
//   岭回归曲面 (一组模型的网格上): 特征 = 各多值参数轴的下标坐标 u ∈ [−1, 1] 的二次项 (u, u² (≥ 3 值的轴), 两两 u · u'), 去均值;
//     ŷ = ȳ + X (XᵀX + λ · n · I)⁻¹ Xᵀ (y − ȳ) = ȳ + H (y − ȳ); H 只看网格, 预先算好; 拟合最高点 = 平原顶
#include "Engine.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <iterator>
#include <string>
#include <vector>

inline constexpr size_t kMinRoll = 250; // 滚动调参窗下限 (日) = 一年
inline constexpr double kRidge = 1;     // 岭罚 λ (相对样本数; 坐标方差 ~0.4, 高次项方差更小、压得更狠)

// 优化目标 (模型), 逐根两项 [a, b] 求和后成分数:
//   sharpe  [r, r²]          → 均值 / 标准差
//   sortino [r, min(r, 0)²]  → 均值 / 下行波动
//   band    [带质量, 振幅]    → Σ带质量 / Σ振幅 (带质量见 terms)
enum Target { kSharpe,
              kSortino,
              kQuality };

inline Target target_of(const std::string &s) {
  assert((s == "sharpe" || s == "sortino" || s == "band") && "优化目标: sharpe | sortino | band");
  return s == "sharpe" ? kSharpe : s == "sortino" ? kSortino
                                                  : kQuality;
}

// 一段 n 根的目标值 (不年化): Σa, Σb 由前缀和相减得
inline double score(Target tg, double a, double b, double n) {
  if (tg == kQuality)
    return b > 0 ? a / b : 0;
  const double mean = a / n, d = tg == kSharpe ? b / n - mean * mean : b / n;
  return d > 0 ? mean / std::sqrt(d) : 0;
}

struct Terms {
  std::vector<double> a, b; // [n]
};

// 一个配置在一个资产上的逐根目标项; band 要该配置的上线 / 下线 (g), 收益类只看 run
//   band: 带质量 = 带外顺向 − 带内来回 − 费率 × 换手, 振幅 = |对数涨跌|; 都是对数价 (≈ 满仓收益) 单位, 带就绪 (0 < 下线 < 上线, 本根与前根) 才计
//     带外顺向: 前根在上线之上 → +对数涨跌, 在下线之下 → −对数涨跌 (随机游走期望 0); 前根在带内、本根破线 → 越线那截记正
//     带内来回: 带内位置 x = (收 − 下线) / (上线 − 下线) ∈ [0, 1] (分层的仓位坐标; 每层仓位相同), 一步的带内路程 = |Δx| · ln(上线 / 下线),
//               x 含带本身的漂移; 一段 = x 离开边界到再碰边界 (或带失效), 来回 = 段内路程 − |段内净位移|, 单边穿过带不罚
//     换手: 实际仓位 (仓位层之后) 的 |Δw| × 单边费率, 窄带的代价
inline Terms terms(Target tg, const Tape &t, const Sig &g, const Run &run, double cost) {
  const size_t n = t.n;
  Terms o{std::vector<double>(n, 0), std::vector<double>(n, 0)};
  if (tg != kQuality) {
    for (size_t i = 0; i < n; ++i) {
      const double r = run.r[i], q = tg == kSharpe ? r : std::min(r, 0.0);
      o.a[i] = r, o.b[i] = q * q;
    }
    return o;
  }
  assert(g.up.size() == n && g.dn.size() == n && "band 目标: 上线 / 下线");
  const Col &c = t.c, &up = g.up, &dn = g.dn;
  const auto ok = [&](size_t i) { return dn[i] > 0 && up[i] > dn[i]; };
  const auto pos = [&](size_t i) { return std::clamp((static_cast<double>(c[i]) - dn[i]) / (up[i] - dn[i]), 0.0, 1.0); };
  double net = 0; // 当前段带内净位移
  for (size_t i = 0; i < n; ++i) {
    o.a[i] -= cost * std::fabs(run.w[i] - (i ? run.w[i - 1] : 0.f));
    if (i == 0 || !ok(i - 1) || !ok(i)) {
      o.a[i] += std::fabs(net), net = 0;
      continue;
    }
    const double r = std::log(static_cast<double>(c[i]) / c[i - 1]);
    o.b[i] = std::fabs(r);
    if (c[i - 1] > up[i - 1])
      o.a[i] += r;
    else if (c[i - 1] < dn[i - 1])
      o.a[i] -= r;
    else {
      if (c[i] > up[i])
        o.a[i] += std::log(static_cast<double>(c[i]) / up[i]);
      else if (c[i] < dn[i])
        o.a[i] += std::log(static_cast<double>(dn[i]) / c[i]);
      const double d = (pos(i) - pos(i - 1)) * std::log(static_cast<double>(up[i]) / dn[i]);
      o.a[i] -= std::fabs(d), net += d;
    }
    if (const double x = pos(i); x <= 0 || x >= 1)
      o.a[i] += std::fabs(net), net = 0;
  }
  return o;
}

// 逐根两项 → 日前缀和 [日][Σa, Σb]; day(i) = 根 i 所在日 (自 0 起, 单调不减)
template <class F>
std::vector<double> day_sums(const Terms &x, size_t nd, F day) {
  std::vector<double> acc(nd * 2, 0);
  for (size_t i = 0; i < x.a.size(); ++i) {
    const size_t d = day(i);
    acc[2 * d] += x.a[i], acc[2 * d + 1] += x.b[i];
  }
  for (size_t k = 2; k < acc.size(); ++k)
    acc[k] += acc[k - 2];
  return acc;
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

// 全网格: 各组模型的组合 × 其参数网格
inline std::vector<Spec> specs_of(double S) {
  std::vector<Spec> out;
  std::array<size_t, kNP> k{};
  do {
    std::vector<Spec> v = specs_of(S, k);
    out.insert(out.end(), std::make_move_iterator(v.begin()), std::make_move_iterator(v.end()));
  } while (tick(k, [](size_t j) { return kParts[j].kinds.size(); }));
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

  std::vector<double> fit(const std::vector<double> &y) const {
    assert(y.size() == n && "岭回归: 样本数");
    double m = 0;
    for (const double v : y) {
      assert(std::isfinite(v) && "岭回归: 目标值");
      m += v;
    }
    m /= static_cast<double>(n);
    std::vector<double> f(n, m);
    for (size_t i = 0; i < n; ++i)
      for (size_t j = 0; j < n; ++j)
        f[i] += H[i * n + j] * (y[j] - m);
    return f;
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
