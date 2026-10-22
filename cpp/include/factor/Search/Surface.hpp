#pragma once

// =============================================================================
// Search 的曲面 (纯逻辑, 宿主 double, 无后端 / IO; 与 Space.hpp 对仗): 已评候选 (坐标由 Space unit 归一) (单位立方坐标 x, 带符号目标 g, 标准误 se) → 高斯过程后验 → 峰
// =============================================================================
//   【模型】g(x) = m + f(x) + ε, f ~ GP(0, σf²·Matérn5/2(r)), r² = Σ_j ((x_j − x'_j) / ℓ_j)², ε_i ~ N(0, se_i²):
//     噪声用各点已知标准误 (Search/Contract【噪声】), 不估计; m 取 GLS 闭式; σf / ℓ_j 极大化边际似然 (对数网格坐标下降, 网格只是
//     优化器的搜索范围, 不是旋钮). 内部按 max|g| 归一, 出口还原原量纲.
//   【峰】已评点 i: 可用 (g_i > 0), μ_i > 0, 且一个长度尺度邻域 (r ≤ 1) 内无 μ 更高的点 (并列下标小者胜). μ 降序.
//   出错策略: 只 assert.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

namespace factor::search::surface {

struct Model {
  int P = 0, N = 0;
  std::vector<double> x;               // [N][P] 观测点 (单位立方)
  std::vector<double> ell;             // [P] 长度尺度 (单位立方)
  double sf2 = 0.0, m = 0.0, ys = 1.0; // 幅度² / 常数均值 (内部量) / 归一尺度 (内部量 = 原量 / ys)
  std::vector<double> L;               // [N][N] K 的 Cholesky 下三角
  std::vector<double> alpha;           // K⁻¹ (y − m)
};

namespace detail {

inline double matern52(double r2) {
  const double s = std::sqrt(5.0 * r2);
  return (1.0 + s + s * s / 3.0) * std::exp(-s);
}
inline double dist2(const double *a, const double *b, const double *ell, int P) {
  double r2 = 0.0;
  for (int j = 0; j < P; ++j) {
    const double d = (a[j] - b[j]) / ell[j];
    r2 += d * d;
  }
  return r2;
}
// 原地 Cholesky (下三角); 非正定 → false
inline bool cholesky(std::vector<double> &K, int N) {
  for (int j = 0; j < N; ++j) {
    double d = K[static_cast<size_t>(j) * N + j];
    for (int k = 0; k < j; ++k)
      d -= K[static_cast<size_t>(j) * N + k] * K[static_cast<size_t>(j) * N + k];
    if (!(d > 0.0))
      return false;
    const double l = std::sqrt(d);
    K[static_cast<size_t>(j) * N + j] = l;
    for (int i = j + 1; i < N; ++i) {
      double s = K[static_cast<size_t>(i) * N + j];
      for (int k = 0; k < j; ++k)
        s -= K[static_cast<size_t>(i) * N + k] * K[static_cast<size_t>(j) * N + k];
      K[static_cast<size_t>(i) * N + j] = s / l;
    }
  }
  return true;
}
// L v = b (前代)
inline void lower(const std::vector<double> &L, int N, std::vector<double> &v) {
  for (int i = 0; i < N; ++i) {
    double s = v[static_cast<size_t>(i)];
    for (int k = 0; k < i; ++k)
      s -= L[static_cast<size_t>(i) * N + k] * v[static_cast<size_t>(k)];
    v[static_cast<size_t>(i)] = s / L[static_cast<size_t>(i) * N + i];
  }
}
// L L^T z = b
inline void solve(const std::vector<double> &L, int N, std::vector<double> &v) {
  lower(L, N, v);
  for (int i = N - 1; i >= 0; --i) {
    double s = v[static_cast<size_t>(i)];
    for (int k = i + 1; k < N; ++k)
      s -= L[static_cast<size_t>(k) * N + i] * v[static_cast<size_t>(k)];
    v[static_cast<size_t>(i)] = s / L[static_cast<size_t>(i) * N + i];
  }
}
// 负对数边际似然 (常数项略); 顺带填 M 的 L / alpha / m. 非正定 → +∞
inline double nll(Model &M, const std::vector<double> &y, const std::vector<double> &s2) {
  const int N = M.N, P = M.P;
  M.L.assign(static_cast<size_t>(N) * N, 0.0);
  for (int i = 0; i < N; ++i)
    for (int j = 0; j <= i; ++j) {
      const double k = M.sf2 * matern52(dist2(&M.x[static_cast<size_t>(i) * P], &M.x[static_cast<size_t>(j) * P], M.ell.data(), P));
      M.L[static_cast<size_t>(i) * N + j] = k + (i == j ? s2[static_cast<size_t>(i)] + 1e-10 * M.sf2 : 0.0); // 抖动只保数值正定
    }
  if (!cholesky(M.L, N))
    return std::numeric_limits<double>::infinity();
  std::vector<double> z1(static_cast<size_t>(N), 1.0), zy = y;
  solve(M.L, N, z1);
  solve(M.L, N, zy);
  double a = 0.0, b = 0.0;
  for (int i = 0; i < N; ++i)
    a += z1[static_cast<size_t>(i)], b += zy[static_cast<size_t>(i)];
  M.m = b / a;
  M.alpha.resize(static_cast<size_t>(N));
  double quad = 0.0, logdet = 0.0;
  for (int i = 0; i < N; ++i) {
    M.alpha[static_cast<size_t>(i)] = zy[static_cast<size_t>(i)] - M.m * z1[static_cast<size_t>(i)];
    quad += (y[static_cast<size_t>(i)] - M.m) * M.alpha[static_cast<size_t>(i)];
    logdet += std::log(M.L[static_cast<size_t>(i) * N + i]);
  }
  return 0.5 * quad + logdet;
}

} // namespace detail

// x[N][P] 单位立方坐标, y[N] 带符号目标, se[N] 标准误 (≥ 0; 0 = 从不成交的点, g ≡ 0 无噪声, 靠抖动保正定)
inline Model fit(const std::vector<std::vector<double>> &x, const std::vector<double> &y, const std::vector<double> &se) {
  const int N = static_cast<int>(x.size());
  assert(N >= 1 && static_cast<int>(y.size()) == N && static_cast<int>(se.size()) == N);
  Model M;
  M.N = N, M.P = static_cast<int>(x[0].size());
  M.x.reserve(static_cast<size_t>(N) * M.P);
  for (const std::vector<double> &r : x) {
    assert(static_cast<int>(r.size()) == M.P);
    M.x.insert(M.x.end(), r.begin(), r.end());
  }
  M.ys = 0.0;
  for (const double v : y)
    M.ys = std::max(M.ys, std::fabs(v));
  if (!(M.ys > 0.0))
    M.ys = 1.0;
  std::vector<double> yn(static_cast<size_t>(N)), s2(static_cast<size_t>(N));
  double mu = 0.0;
  for (int i = 0; i < N; ++i) {
    assert(se[static_cast<size_t>(i)] >= 0.0 && std::isfinite(y[static_cast<size_t>(i)]));
    yn[static_cast<size_t>(i)] = y[static_cast<size_t>(i)] / M.ys;
    s2[static_cast<size_t>(i)] = se[static_cast<size_t>(i)] * se[static_cast<size_t>(i)] / (M.ys * M.ys);
    mu += yn[static_cast<size_t>(i)] / N;
  }
  double var = 0.0;
  for (const double v : yn)
    var += (v - mu) * (v - mu) / N;
  const double sd0 = var > 0.0 ? std::sqrt(var) : 1.0;
  // 优化器搜索范围: ℓ ∈ 2^[−5, 1] (单位立方), σf ∈ sd0 · 2^[−4, 4], 步 2^½
  std::vector<double> ell_grid, sf_grid;
  for (int k = 0; k <= 12; ++k)
    ell_grid.push_back(std::exp2(-5.0 + 0.5 * k));
  for (int k = 0; k <= 16; ++k)
    sf_grid.push_back(sd0 * std::exp2(-4.0 + 0.5 * k));
  M.ell.assign(static_cast<size_t>(M.P), 0.5);
  M.sf2 = sd0 * sd0;
  double best = detail::nll(M, yn, s2);
  for (bool improved = true; improved;) { // 每次接受都严格下降, 网格有限 ⇒ 必停
    improved = false;
    for (int p = 0; p <= M.P; ++p) {
      const std::vector<double> &grid = p < M.P ? ell_grid : sf_grid;
      for (const double v : grid) {
        Model T = M;
        if (p < M.P)
          T.ell[static_cast<size_t>(p)] = v;
        else
          T.sf2 = v * v;
        const double f = detail::nll(T, yn, s2);
        if (f < best - 1e-12) {
          best = f, M = std::move(T), improved = true;
        }
      }
    }
  }
  assert(std::isfinite(best) && "曲面拟合: 任一超参下 K 都不正定");
  detail::nll(M, yn, s2); // M 的 L / alpha / m 与最终超参同步
  return M;
}

// 后验均值 / 标准差 (原量纲)
inline void predict(const Model &M, const std::vector<double> &x, double &mu, double &sd) {
  assert(static_cast<int>(x.size()) == M.P);
  std::vector<double> k(static_cast<size_t>(M.N));
  double m = M.m;
  for (int i = 0; i < M.N; ++i) {
    k[static_cast<size_t>(i)] = M.sf2 * detail::matern52(detail::dist2(x.data(), &M.x[static_cast<size_t>(i) * M.P], M.ell.data(), M.P));
    m += k[static_cast<size_t>(i)] * M.alpha[static_cast<size_t>(i)];
  }
  detail::lower(M.L, M.N, k);
  double v = M.sf2;
  for (const double q : k)
    v -= q * q;
  mu = m * M.ys;
  sd = std::sqrt(std::max(v, 0.0)) * M.ys;
}
inline double mean(const Model &M, const std::vector<double> &x) {
  double mu = 0.0, sd = 0.0;
  predict(M, x, mu, sd);
  return mu;
}

// 峰 (见【峰】): mu[N] = 各观测点的后验均值, usable[N] = 观测可用 (g > 0); 返回观测下标, μ 降序
inline std::vector<int> peaks(const Model &M, const std::vector<double> &mu, const std::vector<uint8_t> &usable) {
  assert(static_cast<int>(mu.size()) == M.N && static_cast<int>(usable.size()) == M.N);
  std::vector<int> out;
  for (int i = 0; i < M.N; ++i) {
    if (!usable[static_cast<size_t>(i)] || !(mu[static_cast<size_t>(i)] > 0.0))
      continue;
    bool peak = true;
    for (int j = 0; peak && j < M.N; ++j)
      if (j != i && detail::dist2(&M.x[static_cast<size_t>(i) * M.P], &M.x[static_cast<size_t>(j) * M.P], M.ell.data(), M.P) <= 1.0)
        peak = mu[static_cast<size_t>(j)] < mu[static_cast<size_t>(i)] || (mu[static_cast<size_t>(j)] == mu[static_cast<size_t>(i)] && j > i);
    if (peak)
      out.push_back(i);
  }
  std::stable_sort(out.begin(), out.end(), [&](int a, int b) { return mu[static_cast<size_t>(a)] > mu[static_cast<size_t>(b)]; });
  return out;
}

} // namespace factor::search::surface
