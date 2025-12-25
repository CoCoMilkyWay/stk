#pragma once

#include <cassert>
#include <algorithm>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Augmented Dickey-Fuller (ADF) Unit Root Test
// ============================================================================
//
// H0: 存在单位根 (非平稳)
// H1: 序列平稳
//
// 判断: p < 0.05 → 拒绝H0 → 序列平稳
//
// 模型: Δy_t = α + γ*y_{t-1} + Σδ_i*Δy_{t-i} + ε_t
//       (constant only, no trend)
//
// ============================================================================

namespace math::stationary {

struct ADFResult {
  float statistic = 0.0f;  // t-statistic for γ
  float pvalue = 0.0f;
  int lag = 0;             // selected lag order
  size_t n_obs = 0;        // effective observations
  bool valid = false;
};

// Critical values for ADF test (constant only, no trend)
// Source: MacKinnon (1994) response surface coefficients
// Interpolated for common sample sizes
namespace detail {

// tau_c critical values at 1%, 5%, 10% significance
// For n = 25, 50, 100, 250, 500, inf
constexpr float ADF_CRITICAL_1PCT[] = {-3.75f, -3.58f, -3.51f, -3.46f, -3.44f, -3.43f};
constexpr float ADF_CRITICAL_5PCT[] = {-3.00f, -2.93f, -2.89f, -2.87f, -2.86f, -2.86f};
constexpr float ADF_CRITICAL_10PCT[] = {-2.63f, -2.60f, -2.58f, -2.57f, -2.57f, -2.57f};
constexpr size_t ADF_N_VALS[] = {25, 50, 100, 250, 500, 10000};
constexpr size_t ADF_N_POINTS = 6;

inline float interpolate_critical(const float* table, size_t n) {
  if (n <= ADF_N_VALS[0]) return table[0];
  if (n >= ADF_N_VALS[ADF_N_POINTS - 1]) return table[ADF_N_POINTS - 1];
  
  for (size_t i = 0; i < ADF_N_POINTS - 1; ++i) {
    if (n <= ADF_N_VALS[i + 1]) {
      float t = static_cast<float>(n - ADF_N_VALS[i]) / 
                static_cast<float>(ADF_N_VALS[i + 1] - ADF_N_VALS[i]);
      return table[i] + t * (table[i + 1] - table[i]);
    }
  }
  return table[ADF_N_POINTS - 1];
}

// Approximate p-value from t-statistic using linear interpolation
inline float compute_pvalue(float t_stat, size_t n) {
  float c1 = interpolate_critical(ADF_CRITICAL_1PCT, n);
  float c5 = interpolate_critical(ADF_CRITICAL_5PCT, n);
  float c10 = interpolate_critical(ADF_CRITICAL_10PCT, n);
  
  if (t_stat <= c1) return 0.005f;   // < 1%
  if (t_stat <= c5) {
    // Linear interpolation between 1% and 5%
    float t = (t_stat - c1) / (c5 - c1);
    return 0.01f + t * 0.04f;
  }
  if (t_stat <= c10) {
    // Linear interpolation between 5% and 10%
    float t = (t_stat - c5) / (c10 - c5);
    return 0.05f + t * 0.05f;
  }
  // > 10%, rough extrapolation
  float beyond = (t_stat - c10) / std::abs(c10);
  return std::min(0.99f, 0.10f + beyond * 0.30f);
}

// Cholesky decomposition: A = L L^T. Supports in-place (A == L).
inline bool cholesky_decomp(std::span<const float> A, size_t k, std::span<float> L) {
  assert(A.size() >= k * k);
  assert(L.size() >= k * k);

  for (size_t i = 0; i < k; ++i) {
    for (size_t j = 0; j <= i; ++j) {
      float sum = A[i * k + j];
      for (size_t p = 0; p < j; ++p) {
        sum -= L[i * k + p] * L[j * k + p];
      }
      if (i == j) {
        if (sum <= 0.0f) return false;
        L[i * k + j] = std::sqrt(sum);
      } else {
        L[i * k + j] = sum / L[j * k + j];
      }
    }
    // Zero upper triangle for this row (needed for in-place)
    for (size_t j = i + 1; j < k; ++j) L[i * k + j] = 0.0f;
  }
  return true;
}

inline void cholesky_solve(std::span<const float> L, size_t k,
                           std::span<const float> b, std::span<float> x,
                           std::span<float> tmp) {
  assert(L.size() >= k * k);
  assert(b.size() >= k);
  assert(x.size() >= k);
  assert(tmp.size() >= k);

  // Forward: L * tmp = b
  for (size_t i = 0; i < k; ++i) {
    float sum = b[i];
    for (size_t j = 0; j < i; ++j) sum -= L[i * k + j] * tmp[j];
    tmp[i] = sum / L[i * k + i];
  }

  // Backward: L' * x = tmp
  for (int ii = static_cast<int>(k) - 1; ii >= 0; --ii) {
    const size_t i = static_cast<size_t>(ii);
    float sum = tmp[i];
    for (size_t j = i + 1; j < k; ++j) sum -= L[j * k + i] * x[j];
    x[i] = sum / L[i * k + i];
  }
}

// diag((X'X)^{-1})_idx via Cholesky: (X'X)^{-1} = L^{-T} L^{-1}
inline float inv_xtx_diag(std::span<const float> L, size_t k, size_t idx,
                          std::span<float> work) {
  assert(L.size() >= k * k && idx < k && work.size() >= k);
  for (size_t i = 0; i < k; ++i) {
    float sum = (i == idx) ? 1.0f : 0.0f;
    for (size_t j = 0; j < i; ++j) sum -= L[i * k + j] * work[j];
    work[i] = sum / L[i * k + i];
  }
  float diag = 0.0f;
  for (size_t i = idx; i < k; ++i) diag += work[i] * work[i];
  return diag;
}

} // namespace detail

struct ADFWorkspace {
  std::vector<float> XtX, Xty, L, beta, tmp, dy;

  void ensure(size_t k) {
    const size_t kk = k * k;
    if (XtX.size() < kk) XtX.resize(kk);
    if (L.size() < kk) L.resize(kk);
    if (Xty.size() < k) Xty.resize(k);
    if (beta.size() < k) beta.resize(k);
    if (tmp.size() < k) tmp.resize(k);
  }
};

inline ADFResult adf_test(std::span<const float> y, int max_lag, ADFWorkspace& ws) {
  ADFResult result;
  const size_t n = y.size();
  if (n < 20) return result;

  const int max_p = std::max(0, std::min(max_lag, static_cast<int>(n / 4)));
  const size_t max_k = 2 + static_cast<size_t>(max_p);

  // Fixed sample: all p use same n_eff (enables incremental XtX)
  const size_t n_eff = n - static_cast<size_t>(max_p) - 1;
  if (n_eff <= max_k || n_eff < 15) return result;

  // Precompute Δy once
  if (ws.dy.size() < n - 1) ws.dy.resize(n - 1);
  for (size_t i = 0; i + 1 < n; ++i) ws.dy[i] = y[i + 1] - y[i];

  ws.ensure(max_k);

  // ========== Pass 1: Build full XtX/Xty for max_p in one scan ==========
  // Layout: XtX is max_k × max_k, stores all cross-products
  // Row/col 0 = const, 1 = y_{t-1}, 2..max_k-1 = Δy lags
  std::fill(ws.XtX.begin(), ws.XtX.begin() + max_k * max_k, 0.0f);
  std::fill(ws.Xty.begin(), ws.Xty.begin() + max_k, 0.0f);
  float yty = 0.0f;

  for (size_t t = 0; t < n_eff; ++t) {
    const size_t idx = t + static_cast<size_t>(max_p) + 1;
    const float Yt = ws.dy[idx - 1];
    const float y_lag = y[idx - 1];

    // Direct accumulation (no temp reg array)
    // Xty: [const, y_lag, dy_lags...]
    ws.Xty[0] += Yt;
    ws.Xty[1] += y_lag * Yt;

    // XtX row 0: const
    ws.XtX[0] += 1.0f;
    // XtX row 1: y_lag
    ws.XtX[1 * max_k + 0] += y_lag;
    ws.XtX[1 * max_k + 1] += y_lag * y_lag;

    // Δy lag columns (2 to max_k-1)
    for (size_t i = 0; i < static_cast<size_t>(max_p); ++i) {
      const float dy_i = ws.dy[idx - 2 - i];
      const size_t col = 2 + i;

      ws.Xty[col] += dy_i * Yt;

      // Cross with const and y_lag
      ws.XtX[col * max_k + 0] += dy_i;
      ws.XtX[col * max_k + 1] += dy_i * y_lag;

      // Cross with previous Δy lags
      for (size_t j = 0; j < i; ++j) {
        const float dy_j = ws.dy[idx - 2 - j];
        ws.XtX[col * max_k + (2 + j)] += dy_i * dy_j;
      }
      // Self
      ws.XtX[col * max_k + col] += dy_i * dy_i;
    }

    yty += Yt * Yt;
  }

  // ========== Pass 2: Extract sub-matrices for each p, solve & score ==========
  int best_p = 0;
  float best_aic = 1e30f;
  float best_sigma2 = 0.0f;
  size_t best_k = 2;

  for (int p = 0; p <= max_p; ++p) {
    const size_t k = 2 + static_cast<size_t>(p);
    if (n_eff <= k) continue;

    // Copy sub-matrix to L (will be overwritten by Cholesky)
    // XtX[0..k-1, 0..k-1] stored in row-major with stride max_k
    for (size_t i = 0; i < k; ++i) {
      for (size_t j = 0; j <= i; ++j) {
        ws.L[i * k + j] = ws.XtX[i * max_k + j];
      }
    }
    // Mirror upper
    for (size_t i = 0; i < k; ++i)
      for (size_t j = 0; j < i; ++j) ws.L[j * k + i] = ws.L[i * k + j];

    // Cholesky in-place on L
    if (!detail::cholesky_decomp({ws.L.data(), k * k}, k, {ws.L.data(), k * k}))
      continue;

    // Solve for beta
    detail::cholesky_solve({ws.L.data(), k * k}, k, {ws.Xty.data(), k},
                           {ws.beta.data(), k}, {ws.tmp.data(), k});

    // SSE = Y'Y - β'X'y
    float beta_xty = 0.0f;
    for (size_t i = 0; i < k; ++i) beta_xty += ws.beta[i] * ws.Xty[i];
    float sse = std::max(0.0f, yty - beta_xty);

    const float dof = static_cast<float>(n_eff - k);
    if (dof <= 0.0f) continue;
    const float sigma2 = sse / dof;
    if (!(sigma2 > 0.0f)) continue;

    // AIC
    const float sigma2_mle = sse / static_cast<float>(n_eff);
    const float aic = static_cast<float>(n_eff) * std::log(sigma2_mle) + 2.0f * static_cast<float>(k);
    if (aic < best_aic) {
      best_aic = aic;
      best_p = p;
      best_k = k;
      best_sigma2 = sigma2;
    }
  }

  // ========== Final: refit best model to get correct L/beta ==========
  {
    const size_t k = best_k;
    for (size_t i = 0; i < k; ++i)
      for (size_t j = 0; j <= i; ++j) ws.L[i * k + j] = ws.XtX[i * max_k + j];
    for (size_t i = 0; i < k; ++i)
      for (size_t j = 0; j < i; ++j) ws.L[j * k + i] = ws.L[i * k + j];

    if (!detail::cholesky_decomp({ws.L.data(), k * k}, k, {ws.L.data(), k * k}))
      return result;

    detail::cholesky_solve({ws.L.data(), k * k}, k, {ws.Xty.data(), k},
                           {ws.beta.data(), k}, {ws.tmp.data(), k});
  }

  // t-statistic for γ (coefficient index 1)
  const float gamma = ws.beta[1];
  const float diag = detail::inv_xtx_diag({ws.L.data(), best_k * best_k}, best_k, 1,
                                          {ws.tmp.data(), best_k});
  const float se = std::sqrt(best_sigma2 * diag);
  if (se <= 0.0f) return result;

  result.statistic = gamma / se;
  result.pvalue = detail::compute_pvalue(result.statistic, n_eff);
  result.lag = best_p;
  result.n_obs = n_eff;
  result.valid = true;
  return result;
}

} // namespace math::stationary


