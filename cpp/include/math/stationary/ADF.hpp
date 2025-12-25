#pragma once

#include <cassert>
#include <cmath>
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

// OLS regression: y = X*β + ε, returns β and residual variance
// X is (n x k) stored row-major, y is (n x 1)
// Returns: β (k x 1), and sets sigma2 = residual variance
inline std::vector<float> ols_solve(const std::vector<float>& X, 
                                     const std::vector<float>& y,
                                     size_t n, size_t k, float& sigma2) {
  assert(X.size() == n * k);
  assert(y.size() == n);
  
  // X'X (k x k)
  std::vector<float> XtX(k * k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    for (size_t j = 0; j <= i; ++j) {
      float sum = 0.0f;
      for (size_t t = 0; t < n; ++t) {
        sum += X[t * k + i] * X[t * k + j];
      }
      XtX[i * k + j] = sum;
      XtX[j * k + i] = sum;
    }
  }
  
  // X'y (k x 1)
  std::vector<float> Xty(k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    float sum = 0.0f;
    for (size_t t = 0; t < n; ++t) {
      sum += X[t * k + i] * y[t];
    }
    Xty[i] = sum;
  }
  
  // Solve XtX * β = Xty using Cholesky decomposition
  // L * L' = XtX
  std::vector<float> L(k * k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    for (size_t j = 0; j <= i; ++j) {
      float sum = XtX[i * k + j];
      for (size_t p = 0; p < j; ++p) {
        sum -= L[i * k + p] * L[j * k + p];
      }
      if (i == j) {
        if (sum <= 0) {
          // Matrix not positive definite, return empty
          sigma2 = 0.0f;
          return {};
        }
        L[i * k + j] = std::sqrt(sum);
      } else {
        L[i * k + j] = sum / L[j * k + j];
      }
    }
  }
  
  // Forward substitution: L * z = Xty
  std::vector<float> z(k);
  for (size_t i = 0; i < k; ++i) {
    float sum = Xty[i];
    for (size_t j = 0; j < i; ++j) {
      sum -= L[i * k + j] * z[j];
    }
    z[i] = sum / L[i * k + i];
  }
  
  // Backward substitution: L' * β = z
  std::vector<float> beta(k);
  for (int i = static_cast<int>(k) - 1; i >= 0; --i) {
    float sum = z[i];
    for (size_t j = i + 1; j < k; ++j) {
      sum -= L[j * k + i] * beta[j];
    }
    beta[i] = sum / L[i * k + i];
  }
  
  // Compute residual variance
  float sse = 0.0f;
  for (size_t t = 0; t < n; ++t) {
    float pred = 0.0f;
    for (size_t i = 0; i < k; ++i) {
      pred += X[t * k + i] * beta[i];
    }
    float resid = y[t] - pred;
    sse += resid * resid;
  }
  sigma2 = sse / static_cast<float>(n - k);
  
  // Compute (X'X)^{-1} diagonal for standard errors
  // We need the diagonal of L^{-1} * L'^{-1}
  // For simplicity, store inv_XtX and extract diagonal
  // Actually we only need SE for γ coefficient (index 1)
  
  return beta;
}

// Compute SE for coefficient at index `idx` given X, sigma2
inline float compute_se(const std::vector<float>& X, size_t n, size_t k, 
                        size_t idx, float sigma2) {
  // Need (X'X)^{-1}[idx, idx]
  // Compute X'X
  std::vector<float> XtX(k * k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    for (size_t j = 0; j <= i; ++j) {
      float sum = 0.0f;
      for (size_t t = 0; t < n; ++t) {
        sum += X[t * k + i] * X[t * k + j];
      }
      XtX[i * k + j] = sum;
      XtX[j * k + i] = sum;
    }
  }
  
  // Cholesky decomposition
  std::vector<float> L(k * k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    for (size_t j = 0; j <= i; ++j) {
      float sum = XtX[i * k + j];
      for (size_t p = 0; p < j; ++p) {
        sum -= L[i * k + p] * L[j * k + p];
      }
      if (i == j) {
        if (sum <= 0) return 0.0f;
        L[i * k + j] = std::sqrt(sum);
      } else {
        L[i * k + j] = sum / L[j * k + j];
      }
    }
  }
  
  // Compute L^{-1}
  std::vector<float> Linv(k * k, 0.0f);
  for (size_t i = 0; i < k; ++i) {
    Linv[i * k + i] = 1.0f / L[i * k + i];
    for (size_t j = i + 1; j < k; ++j) {
      float sum = 0.0f;
      for (size_t p = i; p < j; ++p) {
        sum -= L[j * k + p] * Linv[p * k + i];
      }
      Linv[j * k + i] = sum / L[j * k + j];
    }
  }
  
  // (X'X)^{-1} = L^{-T} * L^{-1}
  // We only need diagonal element [idx, idx]
  float diag = 0.0f;
  for (size_t p = idx; p < k; ++p) {
    diag += Linv[p * k + idx] * Linv[p * k + idx];
  }
  
  return std::sqrt(sigma2 * diag);
}

} // namespace detail

// Select optimal lag using AIC
inline int select_lag_aic(const std::vector<float>& y, int max_lag) {
  const size_t n = y.size();
  if (n < static_cast<size_t>(max_lag + 10)) {
    return 1;
  }
  
  int best_lag = 1;
  float best_aic = 1e30f;
  
  for (int p = 1; p <= max_lag; ++p) {
    size_t n_eff = n - p - 1;
    if (n_eff < 10) continue;
    
    // Quick variance estimate for AIC
    float sum_sq = 0.0f;
    for (size_t t = p + 1; t < n; ++t) {
      float dy = y[t] - y[t - 1];
      sum_sq += dy * dy;
    }
    float var = sum_sq / n_eff;
    
    size_t k = 2 + p;  // const + y_{t-1} + p lags
    float aic = n_eff * std::log(var) + 2.0f * k;
    
    if (aic < best_aic) {
      best_aic = aic;
      best_lag = p;
    }
  }
  
  return best_lag;
}

// Main ADF test function
inline ADFResult adf_test(const std::vector<float>& y, int max_lag = 12) {
  ADFResult result;
  
  const size_t n = y.size();
  if (n < 20) {
    result.valid = false;
    return result;
  }
  
  // Select lag order
  int p = select_lag_aic(y, std::min(max_lag, static_cast<int>(n / 4)));
  result.lag = p;
  
  // Effective sample size
  size_t n_eff = n - p - 1;
  result.n_obs = n_eff;
  
  if (n_eff < 15) {
    result.valid = false;
    return result;
  }
  
  // Build regression matrices
  // Δy_t = α + γ*y_{t-1} + Σδ_i*Δy_{t-i} + ε_t
  // Regressors: [1, y_{t-1}, Δy_{t-1}, ..., Δy_{t-p}]
  // k = 2 + p
  
  size_t k = 2 + p;
  std::vector<float> X(n_eff * k);
  std::vector<float> Y(n_eff);
  
  for (size_t t = 0; t < n_eff; ++t) {
    size_t idx = t + p + 1;  // actual time index
    Y[t] = y[idx] - y[idx - 1];  // Δy_t
    
    X[t * k + 0] = 1.0f;              // constant
    X[t * k + 1] = y[idx - 1];        // y_{t-1}
    
    for (int i = 0; i < p; ++i) {
      X[t * k + 2 + i] = y[idx - 1 - i] - y[idx - 2 - i];  // Δy_{t-1-i}
    }
  }
  
  // OLS estimation
  float sigma2;
  auto beta = detail::ols_solve(X, Y, n_eff, k, sigma2);
  
  if (beta.empty() || sigma2 <= 0) {
    result.valid = false;
    return result;
  }
  
  // t-statistic for γ (coefficient index 1)
  float gamma = beta[1];
  float se_gamma = detail::compute_se(X, n_eff, k, 1, sigma2);
  
  if (se_gamma <= 0) {
    result.valid = false;
    return result;
  }
  
  result.statistic = gamma / se_gamma;
  result.pvalue = detail::compute_pvalue(result.statistic, n_eff);
  result.valid = true;
  
  return result;
}

} // namespace math::stationary

