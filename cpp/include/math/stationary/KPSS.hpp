#pragma once

#include <cassert>
#include <cmath>
#include <vector>

// ============================================================================
// Kwiatkowski-Phillips-Schmidt-Shin (KPSS) Stationarity Test
// ============================================================================
//
// H0: 序列平稳 (level or trend stationary)
// H1: 存在单位根 (非平稳)
//
// 判断: p > 0.05 → 不拒绝H0 → 序列平稳
//
// 模型: y_t = ξ*t + r_t + ε_t
//       r_t = r_{t-1} + u_t (random walk)
//       H0: Var(u_t) = 0
//
// ============================================================================

namespace math::stationary {

struct KPSSResult {
  float statistic = 0.0f;  // LM statistic
  float pvalue = 0.0f;
  size_t n_obs = 0;
  int bandwidth = 0;       // Newey-West bandwidth
  bool valid = false;
};

namespace detail {

// Critical values for KPSS test (level stationarity, constant only)
// Source: Kwiatkowski et al. (1992), Table 1
constexpr float KPSS_LEVEL_10PCT = 0.347f;
constexpr float KPSS_LEVEL_5PCT = 0.463f;
constexpr float KPSS_LEVEL_2_5PCT = 0.574f;
constexpr float KPSS_LEVEL_1PCT = 0.739f;

// Approximate p-value from KPSS statistic
inline float compute_pvalue_level(float stat) {
  // KPSS is a one-sided test; larger values reject H0
  if (stat >= KPSS_LEVEL_1PCT) return 0.005f;
  if (stat >= KPSS_LEVEL_2_5PCT) {
    float t = (stat - KPSS_LEVEL_2_5PCT) / (KPSS_LEVEL_1PCT - KPSS_LEVEL_2_5PCT);
    return 0.025f - t * 0.015f;
  }
  if (stat >= KPSS_LEVEL_5PCT) {
    float t = (stat - KPSS_LEVEL_5PCT) / (KPSS_LEVEL_2_5PCT - KPSS_LEVEL_5PCT);
    return 0.05f - t * 0.025f;
  }
  if (stat >= KPSS_LEVEL_10PCT) {
    float t = (stat - KPSS_LEVEL_10PCT) / (KPSS_LEVEL_5PCT - KPSS_LEVEL_10PCT);
    return 0.10f - t * 0.05f;
  }
  // stat < 0.347, p > 0.10
  float ratio = stat / KPSS_LEVEL_10PCT;
  return std::min(0.99f, 0.10f + (1.0f - ratio) * 0.40f);
}

// Bartlett kernel weight
inline float bartlett_kernel(int j, int bandwidth) {
  if (j == 0) return 1.0f;
  float x = static_cast<float>(std::abs(j)) / static_cast<float>(bandwidth + 1);
  return (x < 1.0f) ? (1.0f - x) : 0.0f;
}

// Newey-West bandwidth selection (automatic)
inline int newey_west_bandwidth(size_t n) {
  // Common rule: floor(4 * (n/100)^(2/9))
  float ratio = static_cast<float>(n) / 100.0f;
  return static_cast<int>(4.0f * std::pow(ratio, 2.0f / 9.0f));
}

} // namespace detail

// Main KPSS test function (level stationarity)
inline KPSSResult kpss_test(const std::vector<float>& y, int bandwidth = -1) {
  KPSSResult result;
  
  const size_t n = y.size();
  if (n < 10) {
    result.valid = false;
    return result;
  }
  
  result.n_obs = n;
  
  // Auto-select bandwidth if not specified
  if (bandwidth < 0) {
    bandwidth = detail::newey_west_bandwidth(n);
  }
  bandwidth = std::max(1, std::min(bandwidth, static_cast<int>(n / 2)));
  result.bandwidth = bandwidth;
  
  // Step 1: Compute residuals from regression y_t = α + ε_t
  // (constant only for level stationarity)
  float mean = 0.0f;
  for (size_t t = 0; t < n; ++t) {
    mean += y[t];
  }
  mean /= static_cast<float>(n);
  
  std::vector<float> resid(n);
  for (size_t t = 0; t < n; ++t) {
    resid[t] = y[t] - mean;
  }
  
  // Step 2: Compute partial sums S_t = Σ_{i=1}^{t} e_i
  std::vector<float> S(n);
  S[0] = resid[0];
  for (size_t t = 1; t < n; ++t) {
    S[t] = S[t - 1] + resid[t];
  }
  
  // Step 3: Compute long-run variance estimator using Bartlett kernel
  // σ²_∞ = γ_0 + 2 * Σ_{j=1}^{l} w_j * γ_j
  // where γ_j = (1/n) * Σ_{t=j+1}^{n} e_t * e_{t-j}
  
  // Compute autocovariances
  std::vector<float> gamma(bandwidth + 1);
  for (int j = 0; j <= bandwidth; ++j) {
    float sum = 0.0f;
    for (size_t t = j; t < n; ++t) {
      sum += resid[t] * resid[t - j];
    }
    gamma[j] = sum / static_cast<float>(n);
  }
  
  // Long-run variance with Bartlett weights
  float s2 = gamma[0];
  for (int j = 1; j <= bandwidth; ++j) {
    float w = detail::bartlett_kernel(j, bandwidth);
    s2 += 2.0f * w * gamma[j];
  }
  
  if (s2 <= 0) {
    result.valid = false;
    return result;
  }
  
  // Step 4: Compute KPSS statistic
  // η = (1/n²) * Σ_{t=1}^{n} S_t² / σ²_∞
  float sum_S2 = 0.0f;
  for (size_t t = 0; t < n; ++t) {
    sum_S2 += S[t] * S[t];
  }
  
  float n2 = static_cast<float>(n) * static_cast<float>(n);
  result.statistic = sum_S2 / (n2 * s2);
  result.pvalue = detail::compute_pvalue_level(result.statistic);
  result.valid = true;
  
  return result;
}

} // namespace math::stationary

