#pragma once

#include <cassert>
#include <algorithm>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Kwiatkowski-Phillips-Schmidt-Shin (KPSS) Level Stationarity Test
// ============================================================================
//
// H0: 序列平稳 (level stationary，仅常数项)
// H1: 存在单位根 (非平稳)
//
// 判断: p > 0.05 → 不拒绝H0 → 序列平稳
//
// 模型: y_t = α + e_t  (constant only, no trend)
//
// 注意:
// - 只实现 level KPSS，不含 trend
// - autocovariance γ_j 使用 1/n 归一化（非 1/(n-j)）
// - p-value 是基于渐近临界值的插值近似，非精确值
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

struct KPSSWorkspace {
  // Reused buffers to avoid per-call allocations.
  // Sized to (bandwidth + 1).
  std::vector<double> gamma;  // double to avoid precision loss on large samples
  std::vector<float> ring;

  void ensure(int bandwidth) {
    assert(bandwidth >= 0);
    const size_t need = static_cast<size_t>(bandwidth) + 1;
    if (gamma.size() < need) gamma.resize(need);
    if (ring.size() < need) ring.resize(need);
  }
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

// Main KPSS test function (level stationarity) - zero-alloc if workspace buffers are pre-sized.
inline KPSSResult kpss_test(std::span<const float> y, int bandwidth, KPSSWorkspace& ws) {
  KPSSResult result;
  
  const size_t n = y.size();
  if (n < 10) return result;
  
  result.n_obs = n;
  
  // Auto-select bandwidth if not specified
  if (bandwidth < 0) {
    bandwidth = detail::newey_west_bandwidth(n);
  }
  bandwidth = std::clamp(bandwidth, 0, static_cast<int>(n / 2));
  result.bandwidth = bandwidth;
  
  // Step 1: Compute residuals from regression y_t = α + ε_t (constant only): e_t = y_t - mean(y)
  double mean = 0.0;
  for (size_t t = 0; t < n; ++t) {
    mean += y[t];
  }
  mean /= static_cast<double>(n);
  
  // Step 2+3: Streaming compute
  // - partial sums S_t (to accumulate Σ S_t^2)
  // - autocovariances sums Σ e_t * e_{t-j} for j=0..bandwidth, using a ring buffer
  ws.ensure(bandwidth);
  const size_t B = static_cast<size_t>(bandwidth) + 1;
  std::fill(ws.gamma.begin(), ws.gamma.begin() + B, 0.0);
  std::fill(ws.ring.begin(), ws.ring.begin() + B, 0.0f);

  double S = 0.0;
  double sum_S2 = 0.0;

  for (size_t t = 0; t < n; ++t) {
    const float e = static_cast<float>(y[t] - mean);
    const size_t pos = (B == 0) ? 0 : (t % B);
    ws.ring[pos] = e;

    const int j_max = static_cast<int>(std::min<size_t>(static_cast<size_t>(bandwidth), t));
    for (int j = 0; j <= j_max; ++j) {
      const size_t p = (pos + B - static_cast<size_t>(j)) % B;
      ws.gamma[static_cast<size_t>(j)] += e * ws.ring[p];
    }

    S += e;
    sum_S2 += S * S;
  }
  
  // Long-run variance with Bartlett weights (γ_j normalized by 1/n)
  const double inv_n = 1.0 / static_cast<double>(n);
  double s2 = ws.gamma[0] * inv_n;
  for (int j = 1; j <= bandwidth; ++j) {
    float w = detail::bartlett_kernel(j, bandwidth);
    s2 += 2.0 * w * (ws.gamma[static_cast<size_t>(j)] * inv_n);
  }
  
  if (s2 <= 0.0) {
    return result;
  }
  
  // Step 4: Compute KPSS statistic
  // η = (1/n²) * Σ_{t=1}^{n} S_t² / σ²_∞
  double n2 = static_cast<double>(n) * static_cast<double>(n);
  result.statistic = static_cast<float>(sum_S2 / (n2 * s2));
  result.pvalue = detail::compute_pvalue_level(result.statistic);
  result.valid = true;
  
  return result;
}

} // namespace math::stationary

