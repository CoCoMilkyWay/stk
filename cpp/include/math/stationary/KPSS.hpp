#pragma once

#include <algorithm>
#include <cassert>
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
  float statistic = 0.0f; // LM statistic
  float pvalue = 0.0f;
  size_t n_obs = 0;
  int bandwidth = 0; // Newey-West bandwidth
  bool valid = false;
};

struct KPSSWorkspace {
  // Reused buffers to avoid per-call allocations.
  // Sized to (bandwidth + 1).
  std::vector<double> gamma; // double to avoid precision loss on large samples
  std::vector<float> ring;

  void ensure(int bandwidth) {
    assert(bandwidth >= 0);
    const size_t need = static_cast<size_t>(bandwidth) + 1;
    if (gamma.size() < need)
      gamma.resize(need);
    if (ring.size() < need)
      ring.resize(need);
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
  if (stat >= KPSS_LEVEL_1PCT)
    return 0.005f;
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
  if (j == 0)
    return 1.0f;
  float x = static_cast<float>(std::abs(j)) / static_cast<float>(bandwidth + 1);
  return (x < 1.0f) ? (1.0f - x) : 0.0f;
}

// Newey-West bandwidth selection (automatic)
// 对于大样本 (n > 10k)，bandwidth 边际收益递减，限制上限避免 O(n*bandwidth) 爆炸
inline int newey_west_bandwidth(size_t n) {
  // Common rule: floor(4 * (n/100)^(2/9))
  float ratio = static_cast<float>(n) / 100.0f;
  int bw = static_cast<int>(4.0f * std::pow(ratio, 2.0f / 9.0f));
  // 上限 12：对于 n > 10k，更大的 bandwidth 几乎不改变结论，但会显著拖慢
  return std::min(bw, 12);
}

} // namespace detail

// ============================================================================
// Fast KPSS for Large Samples
// ============================================================================
//
// - Pass 1: O(n) 计算 mean, sum_S2, γ_0
// - Pass 2: 对每个 lag j，用一次线性扫描计算 γ_j（可被 SIMD 向量化）
// - bandwidth 上限 12（大样本边际收益递减）
//
// ============================================================================

inline KPSSResult kpss_test(std::span<const float> y, int bandwidth, KPSSWorkspace &ws) {
  KPSSResult result;

  const size_t n = y.size();
  if (n < 10)
    return result;

  result.n_obs = n;

  // Auto-select bandwidth if not specified
  if (bandwidth < 0) {
    bandwidth = detail::newey_west_bandwidth(n);
  }
  bandwidth = std::clamp(bandwidth, 0, std::min(12, static_cast<int>(n / 2)));
  result.bandwidth = bandwidth;

  // ========== Pass 1: mean, partial sums S, sum_S2, γ_0 ==========
  // All O(n), no inner loop
  double mean = 0.0;
  for (size_t t = 0; t < n; ++t)
    mean += y[t];
  mean /= static_cast<double>(n);

  double S = 0.0;
  double sum_S2 = 0.0;
  double gamma0 = 0.0;

  for (size_t t = 0; t < n; ++t) {
    const double e = y[t] - mean;
    S += e;
    sum_S2 += S * S;
    gamma0 += e * e;
  }

  // ========== Pass 2: γ_j for j=1..bandwidth ==========
  // 每个 lag 独立计算，内循环简单可向量化
  ws.ensure(bandwidth);
  ws.gamma[0] = gamma0;

  for (int lag = 1; lag <= bandwidth; ++lag) {
    double gj = 0.0;
    const size_t L = static_cast<size_t>(lag);
    // γ_j = Σ_{t=lag}^{n-1} e[t] * e[t-lag]
    for (size_t t = L; t < n; ++t) {
      const double e_t = y[t] - mean;
      const double e_lag = y[t - L] - mean;
      gj += e_t * e_lag;
    }
    ws.gamma[static_cast<size_t>(lag)] = gj;
  }

  // ========== Long-run variance with Bartlett weights ==========
  const double inv_n = 1.0 / static_cast<double>(n);
  double s2 = ws.gamma[0] * inv_n;
  for (int j = 1; j <= bandwidth; ++j) {
    const float w = detail::bartlett_kernel(j, bandwidth);
    s2 += 2.0 * w * (ws.gamma[static_cast<size_t>(j)] * inv_n);
  }

  if (s2 <= 0.0)
    return result;

  // ========== KPSS statistic ==========
  const double n2 = static_cast<double>(n) * static_cast<double>(n);
  result.statistic = static_cast<float>(sum_S2 / (n2 * s2));
  result.pvalue = detail::compute_pvalue_level(result.statistic);
  result.valid = true;

  return result;
}

} // namespace math::stationary
