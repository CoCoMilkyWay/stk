#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

constexpr float PI = 3.14159265358979323846f;

// ============================================================================
// CROSS-SECTIONAL UTILITY FUNCTIONS
// ============================================================================

// Inverse normal CDF (simplified Beasley-Springer-Moro approximation)
inline float inverse_normal_cdf(float p) {
  constexpr float a0 = 2.50662823884f;
  constexpr float a1 = -18.61500062529f;
  constexpr float a2 = 41.39119773534f;
  constexpr float a3 = -25.44106049637f;
  constexpr float b1 = -8.47351093090f;
  constexpr float b2 = 23.08336743743f;
  constexpr float b3 = -21.06224101826f;
  constexpr float b4 = 3.13082909833f;

  if (p <= 0.0f)
    return -6.0f;
  if (p >= 1.0f)
    return 6.0f;

  float t = (p < 0.5f) ? p : (1.0f - p);
  t = std::sqrt(-2.0f * std::log(t));
  float num = a0 + t * (a1 + t * (a2 + t * a3));
  float denom = 1.0f + t * (b1 + t * (b2 + t * (b3 + t * b4)));
  float result = t - num / denom;
  return (p < 0.5f) ? -result : result;
}

// fast-math 安全的有限性判断: 指数位全 1 = inf/NaN.
// 整数域位测试, 不受 -ffinite-math-only 影响 (isnan/isfinite 会被优化掉).
inline bool finite_bits(float x) {
  std::uint32_t b;
  std::memcpy(&b, &x, sizeof(b));
  return (b & 0x7f800000u) != 0x7f800000u;
}

// Compute rank + inverse normal transform (only on valid assets, optimized)
inline void compute_rank_inverse_normal_sparse(const float *input,
                                               const std::vector<size_t> &valid_indices,
                                               float *output) {
  if (valid_indices.empty())
    return;

  std::vector<std::pair<float, size_t>> sorted_vals;
  sorted_vals.reserve(valid_indices.size());

  // Build sort pairs (跳过 inf/NaN: 基本面列缺失 = NaN; NaN 进 std::sort 是 UB)
  for (size_t idx : valid_indices) {
    if (finite_bits(input[idx]))
      sorted_vals.emplace_back(input[idx], idx);
  }

  const size_t N = sorted_vals.size();
  if (N == 0)
    return;

  // Sort by value (use pdqsort-friendly pattern for small N)
  std::sort(sorted_vals.begin(), sorted_vals.end());

  // Compute ranks and inverse normal (vectorizable loop)
  const float scale = 1.0f / (N + 1.0f);
  for (size_t rank = 0; rank < N; ++rank) {
    size_t asset_idx = sorted_vals[rank].second;
    float percentile = (rank + 1.0f) * scale; // Strength reduction
    output[asset_idx] = inverse_normal_cdf(percentile);
  }
}

// Compute cross-sectional z-score (only on valid assets)
inline void compute_zscore_sparse(const float *input,
                                  const std::vector<size_t> &valid_indices,
                                  float *output) {
  if (valid_indices.empty())
    return;

  const size_t N = valid_indices.size();
  float sum = 0.0f, sum_sq = 0.0f;

  for (size_t idx : valid_indices) {
    float val = input[idx];
    sum += val;
    sum_sq += val * val;
  }

  float mean = sum / N;
  float variance = (sum_sq / N) - (mean * mean);
  float stddev = std::sqrt(std::max(variance, 1e-8f));

  for (size_t idx : valid_indices) {
    output[idx] = (input[idx] - mean) / stddev;
  }
}
