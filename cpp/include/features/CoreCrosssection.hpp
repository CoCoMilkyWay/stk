#pragma once

#include "backend/FeatureStore.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

// ============================================================================
// CROSS-SECTIONAL FEATURE COMPUTATION
// ============================================================================
// Single-threaded CS worker that processes all time slots sequentially
// Reads TS features, computes CS transformations, writes CS features

// Helper: Inverse normal CDF (simplified Beasley-Springer-Moro approximation)
inline float inverse_normal_cdf(float p) {
  constexpr float a0 = 2.50662823884f;
  constexpr float a1 = -18.61500062529f;
  constexpr float a2 = 41.39119773534f;
  constexpr float a3 = -25.44106049637f;
  constexpr float b0 = -8.47351093090f;
  constexpr float b1 = 23.08336743743f;
  constexpr float b2 = -21.06224101826f;
  constexpr float b3 = 3.13082909833f;

  float q = p - 0.5f;
  if (std::abs(q) <= 0.425f) {
    float r = 0.180625f - q * q;
    return q * (((a3 * r + a2) * r + a1) * r + a0) /
           ((((b3 * r + b2) * r + b1) * r + b0) * r + 1.0f);
  }

  float r = (q < 0) ? p : 1.0f - p;
  r = std::sqrt(-std::log(r));
  float sign = (q < 0) ? -1.0f : 1.0f;
  return sign * (2.515517f + 0.802853f * r + 0.010328f * r * r) /
         (1.0f + 1.432788f * r + 0.189269f * r * r + 0.001308f * r * r * r);
}

// Compute rank + inverse normal transform (only on valid assets)
inline void compute_rank_inverse_normal_sparse(const float *input,
                                               const std::vector<size_t> &valid_indices,
                                               float *output) {
  size_t n = valid_indices.size();
  if (n == 0)
    return;

  std::vector<std::pair<float, size_t>> indexed(n);
  for (size_t i = 0; i < n; ++i) {
    size_t asset_idx = valid_indices[i];
    indexed[i] = {input[asset_idx], asset_idx};
  }

  std::sort(indexed.begin(), indexed.end());

  for (size_t rank = 0; rank < n; ++rank) {
    size_t asset_idx = indexed[rank].second;
    float percentile = (rank + 0.5f) / n;
    output[asset_idx] = inverse_normal_cdf(percentile);
  }
}

// Compute cross-sectional z-score (only on valid assets)
inline void compute_zscore_sparse(const float *input,
                                  const std::vector<size_t> &valid_indices,
                                  float *output) {
  size_t n = valid_indices.size();
  if (n == 0)
    return;

  double sum = 0;
  for (size_t asset_idx : valid_indices) {
    sum += input[asset_idx];
  }
  double mean = sum / n;

  double sq_sum = 0;
  for (size_t asset_idx : valid_indices) {
    double diff = input[asset_idx] - mean;
    sq_sum += diff * diff;
  }
  double stddev = std::sqrt(sq_sum / n);

  for (size_t asset_idx : valid_indices) {
    output[asset_idx] = (stddev > 1e-8) ? (input[asset_idx] - mean) / stddev : 0.0f;
  }
}

// Compute all CS features for a single time slot
inline void compute_cs_for_timeslot(GlobalFeatureStore *store, const std::string &date, size_t t) {
  const size_t A = store->query_A();
  constexpr size_t level_idx = 0;

  // Read asset_valid flags to filter assets (_Float16 auto-converts to float)
  const _Float16 *valid_flags = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::asset_valid);

  // Build valid asset indices
  std::vector<size_t> valid_indices;
  for (size_t a = 0; a < A; ++a) {
    if (static_cast<float>(valid_flags[a]) > 0.5f) {
      valid_indices.push_back(a);
    }
  }

  if (valid_indices.empty())
    return;

  // Allocate buffers (use float for computation precision)
  std::vector<float> input_fp32(A);
  std::vector<float> output_fp32(A);
  std::vector<_Float16> output_fp16(A);

  // CS feature 1: cs_spread_rank (from spread_momentum)
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::spread_momentum);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a]; // Auto-convert
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a]; // Auto-convert
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_spread_rank, output_fp16.data(), A);
  }

  // CS feature 2: cs_tobi_rank (from tobi_osc)
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::tobi_osc);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_tobi_rank, output_fp16.data(), A);
  }

  // CS feature 3: cs_liquidity_ratio (from signed_volume_imb)
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::signed_volume_imb);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_zscore_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_liquidity_ratio, output_fp16.data(), A);
  }
}
