#pragma once

#include "features/CoreCrosssection.hpp"
#include "features/backend/FeatureStore.hpp"
#include "features/FeaturesHour/Hour_Crosssection.hpp"
#include <vector>

// ============================================================================
// LEVEL 1: Minute-level Cross-sectional Features (Event-driven)
// ============================================================================

// Level 1: Minute-level CS features computation
// Called when a minute boundary is crossed (event-driven, not batch)
inline void compute_cs_minute(GlobalFeatureStore *store,
                              const std::string &date,
                              size_t t_minute,
                              std::vector<size_t> &valid_indices,
                              std::vector<float> &input_fp32,
                              std::vector<float> &output_fp32,
                              std::vector<_Float16> &output_fp16) {
  constexpr size_t level_idx = 1;
  const size_t A = input_fp32.size();

  // Build valid indices
  const _Float16 *valid_flags = CS_READ_ALL(store, date, level_idx, t_minute, L1_FieldOffset::universe_size);
  valid_indices.clear();
  for (size_t a = 0; a < A; ++a) {
    if (static_cast<float>(valid_flags[a]) > 0.5f) {
      valid_indices.push_back(a);
    }
  }

  if (valid_indices.empty())
    return;

  // CS feature 1: cs_min_return_rank
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t_minute, L1_FieldOffset::min_ret_z);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, level_idx, t_minute, L1_FieldOffset::cs_min_return_rank, output_fp16.data(), A);
  }

  // CS feature 2: cs_min_volume_pct
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t_minute, L1_FieldOffset::momentum_15m);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, level_idx, t_minute, L1_FieldOffset::cs_min_volume_pct, output_fp16.data(), A);
  }

  // CS feature 3: cs_min_spread_z
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t_minute, L1_FieldOffset::rv_5m_norm);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_zscore_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, level_idx, t_minute, L1_FieldOffset::cs_min_spread_z, output_fp16.data(), A);
  }

  // Cascade: If this minute crosses hour boundary, trigger hour computation
  size_t t_hour = t_minute / 60;  // Convert minute index to hour index
  if (t_minute % 60 == 0) {
    compute_cs_hour(store, date, t_hour, valid_indices, input_fp32, output_fp32, output_fp16);
  }
}
