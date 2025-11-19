#pragma once

#include "features/CoreCrosssection.hpp"
#include "features/backend/FeatureStore.hpp"
#include "features/FeaturesMinute/Minute_Crosssection.hpp"
#include <vector>

// ============================================================================
// LEVEL 0: Tick-level Cross-sectional Features (Event-driven, optimized)
// ============================================================================

// Fast batch conversion fp16 -> fp32 (compiler auto-vectorization friendly)
inline void convert_fp16_to_fp32(const _Float16 *src, float *dst, size_t count) {
  for (size_t i = 0; i < count; ++i)
    dst[i] = static_cast<float>(src[i]);
}

// Fast batch conversion fp32 -> fp16
inline void convert_fp32_to_fp16(const float *src, _Float16 *dst, size_t count) {
  for (size_t i = 0; i < count; ++i)
    dst[i] = static_cast<_Float16>(src[i]);
}

// Level 0: Tick-level CS features computation (event-driven, optimized)
// Worker calls this for each tick event; cascades to minute/hour on time boundaries
inline void compute_cs_tick(GlobalFeatureStore *store,
                            const std::string &date,
                            size_t t,
                            std::vector<size_t> &valid_indices,
                            std::vector<float> &input_fp32,
                            std::vector<float> &output_fp32,
                            std::vector<_Float16> &output_fp16) {
  constexpr size_t level_idx = 0;
  const size_t A = input_fp32.size();

  // Build valid indices (optimized: check valid_flags once)
  const _Float16 *valid_flags = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::asset_valid);
  valid_indices.clear();
  for (size_t a = 0; a < A; ++a) {
    if (static_cast<float>(valid_flags[a]) > 0.5f) {
      valid_indices.push_back(a);
    }
  }

  if (valid_indices.empty())
    return;

  // CS feature 1: cs_spread_rank (optimized conversion)
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::spread_momentum);
    convert_fp16_to_fp32(input, input_fp32.data(), A);
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    convert_fp32_to_fp16(output_fp32.data(), output_fp16.data(), A);
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_spread_rank, output_fp16.data(), A);
  }

  // CS feature 2: cs_tobi_rank
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::tobi_osc);
    convert_fp16_to_fp32(input, input_fp32.data(), A);
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    convert_fp32_to_fp16(output_fp32.data(), output_fp16.data(), A);
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_tobi_rank, output_fp16.data(), A);
  }

  // CS feature 3: cs_liquidity_ratio
  {
    const _Float16 *input = CS_READ_ALL(store, date, level_idx, t, L0_FieldOffset::signed_volume_imb);
    convert_fp16_to_fp32(input, input_fp32.data(), A);
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_zscore_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    convert_fp32_to_fp16(output_fp32.data(), output_fp16.data(), A);
    CS_WRITE_ALL(store, date, level_idx, t, L0_FieldOffset::cs_liquidity_ratio, output_fp16.data(), A);
  }

  // Cascade: If this tick crosses minute boundary, trigger minute computation
  size_t t_minute = t / 60;  // Convert tick index (seconds) to minute index
  if (t % 60 == 0 && t > 0) {
    compute_cs_minute(store, date, t_minute, valid_indices, input_fp32, output_fp32, output_fp16);
  }
}

