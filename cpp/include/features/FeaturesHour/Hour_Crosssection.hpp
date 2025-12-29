#pragma once

#include "features/CoreCrosssection.hpp"
#include "features/backend/FeatureStore.hpp"
#include <vector>

// ============================================================================
// LEVEL 2: Hour-level Cross-sectional Features (Event-driven, leaf node)
// ============================================================================

// Level 2: Hour-level CS features computation
// Called when an hour boundary is crossed (event-driven, not batch)
inline void compute_cs_hour(GlobalFeatureStore *store,
                            const std::string &date,
                            size_t t_hour,
                            std::vector<size_t> &valid_indices,
                            std::vector<float> &input_fp32,
                            std::vector<float> &output_fp32,
                            std::vector<_Float16> &output_fp16) {
  const size_t A = input_fp32.size();

  // Build valid indices
  const _Float16 *valid_flags = CS_READ_ALL(store, date, 2, t_hour, L2_FieldOffset::_data_valid);
  valid_indices.clear();
  for (size_t a = 0; a < A; ++a) {
    if (static_cast<float>(valid_flags[a]) > 0.5f) {
      valid_indices.push_back(a);
    }
  }

  if (valid_indices.empty())
    return;

  // CS feature 1: cs_hour_return_beta
  {
    const _Float16 *input = CS_READ_ALL(store, date, 2, t_hour, L2_FieldOffset::hour_ret_12h_mom);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, 2, t_hour, L2_FieldOffset::cs_hour_return_beta, output_fp16.data(), A);
  }

  // CS feature 2: cs_hour_liq_adj_ret
  {
    const _Float16 *input = CS_READ_ALL(store, date, 2, t_hour, L2_FieldOffset::hour_volatility);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, 2, t_hour, L2_FieldOffset::cs_hour_liq_adj_ret, output_fp16.data(), A);
  }

  // CS feature 3: cs_hour_range_rank
  {
    const _Float16 *input = CS_READ_ALL(store, date, 2, t_hour, L2_FieldOffset::pivot_dev);
    for (size_t a = 0; a < A; ++a)
      input_fp32[a] = input[a];
    std::fill(output_fp32.begin(), output_fp32.end(), 0.0f);
    compute_rank_inverse_normal_sparse(input_fp32.data(), valid_indices, output_fp32.data());
    for (size_t a = 0; a < A; ++a)
      output_fp16[a] = output_fp32[a];
    CS_WRITE_ALL(store, date, 2, t_hour, L2_FieldOffset::cs_hour_range_rank, output_fp16.data(), A);
  }
  
  // Leaf node - no further cascade
}

