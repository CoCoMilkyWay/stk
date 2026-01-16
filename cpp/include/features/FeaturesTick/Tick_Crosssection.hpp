#pragma once

#include "../misc/misc.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <vector>


// ============================================================================
// LEVEL 0: Tick-level Cross-sectional Features
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

class Tick_Crosssection {
public:
  Tick_Crosssection(GlobalFeatureStore &store,
                    std::vector<size_t> &valid_indices,
                    std::vector<float> &input_fp32,
                    std::vector<float> &output_fp32,
                    std::vector<_Float16> &output_fp16)
      : store_(&store),
        valid_indices_(valid_indices),
        input_fp32_(input_fp32),
        output_fp32_(output_fp32),
        output_fp16_(output_fp16) {}

  void set_date(const std::string &date) { date_str_ = date; }

  void compute_and_store(size_t t) {
    const size_t A = input_fp32_.size();

    // Build valid indices (optimized: check valid_flags once)
    const _Float16 *valid_flags = CS_READ_ALL(store_, date_str_, 0, t, L0_FieldOffset::_data_valid);
    valid_indices_.clear();
    for (size_t a = 0; a < A; ++a) {
      if (static_cast<float>(valid_flags[a]) > 0.5f) {
        valid_indices_.push_back(a);
      }
    }

    if (valid_indices_.empty())
      return;

    // CS feature 1: cs_spread_rank - rank CI1 (Cumulative Imbalance 1-Level) cross-sectionally
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 0, t, L0_FieldOffset::ci_1);
      convert_fp16_to_fp32(input, input_fp32_.data(), A);
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      convert_fp32_to_fp16(output_fp32_.data(), output_fp16_.data(), A);
      CS_WRITE_ALL(store_, date_str_, 0, t, L0_FieldOffset::cs_spread_rank, output_fp16_.data(), A);
    }

    // CS feature 2: cs_tobi_rank - rank CI5 (Cumulative Imbalance 5-Level) cross-sectionally
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 0, t, L0_FieldOffset::ci_5);
      convert_fp16_to_fp32(input, input_fp32_.data(), A);
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      convert_fp32_to_fp16(output_fp32_.data(), output_fp16_.data(), A);
      CS_WRITE_ALL(store_, date_str_, 0, t, L0_FieldOffset::cs_tobi_rank, output_fp16_.data(), A);
    }

    // CS feature 3: cs_liquidity_ratio - z-score OFI1 (Order Flow Imbalance) cross-sectionally
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 0, t, L0_FieldOffset::ofi_1);
      convert_fp16_to_fp32(input, input_fp32_.data(), A);
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_zscore_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      convert_fp32_to_fp16(output_fp32_.data(), output_fp16_.data(), A);
      CS_WRITE_ALL(store_, date_str_, 0, t, L0_FieldOffset::cs_liquidity_ratio, output_fp16_.data(), A);
    }
  }

private:
  GlobalFeatureStore *store_;
  std::string date_str_;

  // Shared buffers (references)
  std::vector<size_t> &valid_indices_;
  std::vector<float> &input_fp32_;
  std::vector<float> &output_fp32_;
  std::vector<_Float16> &output_fp16_;
};
