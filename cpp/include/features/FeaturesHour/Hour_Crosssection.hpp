#pragma once

#include "../misc/misc.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <vector>


// ============================================================================
// LEVEL 2: Hour-level Cross-sectional Features
// ============================================================================

class Hour_Crosssection {
public:
  Hour_Crosssection(GlobalFeatureStore &store,
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

  void compute_and_store(size_t t_hour) {
    const size_t A = input_fp32_.size();

    // Build valid indices
    const _Float16 *valid_flags = CS_READ_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::_data_valid);
    valid_indices_.clear();
    for (size_t a = 0; a < A; ++a) {
      if (static_cast<float>(valid_flags[a]) > 0.5f) {
        valid_indices_.push_back(a);
      }
    }

    if (valid_indices_.empty())
      return;

    // CS feature 1: cs_hour_return_beta
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::hour_ret_12h_mom);
      for (size_t a = 0; a < A; ++a)
        input_fp32_[a] = input[a];
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      for (size_t a = 0; a < A; ++a)
        output_fp16_[a] = output_fp32_[a];
      CS_WRITE_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::cs_hour_return_beta, output_fp16_.data(), A);
    }

    // CS feature 2: cs_hour_liq_adj_ret
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::hour_volatility);
      for (size_t a = 0; a < A; ++a)
        input_fp32_[a] = input[a];
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      for (size_t a = 0; a < A; ++a)
        output_fp16_[a] = output_fp32_[a];
      CS_WRITE_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::cs_hour_liq_adj_ret, output_fp16_.data(), A);
    }

    // CS feature 3: cs_hour_range_rank
    {
      const _Float16 *input = CS_READ_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::pivot_dev);
      for (size_t a = 0; a < A; ++a)
        input_fp32_[a] = input[a];
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      for (size_t a = 0; a < A; ++a)
        output_fp16_[a] = output_fp32_[a];
      CS_WRITE_ALL(store_, date_str_, 2, t_hour, L2_FieldOffset::cs_hour_range_rank, output_fp16_.data(), A);
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
