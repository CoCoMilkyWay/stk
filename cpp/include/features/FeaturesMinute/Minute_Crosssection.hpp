#pragma once

#include "../misc/misc.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <vector>


// ============================================================================
// LEVEL 1: Minute-level Cross-sectional Features
// ============================================================================

class Minute_Crosssection {
public:
  Minute_Crosssection(GlobalFeatureStore &store,
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

  void compute_and_store(size_t t_minute) {
    const size_t A = input_fp32_.size();

    // Build valid indices
    const _Float16 *valid_flags = CS_READ_ALL(store_, date_str_, 1, t_minute, L1_FieldOffset::_data_valid);
    valid_indices_.clear();
    for (size_t a = 0; a < A; ++a) {
      if (static_cast<float>(valid_flags[a]) > 0.5f) {
        valid_indices_.push_back(a);
      }
    }

    if (valid_indices_.empty())
      return;

    // CS feature: cs_spread_rank
    // 从 L0 读取当前分钟结束时刻的 spread (使用 L1_to_L0 映射到秒级索引)
    {
      const size_t t_second = L1_to_L0(t_minute); // 分钟起始秒
      const _Float16 *spread_data = CS_READ_ALL(store_, date_str_, 0, t_second, L0_FieldOffset::spread);
      
      // 读取到 fp32 buffer
      for (size_t a = 0; a < A; ++a) {
        input_fp32_[a] = static_cast<float>(spread_data[a]);
      }
      
      // 计算截面排名 (rank inverse normal)
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      compute_rank_inverse_normal_sparse(input_fp32_.data(), valid_indices_, output_fp32_.data());
      
      // 转换为 fp16 并写入
      for (size_t a = 0; a < A; ++a) {
        output_fp16_[a] = static_cast<_Float16>(output_fp32_[a]);
      }
      CS_WRITE_ALL(store_, date_str_, 1, t_minute, L1_FieldOffset::cs_spread_rank, output_fp16_.data(), A);
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
