#pragma once

#include "../Misc/CSMethods.hpp"
#include "features/Backend/FeatureStore.hpp"
#include <algorithm>
#include <cassert>
#include <vector>

// ============================================================================
// LEVEL 0: Tick-level Cross-sectional Features
// ============================================================================

// 表驱动: FeatureStoreConfig 由字段表 CS(...) 列展开的 L0_CS_DEFS[]; 内核与 L1 同 (cs::apply).
// L0 无中性化输入 (NeutralRank 在 L0 不可用).

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

    const size_t n = valid_indices_.size();
    const size_t t_minute = L0_to_L1(t); // L1 源列用

    for (size_t k = 0; k < L0_CS_COUNT; ++k) {
      const CSFeatureDef &d = L0_CS_DEFS[k];
      assert(d.method != cs::Method::NeutralRank && "L0 has no neutralization inputs");
      const _Float16 *src = d.src_lvl == 0
                                ? CS_READ_ALL(store_, date_str_, 0, t, d.src)
                                : CS_READ_ALL(store_, date_str_, 1, t_minute, d.src);
      float *y = input_fp32_.data();
      for (size_t i = 0; i < n; ++i) {
        y[i] = static_cast<float>(src[valid_indices_[i]]);
      }

      cs::apply(d.method, d.tf, y, n, nullptr, nullptr);

      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      for (size_t i = 0; i < n; ++i) {
        output_fp32_[valid_indices_[i]] = y[i];
      }
      for (size_t a = 0; a < A; ++a) {
        output_fp16_[a] = static_cast<_Float16>(output_fp32_[a]);
      }
      CS_WRITE_ALL(store_, date_str_, 0, t, d.dst, output_fp16_.data(), A);
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
