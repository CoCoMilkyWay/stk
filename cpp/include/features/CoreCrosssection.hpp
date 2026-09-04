#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Misc/CSMethods.hpp"
#include "misc/profiler.hpp"
#include <algorithm>
#include <cassert>
#include <vector>

// ============================================================================
// CoreCrosssection: 截面计算 (CS worker), L0 每秒 + L1 分钟边界级联
//   表驱动: 字段表 CS(src_lvl, src, tf, m) 列 → FeatureStoreConfig L*_CS_DEFS[]; 热循环统一:
//     gather(valid 子集 dense) → cs::apply(method, tf) → scatter (无效资产输出 0)
//   方法 (kernel 在 CSMethods.cpp, precise TU, 逐步复刻 qmt):
//     NormRank    rank → inverse normal
//     WinsorRank  winsor_mad → z → pct_rank                         (= qmt factor_pipeline)
//     NeutralRank winsor_q → 行业+log市值中性化 → z → pct_rank      (= qmt neutral_pipeline, 仅 L1)
// ============================================================================
class CoreCrosssection {
public:
  explicit CoreCrosssection(GlobalFeatureStore &store)
      : store_(store), A_(store.query_A()), input_fp32_(A_), output_fp32_(A_), output_fp16_(A_) {
    valid_indices_.reserve(A_);
  }

  void set_date(const std::string &date_str) { date_str_ = date_str; }

  void compute_and_store(size_t t) noexcept {
    TraceN("CS");
    TraceColor(C_Magenta);
    {
      TraceN("CS_Tick");
      run<0>(t);
    }
    if (t % 60 == 0 && t > 0) { // 分钟边界级联
      TraceN("CS_Minute");
      run<1>(t / 60);
    }
  }

private:
  template <size_t LVL>
  void run(size_t t) {
    const size_t t_other = LVL == 0 ? L0_to_L1(t) : L1_to_L0(t); // 跨层源列的时间索引 (L1 用分钟起始秒)
    constexpr const CSFeatureDef *DEFS = LVL == 0 ? L0_CS_DEFS : L1_CS_DEFS;
    constexpr size_t COUNT = LVL == 0 ? L0_CS_COUNT : L1_CS_COUNT;
    constexpr size_t VALID = LVL == 0 ? size_t(L0_Field::_data_valid) : size_t(L1_Field::_data_valid);

    const _Float16 *valid_flags = fstore::cs_read(store_, date_str_, LVL, t, VALID);
    valid_indices_.clear();
    for (size_t a = 0; a < A_; ++a)
      if (static_cast<float>(valid_flags[a]) > 0.5f)
        valid_indices_.push_back(a);
    if (valid_indices_.empty())
      return;
    const size_t n = valid_indices_.size();

    // 中性化输入 (L1, 每分钟一次, 所有 NeutralRank 特征复用): log(mcap) + 行业
    const float *logmc = nullptr, *industry = nullptr;
    if constexpr (LVL == 1) {
      logmc_dense_.resize(A_);
      industry_dense_.resize(A_);
      gather_(fstore::cs_read(store_, date_str_, 1, t, L1_Field::mcap), logmc_dense_.data(), n);
      cs::prepare_logmc(logmc_dense_.data(), n);
      gather_(fstore::cs_read(store_, date_str_, 1, t, L1_Field::industry_l1), industry_dense_.data(), n);
      logmc = logmc_dense_.data();
      industry = industry_dense_.data();
    }

    for (size_t k = 0; k < COUNT; ++k) {
      const CSFeatureDef &d = DEFS[k];
      if constexpr (LVL == 0)
        assert(d.method != cs::Method::NeutralRank && "L0 has no neutralization inputs");

      float *y = input_fp32_.data();
      gather_(fstore::cs_read(store_, date_str_, d.src_lvl, d.src_lvl == LVL ? t : t_other, d.src), y, n);
      cs::apply(d.method, d.tf, y, n, logmc, industry);

      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      for (size_t i = 0; i < n; ++i)
        output_fp32_[valid_indices_[i]] = y[i];
      for (size_t a = 0; a < A_; ++a)
        output_fp16_[a] = static_cast<_Float16>(output_fp32_[a]);
      fstore::cs_write(store_, date_str_, LVL, t, d.dst, output_fp16_.data(), A_);
    }
  }

  // 源列 valid 子集 → dense fp32
  void gather_(const _Float16 *src, float *dst, size_t n) const {
    for (size_t i = 0; i < n; ++i)
      dst[i] = static_cast<float>(src[valid_indices_[i]]);
  }

  GlobalFeatureStore &store_;
  std::string date_str_;
  size_t A_;

  std::vector<size_t> valid_indices_;
  std::vector<float> input_fp32_;
  std::vector<float> output_fp32_;
  std::vector<_Float16> output_fp16_;
  std::vector<float> logmc_dense_;    // L1 中性化输入
  std::vector<float> industry_dense_; // L1 中性化输入
};
