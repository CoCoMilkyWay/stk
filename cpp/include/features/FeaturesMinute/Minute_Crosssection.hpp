#pragma once

#include "../Misc/CSMethods.hpp"
#include "features/Backend/FeatureStore.hpp"
#include <algorithm>
#include <cstdint>
#include <vector>

// ============================================================================
// LEVEL 1: Minute-level Cross-sectional Features
// ============================================================================
// 配置表驱动: 每行 = 源列 + 元素变换 + 截面方法 → 目标列. 方法 per-特征可选:
//   NormRank    rank → inverse normal (cs_spread_rank 口径)
//   WinsorRank  winsor_mad → z → pct_rank             (= qmt factor_pipeline)
//   NeutralRank winsor_q → 行业+log市值中性化 → z → pct_rank (= qmt neutral_pipeline)
// kernel 在 CSMethods.cpp (precise TU, 逐步复刻 qmt). 热循环统一:
//   gather(valid 子集 dense) → cs::apply → scatter (无效资产输出 0).
// ============================================================================

struct CSFeatureDef {
  bool src_l0;       // 源列所在层: true = L0 (取分钟起始秒), false = L1
  cs::Transform tf;  // 元素变换 (估值比率 → 收益率口径用 Reciprocal)
  cs::Method method; // 截面方法
  std::uint16_t src; // 源列 (L0_FieldOffset / L1_FieldOffset)
  std::uint16_t dst; // L1 目标列
};

inline constexpr CSFeatureDef L1_CS_DEFS[] = {
    // ---- 普通方法 ----
    {true, cs::Transform::None, cs::Method::NormRank, L0_FieldOffset::spread, L1_FieldOffset::cs_spread_rank},
    // ---- qmt 中性化因子 (neutral_pipeline) ----
    {false, cs::Transform::Reciprocal, cs::Method::NeutralRank, L1_FieldOffset::pe, L1_FieldOffset::ep_ttm12},
    {false, cs::Transform::Reciprocal, cs::Method::NeutralRank, L1_FieldOffset::pb, L1_FieldOffset::bp_ttm3},
    {false, cs::Transform::Reciprocal, cs::Method::NeutralRank, L1_FieldOffset::ps, L1_FieldOffset::sp_ttm12},
    {false, cs::Transform::Reciprocal, cs::Method::NeutralRank, L1_FieldOffset::pcf, L1_FieldOffset::cp_ttm12},
    {false, cs::Transform::None, cs::Method::NeutralRank, L1_FieldOffset::dy_raw, L1_FieldOffset::dy_ttm12},
    {false, cs::Transform::None, cs::Method::NeutralRank, L1_FieldOffset::roe_raw, L1_FieldOffset::roe_ttm12},
    {false, cs::Transform::None, cs::Method::NeutralRank, L1_FieldOffset::roa_raw, L1_FieldOffset::roa_ttm12},
    // ---- qmt 因子流水 (factor_pipeline; cffoa 在 qmt 即此流水, 非中性化) ----
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::cffoa_raw, L1_FieldOffset::cffoa_ttm12},
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::mcap, L1_FieldOffset::mcap_cs},
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::fmcap, L1_FieldOffset::fmcap_cs},
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::_ohlc_close, L1_FieldOffset::close_cs},
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::mr_bal, L1_FieldOffset::mr_bal_cs},
    {false, cs::Transform::None, cs::Method::WinsorRank, L1_FieldOffset::ms_bal, L1_FieldOffset::ms_bal_cs},
};

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

    const size_t n = valid_indices_.size();
    const size_t t_second = L1_to_L0(t_minute); // 分钟起始秒 (L0 源列用)

    // 中性化输入 (每分钟一次, 所有 NeutralRank 特征复用): log(mcap) + 行业
    if (logmc_dense_.size() != A) {
      logmc_dense_.resize(A);
      industry_dense_.resize(A);
    }
    gather_dense_(t_minute, L1_FieldOffset::mcap, logmc_dense_.data(), n);
    cs::prepare_logmc(logmc_dense_.data(), n);
    gather_dense_(t_minute, L1_FieldOffset::industry_l1, industry_dense_.data(), n);

    for (const CSFeatureDef &d : L1_CS_DEFS) {
      // gather 源列 valid 子集 → dense fp32 (input_fp32_ 前 n 个)
      const _Float16 *src = d.src_l0
                                ? CS_READ_ALL(store_, date_str_, 0, t_second, d.src)
                                : CS_READ_ALL(store_, date_str_, 1, t_minute, d.src);
      float *y = input_fp32_.data();
      for (size_t i = 0; i < n; ++i) {
        y[i] = static_cast<float>(src[valid_indices_[i]]);
      }

      // 变换 + 截面方法 (precise TU, NaN 语义可靠)
      cs::apply(d.method, d.tf, y, n, logmc_dense_.data(), industry_dense_.data());

      // scatter 回全资产宽度, 无效资产输出 0
      std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
      for (size_t i = 0; i < n; ++i) {
        output_fp32_[valid_indices_[i]] = y[i];
      }
      for (size_t a = 0; a < A; ++a) {
        output_fp16_[a] = static_cast<_Float16>(output_fp32_[a]);
      }
      CS_WRITE_ALL(store_, date_str_, 1, t_minute, d.dst, output_fp16_.data(), A);
    }
  }

private:
  void gather_dense_(size_t t_minute, size_t field, float *dst, size_t n) {
    const _Float16 *src = CS_READ_ALL(store_, date_str_, 1, t_minute, field);
    for (size_t i = 0; i < n; ++i) {
      dst[i] = static_cast<float>(src[valid_indices_[i]]);
    }
  }

  GlobalFeatureStore *store_;
  std::string date_str_;

  // Shared buffers (references)
  std::vector<size_t> &valid_indices_;
  std::vector<float> &input_fp32_;
  std::vector<float> &output_fp32_;
  std::vector<_Float16> &output_fp16_;

  // 中性化输入 dense 缓冲 (lazy 分配)
  std::vector<float> logmc_dense_;
  std::vector<float> industry_dense_;
};
