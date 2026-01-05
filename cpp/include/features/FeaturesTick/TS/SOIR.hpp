#pragma once

// =============================================================================
// SOIR (Step Order Imbalance Ratio) - 逐档订单失衡率因子
// =============================================================================
// Reference: 中信建投《高频订单失衡及价差因子》2021.01.29
//
// 核心思想:
//   衡量各档买卖委托量比率的加权，捕捉当前买卖力量对比
//   SOIR_i = (V_i^B - V_i^A) / (V_i^B + V_i^A)
//   SOIR = Σ w_i × SOIR_i / Σ w_i
//
// vs OIR:
//   OIR: 先加权委托量，再算比率 => 单档量过大会dominate
//   SOIR: 先算各档比率，再加权 => 更稳健
//
// 研报发现:
//   - 高频正向相关 (分钟IC ~10%)
//   - 低频反转 (月频IC -2.68%~-5.37%)
//   - **重要**: 高档位单档效果更好: SOIR5 > SOIR4 > SOIR3 > SOIR2 > SOIR1
//   - 加权合成反而拉低表现 (因为SOIR1-2权重高但效果差)
//   - SOIR5单档: IC -5.37%, 年化多空21.32%, 夏普2.41
//   - SOIR (加权): IC -4.57%, 年化多空18.11%, 夏普1.70
//
// 参数:
//   - N_LEVELS: 使用档位数 (1-30)
//   - SINGLE_LEVEL: 单档模式 (true=只用第N_LEVELS档, false=1~N_LEVELS加权)
//   - 衰减权重: w_i = 1 - (i-1)/N_LEVELS
//
// 注意:
//   使用金额(万元)而非数量(股)，保证截面可比性
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS = 5, bool SINGLE_LEVEL = false,
          size_t DEPTH_SIZE = L2::LOB_DEPTH>
class SOIR {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE,
                "N_LEVELS must be 1-DEPTH_SIZE");

public:
  // 使用金额(万元)而非数量(股)，保证截面可比性
  SOIR(const CBuffer<float, L2::BLEN> (&bid_amt)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_amt)[DEPTH_SIZE],
       CBuffer<float, L2::BLEN> &buffer)
      : bid_amt_(bid_amt), ask_amt_(ask_amt), buffer_(buffer) {
    if constexpr (!SINGLE_LEVEL) {
      // 加权模式: 初始化权重 w_i = 1 - (i-1)/N_LEVELS
      weight_sum_ = 0.0f;
      for (size_t i = 0; i < N_LEVELS; ++i) {
        weights_[i] = 1.0f - static_cast<float>(i) / N_LEVELS;
        weight_sum_ += weights_[i];
      }
    }
  }

  void compute() {
    if constexpr (SINGLE_LEVEL) {
      // 单档模式: 只计算第 N_LEVELS 档 (0-indexed: N_LEVELS-1)
      constexpr size_t level = N_LEVELS - 1;
      float bid_a = bid_amt_[level].back();
      float ask_a = -ask_amt_[level].back();
      float sum_amt = bid_a + ask_a;

      float soir = 0.0f;
      if (sum_amt > 1e-6f) {
        soir = (bid_a - ask_a) / sum_amt;
      }
      buffer_.push_back(soir);
    } else {
      // 加权模式: 1~N_LEVELS档加权平均
      float soir = 0.0f;
      for (size_t i = 0; i < N_LEVELS; ++i) {
        float bid_a = bid_amt_[i].back();
        float ask_a = -ask_amt_[i].back();
        float sum_amt = bid_a + ask_a;

        float soir_i = 0.0f;
        if (sum_amt > 1e-6f) {
          soir_i = (bid_a - ask_a) / sum_amt;
        }
        soir += weights_[i] * soir_i;
      }
      soir /= weight_sum_;
      buffer_.push_back(soir);
    }
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> (&bid_amt_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_amt_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &buffer_;

  // 仅加权模式使用
  float weights_[SINGLE_LEVEL ? 1 : N_LEVELS] = {};
  float weight_sum_ = 1.0f;
};
