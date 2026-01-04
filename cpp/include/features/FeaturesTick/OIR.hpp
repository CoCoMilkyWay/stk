#pragma once

// =============================================================================
// OIR (Order Imbalance Ratio) - 订单失衡率因子
// =============================================================================
// Reference: 中信建投《高频量价选股因子初探》2020.07.09
//
// 核心思想:
//   委托量比率形式的失衡度量，补充VOI的绝对量信息
//   OIR = (V^B - V^A) / (V^B + V^A)
//
// vs VOI:
//   VOI: 绝对增量差，受股票成交量规模影响
//   OIR: 比率形式，[-1, 1]标准化，跨股票可比
//
// 注意:
//   使用金额(万元)而非数量(股)，保证截面可比性
//
// 研报发现:
//   - 高频正向相关 (分钟IC 12.92%)
//   - 低频反转 (月频IC -3.70%)
//   - 剔除Momentum_12m后: IC -4.19%, 年化多空17.75%
//   - 表现优于VOI (IC -3.35%)
//
// 参数:
//   - N_LEVELS: 使用档位数 (1-30)
//   - 衰减权重: w_i = 1 - (i-1)/N_LEVELS
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS = 5, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class OIR {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE,
                "N_LEVELS must be 1-DEPTH_SIZE");

public:
  // 使用金额(万元)而非数量(股)，保证截面可比性
  OIR(const CBuffer<float, L2::BLEN> (&bid_amt)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_amt)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &buffer)
      : bid_amt_(bid_amt), ask_amt_(ask_amt), buffer_(buffer) {
    // 初始化权重: w_i = 1 - (i-1)/N_LEVELS
    weight_sum_ = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] = 1.0f - static_cast<float>(i) / N_LEVELS;
      weight_sum_ += weights_[i];
    }
  }

  void compute() {
    // 计算加权委托金额
    float weighted_bid = 0.0f;
    float weighted_ask = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      float bid_a = bid_amt_[i].back();
      float ask_a = -ask_amt_[i].back(); // 转为正数
      weighted_bid += weights_[i] * bid_a;
      weighted_ask += weights_[i] * ask_a;
    }

    // 归一化
    weighted_bid /= weight_sum_;
    weighted_ask /= weight_sum_;

    // OIR = (A^B - A^A) / (A^B + A^A)
    float sum = weighted_bid + weighted_ask;
    float oir = 0.0f;
    if (sum > 1e-6f) {
      oir = (weighted_bid - weighted_ask) / sum;
    }

    buffer_.push_back(oir);
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> (&bid_amt_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_amt_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &buffer_;

  float weights_[N_LEVELS];
  float weight_sum_;
};

