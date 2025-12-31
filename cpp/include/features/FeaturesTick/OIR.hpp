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
  OIR(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &buffer)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), buffer_(buffer) {
    // 初始化权重: w_i = 1 - (i-1)/N_LEVELS
    weight_sum_ = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] = 1.0f - static_cast<float>(i) / N_LEVELS;
      weight_sum_ += weights_[i];
    }
  }

  void compute() {
    // 计算加权委托量
    float weighted_bid = 0.0f;
    float weighted_ask = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      float bid_q = bid_qty_[i].back();
      float ask_q = -ask_qty_[i].back(); // 转为正数
      weighted_bid += weights_[i] * bid_q;
      weighted_ask += weights_[i] * ask_q;
    }

    // 归一化
    weighted_bid /= weight_sum_;
    weighted_ask /= weight_sum_;

    // OIR = (V^B - V^A) / (V^B + V^A)
    float sum = weighted_bid + weighted_ask;
    float oir = 0.0f;
    if (sum > 1e-6f) {
      oir = (weighted_bid - weighted_ask) / sum;
    }

    buffer_.push_back(oir);
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &buffer_;

  float weights_[N_LEVELS];
  float weight_sum_;
};

