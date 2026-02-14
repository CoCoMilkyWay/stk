#pragma once

// =============================================================================
// GradImba - 梯度失衡
// =============================================================================
// 对比买卖两侧梯度的绝对值大小
//
// 【公式定义】
//   grad_imba = (|grad^B| - |grad^A|) / (|grad^B| + |grad^A|)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_grad (onMinute), ask_grad (onMinute)
//   输出: grad_imba (onMinute)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

class GradImba {
public:
  GradImba(const CBuffer<float, L2::BLEN> &bid_grad,
           const CBuffer<float, L2::BLEN> &ask_grad,
           CBuffer<float, L2::BLEN> &out)
      : bid_grad_(bid_grad), ask_grad_(ask_grad), out_(out) {}

  inline void compute() {
    // 从买卖两侧的梯度CBuffer读取最新值
    float b = bid_grad_.back(); // 买侧梯度
    float a = ask_grad_.back(); // 卖侧梯度

    // 计算绝对值
    float abs_b = std::abs(b);
    float abs_a = std::abs(a);

    // 计算梯度失衡：(|买侧|-|卖侧|) / (|买侧|+|卖侧|)
    // 值域[-1,1]，正值表示买侧梯度绝对值更大
    float denom = abs_b + abs_a;
    value_ = denom > 1e-6f ? (abs_b - abs_a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的梯度失衡写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_grad_;
  const CBuffer<float, L2::BLEN> &ask_grad_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
