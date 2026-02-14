#pragma once

// =============================================================================
// EntropyImba - 熵失衡
// =============================================================================
// 对比买卖两侧熵值的大小
//
// 【公式定义】
//   entropy_imba = (H^B - H^A) / (H^B + H^A)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_entropy (onMinute), ask_entropy (onMinute)
//   输出: entropy_imba (onMinute)
//
// 【备注】
//   - 正值表示买侧分布更均匀（做市意愿强）
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class EntropyImba {
public:
  EntropyImba(const CBuffer<float, L2::BLEN> &bid_entropy,
              const CBuffer<float, L2::BLEN> &ask_entropy,
              CBuffer<float, L2::BLEN> &out)
      : bid_entropy_(bid_entropy), ask_entropy_(ask_entropy), out_(out) {}

  inline void compute() {
    // 从买卖两侧的熵CBuffer读取最新值
    float b = bid_entropy_.back();  // 买侧熵
    float a = ask_entropy_.back();  // 卖侧熵
    // 计算熵失衡：(买侧-卖侧) / (买侧+卖侧)
    // 值域[-1,1]，正值表示买侧分布更均匀（做市意愿强）
    float denom = b + a;
    value_ = denom > 1e-6f ? (b - a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的熵失衡写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_entropy_;
  const CBuffer<float, L2::BLEN> &ask_entropy_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
