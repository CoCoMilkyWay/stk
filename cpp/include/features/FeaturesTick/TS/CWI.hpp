#pragma once

// =============================================================================
// CWI (Convexity-Weighted Imbalance) - 凸加权失衡
// =============================================================================
// 按档位 i^(-γ) 加权的多档失衡
//   w_i = 1 / (i + ε)^γ
//   CWI = Σ w_i*(V_bid - V_ask) / Σ w_i*(V_bid + V_ask)
//
// 模板参数:
//   GAMMA_X10 - γ值的10倍 (10=γ1.0, 20=γ2.0)，避免浮点模板参数
//   BUFFER_DEPTH - 内部缓存深度，用于降频 (0=不缓存直接输出, 5=缓存5个样本平均后输出)
//
// DAG中使用:
//   CWI<10, 0> cwi_1{bid_qty_, ask_qty_, cwi_1_};  // γ=1.0, 秒级直接输出
//   CWI<20, 0> cwi_2{bid_qty_, ask_qty_, cwi_2_};  // γ=2.0, 秒级直接输出
//   CWI<10, 5> cwi_1{bid_qty_, ask_qty_, cwi_1_};  // γ=1.0, 降频到分钟级
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <array>
#include <cmath>

template <int GAMMA_X10, size_t BUFFER_DEPTH = 0, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CWI {
public:
  static constexpr float GAMMA = static_cast<float>(GAMMA_X10) / 10.0f;
  static constexpr float EPSILON = 1e-6f;

  CWI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {
    // 预计算权重
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      weights_[i] = 1.0f / std::pow(static_cast<float>(i + 1) + EPSILON, GAMMA);
    }
  }

  inline void compute() {
    float numer = 0.0f;  // 加权失衡和
    float denom = 0.0f;  // 加权总量

    // 遍历所有档位，计算凸加权失衡
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();      // 买i+1档数量
      float a = -ask_qty_[i].back();     // 卖i+1档数量（取反）
      float w = weights_[i];             // 第i+1档的权重：1/(i+1)^γ

      // 按权重累加失衡和总量
      numer += w * (b - a);  // 加权失衡 = w * (买量-卖量)
      denom += w * (b + a);  // 加权总量 = w * (买量+卖量)
    }

    // 计算凸加权失衡率，值域[-1,1]
    value_ = denom > 1e-6f ? numer / denom : 0.0f;

    // 如果有内部缓存，存入缓存
    if constexpr (BUFFER_DEPTH > 0) {
      buffer_[buffer_idx_] = value_;
      buffer_idx_ = (buffer_idx_ + 1) % BUFFER_DEPTH;
      if (buffer_size_ < BUFFER_DEPTH) {
        buffer_size_++;
      }
    }
  }

  inline void flush() {
    if constexpr (BUFFER_DEPTH == 0) {
      // 无缓存：直接输出
      out_.push_back(value_);
    } else {
      // 有缓存：取最近 N 个样本平均
      float sum = 0.0f;
      for (size_t i = 0; i < buffer_size_; ++i) {
        sum += buffer_[i];
      }
      float avg = buffer_size_ > 0 ? sum / static_cast<float>(buffer_size_) : 0.0f;
      out_.push_back(avg);
      
      // 清空缓存，准备下一轮
      buffer_size_ = 0;
      buffer_idx_ = 0;
    }
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float weights_[DEPTH_SIZE];
  float value_ = 0.0f;

  // 内部缓存（仅当 BUFFER_DEPTH > 0 时使用）
  std::array<float, (BUFFER_DEPTH > 0 ? BUFFER_DEPTH : 1)> buffer_{};
  size_t buffer_idx_ = 0;
  size_t buffer_size_ = 0;
};
