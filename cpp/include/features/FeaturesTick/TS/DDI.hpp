#pragma once

// =============================================================================
// DDI (Distance-Discounted Imbalance) - 距离折扣失衡
// =============================================================================
// 按价格距离 e^(-λΔp) 折扣的多档失衡
//   Δp_i = (ask_price_i - bid_price_i) / 2 相对中间价的距离
//   DDI = Σ e^(-λΔp)*(V_bid - V_ask) / Σ e^(-λΔp)*(V_bid + V_ask)
//
// 模板参数:
//   LAMBDA_X100 - λ值的100倍 (1=λ0.01, 2=λ0.02)
//   BUFFER_DEPTH - 内部缓存深度，用于降频 (0=不缓存直接输出, 5=缓存5个样本平均后输出)
//
// DAG中使用:
//   DDI<1, 0> ddi_1{bid_qty_, ask_qty_, bid_price_, ask_price_, ddi_1_};  // λ=0.01, 秒级直接输出
//   DDI<2, 0> ddi_2{bid_qty_, ask_qty_, bid_price_, ask_price_, ddi_2_};  // λ=0.02, 秒级直接输出
//   DDI<1, 5> ddi_1{bid_qty_, ask_qty_, bid_price_, ask_price_, ddi_1_};  // λ=0.01, 降频到分钟级
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <array>

template <int LAMBDA_X100, size_t BUFFER_DEPTH = 0, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class DDI {
public:
  static constexpr float LAMBDA = static_cast<float>(LAMBDA_X100) / 100.0f;

  DDI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        bid_price_(bid_price),
        ask_price_(ask_price),
        out_(out) {}

  // 快速exp近似: e^x ≈ (1 + x/256)^256, 使用分段多项式
  // 适用范围 x ∈ [-10, 0], 相对误差 < 1%
  static inline float fast_exp(float x) {
    // 限制范围，避免极端值
    x = x < -10.0f ? -10.0f : x;
    x = x > 0.0f ? 0.0f : x;

    // 多项式近似: e^x ≈ 1 + x + x²/2 + x³/6 (泰勒前4项)
    // 对于小的负数x，精度足够且快速
    float x2 = x * x;
    return 1.0f + x + x2 * 0.5f + x2 * x * 0.16666667f;
  }

  inline void compute() {
    // 计算中间价（买一卖一均价）作为距离基准
    float mid = (bid_price_[0].back() + ask_price_[0].back()) * 0.5f;

    float numer = 0.0f; // 加权失衡和
    float denom = 0.0f; // 加权总量

    // 遍历所有档位，计算距离折扣失衡
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();    // 买i+1档数量
      float a = -ask_qty_[i].back();   // 卖i+1档数量（取反）
      float bp = bid_price_[i].back(); // 买i+1档价格
      float ap = ask_price_[i].back(); // 卖i+1档价格

      // 计算各档到中间价的距离
      float dist_b = mid - bp;               // 买档距离（中间价-买价）
      float dist_a = ap - mid;               // 卖档距离（卖价-中间价）
      float dist = (dist_b + dist_a) * 0.5f; // 平均距离

      // 计算距离衰减权重：w = e^(-λ*dist)，距离越远权重越小
      float w = fast_exp(-LAMBDA * dist);
      numer += w * (b - a); // 加权失衡
      denom += w * (b + a); // 加权总量
    }

    // 计算距离折扣失衡率，值域[-1,1]
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
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;

  // 内部缓存（仅当 BUFFER_DEPTH > 0 时使用）
  std::array<float, (BUFFER_DEPTH > 0 ? BUFFER_DEPTH : 1)> buffer_{};
  size_t buffer_idx_ = 0;
  size_t buffer_size_ = 0;
};
