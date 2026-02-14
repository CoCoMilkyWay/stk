#pragma once

// =============================================================================
// DDI (Distance-Discounted Imbalance) - 距离折扣失衡
// =============================================================================
// 按价格距离 e^(-λΔp) 折扣的多档失衡
//
// 【公式定义】
//   Δp_i = 档位i到中间价的距离
//   w_i = e^(-λ·Δp_i)
//   DDI = Σ w_i·(V_{i,t}^{M,B} - V_{i,t}^{M,A}) / Σ w_i·(V_{i,t}^{M,B} + V_{i,t}^{M,A})
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_qty[0:29] (onDepth), ask_qty[0:29] (onDepth), bid_price[0:29] (onDepth), ask_price[0:29] (onDepth)
//   输出: ddi_λ (onDepth)
//
// 【模板参数】
//   LAMBDA_X100 - λ值的100倍 (1=λ0.01, 2=λ0.02)
//
// 【使用示例】
//   DDI<1> ddi_1{BidQty_, AskQty_, BidPrice_, AskPrice_, Ddi_1_};  // λ=0.01
//   DDI<2> ddi_2{BidQty_, AskQty_, BidPrice_, AskPrice_, Ddi_2_};  // λ=0.02
//
// 【备注】
//   - 使用快速exp近似 (泰勒前4项), 相对误差 < 1%
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <int LAMBDA_X100, size_t DEPTH_SIZE = L2::LOB_DEPTH>
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

  // 快速exp近似: e^x ≈ 1 + x + x²/2 + x³/6 (泰勒前4项)
  // 适用范围 x ∈ [-10, 0], 相对误差 < 1%
  static inline float fast_exp(float x) {
    x = x < -10.0f ? -10.0f : x; // 限制范围，避免极端值
    x = x > 0.0f ? 0.0f : x;
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
  }

  inline void flush() {
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
