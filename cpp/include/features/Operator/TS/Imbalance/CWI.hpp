#pragma once

// =============================================================================
// CWI (Convexity-Weighted Imbalance) - 凸加权失衡
// =============================================================================
// 按档位 i^(-γ) 加权的多档失衡
//
// 【公式定义】
//   w_i = 1 / (i + ε)^γ
//   CWI = Σ w_i·(V_{i,t}^{M,B} - V_{i,t}^{M,A}) / Σ w_i·(V_{i,t}^{M,B} + V_{i,t}^{M,A})
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_qty[0:29] (onDepth), ask_qty[0:29] (onDepth)
//   输出: cwi_γ (onDepth)
//
// 【模板参数】
//   GAMMA_X10 - γ值的10倍 (10=γ1.0, 20=γ2.0), 避免浮点模板参数
//
// 【使用示例】
//   CWI<10> cwi_1{BidQty_, AskQty_, Cwi_1_};  // γ=1.0
//   CWI<20> cwi_2{BidQty_, AskQty_, Cwi_2_};  // γ=2.0
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

template <int GAMMA_X10, size_t DEPTH_SIZE = L2::LOB_DEPTH>
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
    float numer = 0.0f; // 加权失衡和
    float denom = 0.0f; // 加权总量

    // 遍历所有档位，计算凸加权失衡
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();  // 买i+1档数量
      float a = -ask_qty_[i].back(); // 卖i+1档数量（取反）
      float w = weights_[i];         // 第i+1档的权重：1/(i+1)^γ

      numer += w * (b - a); // 加权失衡 = w * (买量-卖量)
      denom += w * (b + a); // 加权总量 = w * (买量+卖量)
    }

    // 计算凸加权失衡率，值域[-1,1]
    value_ = denom > 1e-6f ? numer / denom : 0.0f;
  }

  inline void flush() {
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float weights_[DEPTH_SIZE];
  float value_ = 0.0f;
};
