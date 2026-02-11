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
//
// DAG中使用:
//   DDI<1> ddi_1{bid_qty_, ask_qty_, bid_price_, ask_price_, ddi_1_};  // λ=0.01
//   DDI<2> ddi_2{bid_qty_, ask_qty_, bid_price_, ask_price_, ddi_2_};  // λ=0.02
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

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

  inline void compute() {
    // 计算中间价（买一卖一均价）作为距离基准
    float mid = (bid_price_[0].back() + ask_price_[0].back()) * 0.5f;

    float numer = 0.0f;  // 加权失衡和
    float denom = 0.0f;  // 加权总量

    // 遍历所有档位，计算距离折扣失衡
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();       // 买i+1档数量
      float a = -ask_qty_[i].back();      // 卖i+1档数量（取反）
      float bp = bid_price_[i].back();    // 买i+1档价格
      float ap = ask_price_[i].back();    // 卖i+1档价格

      // 计算各档到中间价的距离
      float dist_b = mid - bp;            // 买档距离（中间价-买价）
      float dist_a = ap - mid;            // 卖档距离（卖价-中间价）
      float dist = (dist_b + dist_a) * 0.5f; // 平均距离

      // 计算距离衰减权重：w = e^(-λ*dist)，距离越远权重越小
      float w = std::exp(-LAMBDA * dist);
      numer += w * (b - a);  // 加权失衡
      denom += w * (b + a);  // 加权总量
    }

    // 计算距离折扣失衡率，值域[-1,1]
    value_ = denom > 1e-6f ? numer / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的DDI值写入输出CBuffer
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
