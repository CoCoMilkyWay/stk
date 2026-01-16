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

  void compute() {
    // 获取中间价 (买1卖1均价)
    float mid = (bid_price_[0].back() + ask_price_[0].back()) * 0.5f;

    float numer = 0.0f;
    float denom = 0.0f;

    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();
      float a = -ask_qty_[i].back();
      float bp = bid_price_[i].back();
      float ap = ask_price_[i].back();

      // 距离: 从各档价格到中间价
      float dist_b = mid - bp;
      float dist_a = ap - mid;
      float dist = (dist_b + dist_a) * 0.5f; // 平均距离

      float w = std::exp(-LAMBDA * dist);
      numer += w * (b - a);
      denom += w * (b + a);
    }

    out_.push_back(denom > 1e-6f ? numer / denom : 0.0f);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
};
