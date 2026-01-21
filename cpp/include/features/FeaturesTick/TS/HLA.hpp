#pragma once

// =============================================================================
// HLA (Hidden Liquidity Adjusted) - 潜在流动性调整失衡
// =============================================================================
// 预测后续时刻的失衡 (根据 refill/cancel rate)
//   hla_imba = (Ṽ^B - Ṽ^A) / (Ṽ^B + Ṽ^A)
//   Ṽ^s = V^s * (1 + ρ^s)
//   ρ^s = (|O^M,s| - |O^C,s|) / (|O^M,s| + |O^C,s|)  (refill rate)
//
// 输入频率: PER_ORDER + ON_DEPTH
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class HiddenLiquidityAdjusted {
public:
  HiddenLiquidityAdjusted(TickData &td,
                          const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
                          const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
                          CBuffer<float, L2::BLEN> &hla_imba)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty), hla_imba_(hla_imba) {}

  // 每笔订单调用
  void accumulate() {
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      if (is_bid) vol_maker_bid_ += vol;
      else vol_maker_ask_ += vol;
      break;
    case L2::OrderType::CANCEL:
      if (is_bid) vol_cancel_bid_ += vol;
      else vol_cancel_ask_ += vol;
      break;
    default:
      break;
    }
  }

  // 每秒输出 (ON_DEPTH 时调用)
  void flush() {
    // 计算当前买一卖一量
    float v_bid = bid_qty_[0].back();
    float v_ask = -ask_qty_[0].back(); // ask是负值

    // 计算 refill rate: ρ = (maker - cancel) / (maker + cancel)
    float sum_bid = vol_maker_bid_ + vol_cancel_bid_;
    float sum_ask = vol_maker_ask_ + vol_cancel_ask_;
    float rho_bid = sum_bid > 1e-6f ? (vol_maker_bid_ - vol_cancel_bid_) / sum_bid : 0.0f;
    float rho_ask = sum_ask > 1e-6f ? (vol_maker_ask_ - vol_cancel_ask_) / sum_ask : 0.0f;

    // 调整后的流动性
    float v_tilde_bid = v_bid * (1.0f + rho_bid);
    float v_tilde_ask = v_ask * (1.0f + rho_ask);

    // hla_imba
    float sum = v_tilde_bid + v_tilde_ask;
    hla_imba_.push_back(sum > 1e-6f ? (v_tilde_bid - v_tilde_ask) / sum : 0.0f);

    // 重置累计器
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

private:
  TickData &td_;
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];
  CBuffer<float, L2::BLEN> &hla_imba_;

  // 秒内累计
  float vol_maker_bid_ = 0.0f, vol_maker_ask_ = 0.0f;
  float vol_cancel_bid_ = 0.0f, vol_cancel_ask_ = 0.0f;
};
