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

// compute: 每笔订单时累计, flush: 深度更新时输出（读取深度）
class HLA {
public:
  HLA(TickData &td,
      const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
      const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
      CBuffer<float, L2::BLEN> &hla_imba)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty), hla_imba_(hla_imba) {}

  inline void compute() {
    // 每笔订单时，统计挂单和撤单量（用于计算refill rate）
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    // 只关注挂单和撤单，不关注成交
    switch (lob.order_type) {
    case L2::OrderType::MAKER: // 挂单（补充流动性）
      if (is_bid)
        vol_maker_bid_ += vol;
      else
        vol_maker_ask_ += vol;
      break;
    case L2::OrderType::CANCEL: // 撤单（移除流动性）
      if (is_bid)
        vol_cancel_bid_ += vol;
      else
        vol_cancel_ask_ += vol;
      break;
    default:
      break;
    }
  }

  // 深度更新时输出
  inline void flush() {
    // 1. 从BidQty和AskQty CBuffer读取当前买一卖一量
    float v_bid = bid_qty_[0].back();  // 买一数量
    float v_ask = -ask_qty_[0].back(); // 卖一数量（取反）

    // 2. 计算refill rate（流动性补充率）：ρ = (挂单-撤单) / (挂单+撤单)
    // 使用上次深度更新到本次深度更新间隔内的订单流统计
    // 正值表示净补充流动性，负值表示净移除流动性
    float sum_bid = vol_maker_bid_ + vol_cancel_bid_;
    float sum_ask = vol_maker_ask_ + vol_cancel_ask_;
    float rho_bid = sum_bid > 1e-6f ? (vol_maker_bid_ - vol_cancel_bid_) / sum_bid : 0.0f;
    float rho_ask = sum_ask > 1e-6f ? (vol_maker_ask_ - vol_cancel_ask_) / sum_ask : 0.0f;

    // 3. 计算调整后的潜在流动性：Ṽ = V * (1 + ρ)
    // 预测后续时刻的流动性（考虑refill趋势）
    float v_tilde_bid = v_bid * (1.0f + rho_bid);
    float v_tilde_ask = v_ask * (1.0f + rho_ask);

    // 4. 计算潜在流动性调整失衡：(Ṽ_bid - Ṽ_ask) / (Ṽ_bid + Ṽ_ask)
    // 捕捉隐藏流动性带来的失衡预测
    float sum = v_tilde_bid + v_tilde_ask;
    hla_imba_.push_back(sum > 1e-6f ? (v_tilde_bid - v_tilde_ask) / sum : 0.0f);

    // 重置深度更新间隔内的累计器
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

  // 跨天重置
  inline void reset() {
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

private:
  TickData &td_;
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];
  CBuffer<float, L2::BLEN> &hla_imba_;

  // 深度更新间隔内的累计（上次深度更新到本次深度更新）
  float vol_maker_bid_ = 0.0f, vol_maker_ask_ = 0.0f;
  float vol_cancel_bid_ = 0.0f, vol_cancel_ask_ = 0.0f;
};
