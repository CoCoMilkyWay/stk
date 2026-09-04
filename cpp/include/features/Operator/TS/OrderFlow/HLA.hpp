#pragma once

// =============================================================================
// HLA (Hidden Liquidity Adjusted) - 潜在流动性调整失衡: 按 refill rate 预测后续失衡
// =============================================================================
//   hla_imba = (Ṽ^B - Ṽ^A) / (Ṽ^B + Ṽ^A),  Ṽ^s = V_1^s · (1 + ρ^s)
//   ρ^s = (|O^{M,s}| - |O^{C,s}|) / (|O^{M,s}| + |O^{C,s}|)   (分钟内 refill rate)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class HLA {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  HLA(TickData &td,
      const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
      const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH])
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  // 每笔订单: 累计挂单/撤单量 (成交不计)
  inline void compute() {
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      (is_bid ? vol_maker_bid_ : vol_maker_ask_) += vol;
      break;
    case L2::OrderType::CANCEL:
      (is_bid ? vol_cancel_bid_ : vol_cancel_ask_) += vol;
      break;
    default:
      break;
    }
  }

  // 分钟末结算
  inline void flush() {
    float v_bid = bid_qty_[0].back();
    float v_ask = -ask_qty_[0].back(); // ask 存负值

    float sum_bid = vol_maker_bid_ + vol_cancel_bid_;
    float sum_ask = vol_maker_ask_ + vol_cancel_ask_;
    float rho_bid = sum_bid > 1e-6f ? (vol_maker_bid_ - vol_cancel_bid_) / sum_bid : 0.0f;
    float rho_ask = sum_ask > 1e-6f ? (vol_maker_ask_ - vol_cancel_ask_) / sum_ask : 0.0f;

    float v_tilde_bid = v_bid * (1.0f + rho_bid);
    float v_tilde_ask = v_ask * (1.0f + rho_ask);
    float sum = v_tilde_bid + v_tilde_ask;
    y[value] = sum > 1e-6f ? (v_tilde_bid - v_tilde_ask) / sum : 0.0f;

    reset();
  }

  inline void reset() {
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

private:
  TickData &td_;
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];

  float vol_maker_bid_ = 0.0f, vol_maker_ask_ = 0.0f;
  float vol_cancel_bid_ = 0.0f, vol_cancel_ask_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Hla(N) N(Hla, (HLA), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Hla(X) \
  X(hla_imba, 1, DATA, ORDER_FLOW, RATIO, NONE, "00/100/00", "Hidden-liquidity-adjusted Imba", "潜在流动性失衡", "预测后续时刻的失衡(按照refill/cancel rate)(降频)", R"(\frac{\tilde{V}_{1,t}^{M,B} - \tilde{V}_{1,t}^{M,A}}{\tilde{V}_{1,t}^{M,B} + \tilde{V}_{1,t}^{M,A}}, \quad \tilde{V}_{1,t}^{M,s} = V_{1,t}^{M,s}(1+\rho_t^{s}), \quad \rho_t^{s} = \frac{|O_t^{M,s}| - |O_t^{C,s}|}{|O_t^{M,s}| + |O_t^{C,s}|}, \quad s \in \{B,A\})", OP(Hla))
