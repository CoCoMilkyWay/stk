#pragma once

// =============================================================================
// OFI (Order Flow Imbalance) - 订单流失衡: 前 N 档委托量增量的加权差
// =============================================================================
//   ΔV^B: price↓→0, price=→V-V_prev, price↑→V
//   ΔV^A: price↓→V, price=→V-V_prev, price↑→0
//   OFI_N = Σ w_i·(ΔV_i^B - ΔV_i^A),  w_i = 1 - (i-1)/N 归一化 (近端权重高)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class OFI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  OFI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), bid_price_(bid_price), ask_price_(ask_price) {
    float w_sum = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] = 1.0f - static_cast<float>(i) / static_cast<float>(N_LEVELS);
      w_sum += weights_[i];
    }
    for (size_t i = 0; i < N_LEVELS; ++i)
      weights_[i] /= w_sum;
  }

  inline void compute() {
    float ofi = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float bp = bid_price_[i].back();
      float bq = bid_qty_[i].back();
      float ap = ask_price_[i].back();
      float aq = -ask_qty_[i].back(); // ask 存负值

      float delta_bid = bp < prev_bp_[i] ? 0.0f : bp == prev_bp_[i] ? bq - prev_bq_[i]
                                                                    : bq;
      float delta_ask = ap < prev_ap_[i] ? aq : ap == prev_ap_[i] ? aq - prev_aq_[i]
                                                                  : 0.0f;
      ofi += weights_[i] * (delta_bid - delta_ask);

      prev_bp_[i] = bp;
      prev_bq_[i] = bq;
      prev_ap_[i] = ap;
      prev_aq_[i] = aq;
    }
    y[value] = ofi;
  }

  inline void reset() {
    for (size_t i = 0; i < N_LEVELS; ++i)
      prev_bp_[i] = prev_bq_[i] = prev_ap_[i] = prev_aq_[i] = 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];

  float weights_[N_LEVELS];
  float prev_bp_[N_LEVELS] = {}, prev_bq_[N_LEVELS] = {};
  float prev_ap_[N_LEVELS] = {}, prev_aq_[N_LEVELS] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ofi_1(N) N(Ofi_1, (OFI<1>), (DepthData.bid_qty, DepthData.ask_qty, DepthData.bid_price, DepthData.ask_price), onDepth, onDepth)

#define FIELDS_L0_Ofi_1(X) \
  X(ofi_1, 1, DEPTH, ORDER_FLOW, RAW, NONE, "100/00/00", "Order Flow Imba 1-Level", "订单流失衡1档", "对近端大单挂单变动非常敏感", R"(\Delta V_{1,t}^{M,B} - \Delta V_{1,t}^{M,A}, \quad \Delta V_{1,t}^{M,B} = \begin{cases}0, & P_{1,t}^{M,B} < P_{1,t-1}^{M,B} \\ V_{1,t}^{M,B} - V_{1,t-1}^{M,B}, & P_{1,t}^{M,B} = P_{1,t-1}^{M,B} \\ V_{1,t}^{M,B}, & P_{1,t}^{M,B} > P_{1,t-1}^{M,B} \end{cases}, \quad \Delta V_{1,t}^{M,A} = \begin{cases}V_{1,t}^{M,A}, & P_{1,t}^{M,A} < P_{1,t-1}^{M,A} \\ V_{1,t}^{M,A} - V_{1,t-1}^{M,A}, & P_{1,t}^{M,A} = P_{1,t-1}^{M,A} \\ 0, & P_{1,t}^{M,A} > P_{1,t-1}^{M,A} \end{cases})", OP(Ofi_1))

#define NODE_Ofi_5(N) N(Ofi_5, (OFI<5>), (DepthData.bid_qty, DepthData.ask_qty, DepthData.bid_price, DepthData.ask_price), onDepth, onDepth)

#define FIELDS_L0_Ofi_5(X) \
  X(ofi_5, 1, DEPTH, ORDER_FLOW, RAW, NONE, "100/00/00", "Order Flow Imba 5-Level", "订单流失衡5档加权", "监控5档挂单变化", R"(\Delta V_t^{W,M,B} - \Delta V_t^{W,M,A}, \quad V_t^{W,M,s} = \frac{\sum_{i=1}^N w_i V_{i,t}^{M,s}}{\sum_{i=1}^N w_i}, \quad w_i = 1 - \frac{i-1}{N}, \quad N = 5, \quad s \in \{B,A\})", OP(Ofi_5))
