#pragma once

// =============================================================================
// HLA (Hidden Liquidity Adjusted) - 潜在流动性调整失衡
// =============================================================================
// 预测后续时刻的失衡 (根据 refill/cancel rate)
//
// 【公式定义】
//   hla_imba = (Ṽ^B - Ṽ^A) / (Ṽ^B + Ṽ^A)
//   Ṽ^s = V^s · (1 + ρ^s)
//   ρ^s = (|O^{M,s}| - |O^{C,s}|) / (|O^{M,s}| + |O^{C,s}|)  (refill rate)
//
// 【触发域】
//   compute: onMaker / onCancel
//   flush:   onDepth
//
// 【输入输出】
//   输入: TickData.lob.{order_type, order_dir, volume} (onMaker/onCancel), bid_qty[0] (onDepth), ask_qty[0] (onDepth)
//   输出: hla_imba (onDepth)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class HLA {
public:
  enum Out : size_t { value,
                      kCount };

  HLA(TickData &td,
      const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
      const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
      CBuffer<float, L2::BLEN> (&out)[kCount])
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty), hla_imba_(out[value]) {}

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
    // 符号约定: ask_qty_ 在 DepthData 中存储为负值（卖方用负数表示）
    // 因此这里取反得到正数，使 v_bid 和 v_ask 都为正
    float v_bid = bid_qty_[0].back();  // 买一数量 (正数)
    float v_ask = -ask_qty_[0].back(); // 卖一数量 (负→正)

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

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Hla(N) N(Hla, (HLA), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Hla(X) \
  X(hla_imba, 1, DATA, TS, ORDER_FLOW, RATIO, NONE, "00/100/00", "Hidden-liquidity-adjusted Imba", "潜在流动性失衡", "预测后续时刻的失衡(按照refill/cancel rate)(降频)", R"(\frac{\tilde{V}_{1,t}^{M,B} - \tilde{V}_{1,t}^{M,A}}{\tilde{V}_{1,t}^{M,B} + \tilde{V}_{1,t}^{M,A}}, \quad \tilde{V}_{1,t}^{M,s} = V_{1,t}^{M,s}(1+\rho_t^{s}), \quad \rho_t^{s} = \frac{|O_t^{M,s}| - |O_t^{C,s}|}{|O_t^{M,s}| + |O_t^{C,s}|}, \quad s \in \{B,A\})", OP(Hla))
