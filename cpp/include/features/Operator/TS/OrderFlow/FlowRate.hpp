#pragma once

// =============================================================================
// FLOW_RATE - 订单流率: 每分钟各类订单金额 (万元/分钟)
// =============================================================================
//   mk/cn/tk_{bid,ask} = Σamt^{M/C/T, B/A} / 1min      (挂单/撤单/吃单率)
//   net_ord = (mk_bid - cn_bid) - (mk_ask - cn_ask)    (净订单流)
//   foi     = (ΔA^B - ΔA^A) / (|ΔA^B| + |ΔA^A|)        (订单流失衡)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

class FlowRate {
public:
  enum Out : size_t { mk_bid,
                      mk_ask,
                      cn_bid,
                      cn_ask,
                      tk_bid,
                      tk_ask,
                      net_ord,
                      foi,
                      kCount };
  float y[kCount] = {};

  explicit FlowRate(TickData &td) : td_(td) {}

  // 每笔订单: 按类型 × 方向累计金额 (万元); acc_ 下标与 Out 前 6 口对齐
  inline void compute() {
    const bool is_bid = (td_.lob.order_dir == L2::OrderDirection::BID);
    const float amt = static_cast<float>(td_.lob.volume) * td_.lob.price / 10000.0f;

    switch (td_.lob.order_type) {
    case L2::OrderType::MAKER:
      acc_[is_bid ? mk_bid : mk_ask] += amt;
      break;
    case L2::OrderType::TAKER:
      acc_[is_bid ? tk_bid : tk_ask] += amt;
      break;
    case L2::OrderType::CANCEL:
      acc_[is_bid ? cn_bid : cn_ask] += amt;
      break;
    }
  }

  // 分钟末结算 (Δt = 1 分钟, 金额即速率), 清零累计
  inline void flush() {
    for (size_t i = 0; i < kAcc; ++i)
      y[i] = acc_[i];
    float net_bid = acc_[mk_bid] - acc_[cn_bid];
    float net_ask = acc_[mk_ask] - acc_[cn_ask];
    y[net_ord] = net_bid - net_ask;
    float sum = std::abs(net_bid) + std::abs(net_ask);
    y[foi] = sum > 1e-6f ? y[net_ord] / sum : 0.0f;
    reset();
  }

  inline void reset() {
    for (size_t i = 0; i < kAcc; ++i)
      acc_[i] = 0.0f;
  }

private:
  static constexpr size_t kAcc = tk_ask + 1; // mk/cn/tk × bid/ask
  TickData &td_;
  float acc_[kAcc] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_FlowRate(N) N(FlowRate, (FlowRate), (tick_data), onTick, onMinute)

#define FIELDS_L1_FlowRate(X)                                                                                                                                                                                                                                                           \
  X(mk_bid, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Maker Rate", "买方挂单额", "每分钟买方挂单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,B}| \cdot P_\tau)", OP(FlowRate, mk_bid))                                                                              \
  X(mk_ask, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Maker Rate", "卖方挂单额", "每分钟卖方挂单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,A}| \cdot P_\tau)", OP(FlowRate, mk_ask))                                                                              \
  X(cn_bid, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Cancel Rate", "买方撤单额", "每分钟买方撤单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,B}| \cdot P_\tau)", OP(FlowRate, cn_bid))                                                                             \
  X(cn_ask, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Cancel Rate", "卖方撤单额", "每分钟卖方撤单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,A}| \cdot P_\tau)", OP(FlowRate, cn_ask))                                                                             \
  X(tk_bid, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Taker Rate", "买方吃单额", "每分钟买方吃单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,B}| \cdot P_\tau)", OP(FlowRate, tk_bid))                                                                              \
  X(tk_ask, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Taker Rate", "卖方吃单额", "每分钟卖方吃单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,A}| \cdot P_\tau)", OP(FlowRate, tk_ask))                                                                              \
  X(net_ord, 1, DATA, ORDER_FLOW, RAW, NONE, "00/100/00", "Net Order Flow", "净订单流", "买卖订单流金额净差(万元/分钟)", R"((\mathrm{Amt}_{\Delta t}^{M,B} - \mathrm{Amt}_{\Delta t}^{C,B}) - (\mathrm{Amt}_{\Delta t}^{M,A} - \mathrm{Amt}_{\Delta t}^{C,A}))", OP(FlowRate, net_ord)) \
  X(foi, 1, DATA, ORDER_FLOW, RATIO, NONE, "00/100/00", "Flow Imbalance", "订单流失衡", "买卖订单流失衡(降频)", R"(\frac{\Delta A^{B} - \Delta A^{A}}{|\Delta A^{B}| + |\Delta A^{A}|}, \quad \Delta A^{s} = \mathrm{Amt}_{\Delta t}^{M,s} - \mathrm{Amt}_{\Delta t}^{C,s}, \quad s \in \{B,A\})", OP(FlowRate, foi))
