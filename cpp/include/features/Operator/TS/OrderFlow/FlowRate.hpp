#pragma once

// =============================================================================
// FLOW_RATE - 订单流率特征 (分钟级)
// =============================================================================
// 计算每分钟的订单流金额率 (单位: 万元/分钟)
//
// 【公式定义】
//   mk_bid/ask  = Σamt^{M,B/A} / 1min  (买/卖方挂单率)
//   cn_bid/ask  = Σamt^{C,B/A} / 1min  (买/卖方撤单率)
//   tk_bid/ask  = Σamt^{T,B/A} / 1min  (买/卖方吃单率)
//   net_ord     = (mk_bid - cn_bid) - (mk_ask - cn_ask)  (净订单流)
//   foi         = (ΔV^B - ΔV^A) / (|ΔV^B| + |ΔV^A|)      (订单流失衡)
//
// 【触发域】
//   compute: onTaker / onMaker / onCancel
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, order_dir, volume, price} (onTaker/onMaker/onCancel)
//   输出: mk_bid, mk_ask, cn_bid, cn_ask, tk_bid, tk_ask, net_ord, foi (onMinute)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
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

  FlowRate(TickData &td, CBuffer<float, L2::BLEN> (&out)[kCount])
      : td_(td),
        mk_bid_(out[mk_bid]), mk_ask_(out[mk_ask]),
        cn_bid_(out[cn_bid]), cn_ask_(out[cn_ask]),
        tk_bid_(out[tk_bid]), tk_ask_(out[tk_ask]),
        net_ord_(out[net_ord]), foi_(out[foi]) {}

  // 每笔订单时，按类型和方向累计金额（万元）
  inline void compute() {
    const bool is_bid = (td_.lob.order_dir == L2::OrderDirection::BID);
    const float amt = static_cast<float>(td_.lob.volume) * td_.lob.price / 10000.0f; // 万元

    switch (td_.lob.order_type) {
    case L2::OrderType::MAKER: // 挂单
      if (is_bid)
        amt_mk_bid_ += amt;
      else
        amt_mk_ask_ += amt;
      break;
    case L2::OrderType::TAKER: // 吃单（主动成交）
      if (is_bid)
        amt_tk_bid_ += amt;
      else
        amt_tk_ask_ += amt;
      break;
    case L2::OrderType::CANCEL: // 撤单
      if (is_bid)
        amt_cn_bid_ += amt;
      else
        amt_cn_ask_ += amt;
      break;
    }
  }

  inline void flush() {
    // 输出每分钟订单流金额（万元/分钟，Δt=1分钟，无需除以时间）
    mk_bid_.push_back(amt_mk_bid_); // 买方挂单
    mk_ask_.push_back(amt_mk_ask_); // 卖方挂单
    cn_bid_.push_back(amt_cn_bid_); // 买方撤单
    cn_ask_.push_back(amt_cn_ask_); // 卖方撤单
    tk_bid_.push_back(amt_tk_bid_); // 买方吃单
    tk_ask_.push_back(amt_tk_ask_); // 卖方吃单

    // 净订单流：(买挂单-买撤单) - (卖挂单-卖撤单)
    float net_bid = amt_mk_bid_ - amt_cn_bid_;
    float net_ask = amt_mk_ask_ - amt_cn_ask_;
    float net_ord_val = net_bid - net_ask;
    net_ord_.push_back(net_ord_val);

    // 订单流失衡FOI：(ΔV^B - ΔV^A) / (|ΔV^B| + |ΔV^A|)
    float sum = std::abs(net_bid) + std::abs(net_ask);
    foi_.push_back(sum > 1e-6f ? net_ord_val / sum : 0.0f);

    // 重置分钟内累计
    amt_mk_bid_ = amt_mk_ask_ = 0.0f;
    amt_cn_bid_ = amt_cn_ask_ = 0.0f;
    amt_tk_bid_ = amt_tk_ask_ = 0.0f;
  }

  inline void reset() {
    amt_mk_bid_ = amt_mk_ask_ = 0.0f;
    amt_cn_bid_ = amt_cn_ask_ = 0.0f;
    amt_tk_bid_ = amt_tk_ask_ = 0.0f;
  }

private:
  TickData &td_;

  // 输出 CBuffer (对仗命名: mk=挂单, cn=撤单, tk=吃单)
  CBuffer<float, L2::BLEN> &mk_bid_; // 买方挂单
  CBuffer<float, L2::BLEN> &mk_ask_; // 卖方挂单
  CBuffer<float, L2::BLEN> &cn_bid_; // 买方撤单
  CBuffer<float, L2::BLEN> &cn_ask_; // 卖方撤单
  CBuffer<float, L2::BLEN> &tk_bid_; // 买方吃单
  CBuffer<float, L2::BLEN> &tk_ask_; // 卖方吃单
  CBuffer<float, L2::BLEN> &net_ord_;
  CBuffer<float, L2::BLEN> &foi_;

  // 分钟内累计金额（万元）
  float amt_mk_bid_ = 0.0f, amt_mk_ask_ = 0.0f;
  float amt_cn_bid_ = 0.0f, amt_cn_ask_ = 0.0f;
  float amt_tk_bid_ = 0.0f, amt_tk_ask_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_FlowRate(N) N(FlowRate, (FlowRate), (tick_data), onTick, onMinute)

#define FIELDS_L1_FlowRate(X)                                                                                                                                                                                                                                                               \
  X(mk_bid, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Maker Rate", "买方挂单额", "每分钟买方挂单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,B}| \cdot P_\tau)", OP(FlowRate, mk_bid))                                                                              \
  X(mk_ask, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Maker Rate", "卖方挂单额", "每分钟卖方挂单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,A}| \cdot P_\tau)", OP(FlowRate, mk_ask))                                                                              \
  X(cn_bid, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Cancel Rate", "买方撤单额", "每分钟买方撤单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,B}| \cdot P_\tau)", OP(FlowRate, cn_bid))                                                                             \
  X(cn_ask, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Cancel Rate", "卖方撤单额", "每分钟卖方撤单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,A}| \cdot P_\tau)", OP(FlowRate, cn_ask))                                                                             \
  X(tk_bid, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Bid Taker Rate", "买方吃单额", "每分钟买方吃单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,B}| \cdot P_\tau)", OP(FlowRate, tk_bid))                                                                              \
  X(tk_ask, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Ask Taker Rate", "卖方吃单额", "每分钟卖方吃单金额(万元/分钟)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,A}| \cdot P_\tau)", OP(FlowRate, tk_ask))                                                                              \
  X(net_ord, 1, DATA, TS, ORDER_FLOW, RAW, NONE, "00/100/00", "Net Order Flow", "净订单流", "买卖订单流金额净差(万元/分钟)", R"((\mathrm{Amt}_{\Delta t}^{M,B} - \mathrm{Amt}_{\Delta t}^{C,B}) - (\mathrm{Amt}_{\Delta t}^{M,A} - \mathrm{Amt}_{\Delta t}^{C,A}))", OP(FlowRate, net_ord)) \
  X(foi, 1, DATA, TS, ORDER_FLOW, RATIO, NONE, "00/100/00", "Flow Imbalance", "订单流失衡", "买卖订单流失衡(降频)", R"(\frac{\Delta A^{B} - \Delta A^{A}}{|\Delta A^{B}| + |\Delta A^{A}|}, \quad \Delta A^{s} = \mathrm{Amt}_{\Delta t}^{M,s} - \mathrm{Amt}_{\Delta t}^{C,s}, \quad s \in \{B,A\})", OP(FlowRate, foi))
