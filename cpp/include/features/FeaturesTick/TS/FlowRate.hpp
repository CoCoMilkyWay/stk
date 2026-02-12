#pragma once

// =============================================================================
// FLOW_RATE - 订单流率特征
// =============================================================================
// 计算单位时间内的订单流统计
//   arr_bid/ask = #O^{M,B/A} / Δt   (买/卖单到达率)
//   can_bid/ask = #O^{C,B/A} / Δt   (买/卖单撤单率)
//   trd_buy/sell = #O^{T,B/A} / Δt  (主动买/卖成交率)
//   net_ord = (#O^{M,B} - #O^{C,B}) - (#O^{M,A} - #O^{C,A})  (净订单流)
//   foi = (ΔO^B - ΔO^A) / (ΔO^B + ΔO^A)  (订单流失衡)
//
// 输入频率: PER_ORDER (每笔订单)
// 输出频率: per sec (由外部按秒读取)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

// compute: 每笔订单时按类型和方向累计计数
// flush: 按秒输出订单流率
class FlowRate {
public:
  FlowRate(TickData &td,
           CBuffer<float, L2::BLEN> &arr_bid,
           CBuffer<float, L2::BLEN> &arr_ask,
           CBuffer<float, L2::BLEN> &can_bid,
           CBuffer<float, L2::BLEN> &can_ask,
           CBuffer<float, L2::BLEN> &trd_buy,
           CBuffer<float, L2::BLEN> &trd_sell,
           CBuffer<float, L2::BLEN> &net_ord,
           CBuffer<float, L2::BLEN> &foi)
      : td_(td),
        arr_bid_(arr_bid), arr_ask_(arr_ask),
        can_bid_(can_bid), can_ask_(can_ask),
        trd_buy_(trd_buy), trd_sell_(trd_sell),
        net_ord_(net_ord), foi_(foi) {}

  // 每笔订单时，按类型和方向累计计数
  inline void compute() {
    const bool is_bid = (td_.lob.order_dir == L2::OrderDirection::BID);

    switch (td_.lob.order_type) {
    case L2::OrderType::MAKER:
      if (is_bid) ++cnt_arr_bid_; else ++cnt_arr_ask_;
      break;
    case L2::OrderType::TAKER:
      if (is_bid) ++cnt_trd_buy_; else ++cnt_trd_sell_;
      break;
    case L2::OrderType::CANCEL:
      if (is_bid) ++cnt_can_bid_; else ++cnt_can_ask_;
      break;
    }
  }

  inline void flush() {
    // 输出订单流率（订单笔数/秒）
    arr_bid_.push_back(static_cast<float>(cnt_arr_bid_));
    arr_ask_.push_back(static_cast<float>(cnt_arr_ask_));
    can_bid_.push_back(static_cast<float>(cnt_can_bid_));
    can_ask_.push_back(static_cast<float>(cnt_can_ask_));
    trd_buy_.push_back(static_cast<float>(cnt_trd_buy_));
    trd_sell_.push_back(static_cast<float>(cnt_trd_sell_));

    // 计算净订单流：(买挂单-买撤单) - (卖挂单-卖撤单)
    float net_bid = static_cast<float>(cnt_arr_bid_ - cnt_can_bid_);
    float net_ask = static_cast<float>(cnt_arr_ask_ - cnt_can_ask_);
    float net_ord_val = net_bid - net_ask;
    net_ord_.push_back(net_ord_val);

    // 计算订单流失衡FOI：(ΔO^B - ΔO^A) / (|ΔO^B| + |ΔO^A|)
    float sum = std::abs(net_bid) + std::abs(net_ask);
    foi_.push_back(sum > 1e-6f ? net_ord_val / sum : 0.0f);

    // 重置秒内计数器
    cnt_arr_bid_ = cnt_arr_ask_ = 0;
    cnt_can_bid_ = cnt_can_ask_ = 0;
    cnt_trd_buy_ = cnt_trd_sell_ = 0;
  }

  inline void reset() {
    cnt_arr_bid_ = cnt_arr_ask_ = 0;
    cnt_can_bid_ = cnt_can_ask_ = 0;
    cnt_trd_buy_ = cnt_trd_sell_ = 0;
  }

private:
  TickData &td_;

  // 输出
  CBuffer<float, L2::BLEN> &arr_bid_;
  CBuffer<float, L2::BLEN> &arr_ask_;
  CBuffer<float, L2::BLEN> &can_bid_;
  CBuffer<float, L2::BLEN> &can_ask_;
  CBuffer<float, L2::BLEN> &trd_buy_;
  CBuffer<float, L2::BLEN> &trd_sell_;
  CBuffer<float, L2::BLEN> &net_ord_;
  CBuffer<float, L2::BLEN> &foi_;

  // 秒内累计计数
  int cnt_arr_bid_ = 0, cnt_arr_ask_ = 0;
  int cnt_can_bid_ = 0, cnt_can_ask_ = 0;
  int cnt_trd_buy_ = 0, cnt_trd_sell_ = 0;
};
