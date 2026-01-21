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

// 订单流累计器 (每个tick更新, 每秒输出)
class FlowAccumulator {
public:
  FlowAccumulator(TickData &td,
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

  // 每笔订单调用
  void accumulate() {
    const auto &lob = td_.lob;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    switch (lob.order_type) {
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

  // 每秒输出 (ON_DEPTH 时调用)
  void flush() {
    // 计算 dt (秒), 至少1秒
    float dt = 1.0f;

    // 输出各项率
    arr_bid_.push_back(static_cast<float>(cnt_arr_bid_) / dt);
    arr_ask_.push_back(static_cast<float>(cnt_arr_ask_) / dt);
    can_bid_.push_back(static_cast<float>(cnt_can_bid_) / dt);
    can_ask_.push_back(static_cast<float>(cnt_can_ask_) / dt);
    trd_buy_.push_back(static_cast<float>(cnt_trd_buy_) / dt);
    trd_sell_.push_back(static_cast<float>(cnt_trd_sell_) / dt);

    // net_ord = (arr_bid - can_bid) - (arr_ask - can_ask)
    int net_bid = cnt_arr_bid_ - cnt_can_bid_;
    int net_ask = cnt_arr_ask_ - cnt_can_ask_;
    net_ord_.push_back(static_cast<float>(net_bid - net_ask));

    // foi = (delta_bid - delta_ask) / (delta_bid + delta_ask)
    // delta = trd - can (成交 - 撤单)
    int delta_bid = cnt_trd_buy_ - cnt_can_bid_;
    int delta_ask = cnt_trd_sell_ - cnt_can_ask_;
    int sum = std::abs(delta_bid) + std::abs(delta_ask);
    foi_.push_back(sum > 0 ? static_cast<float>(delta_bid - delta_ask) / static_cast<float>(sum) : 0.0f);

    // 重置计数器
    cnt_arr_bid_ = cnt_arr_ask_ = 0;
    cnt_can_bid_ = cnt_can_ask_ = 0;
    cnt_trd_buy_ = cnt_trd_sell_ = 0;
  }

private:
  TickData &td_;

  // 输出 CBuffer
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

// =============================================================================
// TOXIC_CR - 毒订单流撤单率
// =============================================================================
// toxic_cr = Σ|O^C|_{t-5}^{t} / Σ|O^C|_{t-60}^{t}
// 短窗口撤单占长窗口撤单比例, 检测高频撤单行为
//
// 使用滚动窗口实现
// =============================================================================

class ToxicCancelRatio {
  static constexpr size_t SHORT_WINDOW = 5;   // 5秒
  static constexpr size_t LONG_WINDOW = 60;   // 60秒

public:
  ToxicCancelRatio(CBuffer<float, L2::BLEN> &out)
      : out_(out) {}

  // 每笔撤单调用
  void accumulate(float cancel_volume) {
    cancel_buffer_[write_idx_] += cancel_volume;
  }

  // 每秒输出 (ON_DEPTH 时调用)
  void flush() {
    // 计算短窗口和长窗口的撤单量
    float short_sum = 0.0f;
    float long_sum = 0.0f;

    for (size_t i = 0; i < LONG_WINDOW; ++i) {
      size_t idx = (write_idx_ + LONG_WINDOW - i) % LONG_WINDOW;
      float v = cancel_buffer_[idx];
      long_sum += v;
      if (i < SHORT_WINDOW) {
        short_sum += v;
      }
    }

    float ratio = (long_sum > 1e-6f) ? (short_sum / long_sum) : 0.0f;
    out_.push_back(ratio);

    // 移动到下一秒
    write_idx_ = (write_idx_ + 1) % LONG_WINDOW;
    cancel_buffer_[write_idx_] = 0.0f; // 清空新位置
  }

private:
  CBuffer<float, L2::BLEN> &out_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
};
