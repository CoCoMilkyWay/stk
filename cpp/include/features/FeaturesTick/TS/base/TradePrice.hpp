#pragma once

// =============================================================================
// TradePrice - 成交价数据层
// =============================================================================
// 维护成交价时序，TAKER时更新，否则保持前值
// 供下游因子 (MPB等) 复用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class TradePrice {
public:
  TradePrice(const TickData &tick_data, CBuffer<float, L2::BLEN> &buffer)
      : tick_data_(tick_data), buffer_(buffer) {}

  void compute() {
    if (tick_data_.lob.order_type == L2::OrderType::TAKER) {
      last_trade_price_ = tick_data_.lob.price;
    }
  }

  void flush() {
    buffer_.push_back(last_trade_price_);
  }

  float back() const { return buffer_.back(); }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &buffer_;
  float last_trade_price_ = 0.0f;
};

