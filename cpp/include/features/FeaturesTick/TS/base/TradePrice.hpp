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

  inline void compute() {
    // 只在TAKER（成交）时更新成交价
    // 非TAKER时保持上一次的成交价不变
    if (tick_data_.lob.order_type == L2::OrderType::TAKER) {
      last_trade_price_ = tick_data_.lob.price; // 从tick_data读取最新成交价
    }
  }

  inline void flush() {
    // 将当前维护的成交价写入CBuffer
    // TAKER时是新成交价，非TAKER时是前值
    buffer_.push_back(last_trade_price_);
  }

  float back() const { return buffer_.back(); }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &buffer_;
  float last_trade_price_ = 0.0f;
};

