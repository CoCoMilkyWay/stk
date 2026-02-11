#pragma once

// =============================================================================
// Spread - 买卖价差
// =============================================================================
// Spread = AskPrice1 - BidPrice1
// 从已提取的 BidPrice_[0], AskPrice_[0] CBuffer 读取 (已是元单位)
// 输出: 元 (RMB)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class Spread {
public:
  Spread(const CBuffer<float, L2::BLEN> &bid_price_0,
         const CBuffer<float, L2::BLEN> &ask_price_0,
         CBuffer<float, L2::BLEN> &buffer)
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(buffer) {}

  void compute() {
    float bid = bid_price_0_.back();
    float ask = ask_price_0_.back();
    spread_value_ = (bid > 0 && ask > 0) ? (ask - bid) : 0.0f;
  }

  void flush() { buffer_.push_back(spread_value_); }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float spread_value_ = 0.0f;
};

