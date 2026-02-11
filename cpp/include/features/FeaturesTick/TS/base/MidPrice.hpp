#pragma once

// =============================================================================
// MidPrice - 中间价
// =============================================================================
// 从 BidPrice_[0], AskPrice_[0] CBuffer 读取 (已是元单位)
// 输出: 元 (RMB)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MidPrice {
public:
  MidPrice(const CBuffer<float, L2::BLEN> &bid_price_0,
           const CBuffer<float, L2::BLEN> &ask_price_0,
           CBuffer<float, L2::BLEN> &buffer)
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(buffer) {}

  void compute() {
    float bid = bid_price_0_.back();
    float ask = ask_price_0_.back();
    mid_value_ = (bid + ask) * 0.5f;
  }

  void flush() { buffer_.push_back(mid_value_); }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float mid_value_ = 0.0f;
};