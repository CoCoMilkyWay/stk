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

  inline void compute() {
    // 从BidPrice_[0]和AskPrice_[0] CBuffer读取买一卖一价格（已转换为元）
    float bid = bid_price_0_.back(); // 买一价
    float ask = ask_price_0_.back(); // 卖一价
    // 计算买卖价差：卖一 - 买一，需检查价格有效性
    spread_value_ = (bid > 0 && ask > 0) ? (ask - bid) : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的价差写入CBuffer
    buffer_.push_back(spread_value_);
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float spread_value_ = 0.0f;
};

