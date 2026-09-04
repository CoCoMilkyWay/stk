#pragma once

// =============================================================================
// Spread - 买卖价差
// =============================================================================
// 计算买一卖一价差
//
// 【公式定义】
//   spread = P_{1,t}^{M,A} - P_{1,t}^{M,B}  (单位: 元)
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_price_0 (onDepth), ask_price_0 (onDepth)
//   输出: spread (onDepth)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class Spread {
public:
  enum Out : size_t { value,
                      kCount };

  Spread(const CBuffer<float, L2::BLEN> &bid_price_0,
         const CBuffer<float, L2::BLEN> &ask_price_0,
         CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(out[value]) {}

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

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float spread_value_ = 0.0f;
};
