#pragma once

// =============================================================================
// MicroPrice - 微观价格 (量加权中间价)
// =============================================================================
// 从 BidPrice_[0], AskPrice_[0], BidQty_[0], AskQty_[0] CBuffer 读取 (已是标准单位)
// 输出: 元 (RMB)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MicroPrice {
public:
  MicroPrice(const CBuffer<float, L2::BLEN> &bid_price_0,
             const CBuffer<float, L2::BLEN> &ask_price_0,
             const CBuffer<float, L2::BLEN> &bid_qty_0,
             const CBuffer<float, L2::BLEN> &ask_qty_0,
             CBuffer<float, L2::BLEN> &buffer)
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        bid_qty_0_(bid_qty_0),
        ask_qty_0_(ask_qty_0),
        buffer_(buffer) {}

  inline void compute() {
    // 从 CBuffer 读取买一卖一价格和数量 (已转换为标准单位)
    float bid_price = bid_price_0_.back(); // 买一价 (元)
    float ask_price = ask_price_0_.back(); // 卖一价 (元)
    float bid_qty = bid_qty_0_.back();     // 买一量 (股)
    float ask_qty = -ask_qty_0_.back();    // 卖一量 (股, ask_qty原本为负值)

    // 计算微观价格：量加权中间价
    // micro_price = (ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty)
    micro_value_ = (ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty);
  }

  inline void flush() {
    // 将compute中计算的微观价格写入CBuffer
    buffer_.push_back(micro_value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  const CBuffer<float, L2::BLEN> &bid_qty_0_;
  const CBuffer<float, L2::BLEN> &ask_qty_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float micro_value_ = 0.0f;
};
