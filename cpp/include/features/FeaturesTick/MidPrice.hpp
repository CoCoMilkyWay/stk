#pragma once

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class MidPrice {
public:
  MidPrice(const TickData &tick_data, CBuffer<float, L2::BLEN> &buffer)
      : tick_data_(tick_data), buffer_(buffer) {}

  void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;
    Level *best_bid = depth[L2::LOB_DEPTH];     // buy1
    Level *best_ask = depth[L2::LOB_DEPTH - 1]; // sell1
    float bid_price = static_cast<float>(best_bid->price);
    float ask_price = static_cast<float>(best_ask->price);
    float mid_price = (bid_price + ask_price) * 0.5f;
    buffer_.push_back(mid_price);
  }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &buffer_;
};