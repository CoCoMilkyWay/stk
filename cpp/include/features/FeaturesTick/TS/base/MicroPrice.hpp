#pragma once

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class MicroPrice {
public:
  MicroPrice(const TickData &tick_data, CBuffer<float, L2::BLEN> &buffer)
      : tick_data_(tick_data), buffer_(buffer) {}

  inline void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;
    Level *best_bid = depth[L2::LOB_DEPTH];     // buy1
    Level *best_ask = depth[L2::LOB_DEPTH - 1]; // sell1

    float bid_price = static_cast<float>(best_bid->price);
    float ask_price = static_cast<float>(best_ask->price);
    float bid_qty = static_cast<float>(best_bid->net_quantity);
    float ask_qty = static_cast<float>(-best_ask->net_quantity); // ask is negative

    micro_value_ = (ask_price * bid_qty + bid_price * ask_qty) / (ask_qty + bid_qty);
  }

  inline void flush() { buffer_.push_back(micro_value_); }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &buffer_;
  float micro_value_ = 0.0f;
};
