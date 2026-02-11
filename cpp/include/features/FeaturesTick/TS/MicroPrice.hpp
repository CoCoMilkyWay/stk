#pragma once

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class MicroPrice {
public:
  MicroPrice(const TickData &tick_data, CBuffer<float, L2::BLEN> &buffer)
      : tick_data_(tick_data), buffer_(buffer) {}

  inline void compute() {
    // 从depth_buffer读取买一卖一档位数据
    const auto &depth = tick_data_.lob.depth_buffer;
    Level *best_bid = depth[L2::LOB_DEPTH];     // 买一档
    Level *best_ask = depth[L2::LOB_DEPTH - 1]; // 卖一档

    // 提取价格和数量
    float bid_price = static_cast<float>(best_bid->price);
    float ask_price = static_cast<float>(best_ask->price);
    float bid_qty = static_cast<float>(best_bid->net_quantity);
    float ask_qty = static_cast<float>(-best_ask->net_quantity); // ask是负值，取反

    // 计算微观价格：按数量加权的价格
    // micro_price = (ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty)
    micro_value_ = (ask_price * bid_qty + bid_price * ask_qty) / (ask_qty + bid_qty);
  }

  inline void flush() {
    // 将compute中计算的微观价格写入CBuffer
    buffer_.push_back(micro_value_);
  }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &buffer_;
  float micro_value_ = 0.0f;
};
