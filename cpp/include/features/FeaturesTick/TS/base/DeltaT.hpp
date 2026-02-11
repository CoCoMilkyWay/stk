#pragma once

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class DeltaT {
public:
  DeltaT(const TickData &tick_data,
         CBuffer<float, L2::BLEN> &maker_buffer,
         CBuffer<float, L2::BLEN> &taker_buffer,
         CBuffer<float, L2::BLEN> &cancel_buffer)
      : tick_data_(tick_data),
        maker_buffer_(maker_buffer),
        taker_buffer_(taker_buffer),
        cancel_buffer_(cancel_buffer) {}

  void compute() {
    uint32_t current_time_ms = tick_data_.lob.hour * 3600000 +
                               tick_data_.lob.minute * 60000 +
                               tick_data_.lob.second * 1000 +
                               tick_data_.lob.millisecond * 10;

    switch (tick_data_.lob.order_type) {
    case L2::OrderType::MAKER: {
      if (last_maker_time_ms_ > 0) {
        delta_maker_ms_ = static_cast<float>(current_time_ms - last_maker_time_ms_);
        has_maker_delta_ = true;
      }
      last_maker_time_ms_ = current_time_ms;
      break;
    }
    case L2::OrderType::TAKER: {
      if (last_taker_time_ms_ > 0) {
        delta_taker_ms_ = static_cast<float>(current_time_ms - last_taker_time_ms_);
        has_taker_delta_ = true;
      }
      last_taker_time_ms_ = current_time_ms;
      break;
    }
    case L2::OrderType::CANCEL: {
      if (last_cancel_time_ms_ > 0) {
        delta_cancel_ms_ = static_cast<float>(current_time_ms - last_cancel_time_ms_);
        has_cancel_delta_ = true;
      }
      last_cancel_time_ms_ = current_time_ms;
      break;
    }
    }
  }

  void flush() {
    if (has_maker_delta_) {
      maker_buffer_.push_back(delta_maker_ms_);
      has_maker_delta_ = false;
    }
    if (has_taker_delta_) {
      taker_buffer_.push_back(delta_taker_ms_);
      has_taker_delta_ = false;
    }
    if (has_cancel_delta_) {
      cancel_buffer_.push_back(delta_cancel_ms_);
      has_cancel_delta_ = false;
    }
  }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &maker_buffer_;
  CBuffer<float, L2::BLEN> &taker_buffer_;
  CBuffer<float, L2::BLEN> &cancel_buffer_;

  uint32_t last_maker_time_ms_ = 0;
  uint32_t last_taker_time_ms_ = 0;
  uint32_t last_cancel_time_ms_ = 0;

  float delta_maker_ms_ = 0.0f;
  float delta_taker_ms_ = 0.0f;
  float delta_cancel_ms_ = 0.0f;
  bool has_maker_delta_ = false;
  bool has_taker_delta_ = false;
  bool has_cancel_delta_ = false;
};
