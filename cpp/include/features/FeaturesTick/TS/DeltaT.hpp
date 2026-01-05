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
    // Get current timestamp in milliseconds
    uint32_t current_time_ms = tick_data_.lob.hour * 3600000 +
                                tick_data_.lob.minute * 60000 +
                                tick_data_.lob.second * 1000 +
                                tick_data_.lob.millisecond * 10;

    // Compute delta time based on order type
    switch (tick_data_.lob.order_type) {
      case L2::OrderType::MAKER: {
        if (last_maker_time_ms_ > 0) {
          float delta_ms = static_cast<float>(current_time_ms - last_maker_time_ms_);
          maker_buffer_.push_back(delta_ms);
        }
        last_maker_time_ms_ = current_time_ms;
        break;
      }
      case L2::OrderType::TAKER: {
        if (last_taker_time_ms_ > 0) {
          float delta_ms = static_cast<float>(current_time_ms - last_taker_time_ms_);
          taker_buffer_.push_back(delta_ms);
        }
        last_taker_time_ms_ = current_time_ms;
        break;
      }
      case L2::OrderType::CANCEL: {
        if (last_cancel_time_ms_ > 0) {
          float delta_ms = static_cast<float>(current_time_ms - last_cancel_time_ms_);
          cancel_buffer_.push_back(delta_ms);
        }
        last_cancel_time_ms_ = current_time_ms;
        break;
      }
    }
  }

private:
  const TickData &tick_data_;
  CBuffer<float, L2::BLEN> &maker_buffer_;
  CBuffer<float, L2::BLEN> &taker_buffer_;
  CBuffer<float, L2::BLEN> &cancel_buffer_;

  // Last timestamp for each order type (in milliseconds)
  uint32_t last_maker_time_ms_ = 0;
  uint32_t last_taker_time_ms_ = 0;
  uint32_t last_cancel_time_ms_ = 0;
};
