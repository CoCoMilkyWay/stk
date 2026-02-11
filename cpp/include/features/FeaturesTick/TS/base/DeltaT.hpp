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

  inline void compute() {
    // 从tick_data读取当前时间，转换为毫秒（millisecond单位为10毫秒）
    uint32_t current_time_ms = tick_data_.lob.hour * 3600000 +
                               tick_data_.lob.minute * 60000 +
                               tick_data_.lob.second * 1000 +
                               tick_data_.lob.millisecond * 10;

    // 根据订单类型，分别计算与上一次同类型订单的时间差
    switch (tick_data_.lob.order_type) {
    case L2::OrderType::MAKER:
      if (last_maker_time_ms_ > 0) [[likely]] {
        delta_maker_ms_ = static_cast<float>(current_time_ms - last_maker_time_ms_);
        has_maker_delta_ = true; // 标记有新的delta需要flush
      }
      last_maker_time_ms_ = current_time_ms; // 更新last时间
      break;
    case L2::OrderType::TAKER:
      if (last_taker_time_ms_ > 0) [[likely]] {
        delta_taker_ms_ = static_cast<float>(current_time_ms - last_taker_time_ms_);
        has_taker_delta_ = true;
      }
      last_taker_time_ms_ = current_time_ms;
      break;
    case L2::OrderType::CANCEL:
      if (last_cancel_time_ms_ > 0) [[likely]] {
        delta_cancel_ms_ = static_cast<float>(current_time_ms - last_cancel_time_ms_);
        has_cancel_delta_ = true;
      }
      last_cancel_time_ms_ = current_time_ms;
      break;
    }
  }

  inline void flush() {
    // 将compute中计算的时间差写入对应的CBuffer
    // 只有当has_*_delta_标志为true时才写入（即有新数据）
    if (has_maker_delta_) {
      maker_buffer_.push_back(delta_maker_ms_); // 写入maker时间间隔
      has_maker_delta_ = false; // 重置标志
    }
    if (has_taker_delta_) {
      taker_buffer_.push_back(delta_taker_ms_); // 写入taker时间间隔
      has_taker_delta_ = false;
    }
    if (has_cancel_delta_) {
      cancel_buffer_.push_back(delta_cancel_ms_); // 写入cancel时间间隔
      has_cancel_delta_ = false;
    }
  }

  // 跨天时调用，重置所有last时间
  inline void reset() {
    last_maker_time_ms_ = 0;
    last_taker_time_ms_ = 0;
    last_cancel_time_ms_ = 0;
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
