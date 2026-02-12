#pragma once

// =============================================================================
// OrderFlow - 订单记录层
// =============================================================================
// 记录订单信息，支持 TAKER/MAKER/CANCEL 三种类型
// 输出: price (元), timestamp (毫秒), tickindex, volume (股)
// =============================================================================

#include "features/DataDefine.hpp"

class OrderFlow {
public:
  OrderFlow(const TickData &td,
            CBuffer<float, L2::BLEN> &price,
            CBuffer<float, L2::BLEN> &timestamp,
            CBuffer<float, L2::BLEN> &tickindex,
            CBuffer<float, L2::BLEN> &volume)
      : td_(td), price_(price), timestamp_(timestamp), tickindex_(tickindex), volume_(volume) {}

  inline void compute() {
    // 读取价格
    price_val_ = td_.lob.price;

    // 读取时间戳，转换为毫秒
    timestamp_val_ = static_cast<float>(
        td_.lob.hour * 3600000 +
        td_.lob.minute * 60000 +
        td_.lob.second * 1000 +
        td_.lob.millisecond * 10);

    // 读取tick索引
    tickindex_val_ = static_cast<float>(td_.l0_index);

    // 读取订单量
    volume_val_ = static_cast<float>(td_.lob.volume);
  }

  inline void flush() {
    price_.push_back(price_val_);
    timestamp_.push_back(timestamp_val_);
    tickindex_.push_back(tickindex_val_);
    volume_.push_back(volume_val_);
  }

  inline void reset() {
    price_val_ = 0.0f;
    timestamp_val_ = 0.0f;
    tickindex_val_ = 0.0f;
    volume_val_ = 0.0f;
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &price_;
  CBuffer<float, L2::BLEN> &timestamp_;
  CBuffer<float, L2::BLEN> &tickindex_;
  CBuffer<float, L2::BLEN> &volume_;

  float price_val_ = 0.0f;
  float timestamp_val_ = 0.0f;
  float tickindex_val_ = 0.0f;
  float volume_val_ = 0.0f;
};
