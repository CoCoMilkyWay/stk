#pragma once

// =============================================================================
// ToxicCr - 毒订单流撤单率
// =============================================================================
// 短窗口撤单占长窗口撤单比例, 检测高频撤单行为
//
// 【公式定义】
//   toxic_cr = Σ|O^C|_{t-5s}^{t} / Σ|O^C|_{t-60s}^{t}
//
// 【触发域】
//   compute: onCancel (内部按秒推进环形缓冲区)
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, volume, l0_index} (onCancel)
//   输出: toxic_cr (onMinute)
//
// 【备注】
//   - 使用60秒环形缓冲区，按秒推进
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class ToxicCr {
  static constexpr size_t SHORT_WINDOW = 5; // 5秒
  static constexpr size_t LONG_WINDOW = 60; // 60秒

public:
  enum Out : size_t { value,
                      kCount };

  ToxicCr(TickData &td, CBuffer<float, L2::BLEN> (&out)[kCount])
      : td_(td), out_(out[value]) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进环形缓冲区
    while (last_sec_ < cur_sec) {
      // 将当前槽位累计值加入滚动累加器
      short_sum_ += cancel_buffer_[write_idx_];
      long_sum_ += cancel_buffer_[write_idx_];

      // 推进写指针
      write_idx_ = (write_idx_ + 1) % LONG_WINDOW;

      // 移除离开长窗口的旧值
      long_sum_ -= cancel_buffer_[write_idx_];

      // 移除离开短窗口的旧值
      size_t short_out_idx = (write_idx_ + LONG_WINDOW - SHORT_WINDOW) % LONG_WINDOW;
      short_sum_ -= cancel_buffer_[short_out_idx];

      // 清空新槽位
      cancel_buffer_[write_idx_] = 0.0f;

      ++last_sec_;
    }

    // 累计撤单量到当前秒的槽位
    if (td_.lob.order_type == L2::OrderType::CANCEL) {
      cancel_buffer_[write_idx_] += static_cast<float>(td_.lob.volume);
    }
  }

  inline void flush() {
    // 输出当前比率 (包含当前秒尚未入账的累计量)
    float cur_short = short_sum_ + cancel_buffer_[write_idx_];
    float cur_long = long_sum_ + cancel_buffer_[write_idx_];
    float ratio = (cur_long > 1e-6f) ? (cur_short / cur_long) : 0.0f;
    out_.push_back(ratio);
  }

  inline void reset() {
    for (size_t i = 0; i < LONG_WINDOW; ++i) {
      cancel_buffer_[i] = 0.0f;
    }
    write_idx_ = 0;
    last_sec_ = 0;
    short_sum_ = 0.0f;
    long_sum_ = 0.0f;
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
  uint32_t last_sec_ = 0;
  float short_sum_ = 0.0f;
  float long_sum_ = 0.0f;
};
