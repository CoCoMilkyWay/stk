#pragma once

// =============================================================================
// ToxicCr - 毒订单流撤单率
// =============================================================================
// toxic_cr = Σ|O^C|_{t-5}^{t} / Σ|O^C|_{t-60}^{t}
// 短窗口撤单占长窗口撤单比例, 检测高频撤单行为
//
// 使用滚动窗口实现
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

// compute: 每笔订单时累计 (内部判断CANCEL), flush: 按秒输出
class ToxicCr {
  static constexpr size_t SHORT_WINDOW = 5;   // 5秒
  static constexpr size_t LONG_WINDOW = 60;   // 60秒

public:
  ToxicCr(TickData &td, CBuffer<float, L2::BLEN> &out)
      : td_(td), out_(out) {}

  inline void compute() {
    // 每笔订单时，如果是撤单则累计撤单量到当前秒的缓冲区
    if (td_.lob.order_type == L2::OrderType::CANCEL) {
      cancel_buffer_[write_idx_] += static_cast<float>(td_.lob.volume);
    }
  }

  inline void flush() {
    // 将当前秒累计值加入滚动累加器
    short_sum_ += cancel_buffer_[write_idx_];
    long_sum_ += cancel_buffer_[write_idx_];
    
    // 计算毒订单流撤单率：短窗口撤单量 / 长窗口撤单量
    // 值越大表示最近5秒撤单占比越高，可能存在高频撤单行为
    float ratio = (long_sum_ > 1e-6f) ? (short_sum_ / long_sum_) : 0.0f;
    out_.push_back(ratio);

    // 移动写指针到下一个时间窗口
    write_idx_ = (write_idx_ + 1) % LONG_WINDOW;
    
    // 更新滚动累加器：移除即将被覆盖的旧值（61秒前）
    long_sum_ -= cancel_buffer_[write_idx_];
    
    // 移除短窗口外的值（6秒前）
    size_t short_out_idx = (write_idx_ + LONG_WINDOW - SHORT_WINDOW) % LONG_WINDOW;
    short_sum_ -= cancel_buffer_[short_out_idx];
    
    // 清空新窗口位置
    cancel_buffer_[write_idx_] = 0.0f;
  }

  inline void reset() {
    for (size_t i = 0; i < LONG_WINDOW; ++i) {
      cancel_buffer_[i] = 0.0f;
    }
    write_idx_ = 0;
    short_sum_ = 0.0f;
    long_sum_ = 0.0f;
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
  float short_sum_ = 0.0f;  // 短窗口滚动累加器
  float long_sum_ = 0.0f;   // 长窗口滚动累加器
};
