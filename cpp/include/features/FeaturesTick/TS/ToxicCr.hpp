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
    // 每笔订单时，如果是撤单则累计到当前秒的缓冲区
    if (td_.lob.order_type == L2::OrderType::CANCEL) {
      cancel_buffer_[write_idx_] += 1.0f;
    }
  }

  inline void flush() {
    float short_sum = 0.0f;  // 短窗口（5秒）撤单数
    float long_sum = 0.0f;   // 长窗口（60秒）撤单数

    // 遍历滚动窗口缓冲区，计算短长窗口撤单数
    for (size_t i = 0; i < LONG_WINDOW; ++i) {
      size_t idx = (write_idx_ + LONG_WINDOW - i) % LONG_WINDOW;
      float v = cancel_buffer_[idx];
      long_sum += v;         // 累加到长窗口
      if (i < SHORT_WINDOW) {
        short_sum += v;      // 前5秒累加到短窗口
      }
    }

    // 计算毒订单流撤单率：短窗口撤单 / 长窗口撤单
    // 值越大表示最近5秒撤单占比越高，可能存在高频撤单行为
    float ratio = (long_sum > 1e-6f) ? (short_sum / long_sum) : 0.0f;
    out_.push_back(ratio);

    // 移动写指针到下一个时间窗口
    write_idx_ = (write_idx_ + 1) % LONG_WINDOW;
    cancel_buffer_[write_idx_] = 0.0f;  // 清空新窗口位置
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
};
