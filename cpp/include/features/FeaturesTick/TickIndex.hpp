#pragma once

// =============================================================================
// TickIndex - Tick索引
// =============================================================================
// 从 TickData.l0_index 读取当前秒级索引
// 输出: float (直接转换)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class TickIndex {
public:
  TickIndex(const TickData &td, CBuffer<float, L2::BLEN> &buffer)
      : td_(td), buffer_(buffer) {}

  void compute() { buffer_.push_back(static_cast<float>(td_.l0_index)); }

  float back() const { return buffer_.back(); }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &buffer_;
};
