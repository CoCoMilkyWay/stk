#pragma once

// =============================================================================
// DepthIndex - Depth索引算子
// =============================================================================
// 记录 depth 更新时对应的原始 tick 索引, 供其他 depth 级别的算子使用
// 输出: _depth_index (原始tick索引 [0-15299])
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class DepthIndex {
public:
  DepthIndex(const TickData &td, CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), index_buffer_(index_buffer) {}

  inline void compute() { index_value_ = static_cast<float>(td_.l0_index); }
  inline void flush() { index_buffer_.push_back(index_value_); }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &index_buffer_;
  float index_value_ = 0.0f;
};
