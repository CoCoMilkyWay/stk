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

  void compute() { index_buffer_.push_back(static_cast<float>(td_.l0_index)); }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &index_buffer_;
};
