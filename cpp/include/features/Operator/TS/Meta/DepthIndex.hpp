#pragma once

// =============================================================================
// DepthIndex - Depth索引算子
// =============================================================================
// 记录 depth 更新时对应的原始 tick 索引
//
// 【公式定义】
//   _depth_index = l0_index (原始tick索引 [0-15299])
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: TickData.l0_index (onDepth)
//   输出: _depth_index (onDepth)
//
// 【备注】
//   - 供其他 depth 级别的算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class DepthIndex {
public:
  DepthIndex(const TickData &td, CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), index_buffer_(index_buffer) {}

  inline void compute() {
    // 从tick_data读取当前tick索引，记录depth更新时对应的原始tick位置
    index_value_ = static_cast<float>(td_.l0_index);
  }
  inline void flush() {
    // 将depth索引写入CBuffer，供其他depth级别算子使用
    index_buffer_.push_back(index_value_);
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &index_buffer_;
  float index_value_ = 0.0f;
};
