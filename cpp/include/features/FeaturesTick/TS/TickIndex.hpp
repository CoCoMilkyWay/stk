#pragma once

// =============================================================================
// TickIndex - Tick索引算子
// =============================================================================
// 从 TickData.l0_index 读取当前tick级索引，转换为实际时钟时间
// 输出1: Sec (秒数 [0-59]) - 实际时钟秒数，作为特征
// 输出2: TickIndex (原始索引 [0-15299]) - 交易时间索引，供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"

class TickIndex {
public:
  TickIndex(const TickData &td,
            CBuffer<float, L2::BLEN> &sec_buffer,
            CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), sec_buffer_(sec_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float index = static_cast<float>(td_.l0_index);
    sec_buffer_.push_back(static_cast<float>(index2tick(td_.l0_index).second)); // Sec [0-59]
    index_buffer_.push_back(index);                                             // Index [0-15299]
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &sec_buffer_;
  CBuffer<float, L2::BLEN> &index_buffer_;
};
