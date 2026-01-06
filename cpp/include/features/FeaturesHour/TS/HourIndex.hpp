#pragma once

// =============================================================================
// HourIndex - Hour索引算子
// =============================================================================
// 从 HourData.l2_index 读取当前小时级索引，转换为实际时钟时间
// 输出1: Hour (时钟小时 {9,10,11,13,14}) - 实际时钟小时，作为特征
// 输出2: HourIndex (原始索引 [0-4]) - 交易时间索引，供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"

class HourIndex {
public:
  HourIndex(const HourData &hd,
            CBuffer<float, ::L2::BLEN> &hour_buffer,
            CBuffer<float, ::L2::BLEN> &index_buffer)
      : hd_(hd), hour_buffer_(hour_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float index = static_cast<float>(hd_.l2_index);
    hour_buffer_.push_back(static_cast<float>(index2hour(hd_.l2_index))); // Hour {9,10,11,13,14}
    index_buffer_.push_back(index);                                       // Index [0-4]
  }

private:
  const HourData &hd_;
  CBuffer<float, ::L2::BLEN> &hour_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
};
