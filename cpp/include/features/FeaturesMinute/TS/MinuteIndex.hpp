#pragma once

// =============================================================================
// MinuteIndex - Minute索引算子
// =============================================================================
// 从 MinuteData.l1_index 读取当前分钟级索引，转换为实际时钟时间
// 输出1: Min (分钟数 [0-59]) - 实际时钟分钟数，作为特征
// 输出2: MinuteIndex (原始索引 [0-254]) - 交易时间索引，供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"

class MinuteIndex {
public:
  MinuteIndex(const MinuteData &md,
              CBuffer<float, ::L2::BLEN> &min_buffer,
              CBuffer<float, ::L2::BLEN> &index_buffer)
      : md_(md), min_buffer_(min_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float index = static_cast<float>(md_.l1_index);
    min_buffer_.push_back(static_cast<float>(index2minute(md_.l1_index).minute)); // Min [0-59]
    index_buffer_.push_back(index);                                               // Index [0-254]
  }

private:
  const MinuteData &md_;
  CBuffer<float, ::L2::BLEN> &min_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
};
