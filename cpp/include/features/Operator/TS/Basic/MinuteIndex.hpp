#pragma once

// =============================================================================
// MinuteIndex - Minute索引算子
// =============================================================================
// 时间特征使用实际时钟时间 + 正弦相位嵌入:
//   - 用实际分钟数而非 index, 每天同一时刻值相同, 分布能对应实际时间
//   - sin 相位连续可导, 频谱干净, 梯度友好
// 输出1: min (相位 [-1,1]) - sin(2π * clock_minute / 60), 60分钟一周期
// 输出2: _minute_index (原始索引 [0-254]) - 供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// #include "features/FeaturesDefine.hpp"
#include "features/misc/misc.hpp"

constexpr float MIN_PHASE_SCALE = 2.0f * PI / 60.0f;

class MinuteIndex {
public:
  MinuteIndex(const MinuteData &md,
              CBuffer<float, ::L2::BLEN> &min_buffer,
              CBuffer<float, ::L2::BLEN> &index_buffer)
      : md_(md), min_buffer_(min_buffer), index_buffer_(index_buffer) {}

  void compute() {
    // float clock_min = static_cast<float>(L1_to_Clock(md_.l1_index).minute);
    float clock_min = static_cast<float>(md_.l1_index);
    min_buffer_.push_back(std::sin(clock_min * MIN_PHASE_SCALE));
    index_buffer_.push_back(static_cast<float>(md_.l1_index));
  }

private:
  const MinuteData &md_;
  CBuffer<float, ::L2::BLEN> &min_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
};
