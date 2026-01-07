#pragma once

// =============================================================================
// HourIndex - Hour索引算子
// =============================================================================
// 时间特征使用实际时钟时间 + 正弦相位嵌入:
//   - 用实际小时数而非 index, 每天同一时刻值相同, 分布能对应实际时间
//   - sin 相位连续可导, 频谱干净, 梯度友好
// 输出1: hour (相位 [-1,1]) - sin(2π * clock_hour / 24), 24小时一周期
// 输出2: _hour_index (原始索引 [0-4]) - 供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// #include "features/FeaturesDefine.hpp"
#include "features/misc/misc.hpp"

constexpr float HOUR_PHASE_SCALE = 2.0f * PI / 24.0f;

class HourIndex {
public:
  HourIndex(const HourData &hd,
            CBuffer<float, ::L2::BLEN> &hour_buffer,
            CBuffer<float, ::L2::BLEN> &index_buffer)
      : hd_(hd), hour_buffer_(hour_buffer), index_buffer_(index_buffer) {}

  void compute() {
    // float clock_hour = static_cast<float>(L2_to_Clock(hd_.l2_index));
    float clock_hour = static_cast<float>(hd_.l2_index);
    hour_buffer_.push_back(std::sin(clock_hour * HOUR_PHASE_SCALE));
    index_buffer_.push_back(static_cast<float>(hd_.l2_index));
  }

private:
  const HourData &hd_;
  CBuffer<float, ::L2::BLEN> &hour_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
};
