#pragma once

// =============================================================================
// HourIndex - Hour索引算子
// =============================================================================
// 时间特征使用正弦相位嵌入而非 modulo 标量:
//   - modulo 在周期边界有跳变, 产生无限高频分量, 污染频谱
//   - sin 相位连续可导, 频谱干净, 梯度友好
// 输出1: hour (相位 [-1,1]) - sin(2π * index / 4), 4小时一周期(一个交易日)
// 输出2: _hour_index (原始索引 [0-4]) - 供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/misc/misc.hpp"

constexpr float HOUR_PHASE_SCALE = 2.0f * PI / 4.0f;

class HourIndex {
public:
  HourIndex(const HourData &hd,
            CBuffer<float, ::L2::BLEN> &hour_buffer,
            CBuffer<float, ::L2::BLEN> &index_buffer)
      : hd_(hd), hour_buffer_(hour_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float index = static_cast<float>(hd_.l2_index);
    hour_buffer_.push_back(std::sin(index * HOUR_PHASE_SCALE));
    index_buffer_.push_back(index);
  }

private:
  const HourData &hd_;
  CBuffer<float, ::L2::BLEN> &hour_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
};
