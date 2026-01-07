#pragma once

// =============================================================================
// TickIndex - Tick索引算子
// =============================================================================
// 时间特征使用正弦相位嵌入而非 modulo 标量:
//   - modulo 在周期边界有跳变 (59→0), 产生无限高频分量, 污染频谱
//   - sin 相位连续可导, 频谱干净, 梯度友好
// 输出1: sec (相位 [-1,1]) - sin(2π * index / 60), 60秒一周期
// 输出2: _tick_index (原始索引 [0-15299]) - 供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/misc/misc.hpp"

constexpr float SEC_PHASE_SCALE = 2.0f * PI / 60.0f;

class TickIndex {
public:
  TickIndex(const TickData &td,
            CBuffer<float, L2::BLEN> &sec_buffer,
            CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), sec_buffer_(sec_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float index = static_cast<float>(td_.l0_index);
    sec_buffer_.push_back(std::sin(index * SEC_PHASE_SCALE));
    index_buffer_.push_back(index);
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &sec_buffer_;
  CBuffer<float, L2::BLEN> &index_buffer_;
};
