#pragma once

// =============================================================================
// TickIndex - Tick索引算子
// =============================================================================
// 时间特征使用实际时钟时间 + 正弦相位嵌入:
//   - 用实际秒数而非 index, 每天同一时刻值相同, 分布能对应实际时间
//   - sin 相位连续可导, 频谱干净, 梯度友好
// 输出1: sec (相位 [-1,1]) - sin(2π * clock_second / 60), 60秒一周期
// 输出2: _tick_index (原始索引 [0-15299]) - 供其他算子使用
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// #include "features/FeaturesDefine.hpp"
#include "features/misc/misc.hpp"

constexpr float SEC_PHASE_SCALE = 2.0f * PI / 60.0f;

class TickIndex {
public:
  TickIndex(const TickData &td,
            CBuffer<float, L2::BLEN> &sec_buffer,
            CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), sec_buffer_(sec_buffer), index_buffer_(index_buffer) {}

  void compute() {
    float clock_sec = static_cast<float>(td_.l0_index);
    sec_value_ = std::sin(clock_sec * SEC_PHASE_SCALE);
    index_value_ = static_cast<float>(td_.l0_index);
  }

  void flush() {
    sec_buffer_.push_back(sec_value_);
    index_buffer_.push_back(index_value_);
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &sec_buffer_;
  CBuffer<float, L2::BLEN> &index_buffer_;

  float sec_value_ = 0.0f;
  float index_value_ = 0.0f;
};
