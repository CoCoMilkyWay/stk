#pragma once

// =============================================================================
// MinuteIndex - Minute索引算子
// =============================================================================
// 时间特征使用正弦相位嵌入
//
// 【公式定义】
//   min = sin(2π * l1_index / 60)  (值域[-1,1], 60分钟一周期)
//   _minute_index = l1_index  (原始索引 [0-254])
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: MinuteData (onMinute)
//   输出: min (onMinute), _minute_index (onMinute)
//
// 【备注】
//   - sin 相位连续可导, 频谱干净, 梯度友好
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// #include "features/FeaturesDefine.hpp"
#include "features/Misc/Misc.hpp"

constexpr float MIN_PHASE_SCALE = 2.0f * PI / 60.0f;

// 触发域: ON_MINUTE (分钟级, 每分钟触发一次)
class MinuteIndex {
public:
  enum Out : size_t { min,
                      minute_index,
                      kCount };

  MinuteIndex(const MinuteData &md, CBuffer<float, ::L2::BLEN> (&out)[kCount])
      : md_(md), min_buffer_(out[min]), index_buffer_(out[minute_index]) {}

  // 触发域: ON_MINUTE
  void compute() {
    // 计算分钟相位编码和原始索引
    // float clock_min = static_cast<float>(L1_to_Clock(md_.l1_index).minute);
    float clock_min = static_cast<float>(md_.l1_index);
    min_value_ = std::sin(clock_min * MIN_PHASE_SCALE);
    index_value_ = static_cast<float>(md_.l1_index);
  }

  // 触发域: ON_MINUTE
  void flush() {
    // 将compute中计算的两个值分别写入对应的CBuffer
    min_buffer_.push_back(min_value_);     // 写入正弦相位编码的时间特征
    index_buffer_.push_back(index_value_); // 写入原始分钟索引
  }

  void reset() {}

private:
  const MinuteData &md_;
  CBuffer<float, ::L2::BLEN> &min_buffer_;
  CBuffer<float, ::L2::BLEN> &index_buffer_;
  float min_value_ = 0.0f;
  float index_value_ = 0.0f;
};
