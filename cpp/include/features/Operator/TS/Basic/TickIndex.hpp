#pragma once

// =============================================================================
// TickIndex - Tick索引算子
// =============================================================================
// 时间特征使用分钟内秒位置的正弦相位嵌入
//
// 【公式定义】
//   sec = sin(2π * (index % 60) / 60)  (值域[-1,1], 60秒一周期)
//   _tick_index = l0_index  (原始索引 [0-15299])
//
// 【触发域】
//   compute: onTick
//   flush:   onTick
//
// 【输入输出】
//   输入: TickData (onTick)
//   输出: sec (onTick), _tick_index (onTick)
//
// 【备注】
//   - 使用查找表避免重复计算sin, 提升性能
//   - sin 相位连续可导, 频谱干净, 梯度友好
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// #include "features/FeaturesDefine.hpp"
#include "features/Misc/Misc.hpp"
#include <array>

// 预计算60秒周期的sin查找表（运行时初始化）
inline const std::array<float, 60>& get_sec_phase_lut() {
  static const auto lut = []() {
    std::array<float, 60> result{};
    constexpr float scale = 2.0f * PI / 60.0f;
    for (int i = 0; i < 60; ++i) {
      result[i] = std::sin(static_cast<float>(i) * scale);
    }
    return result;
  }();
  return lut;
}

class TickIndex {
public:
  TickIndex(const TickData &td,
            CBuffer<float, L2::BLEN> &sec_buffer,
            CBuffer<float, L2::BLEN> &index_buffer)
      : td_(td), sec_buffer_(sec_buffer), index_buffer_(index_buffer) {}

  inline void compute() {
    // 从tick_data读取当前tick索引
    uint32_t idx = td_.l0_index;
    // 查表获取正弦相位编码：分钟内秒位置，值域[-1,1]，梯度友好
    sec_value_ = get_sec_phase_lut()[idx % 60];
    // 保存原始索引值
    index_value_ = static_cast<float>(idx);
  }

  inline void flush() {
    // 将compute中计算的两个值分别写入对应的CBuffer
    sec_buffer_.push_back(sec_value_);     // 写入正弦相位编码的时间特征
    index_buffer_.push_back(index_value_); // 写入原始tick索引
  }

private:
  const TickData &td_;
  CBuffer<float, L2::BLEN> &sec_buffer_;
  CBuffer<float, L2::BLEN> &index_buffer_;

  float sec_value_ = 0.0f;
  float index_value_ = 0.0f;
};
