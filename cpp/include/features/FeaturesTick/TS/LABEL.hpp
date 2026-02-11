#pragma once

// =============================================================================
// LABEL - 标签特征
// =============================================================================
// 计算用于监督学习的标签
//   next_tick_ret = log(mid_{t+1} / mid_t)  (下一tick对数收益)
//
// 输入频率: ON_DEPTH
// 输出频率: per sec (滞后1秒输出)
// =============================================================================

#include <cmath>
#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class NextTickReturn {
public:
  NextTickReturn(const CBuffer<float, L2::BLEN> &mid_price,
                 CBuffer<float, L2::BLEN> &next_tick_ret)
      : mid_price_(mid_price), next_tick_ret_(next_tick_ret) {}

  inline void compute() {
    // 从MidPrice CBuffer读取当前中间价
    float current_mid = mid_price_.back();

    // 计算对数收益：log(mid_t / mid_{t-1})
    float ret = 0.0f;
    if (prev_mid_ > 1e-6f && current_mid > 1e-6f) {
      ret = std::log(current_mid / prev_mid_);  // 对数收益率
    }

    // 使用pending机制实现滞后1期：当前输出的是上一期计算的收益
    // 这样输出的是"下一tick收益"（相对于特征计算时刻）
    value_ = pending_ret_;      // 输出上一期的收益
    pending_ret_ = ret;         // 保存本期收益，下次输出
    prev_mid_ = current_mid;    // 更新prev价格
  }

  inline void flush() {
    // 将compute中准备好的下一tick收益写入CBuffer
    // 注意：这个"下一tick"是相对于上一次compute的
    next_tick_ret_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &next_tick_ret_;

  float prev_mid_ = 0.0f;
  float pending_ret_ = 0.0f;
  float value_ = 0.0f;
};
