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

  // 每秒输出 (ON_DEPTH 时调用)
  void compute() {
    float current_mid = mid_price_.back();

    // 计算收益 (相对于上一tick)
    float ret = 0.0f;
    if (prev_mid_ > 1e-6f && current_mid > 1e-6f) {
      ret = std::log(current_mid / prev_mid_);
    }

    // 输出的是上一tick的next_tick_ret (滞后输出)
    next_tick_ret_.push_back(pending_ret_);

    // 更新状态
    pending_ret_ = ret;
    prev_mid_ = current_mid;
  }

private:
  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &next_tick_ret_;

  float prev_mid_ = 0.0f;
  float pending_ret_ = 0.0f; // 待输出的收益
};
