#pragma once

#include "math/Operator.hpp"
#include <cassert>
#include <span>

// ============================================================================
// Moving Average Detrend (移动平均去趋势)
// ============================================================================
//
// 公式: y_t = x_t - MA_W(x_t)
//
// 其中 MA_W(x_t) = (1/W) * Σ_{i=0}^{W-1} x_{t-i}
//
// 特点:
//   - 简单,计算快
//   - 不保证消除单位根
//   - 对窗口大小敏感
//
// ============================================================================

namespace math::stationary {

struct MADetrend {
  static constexpr ParamMeta meta[] = {{"窗口", 10, 10, 500}};
  static constexpr OperatorDef def = {"MA去趋势", meta, 1};

  template <typename GetWindow>
  static void compute(std::span<const float> in, std::span<float> out, GetWindow get_window) {
    ma_detrend(in, out, static_cast<int>(get_window()));
  }
};

// O(n) 实现: 滑动窗口求和
inline void ma_detrend(std::span<const float> in, std::span<float> out, int window) {
  assert(in.size() == out.size());
  assert(window > 0);

  const size_t n = in.size();
  if (n == 0) return;

  const size_t w = static_cast<size_t>(window);
  const float inv_w = 1.0f / static_cast<float>(window);

  // 初始化: 前 window-1 个点用部分窗口
  float sum = 0.0f;
  for (size_t i = 0; i < n; ++i) {
    sum += in[i];

    if (i < w) {
      // 部分窗口: 用 [0, i] 的均值
      float ma = sum / static_cast<float>(i + 1);
      out[i] = in[i] - ma;
    } else {
      // 完整窗口: 用 [i-w+1, i] 的均值
      sum -= in[i - w];
      float ma = sum * inv_w;
      out[i] = in[i] - ma;
    }
  }
}

// 带中心化的版本 (centered MA)
// y_t = x_t - MA_W(x_{t-W/2:t+W/2})
inline void ma_detrend_centered(std::span<const float> in, std::span<float> out, int window) {
  assert(in.size() == out.size());
  assert(window > 0);

  const size_t n = in.size();
  if (n == 0) return;

  const int half = window / 2;

  for (size_t i = 0; i < n; ++i) {
    float sum = 0.0f;
    int count = 0;
    
    int start = static_cast<int>(i) - half;
    int end = static_cast<int>(i) + half;
    
    for (int j = start; j <= end; ++j) {
      if (j >= 0 && j < static_cast<int>(n)) {
        sum += in[j];
        ++count;
      }
    }
    
    float ma = (count > 0) ? sum / count : 0.0f;
    out[i] = in[i] - ma;
  }
}

} // namespace math::stationary
