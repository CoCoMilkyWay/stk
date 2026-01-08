#pragma once

#include <cassert>
#include <span>

// ============================================================================
// Integer Order Differencing (整数阶差分)
// ============================================================================
//
// 公式: (1-L)^d x_t, d ∈ Z+
//
// 其中 L 是滞后算子: L x_t = x_{t-1}
//
// d=1: Δx_t = x_t - x_{t-1}
// d=2: Δ²x_t = x_t - 2x_{t-1} + x_{t-2}
// d=3: Δ³x_t = x_t - 3x_{t-1} + 3x_{t-2} - x_{t-3}
//
// 特点:
//   - 强力消除单位根
//   - 可能过度平稳 (损失长期记忆)
//   - 理论保证平稳性
//
// ============================================================================

namespace math::stationary {

// 单次差分
inline void diff_once(std::span<const float> in, std::span<float> out) {
  const size_t n = in.size();
  assert(out.size() >= n);
  
  if (n == 0) return;
  
  out[0] = 0.0f; // 第一个点无差分
  for (size_t i = 1; i < n; ++i) {
    out[i] = in[i] - in[i - 1];
  }
}

// 整数阶差分
inline void int_diff(std::span<const float> in, std::span<float> out, int order) {
  assert(in.size() == out.size());
  assert(order >= 0 && order <= 3);

  const size_t n = in.size();
  if (n == 0) return;

  if (order == 0) {
    // 无差分
    for (size_t i = 0; i < n; ++i) {
      out[i] = in[i];
    }
    return;
  }

  // 差分系数 (二项式展开)
  // d=1: [1, -1]
  // d=2: [1, -2, 1]
  // d=3: [1, -3, 3, -1]
  static constexpr float COEF_1[] = {1.0f, -1.0f};
  static constexpr float COEF_2[] = {1.0f, -2.0f, 1.0f};
  static constexpr float COEF_3[] = {1.0f, -3.0f, 3.0f, -1.0f};

  const float *coef = nullptr;
  size_t coef_len = 0;

  switch (order) {
  case 1:
    coef = COEF_1;
    coef_len = 2;
    break;
  case 2:
    coef = COEF_2;
    coef_len = 3;
    break;
  case 3:
    coef = COEF_3;
    coef_len = 4;
    break;
  default:
    return;
  }

  // 应用差分
  for (size_t i = 0; i < n; ++i) {
    if (i < coef_len - 1) {
      // 边界: 部分差分或置零
      out[i] = 0.0f;
    } else {
      float sum = 0.0f;
      for (size_t j = 0; j < coef_len; ++j) {
        sum += coef[j] * in[i - j];
      }
      out[i] = sum;
    }
  }
}

} // namespace math::stationary
