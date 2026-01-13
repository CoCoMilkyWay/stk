#pragma once

#include "math/Operator.hpp"
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

struct IntDiff {
  static constexpr ParamMeta meta[] = {{"阶数", 1, 1, 3}};
  static constexpr OperatorDef def = {"整数差分", meta, 1};

  template <typename GetOrder>
  static void compute(std::span<const float> in, std::span<float> out, GetOrder get_order) {
    int_diff(in, out, static_cast<int>(get_order()));
  }
};

// 一阶差分 (循环展开, 4x unroll)
inline void int_diff_1(const float *__restrict in, float *__restrict out, size_t n) {
  assert(n > 0);

  out[0] = 0.0f;

  size_t i = 1;
  const size_t n4 = ((n - 1) / 4) * 4 + 1;

  // 主循环: 4x展开
  for (; i < n4; i += 4) [[likely]] {
    out[i] = in[i] - in[i - 1];
    out[i + 1] = in[i + 1] - in[i];
    out[i + 2] = in[i + 2] - in[i + 1];
    out[i + 3] = in[i + 3] - in[i + 2];
  }

  // 尾部处理
  for (; i < n; ++i) [[unlikely]] {
    out[i] = in[i] - in[i - 1];
  }
}

// 二阶差分 (循环展开, 4x unroll)
inline void int_diff_2(const float *__restrict in, float *__restrict out, size_t n) {
  assert(n > 0);

  out[0] = 0.0f;
  if (n > 1) [[likely]]
    out[1] = 0.0f;

  if (n <= 2) [[unlikely]]
    return;

  size_t i = 2;
  const size_t n4 = ((n - 2) / 4) * 4 + 2;

  // 主循环: 4x展开
  for (; i < n4; i += 4) [[likely]] {
    out[i] = in[i] - 2.0f * in[i - 1] + in[i - 2];
    out[i + 1] = in[i + 1] - 2.0f * in[i] + in[i - 1];
    out[i + 2] = in[i + 2] - 2.0f * in[i + 1] + in[i];
    out[i + 3] = in[i + 3] - 2.0f * in[i + 2] + in[i + 1];
  }

  // 尾部处理
  for (; i < n; ++i) [[unlikely]] {
    out[i] = in[i] - 2.0f * in[i - 1] + in[i - 2];
  }
}

// 三阶差分 (循环展开, 4x unroll)
inline void int_diff_3(const float *__restrict in, float *__restrict out, size_t n) {
  assert(n > 0);

  out[0] = 0.0f;
  if (n > 1) [[likely]]
    out[1] = 0.0f;
  if (n > 2) [[likely]]
    out[2] = 0.0f;

  if (n <= 3) [[unlikely]]
    return;

  size_t i = 3;
  const size_t n4 = ((n - 3) / 4) * 4 + 3;

  // 主循环: 4x展开
  for (; i < n4; i += 4) [[likely]] {
    out[i] = in[i] - 3.0f * in[i - 1] + 3.0f * in[i - 2] - in[i - 3];
    out[i + 1] = in[i + 1] - 3.0f * in[i] + 3.0f * in[i - 1] - in[i - 2];
    out[i + 2] = in[i + 2] - 3.0f * in[i + 1] + 3.0f * in[i] - in[i - 1];
    out[i + 3] = in[i + 3] - 3.0f * in[i + 2] + 3.0f * in[i + 1] - in[i];
  }

  // 尾部处理
  for (; i < n; ++i) [[unlikely]] {
    out[i] = in[i] - 3.0f * in[i - 1] + 3.0f * in[i - 2] - in[i - 3];
  }
}

// 整数阶差分 (分派到专用函数)
inline void int_diff(const float *__restrict in, float *__restrict out, size_t n, int order) {
  assert(order >= 0 && order <= 3);

  if (n == 0) [[unlikely]]
    return;

  switch (order) {
  case 0:
    for (size_t i = 0; i < n; ++i)
      out[i] = in[i];
    break;
  case 1:
    int_diff_1(in, out, n);
    break;
  case 2:
    int_diff_2(in, out, n);
    break;
  case 3:
    int_diff_3(in, out, n);
    break;
  }
}

// span版本
inline void int_diff(std::span<const float> in, std::span<float> out, int order) {
  assert(in.size() == out.size());
  int_diff(in.data(), out.data(), in.size(), order);
}

} // namespace math::stationary
