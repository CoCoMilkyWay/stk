#pragma once

#include <cassert>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Fractional Differencing (分数阶差分)
// ============================================================================
//
// 公式: (1-L)^d x_t, d ∈ R
//
// 使用 Fixed-width window Fracdiff (FFD) 方法
// 权重: w_k = -w_{k-1} * (d - k + 1) / k, w_0 = 1
//
// 特点:
//   - 保留长期记忆
//   - 温和去单位根 (最小d原则)
//   - 可控的平稳化程度
//
// 参考:
//   Marcos López de Prado, "Advances in Financial Machine Learning", Ch. 5
//
// ============================================================================

namespace math::stationary {

// 计算FFD权重
inline std::vector<float> compute_ffd_weights(float d, int window, float threshold = 1e-5f) {
  std::vector<float> weights;
  weights.reserve(window);
  
  float w = 1.0f;
  weights.push_back(w);
  
  for (int k = 1; k < window; ++k) {
    w = -w * (d - k + 1) / k;
    if (std::abs(w) < threshold) break;
    weights.push_back(w);
  }
  
  return weights;
}

// FFD分数阶差分
inline void frac_diff(std::span<const float> in, std::span<float> out, 
                      float d, int window) {
  assert(in.size() == out.size());
  assert(d >= 0.0f && d <= 1.0f);
  assert(window > 0);

  const size_t n = in.size();
  if (n == 0) return;

  // d=0 时不做变换
  if (std::abs(d) < 1e-6f) {
    for (size_t i = 0; i < n; ++i) {
      out[i] = in[i];
    }
    return;
  }

  // d=1 时退化为一阶差分
  if (std::abs(d - 1.0f) < 1e-6f) {
    out[0] = 0.0f;
    for (size_t i = 1; i < n; ++i) {
      out[i] = in[i] - in[i - 1];
    }
    return;
  }

  // 计算权重
  auto weights = compute_ffd_weights(d, window);
  const size_t w_len = weights.size();

  // 应用FFD
  for (size_t i = 0; i < n; ++i) {
    if (i < w_len - 1) {
      // 边界处理: 用可用的权重
      float sum = 0.0f;
      for (size_t j = 0; j <= i; ++j) {
        sum += weights[j] * in[i - j];
      }
      out[i] = sum;
    } else {
      // 完整窗口
      float sum = 0.0f;
      for (size_t j = 0; j < w_len; ++j) {
        sum += weights[j] * in[i - j];
      }
      out[i] = sum;
    }
  }
}

// 找到最小的d使序列平稳 (ADF p-value < threshold)
// 二分搜索, 返回最优d
inline float find_min_d(std::span<const float> in, float pval_threshold = 0.05f,
                        int window = 100, int max_iter = 20) {
  // TODO: 实现二分搜索找最小d
  // 需要调用ADF测试
  (void)in;
  (void)pval_threshold;
  (void)window;
  (void)max_iter;
  return 0.5f; // 默认返回0.5
}

} // namespace math::stationary
