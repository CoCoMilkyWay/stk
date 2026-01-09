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
// 实现: 阈值截断FFD (window为上限, threshold提前截断)
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

// 权重缓存 (预计算一次, 多次使用)
// 使用阈值截断的FFD, window为上限
struct FFDWeights {
  std::vector<float> data;
  float d_ = -1.0f;
  int window_ = 0;
  float threshold_ = 0.0f;
  
  void compute(float d, int window, float threshold = 1e-5f) {
    if (std::abs(d - d_) < 1e-7f &&
        window == window_ &&
        std::abs(threshold - threshold_) < 1e-7f &&
        !data.empty()) [[unlikely]] return;
    
    d_ = d;
    window_ = window;
    threshold_ = threshold;
    
    data.clear();
    data.reserve(window);
    
    float w = 1.0f;
    data.push_back(w);
    
    for (int k = 1; k < window; ++k) [[likely]] {
      w = -w * (d - static_cast<float>(k) + 1.0f) / static_cast<float>(k);
      if (std::abs(w) < threshold) [[unlikely]] break;
      data.push_back(w);
    }
  }
  
  [[nodiscard]] size_t size() const { return data.size(); }
  [[nodiscard]] const float* ptr() const { return data.data(); }
};

// FFD分数阶差分 (使用预计算权重, 0 alloc in loop)
inline void frac_diff(const float* __restrict in, float* __restrict out, size_t n,
                      const float* __restrict weights, size_t w_len) {
  assert(w_len > 0);
  
  if (n == 0) [[unlikely]] return;
  
  // 边界处理: 置零
  const size_t boundary = w_len - 1;
  for (size_t i = 0; i < boundary && i < n; ++i) [[unlikely]] {
    out[i] = 0.0f;
  }
  
  if (n <= boundary) [[unlikely]] return;
  
  // 主循环: 4x展开
  size_t i = boundary;
  const size_t n4 = ((n - boundary) / 4) * 4 + boundary;
  
  for (; i < n4; i += 4) [[likely]] {
    float sum0 = 0.0f, sum1 = 0.0f, sum2 = 0.0f, sum3 = 0.0f;
    
    for (size_t j = 0; j < w_len; ++j) [[likely]] {
      const float wj = weights[j];
      sum0 += wj * in[i     - j];
      sum1 += wj * in[i + 1 - j];
      sum2 += wj * in[i + 2 - j];
      sum3 += wj * in[i + 3 - j];
    }
    
    out[i]     = sum0;
    out[i + 1] = sum1;
    out[i + 2] = sum2;
    out[i + 3] = sum3;
  }
  
  // 尾部处理
  for (; i < n; ++i) [[unlikely]] {
    float sum = 0.0f;
    for (size_t j = 0; j < w_len; ++j) {
      sum += weights[j] * in[i - j];
    }
    out[i] = sum;
  }
}

// 使用FFDWeights结构体
inline void frac_diff(const float* __restrict in, float* __restrict out, size_t n,
                      const FFDWeights& weights) {
  frac_diff(in, out, n, weights.ptr(), weights.size());
}

// span版本
inline void frac_diff(std::span<const float> in, std::span<float> out,
                      const FFDWeights& weights) {
  assert(in.size() == out.size());
  frac_diff(in.data(), out.data(), in.size(), weights);
}

// 便捷版本 (内部分配权重, 适合一次性调用)
inline void frac_diff(std::span<const float> in, std::span<float> out,
                      float d, int window) {
  assert(in.size() == out.size());
  assert(d >= 0.0f && d <= 1.0f);
  
  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;
  
  // d=0 时不做变换
  if (std::abs(d) < 1e-6f) [[unlikely]] {
    for (size_t i = 0; i < n; ++i) out[i] = in[i];
    return;
  }
  
  // d=1 时退化为一阶差分
  if (std::abs(d - 1.0f) < 1e-6f) [[unlikely]] {
    out[0] = 0.0f;
    for (size_t i = 1; i < n; ++i) out[i] = in[i] - in[i - 1];
    return;
  }
  
  FFDWeights weights;
  weights.compute(d, window);
  frac_diff(in.data(), out.data(), n, weights);
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
