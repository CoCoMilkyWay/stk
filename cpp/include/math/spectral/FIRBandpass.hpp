#pragma once

#include "math/Operator.hpp"
#include <cassert>
#include <cmath>
#include <numbers>
#include <span>
#include <vector>

// ============================================================================
// FIR Bandpass Filter (带通FIR滤波器)
// ============================================================================
//
// 设计方法: 窗函数法
//   h[n] = (sinc(f_hi * n) - sinc(f_lo * n)) * window[n]
//
// 窗函数:
//   0 = Hann:     0.5 - 0.5*cos(2πn/(N-1))
//   1 = Hamming:  0.54 - 0.46*cos(2πn/(N-1))
//   2 = Blackman: 0.42 - 0.5*cos(2πn/(N-1)) + 0.08*cos(4πn/(N-1))
//
// 参数:
//   f_lo, f_hi: 归一化频率 (0-1, 1=Nyquist)
//   order: 滤波器阶数 (奇数, 对称)
//
// ============================================================================

namespace math::spectral {

// ============================================================================
// 窗函数类型
// ============================================================================

enum class FIRWindow : int { Hann = 0, Hamming = 1, Blackman = 2 };

// ============================================================================
// 窗函数计算
// ============================================================================

namespace detail {

inline float window_hann(int n, int N) {
  return 0.5f - 0.5f * std::cos(2.0f * std::numbers::pi_v<float> * n / (N - 1));
}

inline float window_hamming(int n, int N) {
  return 0.54f - 0.46f * std::cos(2.0f * std::numbers::pi_v<float> * n / (N - 1));
}

inline float window_blackman(int n, int N) {
  const float t = 2.0f * std::numbers::pi_v<float> * n / (N - 1);
  return 0.42f - 0.5f * std::cos(t) + 0.08f * std::cos(2.0f * t);
}

inline float window_value(int n, int N, FIRWindow type) {
  switch (type) {
  case FIRWindow::Hann: return window_hann(n, N);
  case FIRWindow::Hamming: return window_hamming(n, N);
  case FIRWindow::Blackman: return window_blackman(n, N);
  }
  return window_hann(n, N);
}

// sinc(x) = sin(πx) / (πx), sinc(0) = 1
inline float sinc(float x) {
  if (std::abs(x) < 1e-7f) return 1.0f;
  const float px = std::numbers::pi_v<float> * x;
  return std::sin(px) / px;
}

} // namespace detail

// ============================================================================
// FIR系数缓存
// ============================================================================

struct FIRCoeffs {
  std::vector<float> data;
  float f_lo_ = -1.0f;
  float f_hi_ = -1.0f;
  int order_ = 0;
  FIRWindow window_ = FIRWindow::Hann;

  // 计算带通FIR系数
  void compute(float f_lo, float f_hi, int order, FIRWindow window = FIRWindow::Hann) {
    // 确保阶数为奇数 (对称滤波器)
    if (order % 2 == 0) ++order;
    
    // 检查是否需要重新计算
    if (std::abs(f_lo - f_lo_) < 1e-7f &&
        std::abs(f_hi - f_hi_) < 1e-7f &&
        order == order_ &&
        window == window_ &&
        !data.empty()) [[unlikely]] return;

    f_lo_ = f_lo;
    f_hi_ = f_hi;
    order_ = order;
    window_ = window;

    data.resize(order);
    const int M = order - 1;
    const int center = M / 2;

    // 带通 = 低通(f_hi) - 低通(f_lo)
    // h_bp[n] = 2*f_hi*sinc(2*f_hi*(n-M/2)) - 2*f_lo*sinc(2*f_lo*(n-M/2))
    for (int n = 0; n < order; ++n) {
      const float t = static_cast<float>(n - center);
      const float h_hi = 2.0f * f_hi * detail::sinc(2.0f * f_hi * t);
      const float h_lo = 2.0f * f_lo * detail::sinc(2.0f * f_lo * t);
      const float w = detail::window_value(n, order, window);
      data[n] = (h_hi - h_lo) * w;
    }
  }

  [[nodiscard]] size_t size() const { return data.size(); }
  [[nodiscard]] const float* ptr() const { return data.data(); }
};

// ============================================================================
// FIR卷积 (零相位: 前向+反向)
// ============================================================================

// 单向FIR滤波 (因果)
inline void fir_filter_forward(const float* __restrict in, float* __restrict out, size_t n,
                               const float* __restrict coeffs, size_t n_coeffs) {
  assert(n_coeffs > 0);
  
  if (n == 0) [[unlikely]] return;

  const size_t delay = n_coeffs / 2;

  // 边界: 置零
  for (size_t i = 0; i < delay && i < n; ++i) {
    out[i] = 0.0f;
  }

  if (n <= delay) [[unlikely]] return;

  // 主循环: 4x展开
  size_t i = delay;
  const size_t n4 = ((n - delay) / 4) * 4 + delay;

  for (; i < n4; i += 4) [[likely]] {
    float sum0 = 0.0f, sum1 = 0.0f, sum2 = 0.0f, sum3 = 0.0f;

    for (size_t j = 0; j < n_coeffs; ++j) [[likely]] {
      const float cj = coeffs[j];
      sum0 += cj * in[i     - delay + j];
      sum1 += cj * in[i + 1 - delay + j];
      sum2 += cj * in[i + 2 - delay + j];
      sum3 += cj * in[i + 3 - delay + j];
    }

    out[i]     = sum0;
    out[i + 1] = sum1;
    out[i + 2] = sum2;
    out[i + 3] = sum3;
  }

  // 尾部处理
  for (; i < n; ++i) [[unlikely]] {
    float sum = 0.0f;
    for (size_t j = 0; j < n_coeffs; ++j) {
      sum += coeffs[j] * in[i - delay + j];
    }
    out[i] = sum;
  }
}

// 零相位FIR滤波 (前向+反向, 需要临时buffer)
inline void fir_filter_zero_phase(const float* __restrict in, float* __restrict out, size_t n,
                                  const float* __restrict coeffs, size_t n_coeffs,
                                  float* __restrict tmp) {
  // 前向滤波
  fir_filter_forward(in, tmp, n, coeffs, n_coeffs);
  
  // 反向滤波 (in-place反转 -> 滤波 -> 反转)
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(tmp[i], tmp[n - 1 - i]);
  }
  fir_filter_forward(tmp, out, n, coeffs, n_coeffs);
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(out[i], out[n - 1 - i]);
  }
}

// ============================================================================
// 算子定义
// ============================================================================

struct FIRBandpass {
  static constexpr ParamMeta meta[] = {
      {"低频", 0.1f, 0.001f, 0.999f},
      {"高频", 0.3f, 0.001f, 0.999f},
      {"阶数", 64, 8, 512},
      {"窗", 0, 0, 2},
  };
  static constexpr OperatorDef def = {"FIR带通", meta, 4};

  template <typename GetLoFreq, typename GetHiFreq, typename GetOrder, typename GetWindow>
  static void compute(std::span<const float> in, std::span<float> out,
                      GetLoFreq get_lo, GetHiFreq get_hi, GetOrder get_order, GetWindow get_window) {
    fir_bandpass(in, out, get_lo(), get_hi(), static_cast<int>(get_order()),
                 static_cast<FIRWindow>(static_cast<int>(get_window())));
  }
};

// 便捷函数 (内部分配)
inline void fir_bandpass(std::span<const float> in, std::span<float> out,
                         float f_lo, float f_hi, int order, FIRWindow window = FIRWindow::Hann) {
  assert(in.size() == out.size());
  assert(f_lo >= 0.0f && f_lo <= 1.0f);
  assert(f_hi >= 0.0f && f_hi <= 1.0f);
  assert(f_lo < f_hi);

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  FIRCoeffs coeffs;
  coeffs.compute(f_lo, f_hi, order, window);

  std::vector<float> tmp(n);
  fir_filter_zero_phase(in.data(), out.data(), n, coeffs.ptr(), coeffs.size(), tmp.data());
}

// 使用预计算系数 (高效)
inline void fir_bandpass(std::span<const float> in, std::span<float> out,
                         const FIRCoeffs& coeffs, std::span<float> tmp) {
  assert(in.size() == out.size());
  assert(tmp.size() >= in.size());

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  fir_filter_zero_phase(in.data(), out.data(), n, coeffs.ptr(), coeffs.size(), tmp.data());
}

} // namespace math::spectral
