#pragma once

#include "math/Operator.hpp"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <complex>
#include <numbers>
#include <span>
#include <vector>

// ============================================================================
// FIR Bandpass Filter (带通FIR滤波器)
// ============================================================================
//
// 设计方法: 窗函数法 + 归一化
//   h[n] = (sinc(f_hi * n) - sinc(f_lo * n)) * window[n]
//   然后在中心频率处归一化增益为 1
//
// 窗函数:
//   0 = Hann:     0.5 - 0.5*cos(2πn/(N-1))
//   1 = Hamming:  0.54 - 0.46*cos(2πn/(N-1))
//   2 = Blackman: 0.42 - 0.5*cos(2πn/(N-1)) + 0.08*cos(4πn/(N-1))
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
  if (N <= 1) return 1.0f;
  return 0.5f - 0.5f * std::cos(2.0f * std::numbers::pi_v<float> * n / (N - 1));
}

inline float window_hamming(int n, int N) {
  if (N <= 1) return 1.0f;
  return 0.54f - 0.46f * std::cos(2.0f * std::numbers::pi_v<float> * n / (N - 1));
}

inline float window_blackman(int n, int N) {
  if (N <= 1) return 1.0f;
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

// 计算 FIR 在频率 f 处的复数响应
inline std::complex<float> fir_response(const float* coeffs, size_t n_coeffs, float f) {
  std::complex<float> H(0.0f, 0.0f);
  const float w = 2.0f * std::numbers::pi_v<float> * f;
  
  for (size_t n = 0; n < n_coeffs; ++n) {
    // H(f) = Σ h[n] * e^(-j*w*n)
    const float phase = -w * static_cast<float>(n);
    H += coeffs[n] * std::complex<float>(std::cos(phase), std::sin(phase));
  }
  
  return H;
}

} // namespace detail

// ============================================================================
// FIR系数缓存
// ============================================================================

struct FIRCoeffs {
  std::vector<float> hp_coeffs;  // 高通系数
  std::vector<float> lp_coeffs;  // 低通系数
  float f_lo_ = -1.0f;
  float f_hi_ = -1.0f;
  int order_ = 0;
  FIRWindow window_ = FIRWindow::Hann;

  // 计算带通FIR系数 (HP + LP 级联)
  void compute(float f_lo, float f_hi, int order, FIRWindow window = FIRWindow::Hann) {
    // 确保阶数为奇数 (对称滤波器) 且至少为 3
    if (order < 3) order = 3;
    if (order % 2 == 0) ++order;
    
    // 检查是否需要重新计算
    if (std::abs(f_lo - f_lo_) < 1e-7f &&
        std::abs(f_hi - f_hi_) < 1e-7f &&
        order == order_ &&
        window == window_ &&
        !hp_coeffs.empty()) [[unlikely]] return;

    f_lo_ = f_lo;
    f_hi_ = f_hi;
    order_ = order;
    window_ = window;

    // 转换为相对于 Fs 的频率 (输入是相对于 Nyquist 的 0-1)
    const float fc_lo = f_lo / 2.0f;
    const float fc_hi = f_hi / 2.0f;

    // 设计高通滤波器 (截止频率 fc_lo)
    design_highpass(fc_lo, order, window);
    
    // 设计低通滤波器 (截止频率 fc_hi)
    design_lowpass(fc_hi, order, window);
  }

  [[nodiscard]] size_t hp_size() const { return hp_coeffs.size(); }
  [[nodiscard]] size_t lp_size() const { return lp_coeffs.size(); }
  [[nodiscard]] const float* hp_ptr() const { return hp_coeffs.data(); }
  [[nodiscard]] const float* lp_ptr() const { return lp_coeffs.data(); }

private:
  // 设计低通 FIR
  void design_lowpass(float fc, int order, FIRWindow window) {
    lp_coeffs.resize(order);
    const int center = order / 2;
    
    // h_lp[n] = 2*fc*sinc(2*fc*(n-center)) * window[n]
    for (int n = 0; n < order; ++n) {
      const float t = static_cast<float>(n - center);
      const float h = 2.0f * fc * detail::sinc(2.0f * fc * t);
      const float w = detail::window_value(n, order, window);
      lp_coeffs[n] = h * w;
    }
    
    // 归一化: DC 增益 = 1
    normalize_coeffs(lp_coeffs, 0.0f);
  }
  
  // 设计高通 FIR (从低通转换: HP = delta - LP)
  void design_highpass(float fc, int order, FIRWindow window) {
    hp_coeffs.resize(order);
    const int center = order / 2;
    
    // h_hp[n] = delta[n-center] - h_lp[n]
    // 先设计低通
    for (int n = 0; n < order; ++n) {
      const float t = static_cast<float>(n - center);
      const float h_lp = 2.0f * fc * detail::sinc(2.0f * fc * t);
      const float w = detail::window_value(n, order, window);
      
      // 高通 = delta - 低通
      float h_hp = -h_lp * w;
      if (n == center) {
        h_hp += 1.0f;  // delta[0] = 1
      }
      hp_coeffs[n] = h_hp;
    }
    
    // 归一化: Nyquist 增益 = 1
    normalize_coeffs(hp_coeffs, 0.5f);
  }
  
  // 归一化系数使得在指定频率处增益 = 1
  void normalize_coeffs(std::vector<float>& coeffs, float f_norm) {
    if (coeffs.empty()) return;
    
    auto H = detail::fir_response(coeffs.data(), coeffs.size(), f_norm);
    float gain = std::abs(H);
    
    // 防止除零或异常值
    if (gain < 1e-10f || !std::isfinite(gain)) {
      gain = 1.0f;
    }
    
    const float inv_gain = 1.0f / gain;
    for (auto& coeff : coeffs) {
      coeff *= inv_gain;
    }
  }
};

// ============================================================================
// FIR卷积 (零相位: 前向+反向)
// ============================================================================

// 单向FIR滤波 (因果，带正确的边界处理)
inline void fir_filter_forward(const float* __restrict in, float* __restrict out, size_t n,
                               const float* __restrict coeffs, size_t n_coeffs) {
  if (n == 0 || n_coeffs == 0) [[unlikely]] return;

  const int half = static_cast<int>(n_coeffs) / 2;
  const int n_int = static_cast<int>(n);
  const int nc_int = static_cast<int>(n_coeffs);

  for (int i = 0; i < n_int; ++i) {
    float sum = 0.0f;
    
    for (int j = 0; j < nc_int; ++j) {
      // 输入索引：i - half + j
      int idx = i - half + j;
      
      // 边界处理：镜像扩展
      if (idx < 0) {
        idx = -idx;
      } else if (idx >= n_int) {
        idx = 2 * n_int - 2 - idx;
      }
      
      // 确保索引在有效范围内
      idx = std::clamp(idx, 0, n_int - 1);
      
      sum += coeffs[j] * in[idx];
    }
    
    out[i] = sum;
  }
}

// 零相位FIR滤波 (前向+反向，单个滤波器)
inline void fir_filter_zero_phase_single(const float* __restrict in, float* __restrict out, size_t n,
                                         const float* __restrict coeffs, size_t n_coeffs,
                                         float* __restrict tmp) {
  if (n == 0) [[unlikely]] return;
  
  // 前向滤波
  fir_filter_forward(in, tmp, n, coeffs, n_coeffs);
  
  // 反向滤波
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(tmp[i], tmp[n - 1 - i]);
  }
  fir_filter_forward(tmp, out, n, coeffs, n_coeffs);
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(out[i], out[n - 1 - i]);
  }
}

// 零相位带通滤波 (HP + LP 级联)
inline void fir_filter_zero_phase_bandpass(const float* __restrict in, float* __restrict out, size_t n,
                                           const FIRCoeffs& coeffs,
                                           float* __restrict tmp1, float* __restrict tmp2) {
  if (n == 0) [[unlikely]] return;
  
  // 第一步: 高通滤波 (滤掉低频)
  fir_filter_zero_phase_single(in, tmp1, n, coeffs.hp_ptr(), coeffs.hp_size(), tmp2);
  
  // 第二步: 低通滤波 (滤掉高频)
  fir_filter_zero_phase_single(tmp1, out, n, coeffs.lp_ptr(), coeffs.lp_size(), tmp2);
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
  assert(f_lo >= 0.001f && f_lo <= 0.999f);
  assert(f_hi >= 0.001f && f_hi <= 0.999f);
  assert(f_lo < f_hi);

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  FIRCoeffs coeffs;
  coeffs.compute(f_lo, f_hi, order, window);

  std::vector<float> tmp1(n), tmp2(n);
  fir_filter_zero_phase_bandpass(in.data(), out.data(), n, coeffs, tmp1.data(), tmp2.data());
}

// 使用预计算系数 (高效)
inline void fir_bandpass(std::span<const float> in, std::span<float> out,
                         const FIRCoeffs& coeffs, std::span<float> tmp1, std::span<float> tmp2) {
  assert(in.size() == out.size());
  assert(tmp1.size() >= in.size());
  assert(tmp2.size() >= in.size());

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  fir_filter_zero_phase_bandpass(in.data(), out.data(), n, coeffs, tmp1.data(), tmp2.data());
}

} // namespace math::spectral
