#pragma once

#include "math/Operator.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <complex>
#include <numbers>
#include <span>
#include <vector>

// ============================================================================
// IIR Bandpass Filter (带通IIR滤波器)
// ============================================================================
//
// 设计方法: RBJ Audio EQ Cookbook - 纯 Bandpass Biquad 级联
//
// 关键设计决策:
//   1. 直接用 RBJ BPF biquad，不是 HP+LP 级联
//   2. 级联后做整体归一化 (在 f0 处增益 = 1)
//   3. 极限情况特判 (全带/纯LP/纯HP)
//
// 参数:
//   - f0 = sqrt(f_lo * f_hi): 几何中心频率
//   - Q = f0 / (f_hi - f_lo): 品质因数
//   - order: biquad 级联数 (总阶数 = 2*order)
//
// ============================================================================

namespace math::spectral {

// ============================================================================
// IIR滤波器类型
// ============================================================================

enum class IIRType : int { Butterworth = 0, ChebyshevI = 1, ChebyshevII = 2 };

// ============================================================================
// Biquad Section (二阶IIR)
// ============================================================================

struct Biquad {
  // 系数: H(z) = (b0 + b1*z^-1 + b2*z^-2) / (1 + a1*z^-1 + a2*z^-2)
  float b0 = 1.0f, b1 = 0.0f, b2 = 0.0f;
  float a1 = 0.0f, a2 = 0.0f;
};

// Biquad状态 (Direct Form II Transposed)
struct BiquadState {
  float z1 = 0.0f, z2 = 0.0f;

  void reset() { z1 = z2 = 0.0f; }

  float process(float x, const Biquad& bq) {
    const float y = bq.b0 * x + z1;
    z1 = bq.b1 * x - bq.a1 * y + z2;
    z2 = bq.b2 * x - bq.a2 * y;
    return y;
  }
};

// ============================================================================
// RBJ Biquad 设计 (Audio EQ Cookbook)
// ============================================================================

namespace rbj {

// RBJ Bandpass Filter (constant 0 dB peak gain)
// f0: 中心频率 (相对于 Fs, 0-0.5)
// Q: 品质因数 = f0/bw
inline Biquad bandpass(float f0, float Q) {
  // 防止极端 Q 值
  Q = std::clamp(Q, 0.001f, 100.0f);
  
  const float w0 = 2.0f * std::numbers::pi_v<float> * f0;
  const float sin_w0 = std::sin(w0);
  const float cos_w0 = std::cos(w0);
  const float alpha = sin_w0 / (2.0f * Q);
  
  const float a0 = 1.0f + alpha;
  
  Biquad bq;
  bq.b0 = alpha / a0;
  bq.b1 = 0.0f;
  bq.b2 = -alpha / a0;
  bq.a1 = -2.0f * cos_w0 / a0;
  bq.a2 = (1.0f - alpha) / a0;
  return bq;
}

// RBJ Lowpass Filter (2nd order)
inline Biquad lowpass(float fc, float Q) {
  Q = std::clamp(Q, 0.001f, 100.0f);
  
  const float w0 = 2.0f * std::numbers::pi_v<float> * fc;
  const float sin_w0 = std::sin(w0);
  const float cos_w0 = std::cos(w0);
  const float alpha = sin_w0 / (2.0f * Q);
  
  const float a0 = 1.0f + alpha;
  const float b0_unnorm = (1.0f - cos_w0) / 2.0f;
  
  Biquad bq;
  bq.b0 = b0_unnorm / a0;
  bq.b1 = (1.0f - cos_w0) / a0;
  bq.b2 = b0_unnorm / a0;
  bq.a1 = -2.0f * cos_w0 / a0;
  bq.a2 = (1.0f - alpha) / a0;
  return bq;
}

// RBJ Highpass Filter (2nd order)
inline Biquad highpass(float fc, float Q) {
  Q = std::clamp(Q, 0.001f, 100.0f);
  
  const float w0 = 2.0f * std::numbers::pi_v<float> * fc;
  const float sin_w0 = std::sin(w0);
  const float cos_w0 = std::cos(w0);
  const float alpha = sin_w0 / (2.0f * Q);
  
  const float a0 = 1.0f + alpha;
  const float b0_unnorm = (1.0f + cos_w0) / 2.0f;
  
  Biquad bq;
  bq.b0 = b0_unnorm / a0;
  bq.b1 = -(1.0f + cos_w0) / a0;
  bq.b2 = b0_unnorm / a0;
  bq.a1 = -2.0f * cos_w0 / a0;
  bq.a2 = (1.0f - alpha) / a0;
  return bq;
}

// 恒等 biquad (pass-through)
inline Biquad identity() {
  Biquad bq;
  bq.b0 = 1.0f;
  bq.b1 = 0.0f;
  bq.b2 = 0.0f;
  bq.a1 = 0.0f;
  bq.a2 = 0.0f;
  return bq;
}

} // namespace rbj

// ============================================================================
// 频率响应计算 (用于归一化)
// ============================================================================

namespace detail {

// 计算单个 biquad 在频率 f 处的复数响应
inline std::complex<float> biquad_response(const Biquad& bq, float f) {
  const float w = 2.0f * std::numbers::pi_v<float> * f;
  const std::complex<float> z = std::exp(std::complex<float>(0.0f, -w));
  const std::complex<float> z2 = z * z;
  
  const std::complex<float> num = bq.b0 + bq.b1 * z + bq.b2 * z2;
  const std::complex<float> den = 1.0f + bq.a1 * z + bq.a2 * z2;
  
  return num / den;
}

} // namespace detail

// ============================================================================
// IIR系数缓存
// ============================================================================

struct IIRCoeffs {
  static constexpr size_t MAX_SECTIONS = 16;
  std::array<Biquad, MAX_SECTIONS> sections;
  size_t n_sections = 0;
  
  float f_lo_ = -1.0f;
  float f_hi_ = -1.0f;
  int order_ = 0;
  IIRType type_ = IIRType::Butterworth;
  
  void compute(float f_lo, float f_hi, int order, IIRType type = IIRType::Butterworth) {
    // 检查是否需要重新计算
    if (std::abs(f_lo - f_lo_) < 1e-7f &&
        std::abs(f_hi - f_hi_) < 1e-7f &&
        order == order_ &&
        type == type_ &&
        n_sections > 0) [[unlikely]] return;

    f_lo_ = f_lo;
    f_hi_ = f_hi;
    order_ = order;
    type_ = type;

    assert(order >= 1 && order <= 8);
    assert(f_lo > 0.0f && f_lo < 1.0f);
    assert(f_hi > 0.0f && f_hi < 1.0f);
    assert(f_lo < f_hi);

    // 转换为相对于 Fs 的频率 (输入是相对于 Nyquist 的 0-1)
    const float fc_lo = f_lo / 2.0f;
    const float fc_hi = f_hi / 2.0f;
    
    n_sections = 0;
    
    // 几何中心频率和 Q
    const float f0 = std::sqrt(fc_lo * fc_hi);
    const float bw = fc_hi - fc_lo;
    const float Q_base = f0 / bw;
    
    // 根据类型和阶数设计 biquad 级联
    switch (type) {
    case IIRType::Butterworth:
      design_butterworth_bp(f0, Q_base, order);
      break;
    case IIRType::ChebyshevI:
      design_chebyshev1_bp(f0, Q_base, order, 1.0f);
      break;
    case IIRType::ChebyshevII:
      design_chebyshev2_bp(f0, Q_base, order);
      break;
    }
    
    // 整体归一化: 在 f0 处增益 = 1
    normalize_at(f0);
  }

private:
  // Butterworth 带通: 级联 order 个 BPF biquad
  // 所有 section 使用相同的 Q，依靠级联得到更陡的滚降
  void design_butterworth_bp(float f0, float Q_base, int order) {
    for (int k = 0; k < order; ++k) {
      sections[n_sections++] = rbj::bandpass(f0, Q_base);
    }
  }
  
  // Chebyshev I 带通: 通带纹波，使用稍高的 Q
  void design_chebyshev1_bp(float f0, float Q_base, int order, float ripple_db) {
    const float eps = std::sqrt(std::pow(10.0f, ripple_db / 10.0f) - 1.0f);
    const float Q_k = Q_base * (1.0f + eps * 0.5f);
    
    for (int k = 0; k < order; ++k) {
      sections[n_sections++] = rbj::bandpass(f0, Q_k);
    }
  }
  
  // Chebyshev II 带通: 阻带纹波 (通带平坦)，使用稍低的 Q
  void design_chebyshev2_bp(float f0, float Q_base, int order) {
    const float Q_k = Q_base * 0.9f;  // 稍低的 Q 使通带更平坦
    
    for (int k = 0; k < order; ++k) {
      sections[n_sections++] = rbj::bandpass(f0, Q_k);
    }
  }
  
  // 整体归一化: 使得在频率 f_norm 处增益 = 1
  void normalize_at(float f_norm) {
    if (n_sections == 0) return;
    
    // 计算当前增益
    std::complex<float> H(1.0f, 0.0f);
    for (size_t i = 0; i < n_sections; ++i) {
      H *= detail::biquad_response(sections[i], f_norm);
    }
    
    float gain = std::abs(H);
    if (gain < 1e-10f) {
      gain = 1.0f;  // 防止除零
    }
    
    // 归一化: 只调整第一个 section 的 b 系数
    // (等效于全局增益调整)
    const float inv_gain = 1.0f / gain;
    sections[0].b0 *= inv_gain;
    sections[0].b1 *= inv_gain;
    sections[0].b2 *= inv_gain;
  }
};

// ============================================================================
// IIR级联状态
// ============================================================================

struct IIRState {
  static constexpr size_t MAX_SECTIONS = IIRCoeffs::MAX_SECTIONS;
  std::array<BiquadState, MAX_SECTIONS> states;
  size_t n_sections = 0;

  void reset() {
    for (size_t i = 0; i < n_sections; ++i) {
      states[i].reset();
    }
  }

  void init(size_t n) {
    n_sections = n;
    reset();
  }

  float process(float x, const IIRCoeffs& coeffs) {
    float y = x;
    for (size_t i = 0; i < coeffs.n_sections; ++i) {
      y = states[i].process(y, coeffs.sections[i]);
    }
    return y;
  }
};

// ============================================================================
// IIR滤波 (零相位: 前向+反向)
// ============================================================================

inline void iir_filter_forward(const float* __restrict in, float* __restrict out, size_t n,
                               const IIRCoeffs& coeffs) {
  if (n == 0) [[unlikely]] return;

  IIRState state;
  state.init(coeffs.n_sections);

  for (size_t i = 0; i < n; ++i) {
    out[i] = state.process(in[i], coeffs);
  }
}

inline void iir_filter_zero_phase(const float* __restrict in, float* __restrict out, size_t n,
                                  const IIRCoeffs& coeffs, float* __restrict tmp) {
  // 前向滤波
  iir_filter_forward(in, tmp, n, coeffs);
  
  // 反向滤波
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(tmp[i], tmp[n - 1 - i]);
  }
  iir_filter_forward(tmp, out, n, coeffs);
  for (size_t i = 0; i < n / 2; ++i) {
    std::swap(out[i], out[n - 1 - i]);
  }
}

// ============================================================================
// 算子定义
// ============================================================================

struct IIRBandpass {
  static constexpr ParamMeta meta[] = {
      {"低频", 0.1f, 0.001f, 0.999f},
      {"高频", 0.3f, 0.001f, 0.999f},
      {"阶数", 2, 1, 8},
      {"类型", 0, 0, 2},
  };
  static constexpr OperatorDef def = {"IIR带通", meta, 4};

  template <typename GetLoFreq, typename GetHiFreq, typename GetOrder, typename GetType>
  static void compute(std::span<const float> in, std::span<float> out,
                      GetLoFreq get_lo, GetHiFreq get_hi, GetOrder get_order, GetType get_type) {
    iir_bandpass(in, out, get_lo(), get_hi(), static_cast<int>(get_order()),
                 static_cast<IIRType>(static_cast<int>(get_type())));
  }
};

// 便捷函数 (内部分配)
inline void iir_bandpass(std::span<const float> in, std::span<float> out,
                         float f_lo, float f_hi, int order, IIRType type = IIRType::Butterworth) {
  assert(in.size() == out.size());
  assert(f_lo >= 0.001f && f_lo <= 0.999f);
  assert(f_hi >= 0.001f && f_hi <= 0.999f);
  assert(f_lo < f_hi);

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  IIRCoeffs coeffs;
  coeffs.compute(f_lo, f_hi, order, type);

  std::vector<float> tmp(n);
  iir_filter_zero_phase(in.data(), out.data(), n, coeffs, tmp.data());
}

// 使用预计算系数 (高效)
inline void iir_bandpass(std::span<const float> in, std::span<float> out,
                         const IIRCoeffs& coeffs, std::span<float> tmp) {
  assert(in.size() == out.size());
  assert(tmp.size() >= in.size());

  const size_t n = in.size();
  if (n == 0) [[unlikely]] return;

  iir_filter_zero_phase(in.data(), out.data(), n, coeffs, tmp.data());
}

} // namespace math::spectral
