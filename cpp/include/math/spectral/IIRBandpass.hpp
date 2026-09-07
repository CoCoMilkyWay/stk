#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <complex>
#include <numbers>

// ============================================================================
// IIR Bandpass Filter (带通IIR滤波器) —— 因果流式
// ============================================================================
//
// 设计方法: RBJ Audio EQ Cookbook - 纯 Bandpass Biquad 级联
//   1. 直接用 RBJ BPF biquad，不是 HP+LP 级联
//   2. 级联后做整体归一化 (在 f0 处增益 = 1)
//
// 参数:
//   - f0 = sqrt(f_lo * f_hi): 几何中心频率
//   - Q = f0 / (f_hi - f_lo): 品质因数
//   - order: biquad 级联数 (总阶数 = 2*order)
//
// 只有前向 (因果) 滤波: y_t 只依赖 x_{≤t}, 任何 TS 算子都能逐值复现 (DataDefine.hpp 红线).
// 零相位 filtfilt / FIR 中心对齐都用了未来值, 已删. 用法:
//   IIRCoeffs c; c.compute(f_lo, f_hi, order, type);   // 一次设计 (相对 Nyquist 的 0-1)
//   IIRState s;  s.init(c.n_sections);                  // 每段 (每天) 复位, 隔夜跳空不进滤波器
//   y = s.process(x, c);                                // 逐样本流式
// ============================================================================

namespace math::spectral {

// ============================================================================
// IIR滤波器类型
// ============================================================================

enum class IIRType : int { Butterworth = 0,
                           ChebyshevI = 1,
                           ChebyshevII = 2 };

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

  float process(float x, const Biquad &bq) {
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
inline std::complex<float> biquad_response(const Biquad &bq, float f) {
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
        n_sections > 0) [[unlikely]]
      return;

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
    const float Q_k = Q_base * 0.9f; // 稍低的 Q 使通带更平坦

    for (int k = 0; k < order; ++k) {
      sections[n_sections++] = rbj::bandpass(f0, Q_k);
    }
  }

  // 整体归一化: 使得在频率 f_norm 处增益 = 1
  void normalize_at(float f_norm) {
    if (n_sections == 0)
      return;

    // 计算当前增益
    std::complex<float> H(1.0f, 0.0f);
    for (size_t i = 0; i < n_sections; ++i) {
      H *= detail::biquad_response(sections[i], f_norm);
    }

    float gain = std::abs(H);
    if (gain < 1e-10f) {
      gain = 1.0f; // 防止除零
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

  float process(float x, const IIRCoeffs &coeffs) {
    float y = x;
    for (size_t i = 0; i < coeffs.n_sections; ++i) {
      y = states[i].process(y, coeffs.sections[i]);
    }
    return y;
  }
};

// ============================================================================
// 一段前向滤波 (状态从零起, 段 = 一天): 原地允许 (in == out)
// ============================================================================

inline void iir_filter_forward(const float *in, float *out, size_t n, const IIRCoeffs &coeffs) {
  IIRState state;
  state.init(coeffs.n_sections);
  for (size_t i = 0; i < n; ++i)
    out[i] = state.process(in[i], coeffs);
}

} // namespace math::spectral
