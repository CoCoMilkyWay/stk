// FFT.hpp - Radix-2 Cooley-Tukey FFT (in-place, iterative)
// ============================================================================
//
// 简单高效的2的幂次FFT实现
// - 无外部依赖
// - 固定大小优化 (N=512)
// - 预计算twiddle因子
//
// ============================================================================
#pragma once

#include <array>
#include <cassert>
#include <cmath>
#include <complex>
#include <cstddef>

namespace math::spectral {

// ============================================================================
// Compile-time constants
// ============================================================================

inline constexpr size_t N_FFT = 512;
inline constexpr size_t N_FREQS = N_FFT / 2 + 1;  // 257 (正频率 + DC)
inline constexpr size_t LOG2_N = 9;  // log2(512)

static_assert((1 << LOG2_N) == N_FFT, "N_FFT must be power of 2");

// ============================================================================
// Twiddle factors (运行时初始化, 静态存储)
// ============================================================================

namespace detail {

inline constexpr double PI = 3.14159265358979323846;

// W_N^k = exp(-2πi·k/N)
struct TwiddleTable {
  std::array<std::complex<float>, N_FFT / 2> factors{};

  TwiddleTable() {
    for (size_t k = 0; k < N_FFT / 2; ++k) {
      double angle = -2.0 * PI * k / N_FFT;
      factors[k] = std::complex<float>(
          static_cast<float>(std::cos(angle)),
          static_cast<float>(std::sin(angle)));
    }
  }
};

inline const TwiddleTable &get_twiddle() {
  static const TwiddleTable table;
  return table;
}

// Bit-reversal permutation table
struct BitRevTable {
  std::array<uint16_t, N_FFT> indices{};

  constexpr BitRevTable() {
    for (size_t i = 0; i < N_FFT; ++i) {
      size_t rev = 0;
      size_t x = i;
      for (size_t j = 0; j < LOG2_N; ++j) {
        rev = (rev << 1) | (x & 1);
        x >>= 1;
      }
      indices[i] = static_cast<uint16_t>(rev);
    }
  }
};

inline constexpr BitRevTable BITREV{};

}  // namespace detail

// ============================================================================
// FFT Workspace (固定大小, 可复用)
// ============================================================================

struct FFTWorkspace {
  std::array<std::complex<float>, N_FFT> buf;  // 内部缓冲
};

// ============================================================================
// FFT Functions
// ============================================================================

// In-place Radix-2 DIT FFT
// 输入: workspace.buf (时域, bit-reversed order)
// 输出: workspace.buf (频域)
inline void fft_inplace(FFTWorkspace &ws) {
  auto &x = ws.buf;
  const auto &twiddle = detail::get_twiddle();

  // Cooley-Tukey iterative, DIT
  for (size_t s = 1; s <= LOG2_N; ++s) {
    const size_t m = 1 << s;           // 当前蝶形大小
    const size_t m2 = m >> 1;          // 半蝶形
    const size_t step = N_FFT >> s;    // twiddle步长

    for (size_t k = 0; k < N_FFT; k += m) {
      for (size_t j = 0; j < m2; ++j) {
        const auto &w = twiddle.factors[j * step];
        const auto t = w * x[k + j + m2];
        const auto u = x[k + j];
        x[k + j] = u + t;
        x[k + j + m2] = u - t;
      }
    }
  }
}

// 加载实数序列到workspace (带bit-reversal)
inline void fft_load_real(FFTWorkspace &ws, const float *input) {
  for (size_t i = 0; i < N_FFT; ++i) {
    ws.buf[detail::BITREV.indices[i]] = std::complex<float>(input[i], 0.0f);
  }
}

// 提取功率谱 (正频率部分)
// out: [N_FREQS] = |X[k]|² / N_FFT
inline void fft_power_spectrum(const FFTWorkspace &ws, float *out) {
  constexpr float scale = 1.0f / static_cast<float>(N_FFT);
  for (size_t k = 0; k < N_FREQS; ++k) {
    const auto &c = ws.buf[k];
    out[k] = (c.real() * c.real() + c.imag() * c.imag()) * scale;
  }
}

// 完整流程: 实数输入 → 功率谱
inline void fft_real_to_power(const float *input, float *power, FFTWorkspace &ws) {
  fft_load_real(ws, input);
  fft_inplace(ws);
  fft_power_spectrum(ws, power);
}

}  // namespace math::spectral

