#pragma once

#include "math/spectral/MultiResPSD.hpp" // FFTWorkspaceT / fft_real_to_power

#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>

// ============================================================================
// DayPSD: 单日功率谱 (按天分段, 与 TS 链的逐天处理一致)
// ============================================================================
// 一天 VR 个样本 (NaN = 缺): 有效样本去均值, 缺样本补 0, Hann(VR) 加窗, 零填到 N = 2^k ≥ VR,
// 实 FFT → 单边功率 P[k], k ∈ [0, N/2]. 频率轴: bin k ↔ 周期 N/k 个样本 (L1: 分钟).
// 归一化: 除以有效样本上的窗能量 Σw², 缺得多的天不会因补零显得"更安静".
// 不跨天拼接 → 隔夜跳空不进谱; 多天/多资产的谱在调用方做算术平均 (逐批收敛).
// ============================================================================

namespace math::spectral {

constexpr size_t next_pow2(size_t v) {
  size_t n = 1;
  while (n < v)
    n <<= 1;
  return n;
}

template <size_t VR>
struct DayPSD {
  static constexpr size_t N = next_pow2(VR);
  static constexpr size_t N_FREQS = N / 2 + 1;
  static constexpr size_t kMinValid = VR / 4; // 有效样本太少 → 该天不计谱

  static constexpr float period_of(size_t k) { return k == 0 ? 0.0f : static_cast<float>(N) / static_cast<float>(k); }

  FFTWorkspaceT<N> ws;
  std::array<float, N> buf{};
  std::array<float, N_FREQS> power{};
  std::array<float, VR> hann{};

  DayPSD() {
    for (size_t i = 0; i < VR; ++i)
      hann[i] = static_cast<float>(0.5 * (1.0 - std::cos(2.0 * PI * static_cast<double>(i) / static_cast<double>(VR - 1))));
  }

  // 返回 false = 有效样本不足, power 未写
  bool compute(const float *x) {
    double sum = 0.0;
    size_t n_valid = 0;
    for (size_t t = 0; t < VR; ++t) {
      const float v = x[t];
      if (v == v) {
        sum += v;
        ++n_valid;
      }
    }
    if (n_valid < kMinValid)
      return false;
    const float mean = static_cast<float>(sum / static_cast<double>(n_valid));
    double w2 = 0.0;
    for (size_t t = 0; t < VR; ++t) {
      const float v = x[t];
      if (v == v) {
        buf[t] = (v - mean) * hann[t];
        w2 += static_cast<double>(hann[t]) * hann[t];
      } else {
        buf[t] = 0.0f;
      }
    }
    for (size_t t = VR; t < N; ++t)
      buf[t] = 0.0f;
    fft_real_to_power<N>(buf.data(), power.data(), ws);
    const float inv = static_cast<float>(static_cast<double>(N) / w2); // fft_power_spectrum 已除 N, 还回来再除 Σw²
    for (float &p : power)
      p *= inv;
    return true;
  }
};

} // namespace math::spectral
