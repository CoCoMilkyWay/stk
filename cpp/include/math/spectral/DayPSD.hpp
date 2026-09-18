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
// 去均值偏差修正 (unbias): 减掉的样本均值 ε̄ 与各 bin 相关, 白噪声下 E|X_k|² = σ²(Σw² − |W_k|²/L),
// W_k = Hann 零填后的 DFT, L = VR. |W_k|² 只在主瓣 (k ≲ 2N/L) 显著: 单日 (255/256) k=1 压 17%,
// k=2 起可忽略. 逐 bin 乘 1/(1 − |W_k|²/(L·Σw²)) 还回去, 白噪声下精确; 有色谱 / 带缺口是近似
// (只动前几 bin, 幅度如上).
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
  std::array<float, N_FREQS> unbias{}; // 去均值偏差修正 1/(1 − |W_k|²/(L·Σw²)), 见文件头

  DayPSD() {
    double w2 = 0.0;
    for (size_t i = 0; i < VR; ++i) {
      hann[i] = static_cast<float>(0.5 * (1.0 - std::cos(2.0 * PI * static_cast<double>(i) / static_cast<double>(VR - 1))));
      w2 += static_cast<double>(hann[i]) * hann[i];
    }
    // |W_k|² = 窗零填后的功率谱 (fft_power_spectrum 已除 N, 乘回)
    for (size_t t = 0; t < N; ++t)
      buf[t] = t < VR ? hann[t] : 0.0f;
    fft_real_to_power<N>(buf.data(), power.data(), ws);
    for (size_t k = 0; k < N_FREQS; ++k) {
      const double wk2 = static_cast<double>(power[k]) * static_cast<double>(N);
      const double d = 1.0 - wk2 / (static_cast<double>(VR) * w2);
      assert(d > 0.0); // k=0 处 = 1/3 (Hann: (Σw)²/(L·Σw²) = 2/3), 其余更接近 1
      unbias[k] = static_cast<float>(1.0 / d);
    }
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
    for (size_t k = 0; k < N_FREQS; ++k)
      power[k] *= inv * unbias[k];
    return true;
  }
};

} // namespace math::spectral
