// MultiResPSD.hpp - Multi-Resolution Power Spectral Density
// ============================================================================
//
// 三级FFT多分辨率功率谱分析 (每天一次FFT, 无Welch):
//   L0 (1秒采样):  FFT=16384, 覆盖2s~4.5h
//   L1 (1分钟采样): FFT=8192, 覆盖1min~137h
//   L2 (1小时采样): FFT=128,  覆盖1h~128h
//
// 尺度Bin定义 (共128个):
//   秒级:   2,3,...,59    → 58个 (idx 0~57)
//   分钟级: 1,2,...,59    → 59个 (idx 58~116)
//   小时级: 1,2,...,10    → 10个 (idx 117~126)
//   DC:                   → 1个  (idx 127)
//
// ============================================================================
#pragma once

#include "define/CBuffer.hpp"
#include <array>
#include <cassert>
#include <cmath>
#include <complex>
#include <cstddef>
#include <span>

namespace math::spectral {

// ============================================================================
// Constants
// ============================================================================

inline constexpr size_t N_SEC_BINS = 58;      // 2-59秒
inline constexpr size_t N_MIN_BINS = 59;      // 1-59分钟
inline constexpr size_t N_HOUR_BINS = 10;     // 1-10小时
inline constexpr size_t N_SCALE_BINS = 128;   // 58+59+10+1(DC)

inline constexpr size_t FFT_SIZE_L0 = 16384;  // 1秒采样 ~4.5h
inline constexpr size_t FFT_SIZE_L1 = 8192;   // 1分钟采样 ~137h
inline constexpr size_t FFT_SIZE_L2 = 128;    // 1小时采样 ~128h

inline constexpr double PI = 3.14159265358979323846;

// ============================================================================
// Template FFT (支持不同大小)
// ============================================================================

namespace detail {

template <size_t N>
constexpr size_t log2_v = []() {
  size_t result = 0;
  size_t n = N;
  while (n > 1) {
    n >>= 1;
    ++result;
  }
  return result;
}();

template <size_t N>
struct TwiddleTable {
  std::array<std::complex<float>, N / 2> factors{};

  TwiddleTable() {
    for (size_t k = 0; k < N / 2; ++k) {
      double angle = -2.0 * PI * k / N;
      factors[k] = std::complex<float>(
          static_cast<float>(std::cos(angle)),
          static_cast<float>(std::sin(angle)));
    }
  }
};

template <size_t N>
const TwiddleTable<N> &get_twiddle() {
  static const TwiddleTable<N> table;
  return table;
}

template <size_t N>
struct BitRevTable {
  std::array<uint16_t, N> indices{};

  constexpr BitRevTable() {
    constexpr size_t LOG2_N = log2_v<N>;
    for (size_t i = 0; i < N; ++i) {
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

template <size_t N>
inline constexpr BitRevTable<N> BITREV{};

template <size_t N>
struct HannWindow {
  std::array<float, N> coeffs{};
  float power = 0.0f;

  HannWindow() {
    double sum = 0.0;
    for (size_t i = 0; i < N; ++i) {
      double w = 0.5 * (1.0 - std::cos(2.0 * PI * i / (N - 1)));
      coeffs[i] = static_cast<float>(w);
      sum += w * w;
    }
    power = static_cast<float>(sum / N);
  }
};

template <size_t N>
const HannWindow<N> &get_hann() {
  static const HannWindow<N> window;
  return window;
}

}  // namespace detail

// ============================================================================
// FFT Workspace (模板化)
// ============================================================================

template <size_t N>
struct FFTWorkspaceT {
  static_assert((N & (N - 1)) == 0, "N must be power of 2");
  std::array<std::complex<float>, N> buf;
};

template <size_t N>
void fft_inplace(FFTWorkspaceT<N> &ws) {
  constexpr size_t LOG2_N = detail::log2_v<N>;
  auto &x = ws.buf;
  const auto &twiddle = detail::get_twiddle<N>();

  for (size_t s = 1; s <= LOG2_N; ++s) {
    const size_t m = 1ULL << s;
    const size_t m2 = m >> 1;
    const size_t step = N >> s;

    for (size_t k = 0; k < N; k += m) {
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

template <size_t N>
void fft_load_real(FFTWorkspaceT<N> &ws, const float *input) {
  const auto &bitrev = detail::BITREV<N>;
  for (size_t i = 0; i < N; ++i) {
    ws.buf[bitrev.indices[i]] = std::complex<float>(input[i], 0.0f);
  }
}

template <size_t N>
void fft_power_spectrum(const FFTWorkspaceT<N> &ws, float *out) {
  constexpr float scale = 1.0f / static_cast<float>(N);
  constexpr size_t n_freqs = N / 2 + 1;
  for (size_t k = 0; k < n_freqs; ++k) {
    const auto &c = ws.buf[k];
    out[k] = (c.real() * c.real() + c.imag() * c.imag()) * scale;
  }
}

template <size_t N>
void fft_real_to_power(const float *input, float *power, FFTWorkspaceT<N> &ws) {
  fft_load_real<N>(ws, input);
  fft_inplace<N>(ws);
  fft_power_spectrum<N>(ws, power);
}

// ============================================================================
// Bin Mapping (FFT bin → Scale bin)
// ============================================================================

struct BinMap {
  uint8_t scale_idx;
  float weight;
};

// L0: 1秒采样, FFT=16384, df = 1/16384 Hz
inline void build_mapping_L0(std::array<BinMap, FFT_SIZE_L0 / 2 + 1> &mapping) {
  for (size_t j = 0; j <= FFT_SIZE_L0 / 2; ++j) {
    float f = static_cast<float>(j) / FFT_SIZE_L0;  // Hz
    float T = (f > 1e-6f) ? 1.0f / f : 1e6f;        // 周期(秒)

    if (T >= 1.5f && T < 59.5f) {
      // 秒级: 2-59秒 → idx 0-57
      size_t idx = static_cast<size_t>(std::round(T)) - 2;
      idx = std::min(idx, size_t{57});
      mapping[j] = {static_cast<uint8_t>(idx), 1.0f};
    } else if (T >= 30.0f && T < 3570.0f) {
      // 分钟级: 1-59分钟 → idx 58-116
      size_t min_val = static_cast<size_t>(std::round(T / 60.0f));
      min_val = std::clamp(min_val, size_t{1}, size_t{59});
      size_t idx = 58 + min_val - 1;
      mapping[j] = {static_cast<uint8_t>(idx), 1.0f};
    } else {
      mapping[j] = {127, 1.0f};  // DC
    }
  }
}

// L1: 1分钟采样, FFT=8192, df = 1/8192 cycles/min
inline void build_mapping_L1(std::array<BinMap, FFT_SIZE_L1 / 2 + 1> &mapping) {
  for (size_t j = 0; j <= FFT_SIZE_L1 / 2; ++j) {
    float f = static_cast<float>(j) / FFT_SIZE_L1;  // cycles/min
    float T = (f > 1e-6f) ? 1.0f / f : 1e6f;        // 周期(分钟)

    if (T >= 0.5f && T < 59.5f) {
      // 分钟级: 1-59分钟 → idx 58-116
      size_t min_val = static_cast<size_t>(std::round(T));
      min_val = std::clamp(min_val, size_t{1}, size_t{59});
      size_t idx = 58 + min_val - 1;
      mapping[j] = {static_cast<uint8_t>(idx), 1.0f};
    } else if (T >= 30.0f && T < 630.0f) {
      // 小时级: 1-10小时 = 60-600分钟 → idx 117-126
      size_t hour_val = static_cast<size_t>(std::round(T / 60.0f));
      hour_val = std::clamp(hour_val, size_t{1}, size_t{10});
      size_t idx = 117 + hour_val - 1;
      mapping[j] = {static_cast<uint8_t>(idx), 1.0f};
    } else {
      mapping[j] = {127, 1.0f};  // DC
    }
  }
}

// L2: 1小时采样, FFT=128, df = 1/128 cycles/hour
inline void build_mapping_L2(std::array<BinMap, FFT_SIZE_L2 / 2 + 1> &mapping) {
  for (size_t j = 0; j <= FFT_SIZE_L2 / 2; ++j) {
    float f = static_cast<float>(j) / FFT_SIZE_L2;  // cycles/hour
    float T = (f > 1e-6f) ? 1.0f / f : 1e6f;        // 周期(小时)

    if (T >= 0.5f && T < 10.5f) {
      // 小时级: 1-10小时 → idx 117-126
      size_t hour_val = static_cast<size_t>(std::round(T));
      hour_val = std::clamp(hour_val, size_t{1}, size_t{10});
      size_t idx = 117 + hour_val - 1;
      mapping[j] = {static_cast<uint8_t>(idx), 1.0f};
    } else {
      mapping[j] = {127, 1.0f};  // DC
    }
  }
}

// ============================================================================
// Level State (单级别的CBuffer + FFT状态, 无Welch)
// ============================================================================

template <size_t N>
struct LevelState {
  static constexpr size_t FFT_SIZE = N;
  static constexpr size_t N_FREQS = N / 2 + 1;

  CBuffer<float, N> buffer;
  FFTWorkspaceT<N> fft_ws;
  std::array<float, N> windowed;
  std::array<float, N_FREQS> power;

  void reset() { buffer.clear(); }

  // 从当前buffer计算FFT功率谱，输出到power
  // 返回是否有足够数据
  bool compute() {
    if (buffer.size() < N) return false;

    auto split = buffer.tail(N);
    const auto &hann = detail::get_hann<N>();

    size_t idx = 0;
    for (float v : split.head) {
      windowed[idx] = v * hann.coeffs[idx];
      ++idx;
    }
    for (float v : split.tail) {
      windowed[idx] = v * hann.coeffs[idx];
      ++idx;
    }

    fft_real_to_power<N>(windowed.data(), power.data(), fft_ws);
    return true;
  }
};

// ============================================================================
// Multi-Resolution PSD Workspace
// ============================================================================

struct MultiResPSDWorkspace {
  // 三级状态
  LevelState<FFT_SIZE_L0> L0;
  LevelState<FFT_SIZE_L1> L1;
  LevelState<FFT_SIZE_L2> L2;

  // 预计算映射表
  std::array<BinMap, FFT_SIZE_L0 / 2 + 1> map_L0;
  std::array<BinMap, FFT_SIZE_L1 / 2 + 1> map_L1;
  std::array<BinMap, FFT_SIZE_L2 / 2 + 1> map_L2;

  bool initialized = false;

  void init() {
    build_mapping_L0(map_L0);
    build_mapping_L1(map_L1);
    build_mapping_L2(map_L2);
    L0.reset();
    L1.reset();
    L2.reset();
    initialized = true;
  }

  void reset() {
    L0.reset();
    L1.reset();
    L2.reset();
  }

  // 处理单个样本 (push到buffer，不触发FFT)
  void push_L0(float x) { L0.buffer.push_back(x); }
  void push_L1(float x) { L1.buffer.push_back(x); }
  void push_L2(float x) { L2.buffer.push_back(x); }

  // 每天结束时调用: 从当前buffer计算FFT并输出到scale bins
  // CBuffer不清空，跨天保留
  void compute_day(std::span<float> out) {
    assert(out.size() >= N_SCALE_BINS);
    std::fill(out.begin(), out.begin() + N_SCALE_BINS, 0.0f);

    compute_level(L0, map_L0, out);
    compute_level(L1, map_L1, out);
    compute_level(L2, map_L2, out);
  }

private:
  template <size_t N>
  void compute_level(LevelState<N> &st,
                     const std::array<BinMap, N / 2 + 1> &mapping,
                     std::span<float> out) {
    if (!st.compute()) return;

    const float window_power = detail::get_hann<N>().power;

    for (size_t j = 0; j < mapping.size(); ++j) {
      const auto &m = mapping[j];
      float power = st.power[j] / window_power;
      out[m.scale_idx] += power * m.weight * m.weight;
    }
  }
};

// ============================================================================
// Helper: 获取尺度bin的标签
// ============================================================================

inline const char *get_scale_label(size_t idx) {
  static char buf[32];
  if (idx < N_SEC_BINS) {
    std::snprintf(buf, sizeof(buf), "%zus", idx + 2);
  } else if (idx < N_SEC_BINS + N_MIN_BINS) {
    std::snprintf(buf, sizeof(buf), "%zumin", idx - N_SEC_BINS + 1);
  } else if (idx < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS) {
    std::snprintf(buf, sizeof(buf), "%zuh", idx - N_SEC_BINS - N_MIN_BINS + 1);
  } else {
    std::snprintf(buf, sizeof(buf), "DC");
  }
  return buf;
}

inline float get_scale_period_seconds(size_t idx) {
  if (idx < N_SEC_BINS) {
    return static_cast<float>(idx + 2);  // 2-59秒
  } else if (idx < N_SEC_BINS + N_MIN_BINS) {
    return static_cast<float>((idx - N_SEC_BINS + 1) * 60);  // 1-59分钟
  } else if (idx < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS) {
    return static_cast<float>((idx - N_SEC_BINS - N_MIN_BINS + 1) * 3600);  // 1-10小时
  } else {
    return 1e9f;  // DC
  }
}

}  // namespace math::spectral
