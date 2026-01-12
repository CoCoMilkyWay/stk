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
// Bin Mapping (FFT bin → Scale bins, 支持一对多映射)
// ============================================================================

struct BinMapEntry {
  uint8_t scale_idx;
  float weight;
};

struct MultiTargetBinMap {
  static constexpr size_t MAX_TARGETS = 12;
  std::array<BinMapEntry, MAX_TARGETS> targets{};
  uint8_t n_targets = 0;

  void add(uint8_t idx, float w) {
    assert(n_targets < MAX_TARGETS);
    targets[n_targets++] = {idx, w};
  }
};

// ============================================================================
// 非标 bin 的 FFT index 边界 (精确浮点数，在 FFT index 空间计算)
// ============================================================================

// 将非标 bin 的周期范围转换为 FFT index 范围
// 返回 [j_lo, j_hi]：低频边界对应长周期，高频边界对应短周期
// 
// 公式: FFT_index = frequency * FFT_SIZE = FFT_SIZE / period
//
// 对于周期 T (单位同采样率)，周期范围 [T-0.5, T+0.5]
//   频率范围: [1/(T+0.5), 1/(T-0.5)]
//   FFT index 范围: [N/(T+0.5), N/(T-0.5)]

struct FFTIndexRange {
  float j_lo;  // 低频边界 (对应长周期，较小的 FFT index)
  float j_hi;  // 高频边界 (对应短周期，较大的 FFT index)
};

// L0: 1秒采样，非标 bin 周期转换到秒
template <size_t N>
inline FFTIndexRange get_bin_fft_range_L0(size_t bin_idx) {
  if (bin_idx < N_SEC_BINS) {
    // 秒级 bin k: 周期 T = k+2 秒，范围 [k+1.5, k+2.5] 秒
    float T = static_cast<float>(bin_idx + 2);
    float T_lo = T - 0.5f;  // 短周期边界
    float T_hi = T + 0.5f;  // 长周期边界
    return {static_cast<float>(N) / T_hi, static_cast<float>(N) / T_lo};
  } else if (bin_idx < N_SEC_BINS + N_MIN_BINS) {
    // 分钟级 bin k: 周期 T = (k-57) 分钟 = (k-57)*60 秒
    float T_min = static_cast<float>(bin_idx - N_SEC_BINS + 1);
    float T_lo_sec = (T_min - 0.5f) * 60.0f;
    float T_hi_sec = (T_min + 0.5f) * 60.0f;
    return {static_cast<float>(N) / T_hi_sec, static_cast<float>(N) / T_lo_sec};
  } else if (bin_idx < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS) {
    // 小时级 bin k: 周期 T = (k-116) 小时 = (k-116)*3600 秒
    float T_hour = static_cast<float>(bin_idx - N_SEC_BINS - N_MIN_BINS + 1);
    float T_lo_sec = (T_hour - 0.5f) * 3600.0f;
    float T_hi_sec = (T_hour + 0.5f) * 3600.0f;
    return {static_cast<float>(N) / T_hi_sec, static_cast<float>(N) / T_lo_sec};
  }
  return {0.0f, 0.0f};  // DC
}

// L1: 1分钟采样，非标 bin 周期转换到分钟
template <size_t N>
inline FFTIndexRange get_bin_fft_range_L1(size_t bin_idx) {
  if (bin_idx < N_SEC_BINS + N_MIN_BINS) {
    // 分钟级 bin k: 周期 T = (k-57) 分钟
    float T_min = static_cast<float>(bin_idx - N_SEC_BINS + 1);
    float T_lo = T_min - 0.5f;
    float T_hi = T_min + 0.5f;
    return {static_cast<float>(N) / T_hi, static_cast<float>(N) / T_lo};
  } else if (bin_idx < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS) {
    // 小时级 bin k: 周期 T = (k-116) 小时 = (k-116)*60 分钟
    float T_hour = static_cast<float>(bin_idx - N_SEC_BINS - N_MIN_BINS + 1);
    float T_lo_min = (T_hour - 0.5f) * 60.0f;
    float T_hi_min = (T_hour + 0.5f) * 60.0f;
    return {static_cast<float>(N) / T_hi_min, static_cast<float>(N) / T_lo_min};
  }
  return {0.0f, 0.0f};  // DC
}

// L2: 1小时采样，非标 bin 周期转换到小时
template <size_t N>
inline FFTIndexRange get_bin_fft_range_L2(size_t bin_idx) {
  if (bin_idx >= N_SEC_BINS + N_MIN_BINS && bin_idx < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS) {
    // 小时级 bin k: 周期 T = (k-116) 小时
    float T_hour = static_cast<float>(bin_idx - N_SEC_BINS - N_MIN_BINS + 1);
    float T_lo = T_hour - 0.5f;
    float T_hi = T_hour + 0.5f;
    return {static_cast<float>(N) / T_hi, static_cast<float>(N) / T_lo};
  }
  return {0.0f, 0.0f};  // DC
}

// 计算两个区间的重叠长度 (在 FFT index 空间)
inline float overlap_length(float a_lo, float a_hi, float b_lo, float b_hi) {
  float lo = std::max(a_lo, b_lo);
  float hi = std::min(a_hi, b_hi);
  return std::max(0.0f, hi - lo);
}

// L0: 1秒采样
// 覆盖秒级(2-59s)、分钟级(1-59min)、小时级(1-4h，受限于 FFT 长度)
template <size_t N>
inline void build_mapping_L0(std::array<MultiTargetBinMap, N / 2 + 1> &mapping) {
  for (size_t j = 0; j <= N / 2; ++j) {
    mapping[j] = {};

    // FFT 频点 j 的 index 范围 [j-0.5, j+0.5]
    float fft_j_lo = (j == 0) ? 0.0f : (j - 0.5f);
    float fft_j_hi = j + 0.5f;

    // j=0 是 DC，直接归入 DC bin
    if (j == 0) {
      mapping[j].add(127, 1.0f);
      continue;
    }

    float total_weight = 0.0f;

    // 秒级 bins (idx 0-57)
    for (size_t k = 0; k < N_SEC_BINS; ++k) {
      auto range = get_bin_fft_range_L0<N>(k);
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;  // FFT 宽度总是 1
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    // 分钟级 bins (idx 58-116)
    for (size_t k = N_SEC_BINS; k < N_SEC_BINS + N_MIN_BINS; ++k) {
      auto range = get_bin_fft_range_L0<N>(k);
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    // 小时级 bins (idx 117-126)
    for (size_t k = N_SEC_BINS + N_MIN_BINS; k < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS; ++k) {
      auto range = get_bin_fft_range_L0<N>(k);
      // 跳过超出 FFT 范围的 bins (j_lo < 1 表示周期超过 FFT 长度)
      if (range.j_lo < 0.5f) continue;
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    // 未分配的能量归入 DC
    if (total_weight < 0.999f) {
      mapping[j].add(127, 1.0f - total_weight);
    }
  }
}

// L1: 1分钟采样
// 覆盖分钟级(1-59min)和小时级(1-10h)
template <size_t N>
inline void build_mapping_L1(std::array<MultiTargetBinMap, N / 2 + 1> &mapping) {
  for (size_t j = 0; j <= N / 2; ++j) {
    mapping[j] = {};

    float fft_j_lo = (j == 0) ? 0.0f : (j - 0.5f);
    float fft_j_hi = j + 0.5f;

    if (j == 0) {
      mapping[j].add(127, 1.0f);
      continue;
    }

    float total_weight = 0.0f;

    // 分钟级 bins (idx 58-116)
    for (size_t k = N_SEC_BINS; k < N_SEC_BINS + N_MIN_BINS; ++k) {
      auto range = get_bin_fft_range_L1<N>(k);
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    // 小时级 bins (idx 117-126)
    for (size_t k = N_SEC_BINS + N_MIN_BINS; k < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS; ++k) {
      auto range = get_bin_fft_range_L1<N>(k);
      if (range.j_lo < 0.5f) continue;
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    if (total_weight < 0.999f) {
      mapping[j].add(127, 1.0f - total_weight);
    }
  }
}

// L2: 1小时采样
// 覆盖小时级(1-10h)
template <size_t N>
inline void build_mapping_L2(std::array<MultiTargetBinMap, N / 2 + 1> &mapping) {
  for (size_t j = 0; j <= N / 2; ++j) {
    mapping[j] = {};

    float fft_j_lo = (j == 0) ? 0.0f : (j - 0.5f);
    float fft_j_hi = j + 0.5f;

    if (j == 0) {
      mapping[j].add(127, 1.0f);
      continue;
    }

    float total_weight = 0.0f;

    // 小时级 bins (idx 117-126)
    for (size_t k = N_SEC_BINS + N_MIN_BINS; k < N_SEC_BINS + N_MIN_BINS + N_HOUR_BINS; ++k) {
      auto range = get_bin_fft_range_L2<N>(k);
      if (range.j_lo < 0.5f) continue;
      float overlap = overlap_length(fft_j_lo, fft_j_hi, range.j_lo, range.j_hi);
      if (overlap > 0) {
        float w = overlap;
        mapping[j].add(static_cast<uint8_t>(k), w);
        total_weight += w;
      }
    }

    if (total_weight < 0.999f) {
      mapping[j].add(127, 1.0f - total_weight);
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
// Multi-Resolution PSD Workspace (Template)
// ============================================================================

template <size_t N0 = FFT_SIZE_L0, size_t N1 = FFT_SIZE_L1, size_t N2 = FFT_SIZE_L2>
struct MultiResPSDWorkspace {
  // 三级状态
  LevelState<N0> L0;
  LevelState<N1> L1;
  LevelState<N2> L2;

  // 预计算映射表 (支持一对多映射)
  std::array<MultiTargetBinMap, N0 / 2 + 1> map_L0;
  std::array<MultiTargetBinMap, N1 / 2 + 1> map_L1;
  std::array<MultiTargetBinMap, N2 / 2 + 1> map_L2;

  bool initialized = false;

  void init() {
    build_mapping_L0<N0>(map_L0);
    build_mapping_L1<N1>(map_L1);
    build_mapping_L2<N2>(map_L2);
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
                     const std::array<MultiTargetBinMap, N / 2 + 1> &mapping,
                     std::span<float> out) {
    if (!st.compute()) return;

    const float window_power = detail::get_hann<N>().power;

    for (size_t j = 0; j < mapping.size(); ++j) {
      const auto &m = mapping[j];
      float power = st.power[j] / window_power;

      // 按权重分配能量到多个目标 bins
      for (uint8_t t = 0; t < m.n_targets; ++t) {
        const auto &entry = m.targets[t];
        out[entry.scale_idx] += power * entry.weight;
      }
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
