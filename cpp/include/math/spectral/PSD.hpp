// PSD.hpp - Power Spectral Density via Welch Method
// ============================================================================
//
// 设计原则:
//   - Zero-copy: 从MonthTensor直接读取
//   - Zero-allocate: workspace预分配, 跨天复用
//   - 层级无感知: 上层提供参数, 算法统一处理
//
// 数据流:
//   MonthTensor → 按天转置 → 每资产分段FFT → 跨资产Welch → 热力图
//
// ============================================================================
#pragma once

#include "math/spectral/FFT.hpp"
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <span>
#include <vector>

namespace math::spectral {

// ============================================================================
// PSD Parameters (层级无感知, 上层设置)
// ============================================================================

struct PSDParams {
  size_t n_fft = N_FFT;           // FFT窗口点数 (固定512)
  float overlap_ratio = 0.2f;     // 重叠比例
  float sample_rate = 1.0f;       // 采样率 (Hz)

  // 派生参数
  size_t hop_size() const {
    return static_cast<size_t>(n_fft * (1.0f - overlap_ratio));
  }
  size_t n_freqs() const { return N_FREQS; }
  float freq_resolution() const { return sample_rate / n_fft; }
  float nyquist_freq() const { return sample_rate / 2.0f; }
};

// ============================================================================
// Hanning Window (运行时初始化, 静态存储)
// ============================================================================

namespace detail {

// PI 已在 FFT.hpp 中定义

struct HanningWindow {
  std::array<float, N_FFT> coeffs{};
  float power = 0.0f;

  HanningWindow() {
    double sum = 0.0;
    for (size_t i = 0; i < N_FFT; ++i) {
      double w = 0.5 * (1.0 - std::cos(2.0 * PI * i / (N_FFT - 1)));
      coeffs[i] = static_cast<float>(w);
      sum += w * w;
    }
    power = static_cast<float>(sum / N_FFT);
  }
};

inline const HanningWindow &get_hanning() {
  static const HanningWindow window;
  return window;
}

}  // namespace detail

// ============================================================================
// PSD Day Workspace (每线程一个, 跨天复用)
// ============================================================================

struct PSDDayWorkspace {
  // ===== 容量 =====
  size_t max_A = 0;      // 最大资产数
  size_t max_T = 0;      // 每天最大时间点数

  // ===== 当前状态 =====
  size_t A = 0;          // 当前资产数
  size_t T_day = 0;      // 当前天时间点数

  // ===== 数据缓冲 (按天, 转置后 [A][T]) =====
  std::vector<float> day_data;

  // ===== FFT workspace =====
  FFTWorkspace fft_ws;
  std::array<float, N_FFT> windowed_input;
  std::array<float, N_FREQS> power_buf;

  // ===== 单资产 Welch 累加 (double精度) =====
  std::array<double, N_FREQS> asset_accum;
  size_t asset_accum_count = 0;

  // ===== Per-asset PSD 输出 [max_A * N_FREQS] =====
  std::vector<float> per_asset_psd;

  // ===== 初始化 (一次分配) =====
  void init(size_t max_assets, size_t max_time_points) {
    max_A = max_assets;
    max_T = max_time_points;
    day_data.resize(max_A * max_T);
    per_asset_psd.resize(max_A * N_FREQS);
  }

  // ===== 设置当前天参数 =====
  void begin_day(size_t n_assets, size_t time_points) {
    assert(n_assets <= max_A);
    assert(time_points <= max_T);
    A = n_assets;
    T_day = time_points;
  }

  // ===== 开始单个资产的累加 =====
  void begin_asset() {
    asset_accum_count = 0;
  }

  // ===== 获取资产序列 (zero-copy span) =====
  std::span<const float> asset_series(size_t asset_idx) const {
    assert(asset_idx < A);
    return {day_data.data() + asset_idx * max_T, T_day};
  }

  std::span<float> asset_series_mut(size_t asset_idx) {
    assert(asset_idx < A);
    return {day_data.data() + asset_idx * max_T, T_day};
  }

  // ===== 获取单个资产的 PSD 输出 =====
  std::span<float> asset_psd(size_t asset_idx) {
    assert(asset_idx < max_A);
    return {per_asset_psd.data() + asset_idx * N_FREQS, N_FREQS};
  }

  std::span<const float> asset_psd(size_t asset_idx) const {
    assert(asset_idx < max_A);
    return {per_asset_psd.data() + asset_idx * N_FREQS, N_FREQS};
  }
};

// ============================================================================
// Core Functions (分层 Welch: 先 per-asset, 再跨 asset 平均)
// ============================================================================

// 对单个资产序列做分段FFT, 累加到 ws.asset_accum
// 调用前需先调用 ws.begin_asset()
inline void accumulate_asset_psd(
    std::span<const float> series,
    const PSDParams &params,
    PSDDayWorkspace &ws) {

  const size_t n = series.size();
  const size_t hop = params.hop_size();
  const auto &hanning = detail::get_hanning();

  if (n < N_FFT) return;  // 序列太短

  // 滑动窗口
  for (size_t start = 0; start + N_FFT <= n; start += hop) {
    // 加窗
    for (size_t i = 0; i < N_FFT; ++i) {
      ws.windowed_input[i] = series[start + i] * hanning.coeffs[i];
    }

    // FFT → 功率谱
    fft_real_to_power(ws.windowed_input.data(), ws.power_buf.data(), ws.fft_ws);

    // 累加到单资产buffer (首次时初始化)
    if (ws.asset_accum_count == 0) {
      for (size_t k = 0; k < N_FREQS; ++k) {
        ws.asset_accum[k] = ws.power_buf[k];
      }
    } else {
      for (size_t k = 0; k < N_FREQS; ++k) {
        ws.asset_accum[k] += ws.power_buf[k];
      }
    }
    ++ws.asset_accum_count;
  }
}

// 归一化单个资产的PSD到 ws.per_asset_psd[asset_idx]
inline void finalize_asset_psd(PSDDayWorkspace &ws, size_t asset_idx) {
  auto out = ws.asset_psd(asset_idx);

  if (ws.asset_accum_count == 0) {
    for (size_t k = 0; k < N_FREQS; ++k) {
      out[k] = 0.0f;
    }
    return;
  }

  // 归一化: 除以窗口数和窗函数功率
  const double window_power = detail::get_hanning().power;
  const double scale = 1.0 / (ws.asset_accum_count * window_power);
  for (size_t k = 0; k < N_FREQS; ++k) {
    out[k] = static_cast<float>(ws.asset_accum[k] * scale);
  }
}

// 对所有资产的PSD求平均, 输出到 day_psd
inline void average_assets_psd(const PSDDayWorkspace &ws, float *day_psd) {
  const size_t A = ws.A;
  if (A == 0) {
    for (size_t k = 0; k < N_FREQS; ++k) {
      day_psd[k] = 0.0f;
    }
    return;
  }

  // 先清零
  for (size_t k = 0; k < N_FREQS; ++k) {
    day_psd[k] = 0.0f;
  }

  // 累加所有资产
  for (size_t a = 0; a < A; ++a) {
    auto psd = ws.asset_psd(a);
    for (size_t k = 0; k < N_FREQS; ++k) {
      day_psd[k] += psd[k];
    }
  }

  // 平均
  const float inv_A = 1.0f / static_cast<float>(A);
  for (size_t k = 0; k < N_FREQS; ++k) {
    day_psd[k] *= inv_A;
  }
}

// ============================================================================
// Day-level Processing (从MonthTensor加载一天数据)
// ============================================================================

// 从MonthTensor提取一天数据, 转置到workspace
// MonthTensor布局: [T][F][A], 我们只取 F=0 (primary feature)
// 输出布局: workspace.day_data = [A][T] (连续存储)
template <typename MonthTensor>
inline void load_day_transpose(
    const MonthTensor &tensor,
    size_t day_idx,
    size_t feature_idx,  // F_selected中的索引
    PSDDayWorkspace &ws) {

  const size_t A = tensor.A;
  const size_t F_selected = tensor.feature_indices.size();
  const size_t t_start = tensor.day_offsets[day_idx];
  const size_t t_end = tensor.day_offsets[day_idx + 1];
  const size_t T = t_end - t_start;

  ws.begin_day(A, T);

  // 转置: [T][F][A] → [A][T]
  for (size_t t_local = 0; t_local < T; ++t_local) {
    const size_t t_global = t_start + t_local;
    const size_t src_base = t_global * F_selected * A + feature_idx * A;

    for (size_t a = 0; a < A; ++a) {
      ws.day_data[a * ws.max_T + t_local] =
          static_cast<float>(tensor.data[src_base + a]);
    }
  }
}

// ============================================================================
// PSD Cache (热力图数据, 存储在TimeSeries中)
// ============================================================================

struct PSDCache {
  std::vector<float> data;         // [n_days * N_FREQS] 连续存储
  std::vector<std::string> dates;  // [n_days]
  size_t n_days = 0;
  size_t n_freqs = N_FREQS;

  // 频率范围
  float freq_resolution = 0.0f;    // Hz/bin
  float nyquist_freq = 0.0f;       // Hz

  bool valid = false;

  void clear() { *this = PSDCache{}; }

  void init(size_t days, float sample_rate) {
    n_days = days;
    n_freqs = N_FREQS;
    freq_resolution = sample_rate / N_FFT;
    nyquist_freq = sample_rate / 2.0f;
    data.resize(n_days * n_freqs);
    dates.resize(n_days);
    valid = false;
  }

  // 获取某天的PSD (for writing)
  std::span<float> day_psd(size_t day_idx) {
    assert(day_idx < n_days);
    return {data.data() + day_idx * n_freqs, n_freqs};
  }

  // 获取某天的PSD (for reading)
  std::span<const float> day_psd(size_t day_idx) const {
    assert(day_idx < n_days);
    return {data.data() + day_idx * n_freqs, n_freqs};
  }

  // 获取某频率bin的所有天数据 (for plotting)
  float get(size_t day_idx, size_t freq_idx) const {
    assert(day_idx < n_days && freq_idx < n_freqs);
    return data[day_idx * n_freqs + freq_idx];
  }
};

}  // namespace math::spectral

