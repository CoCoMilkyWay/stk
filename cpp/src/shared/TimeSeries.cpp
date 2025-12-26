#include "shared/TimeSeries.hpp"
#include "features/backend/FeatureReader.hpp"
#include "math/spectral/PSD.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <span>

// ============================================================================
// Build Single Month Stationarity
// ============================================================================

void TimeSeries::build_stationarity_month(size_t cache_idx,
                                          const std::string &features_dir,
                                          const Feature &feature,
                                          const Asset &asset) {
  TraceN("BuildStationarityMonth");

  assert(cache_idx < stationarity_cache.size());
  auto &mc = stationarity_cache[cache_idx];

  if (compute.cancel.load())
    return;

  const int level = feature.selection.selected_level;
  const int primary_idx = feature.selection.primary_feature_idx;
  assert(primary_idx >= 0);
  assert(level >= 0 && level < 3);

  const size_t n_assets = asset.items.size();
  mc.init(n_assets);

  // Get feature metadata for valid_type
  const FeatureMetadata *meta_list = nullptr;
  size_t meta_count = 0;
  if (level == 0) {
    meta_list = feature.metadata.features_l0.data();
    meta_count = feature.metadata.features_l0.size();
  } else if (level == 1) {
    meta_list = feature.metadata.features_l1.data();
    meta_count = feature.metadata.features_l1.size();
  } else {
    meta_list = feature.metadata.features_l2.data();
    meta_count = feature.metadata.features_l2.size();
  }

  // Determine columns to load
  std::vector<size_t> columns = {static_cast<size_t>(primary_idx)};

  L2::ValidType valid_type = L2::ValidType::ALL;
  if (primary_idx >= 0 && static_cast<size_t>(primary_idx) < meta_count) {
    valid_type = meta_list[primary_idx].valid_type;
  }

  // Find valid flag index
  if (valid_type != L2::ValidType::ALL) {
    const char *flag_name =
        (valid_type == L2::ValidType::DEPTH) ? "_depth_valid" : "_data_valid";
    for (size_t i = 0; i < meta_count; ++i) {
      if (std::strcmp(meta_list[i].code, flag_name) == 0) {
        columns.push_back(i);
        break;
      }
    }
  }

  // Batch load entire month
  FeatureReader reader(features_dir);
  std::string year = mc.month.substr(0, 4);
  std::string month_str = mc.month.substr(4, 2);

  FeatureReader::MonthTensor month_tensor;
  {
    TraceN("PreallocateTensor");
    month_tensor.preallocate(n_assets, 31, columns.size(), level);
  }

  {
    TraceN("LoadMonthData");
    reader.load_month_columns(year, month_str, columns, month_tensor);
  }

  if (compute.cancel.load())
    return;

  const size_t A = month_tensor.A;
  assert(A == n_assets);
  const size_t F_selected = columns.size();
  const bool has_valid_flag = (F_selected > 1);

  // Pre-allocate sample buffers (per-asset time series)
  size_t total_T = month_tensor.day_offsets.back();
  std::vector<std::vector<float>> asset_series(A);
  {
    TraceN("PreallocateSeries");
    for (auto &v : asset_series) {
      v.reserve(total_T);
    }
  }

  // Process entire month: collect time series per asset
  {
    TraceN("CollectSeries");
    for (size_t day_idx = 0; day_idx < month_tensor.dates.size(); ++day_idx) {
      if (compute.cancel.load())
        return;

      size_t t_start = month_tensor.day_offsets[day_idx];
      size_t t_end = month_tensor.day_offsets[day_idx + 1];

      for (size_t t = t_start; t < t_end; ++t) {
        // Zero-copy pointers into month tensor
        const feature_storage_t *values =
            &month_tensor.data[t * F_selected * A];
        const feature_storage_t *valid_flags =
            has_valid_flag ? &month_tensor.data[t * F_selected * A + A]
                           : nullptr;

        for (size_t a = 0; a < A; ++a) {
          float val = static_cast<float>(values[a]);

          // Check valid flag
          if (valid_flags && static_cast<float>(valid_flags[a]) <= 0.5f)
            continue;

          // Check NaN/Inf
          if (val != val)
            continue; // NaN
          if (val > 1e38f || val < -1e38f)
            continue; // Inf

          asset_series[a].push_back(val);
        }
      }
    }
  }

  // Run ADF/KPSS tests for each asset
  {
    TraceN("RunTests");
    math::stationary::ADFWorkspace adf_ws;
    math::stationary::KPSSWorkspace kpss_ws;
    for (size_t a = 0; a < A; ++a) {
      auto &cell = mc.by_asset[a];
      const auto &series = asset_series[a];

      cell.n_samples = series.size();

      // Need minimum samples for meaningful test
      if (series.size() < 30) {
        cell.valid = false;
        continue;
      }

      const std::span<const float> s(series.data(), series.size());

      // ADF test
      auto adf_result = math::stationary::adf_test(s, /*max_lag=*/12, adf_ws);
      if (adf_result.valid) {
        cell.adf_statistic = adf_result.statistic;
        cell.adf_pvalue = adf_result.pvalue;
        cell.adf_pass = (adf_result.pvalue < 0.05f);
      }

      // KPSS test
      auto kpss_result = math::stationary::kpss_test(s, /*bandwidth=*/-1, kpss_ws);
      if (kpss_result.valid) {
        cell.kpss_statistic = kpss_result.statistic;
        cell.kpss_pvalue = kpss_result.pvalue;
        cell.kpss_pass = (kpss_result.pvalue > 0.05f);
      }

      cell.valid = adf_result.valid && kpss_result.valid;
    }
  }

  mc.valid = true;
}

// ============================================================================
// Build All Months (Parallel)
// ============================================================================

void TimeSeries::build_stationarity(
    const std::vector<std::string> &months, const std::string &features_dir,
    const Feature &feature, const Asset &asset,
    std::function<void(std::function<void()>)> submit) {
  compute.reset();
  compute.status = Compute::Status::Building;
  compute.total = months.size();

  // Initialize cache
  stationarity_cache.clear();
  stationarity_cache.resize(months.size());
  for (size_t i = 0; i < months.size(); ++i) {
    stationarity_cache[i].month = months[i];
  }

  // Dispatch tasks
  for (size_t i = 0; i < months.size(); ++i) {
    submit([this, i, &features_dir, &feature, &asset]() {
      build_stationarity_month(i, features_dir, feature, asset);
      compute.done.fetch_add(1);
    });
  }
}

// ============================================================================
// Finalize (after build completes)
// ============================================================================

void TimeSeries::finalize_stationarity() {
  if (stationarity_cache.empty()) {
    step0_stationarity.valid = false;
    return;
  }

  // Collect all valid p-values for aggregate statistics
  std::vector<float> all_adf_pvalues;
  std::vector<float> all_kpss_pvalues;

  size_t n_total = 0;
  size_t n_adf_pass = 0;
  size_t n_kpss_pass = 0;
  size_t n_both_pass = 0;

  for (const auto &mc : stationarity_cache) {
    if (!mc.valid)
      continue;

    for (const auto &cell : mc.by_asset) {
      if (!cell.valid)
        continue;

      n_total++;
      all_adf_pvalues.push_back(cell.adf_pvalue);
      all_kpss_pvalues.push_back(cell.kpss_pvalue);

      if (cell.adf_pass)
        n_adf_pass++;
      if (cell.kpss_pass)
        n_kpss_pass++;
      if (cell.adf_pass && cell.kpss_pass)
        n_both_pass++;
    }
  }

  if (all_adf_pvalues.empty()) {
    step0_stationarity.valid = false;
    return;
  }

  // Compute median p-values
  std::sort(all_adf_pvalues.begin(), all_adf_pvalues.end());
  std::sort(all_kpss_pvalues.begin(), all_kpss_pvalues.end());

  size_t mid = all_adf_pvalues.size() / 2;
  step0_stationarity.adf_pvalue = all_adf_pvalues[mid];
  step0_stationarity.kpss_pvalue = all_kpss_pvalues[mid];

  // Pass if majority passes
  step0_stationarity.adf_pass = (n_adf_pass > n_total / 2);
  step0_stationarity.kpss_pass = (n_kpss_pass > n_total / 2);

  step0_stationarity.valid = true;
}

// ============================================================================
// Build Single Month PSD
// ============================================================================

void TimeSeries::build_psd_month(size_t psd_day_offset,
                                 const std::string &features_dir,
                                 const Feature &feature, const Asset &asset,
                                 const std::string &month) {
  TraceN("BuildPSDMonth");

  if (compute.cancel.load())
    return;

  const int level = feature.selection.selected_level;
  const int primary_idx = feature.selection.primary_feature_idx;
  assert(primary_idx >= 0);
  assert(level >= 0 && level < 3);

  const size_t n_assets = asset.items.size();

  // Get feature metadata for valid_type
  const FeatureMetadata *meta_list = nullptr;
  size_t meta_count = 0;
  if (level == 0) {
    meta_list = feature.metadata.features_l0.data();
    meta_count = feature.metadata.features_l0.size();
  } else if (level == 1) {
    meta_list = feature.metadata.features_l1.data();
    meta_count = feature.metadata.features_l1.size();
  } else {
    meta_list = feature.metadata.features_l2.data();
    meta_count = feature.metadata.features_l2.size();
  }

  // Determine columns to load
  std::vector<size_t> columns = {static_cast<size_t>(primary_idx)};

  L2::ValidType valid_type = L2::ValidType::ALL;
  if (primary_idx >= 0 && static_cast<size_t>(primary_idx) < meta_count) {
    valid_type = meta_list[primary_idx].valid_type;
  }

  // Find valid flag index
  if (valid_type != L2::ValidType::ALL) {
    const char *flag_name =
        (valid_type == L2::ValidType::DEPTH) ? "_depth_valid" : "_data_valid";
    for (size_t i = 0; i < meta_count; ++i) {
      if (std::strcmp(meta_list[i].code, flag_name) == 0) {
        columns.push_back(i);
        break;
      }
    }
  }

  // Batch load entire month
  FeatureReader reader(features_dir);
  std::string year = month.substr(0, 4);
  std::string month_str = month.substr(4, 2);

  FeatureReader::MonthTensor month_tensor;
  {
    TraceN("PreallocateTensor");
    month_tensor.preallocate(n_assets, 31, columns.size(), level);
  }

  {
    TraceN("LoadMonthData");
    reader.load_month_columns(year, month_str, columns, month_tensor);
  }

  if (compute.cancel.load())
    return;

  const size_t A = month_tensor.A;
  assert(A == n_assets);
  const size_t F_selected = columns.size();
  const bool has_valid_flag = (F_selected > 1);
  const size_t n_days = month_tensor.dates.size();

  // PSD 参数 (根据层级设置采样率)
  math::spectral::PSDParams params;
  params.n_fft = math::spectral::N_FFT;
  params.overlap_ratio = 0.2f;
  // 采样率: L0=1Hz(秒级), L1=1/60Hz(分钟级), L2=1/3600Hz(小时级)
  if (level == 0) {
    params.sample_rate = 1.0f;
  } else if (level == 1) {
    params.sample_rate = 1.0f / 60.0f;
  } else {
    params.sample_rate = 1.0f / 3600.0f;
  }

  // 创建 per-thread workspace
  const size_t T_per_day = MAX_ROWS_PER_LEVEL[level];
  math::spectral::PSDDayWorkspace ws;
  ws.init(A, T_per_day);

  // 处理每一天
  for (size_t day_idx = 0; day_idx < n_days; ++day_idx) {
    if (compute.cancel.load())
      return;

    TraceN("ProcessDay");

    const size_t t_start = month_tensor.day_offsets[day_idx];
    const size_t t_end = month_tensor.day_offsets[day_idx + 1];
    const size_t T_day = t_end - t_start;

    // 开始新的一天
    ws.begin_day(A, T_day);

    // 转置数据: [T][F][A] → ws.day_data[A][T]
    // 同时处理 valid flag 和 NaN (前向填充)
    {
      TraceN("TransposeDay");
      for (size_t a = 0; a < A; ++a) {
        float *dest = ws.day_data.data() + a * ws.max_T;
        float last_valid = 0.0f;

        for (size_t t_local = 0; t_local < T_day; ++t_local) {
          const size_t t_global = t_start + t_local;
          const size_t src_base = t_global * F_selected * A;

          float val = static_cast<float>(month_tensor.data[src_base + a]);

          // 检查 valid flag
          if (has_valid_flag) {
            float valid_flag =
                static_cast<float>(month_tensor.data[src_base + A + a]);
            if (valid_flag <= 0.5f) {
              val = last_valid;  // 前向填充
            }
          }

          // 检查 NaN/Inf
          if (val != val || val > 1e38f || val < -1e38f) {
            val = last_valid;  // 前向填充
          } else {
            last_valid = val;
          }

          dest[t_local] = val;
        }
      }
    }

    // 分层 Welch: 先 per-asset 日内 Welch, 再跨 asset 平均
    {
      TraceN("PerAssetWelch");
      for (size_t a = 0; a < A; ++a) {
        ws.begin_asset();  // 清空单资产累加器
        std::span<const float> series = ws.asset_series(a);
        math::spectral::accumulate_asset_psd(series, params, ws);
        math::spectral::finalize_asset_psd(ws, a);
      }
    }

    // 跨 asset 平均并写入 psd_cache
    {
      TraceN("FinalizePSD");
      const size_t global_day_idx = psd_day_offset + day_idx;
      assert(global_day_idx < psd_cache.n_days);

      // 跨资产平均得到当天总 PSD
      math::spectral::average_assets_psd(ws, psd_cache.day_psd(global_day_idx));
      psd_cache.dates[global_day_idx] = month_tensor.dates[day_idx];

      // 拷贝 per-asset PSD 到 psd_cache
      for (size_t a = 0; a < A; ++a) {
        auto src = ws.asset_psd(a);
        float *dst = psd_cache.asset_day_psd(global_day_idx, a);
        std::memcpy(dst, src.data(), src.size_bytes());
      }
    }
  }

  compute.done.fetch_add(1);
}

// ============================================================================
// Build All Months PSD (Parallel)
// ============================================================================

void TimeSeries::build_psd(const std::vector<std::string> &months,
                           const std::string &features_dir,
                           const Feature &feature, const Asset &asset,
                           std::function<void(std::function<void()>)> submit) {
  compute.reset();
  compute.status = Compute::Status::Building;
  compute.total = months.size();

  const int level = feature.selection.selected_level;
  const size_t n_assets = asset.items.size();

  // 计算总天数并预分配 psd_cache
  // 估算: 每月最多31天
  size_t estimated_days = months.size() * 31;

  // 采样率 (与 build_psd_month 一致)
  float sample_rate = 1.0f;
  if (level == 1) {
    sample_rate = 1.0f / 60.0f;
  } else if (level == 2) {
    sample_rate = 1.0f / 3600.0f;
  }

  psd_cache.init(estimated_days, n_assets, sample_rate);

  // Dispatch tasks
  // 按值捕获 offset 和 month (避免局部变量生命周期问题)
  for (size_t i = 0; i < months.size(); ++i) {
    const size_t day_offset = i * 31;
    const std::string month = months[i];
    submit([this, day_offset, month, &features_dir, &feature, &asset]() {
      build_psd_month(day_offset, features_dir, feature, asset, month);
    });
  }
}

// ============================================================================
// Finalize PSD (after build completes)
// ============================================================================

void TimeSeries::finalize_psd() {
  if (psd_cache.data.empty()) {
    step1_frequency.valid = false;
    return;
  }

  constexpr size_t N_FREQS = PSDHeatmap::N_FREQS;

  // ========== 1. 收集有效天索引 ==========
  psd_cache.valid_indices.clear();
  for (size_t d = 0; d < psd_cache.n_days; ++d) {
    if (!psd_cache.dates[d].empty()) {
      psd_cache.valid_indices.push_back(d);
    }
  }

  const size_t valid_days = psd_cache.valid_indices.size();
  if (valid_days == 0) {
    step1_frequency.valid = false;
    return;
  }

  // ========== 2. 转置+log → render_data ==========
  psd_cache.render_data.resize(N_FREQS * valid_days);
  std::vector<float> all_values;
  all_values.reserve(N_FREQS * valid_days);

  for (size_t i = 0; i < valid_days; ++i) {
    const float *src = psd_cache.day_psd(psd_cache.valid_indices[i]);
    for (size_t k = 0; k < N_FREQS; ++k) {
      float val = src[k];
      float log_val = (val > 1e-20f) ? std::log10(val) : -20.0f;
      psd_cache.render_data[k * valid_days + i] = log_val;  // [freq][day]
      all_values.push_back(log_val);
    }
  }

  // ========== 3. 计算 scale_min/max (95% percentile) ==========
  std::sort(all_values.begin(), all_values.end());
  psd_cache.scale_max = all_values[all_values.size() * 95 / 100];
  psd_cache.scale_min = -1.0f;  // 固定 min = -1

  // ========== 4. 生成轴刻度 (频率bin索引 + 周期标签) ==========
  // 轴直接用频率 bin 索引 [0, N_FREQS)，均匀显示
  // 刻度标注对应的周期值
  psd_cache.tick_positions.clear();
  psd_cache.tick_labels.clear();

  auto format_period = [](double period_sec) -> std::string {
    char buf[32];
    if (period_sec >= 86400.0) {
      snprintf(buf, sizeof(buf), "%.0fd", period_sec / 86400.0);
    } else if (period_sec >= 3600.0) {
      snprintf(buf, sizeof(buf), "%.0fh", period_sec / 3600.0);
    } else if (period_sec >= 60.0) {
      snprintf(buf, sizeof(buf), "%.0fm", period_sec / 60.0);
    } else {
      snprintf(buf, sizeof(buf), "%.0fs", period_sec);
    }
    return buf;
  };

  // 候选周期值 (秒)
  double candidate_periods[] = {
      2, 5, 10, 20, 30,
      60, 120, 300, 600, 1200, 1800,
      3600, 7200, 18000, 36000,
      86400, 172800, 432000, 864000, 1728000
  };

  double period_min = 1.0 / psd_cache.nyquist_freq;
  double period_max = 1.0 / psd_cache.freq_resolution;

  for (double p : candidate_periods) {
    if (p >= period_min && p <= period_max) {
      // 周期 p 对应的频率 bin 索引
      double freq = 1.0 / p;
      double bin_idx = freq / psd_cache.freq_resolution;
      psd_cache.tick_positions.push_back(bin_idx);
      psd_cache.tick_labels.push_back(format_period(p));
    }
  }

  // ========== 6. 计算平均功率谱 ==========
  step1_frequency.avg_power_spectrum.resize(N_FREQS);
  std::vector<double> accum(N_FREQS, 0.0);

  for (size_t d : psd_cache.valid_indices) {
    const float *psd = psd_cache.day_psd(d);
    for (size_t k = 0; k < N_FREQS; ++k) {
      accum[k] += psd[k];
    }
  }

  for (size_t k = 0; k < N_FREQS; ++k) {
    step1_frequency.avg_power_spectrum[k] =
        static_cast<float>(accum[k] / valid_days);
  }

  // ========== 7. 峰值/Q因子/低频占比 ==========
  float max_power = 0.0f;
  size_t peak_idx = 0;
  for (size_t k = 1; k < N_FREQS; ++k) {
    if (step1_frequency.avg_power_spectrum[k] > max_power) {
      max_power = step1_frequency.avg_power_spectrum[k];
      peak_idx = k;
    }
  }

  step1_frequency.peak_frequency = peak_idx * psd_cache.freq_resolution;
  step1_frequency.nyquist_freq = psd_cache.nyquist_freq;

  float half_power = max_power / 2.0f;
  size_t low_idx = peak_idx, high_idx = peak_idx;

  for (size_t k = peak_idx; k > 0; --k) {
    if (step1_frequency.avg_power_spectrum[k] < half_power) {
      low_idx = k;
      break;
    }
  }
  for (size_t k = peak_idx; k < N_FREQS; ++k) {
    if (step1_frequency.avg_power_spectrum[k] < half_power) {
      high_idx = k;
      break;
    }
  }

  float bandwidth = (high_idx - low_idx) * psd_cache.freq_resolution;
  step1_frequency.peak_bandwidth = bandwidth;
  step1_frequency.q_factor =
      (bandwidth > 0) ? step1_frequency.peak_frequency / bandwidth : 0.0f;
  step1_frequency.q_factor_pass = (step1_frequency.q_factor < 3.0f);

  size_t low_freq_bins = N_FREQS / 10;
  double low_power = 0.0, total_power = 0.0;
  for (size_t k = 0; k < N_FREQS; ++k) {
    total_power += step1_frequency.avg_power_spectrum[k];
    if (k < low_freq_bins) {
      low_power += step1_frequency.avg_power_spectrum[k];
    }
  }
  step1_frequency.low_freq_power_ratio =
      (total_power > 0) ? static_cast<float>(low_power / total_power) : 0.0f;
  step1_frequency.has_low_freq = (step1_frequency.low_freq_power_ratio > 0.5f);

  step1_frequency.has_peaks = (step1_frequency.q_factor >= 3.0f);
  step1_frequency.n_significant_peaks = step1_frequency.has_peaks ? 1 : 0;

  psd_cache.valid = true;
  step1_frequency.valid = true;
}

