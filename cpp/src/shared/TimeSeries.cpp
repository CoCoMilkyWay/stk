#include "shared/TimeSeries.hpp"
#include "features/backend/FeatureReader.hpp"
#include "math/spectral/MultiResPSD.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <span>
#include <thread>

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
// Build PSD - 单次提交，自动两阶段
// ============================================================================

// 构建连续时间的 day_ranges
static void build_day_ranges_internal(TimeSeries::AllMonthsData &temp_months) {
  temp_months.day_ranges.clear();

  for (size_t m = 0; m < temp_months.months.size(); ++m) {
    const auto &tensor = temp_months.months[m];
    for (size_t d = 0; d < tensor.dates.size(); ++d) {
      TimeSeries::DayRange dr;
      dr.month_idx = m;
      dr.day_in_month = d;
      dr.t_start = tensor.day_offsets[d];
      dr.t_end = tensor.day_offsets[d + 1];
      dr.date = tensor.dates[d];
      temp_months.day_ranges.push_back(dr);
    }
  }
}

void TimeSeries::build_psd(const std::vector<std::string> &months,
                           const std::string &features_dir,
                           const Feature &feature, const Asset &asset,
                           std::function<void(std::function<void()>)> submit) {
  const size_t n_months = months.size();
  const size_t n_assets = asset.items.size();

  if (n_months == 0 || n_assets == 0) {
    compute.status = Compute::Status::Done;
    return;
  }

  // 初始化
  compute.reset();
  compute.status = Compute::Status::Loading;
  compute.total = n_assets;  // 最终进度以 assets 为单位

  temp_months.clear();
  temp_months.months.resize(n_months);

  // 决定 worker 数量 = n_months (每个 worker 负责一个月的加载 + 一批 assets 的计算)
  const size_t n_workers = n_months;
  const int level = feature.selection.selected_level;

  // 预计算每个 worker 负责的 asset 范围
  std::vector<size_t> asset_starts(n_workers + 1);
  for (size_t w = 0; w <= n_workers; ++w) {
    asset_starts[w] = w * n_assets / n_workers;
  }

  // Barrier: 用于等待所有 worker 完成 Phase 1
  auto phase1_ready = std::make_shared<std::atomic<size_t>>(0);

  // 共享状态: day_ranges 构建完成标志
  auto day_ranges_built = std::make_shared<std::atomic<bool>>(false);

  // 预分配 psd_cache (只由 worker 0 初始化)
  // 估算总天数
  const size_t estimated_days = n_months * 31;

  for (size_t w = 0; w < n_workers; ++w) {
    const std::string month = months[w];
    const size_t a_start = asset_starts[w];
    const size_t a_end = asset_starts[w + 1];

    submit([this, w, n_workers, month, a_start, a_end, n_assets, estimated_days, level,
            phase1_ready, day_ranges_built,
            &features_dir, &feature, &asset]() {
      // ========== Phase 1: 加载本 worker 负责的月数据 ==========
      if (compute.cancel.load()) return;
      {
        TraceN("LoadMonth");
        const int primary_idx = feature.selection.primary_feature_idx;

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

        std::vector<size_t> columns = {static_cast<size_t>(primary_idx)};
        L2::ValidType valid_type = L2::ValidType::ALL;
        if (primary_idx >= 0 && static_cast<size_t>(primary_idx) < meta_count) {
          valid_type = meta_list[primary_idx].valid_type;
        }
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

        FeatureReader reader(features_dir);
        std::string year = month.substr(0, 4);
        std::string month_str = month.substr(4, 2);

        auto &tensor = temp_months.months[w];
        tensor.preallocate(n_assets, 31, columns.size(), level);
        reader.load_month_columns(year, month_str, columns, tensor);
      }

      // 标记 Phase 1 完成，等待所有 worker
      size_t ready_count = phase1_ready->fetch_add(1) + 1;

      // Barrier: spin-wait 直到所有 worker 都 ready
      while (phase1_ready->load() < n_workers) {
        if (compute.cancel.load()) return;
        std::this_thread::yield();
      }

      // ========== Worker 0 负责构建 day_ranges 和初始化 psd_cache ==========
      if (w == 0) {
        TraceN("BuildDayRanges");
        build_day_ranges_internal(temp_months);

        const size_t n_days = temp_months.total_days();
        psd_cache.init(n_days, n_assets, level);

        for (size_t d = 0; d < n_days; ++d) {
          psd_cache.dates[d] = temp_months.day_ranges[d].date;
        }

        compute.status = Compute::Status::Building;
        day_ranges_built->store(true);
      }

      // 等待 day_ranges 构建完成
      while (!day_ranges_built->load()) {
        if (compute.cancel.load()) return;
        std::this_thread::yield();
      }

      // ========== Phase 2: 计算本 worker 负责的 assets ==========
      if (compute.cancel.load()) return;
      {
        TraceN("ComputeAssets");
        const size_t n_days = temp_months.total_days();

        // 检查是否有 valid flag 列
        bool has_valid_flag = false;
        size_t F_selected = 1;
        if (!temp_months.months.empty()) {
          const auto &first = temp_months.months[0];
          F_selected = first.feature_indices.size();
          has_valid_flag = (F_selected > 1);
        }

        thread_local math::spectral::MultiResPSDWorkspace ws;
        if (!ws.initialized) ws.init();

        std::array<float, PSDHeatmap::N_SCALE_BINS> out_buf;

        for (size_t a = a_start; a < a_end; ++a) {
          if (compute.cancel.load()) return;

          ws.reset();

          for (size_t day_idx = 0; day_idx < n_days; ++day_idx) {
            const auto &dr = temp_months.day_ranges[day_idx];
            const auto &tensor = temp_months.months[dr.month_idx];
            const size_t A = tensor.A;

            for (size_t t = dr.t_start; t < dr.t_end; ++t) {
              const size_t src_base = t * F_selected * A;
              float val = static_cast<float>(tensor.data[src_base + a]);

              if (has_valid_flag) {
                float valid_flag = static_cast<float>(tensor.data[src_base + A + a]);
                if (valid_flag <= 0.5f) continue;
              }

              if (val != val || val > 1e38f || val < -1e38f) continue;

              if (level == 0) {
                ws.push_L0(val);
              } else if (level == 1) {
                ws.push_L1(val);
              } else {
                ws.push_L2(val);
              }
            }

            ws.compute_day(out_buf);

            float *dst = psd_cache.asset_day_psd(day_idx, a);
            std::memcpy(dst, out_buf.data(), sizeof(out_buf));
          }

          compute.done.fetch_add(1);
        }
      }
    });
  }
}

// ============================================================================
// Finalize PSD (UI线程调用)
// ============================================================================

void TimeSeries::finalize_psd() {
  // Phase 2 已经填充了 per_asset_data，现在做渲染数据准备

  const size_t n_days = psd_cache.n_days;
  const size_t n_assets = psd_cache.n_assets;

  if (n_days == 0 || n_assets == 0) {
    step1_frequency.valid = false;
    psd_cache.valid = false;
    temp_months.clear();
    return;
  }

  constexpr size_t N_BINS = PSDHeatmap::N_SCALE_BINS;

  // ========== 1. 收集有效天索引 ==========
  psd_cache.valid_indices.clear();
  for (size_t d = 0; d < n_days; ++d) {
    if (!psd_cache.dates[d].empty()) {
      psd_cache.valid_indices.push_back(d);
    }
  }

  const size_t valid_days = psd_cache.valid_indices.size();
  if (valid_days == 0) {
    step1_frequency.valid = false;
    psd_cache.valid = false;
    temp_months.clear();
    return;
  }

  // ========== 2. 找到第一个FFT有效的天 (default_y_start范围内有数据) ==========
  psd_cache.first_valid_day = 0;
  for (size_t i = 0; i < valid_days; ++i) {
    const size_t d = psd_cache.valid_indices[i];
    bool day_has_data = false;
    for (size_t a = 0; a < n_assets && !day_has_data; ++a) {
      const float *src = psd_cache.asset_day_psd(d, a);
      for (size_t k = psd_cache.default_y_start; k < N_BINS; ++k) {
        if (src[k] > 0) { day_has_data = true; break; }
      }
    }
    if (day_has_data) {
      psd_cache.first_valid_day = i;
      break;
    }
  }

  // ========== 3. 计算 plot_x ==========
  psd_cache.plot_x.resize(N_BINS);
  for (size_t k = 0; k < N_BINS; ++k) {
    psd_cache.plot_x[k] = static_cast<float>(k);
  }

  // ========== 4. 计算跨资产平均并转换为 render_data ==========
  // render_data 布局: [N_BINS * valid_days]，row-major
  // PlotHeatmap 把 row 0 画在顶部，所以需要反转行顺序：
  //   row 0 存储 bin 127 (DC) -> 画在顶部
  //   row 127 存储 bin 0 (高频) -> 画在底部
  // 这样刻度 Y=k+0.5 就能正确对应 bin k 的数据
  psd_cache.render_data.resize(N_BINS * valid_days);
  std::vector<float> all_values;
  all_values.reserve(N_BINS * valid_days);

  // 临时buffer存储跨资产平均
  std::vector<float> day_avg(N_BINS);

  for (size_t i = 0; i < valid_days; ++i) {
    const size_t d = psd_cache.valid_indices[i];

    // 计算跨资产平均
    std::fill(day_avg.begin(), day_avg.end(), 0.0f);
    size_t valid_asset_count = 0;

    for (size_t a = 0; a < n_assets; ++a) {
      const float *src = psd_cache.asset_day_psd(d, a);

      // 检查是否有数据
      bool has_data = false;
      for (size_t k = 0; k < N_BINS; ++k) {
        if (src[k] > 0) { has_data = true; break; }
      }

      if (has_data) {
        ++valid_asset_count;
        for (size_t k = 0; k < N_BINS; ++k) {
          day_avg[k] += src[k];
        }
      }
    }

    if (valid_asset_count > 0) {
      float inv = 1.0f / valid_asset_count;
      for (size_t k = 0; k < N_BINS; ++k) {
        day_avg[k] *= inv;
      }
    }

    // log变换并存入 render_data (反转行顺序)
    for (size_t k = 0; k < N_BINS; ++k) {
      float val = day_avg[k];
      float log_val = (val > 1e-20f) ? std::log10(val) : -20.0f;
      // bin k -> row (N_BINS - 1 - k)
      size_t row = N_BINS - 1 - k;
      psd_cache.render_data[row * valid_days + i] = log_val;
      all_values.push_back(log_val);
    }
  }

  // ========== 5. 计算 scale_min/max ==========
  std::sort(all_values.begin(), all_values.end());
  psd_cache.scale_max = all_values[all_values.size() * 95 / 100];
  psd_cache.scale_min = -1.0f;

  // ========== 6. 生成轴刻度 (位置是bin索引，不加0.5) ==========
  psd_cache.tick_positions.clear();
  psd_cache.tick_labels.clear();

  // 秒级: idx 0-57 → 2s-59s
  for (size_t s = 10; s < 60; s += 10) {
    size_t idx = s - 2;
    psd_cache.tick_positions.push_back(static_cast<double>(idx));
    psd_cache.tick_labels.push_back(std::to_string(s) + "s");
  }

  // 分钟级: idx 58-116 → 1min-59min
  for (size_t m = 10; m < 60; m += 10) {
    size_t idx = 58 + m - 1;
    psd_cache.tick_positions.push_back(static_cast<double>(idx));
    psd_cache.tick_labels.push_back(std::to_string(m) + "m");
  }

  // 小时级: idx 117-126 → 1h-10h
  for (size_t h = 2; h <= 10; h += 2) {
    size_t idx = 117 + h - 1;
    psd_cache.tick_positions.push_back(static_cast<double>(idx));
    psd_cache.tick_labels.push_back(std::to_string(h) + "h");
  }

  psd_cache.selected_day = static_cast<int>(psd_cache.first_valid_day);

  // ========== 6. 计算平均功率谱 ==========
  step1_frequency.avg_power_spectrum.resize(N_BINS);
  std::vector<double> accum(N_BINS, 0.0);

  for (size_t i = 0; i < valid_days; ++i) {
    const size_t d = psd_cache.valid_indices[i];

    // 重新计算跨资产平均 (或者复用 render_data 但需要还原 log)
    std::fill(day_avg.begin(), day_avg.end(), 0.0f);
    size_t valid_asset_count = 0;

    for (size_t a = 0; a < n_assets; ++a) {
      const float *src = psd_cache.asset_day_psd(d, a);
      bool has_data = false;
      for (size_t k = 0; k < N_BINS; ++k) {
        if (src[k] > 0) { has_data = true; break; }
      }
      if (has_data) {
        ++valid_asset_count;
        for (size_t k = 0; k < N_BINS; ++k) {
          day_avg[k] += src[k];
        }
      }
    }

    if (valid_asset_count > 0) {
      float inv = 1.0f / valid_asset_count;
      for (size_t k = 0; k < N_BINS; ++k) {
        accum[k] += day_avg[k] * inv;
      }
    }
  }

  for (size_t k = 0; k < N_BINS; ++k) {
    step1_frequency.avg_power_spectrum[k] =
        static_cast<float>(accum[k] / valid_days);
  }

  // ========== 7. 计算各频段能量占比 ==========
  // bin 0-57: 秒级 (2s-59s)
  // bin 58-116: 分钟级 (1min-59min)
  // bin 117-127: 小时级 (1h-10h + DC)
  double sec_power = 0, min_power = 0, hour_power = 0, total_power = 0;
  for (size_t k = 0; k < N_BINS; ++k) {
    float p = step1_frequency.avg_power_spectrum[k];
    total_power += p;
    if (k < 58) {
      sec_power += p;
    } else if (k < 117) {
      min_power += p;
    } else {
      hour_power += p;  // 包括 DC (bin 127)
    }
  }

  if (total_power > 0) {
    step1_frequency.low_freq_power_ratio = static_cast<float>(sec_power / total_power);
    step1_frequency.mid_freq_power_ratio = static_cast<float>(min_power / total_power);
    step1_frequency.high_freq_power_ratio = static_cast<float>(hour_power / total_power);
  }

  psd_cache.valid = true;
  step1_frequency.valid = true;

  // ========== Phase 3: 释放临时数据 ==========
  temp_months.clear();
}
