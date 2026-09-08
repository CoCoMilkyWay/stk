#include "shared/TimeSeries.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "math/spectral/MultiResPSD.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "math/timeseries/AutoCorrelation.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <span>
#include <thread>

// ============================================================================
// Helper: 构建 day_ranges
// ============================================================================

static void build_day_ranges(TimeSeries::SharedMonthData &shared) {
  shared.day_ranges.clear();

  for (size_t m = 0; m < shared.months.size(); ++m) {
    const auto &tensor = shared.months[m];
    for (size_t d = 0; d < tensor.dates.size(); ++d) {
      TimeSeries::DayRange dr;
      dr.month_idx = m;
      dr.day_in_month = d;
      dr.t_start = tensor.day_start(d);
      // 日步长 = 落盘行数 (含末尾哨兵); 时间轴只到有效行
      dr.t_end = dr.t_start + level_valid_rows(static_cast<size_t>(shared.level));
      dr.date = tensor.dates[d];
      shared.day_ranges.push_back(dr);
    }
  }

  shared.n_days = shared.day_ranges.size();
}

// ============================================================================
// Helper: 获取 feature 元数据和列索引
// ============================================================================

struct FeatureConfig {
  std::vector<size_t> columns;
  const FeatureMetadata *meta_list = nullptr;
  size_t meta_count = 0;
  bool has_valid_flag = false;
  L2::ValidType valid_type = L2::ValidType::ALL;
};

static FeatureConfig get_feature_config(const Feature &feature) {
  FeatureConfig cfg;

  const int level = feature.selection.selected_level;
  const int primary_idx = feature.selection.primary_feature_idx();
  assert(primary_idx >= 0);
  assert(level >= 0 && level < 2);

  cfg.meta_list = feature.metadata.features[level].data();
  cfg.meta_count = feature.metadata.features[level].size();

  cfg.columns = {static_cast<size_t>(primary_idx)};

  L2::ValidType valid_type = L2::ValidType::ALL;
  if (primary_idx >= 0 && static_cast<size_t>(primary_idx) < cfg.meta_count) {
    valid_type = cfg.meta_list[primary_idx].valid_type;
  }
  cfg.valid_type = valid_type;

  // 门控列 = 同层 _meta (按 valid_type 测 data/depth 位, 编码见 Meta.hpp)
  if (valid_type != L2::ValidType::ALL) {
    for (size_t i = 0; i < cfg.meta_count; ++i) {
      if (std::strcmp(cfg.meta_list[i].code, "_meta") == 0) {
        cfg.columns.push_back(i);
        break;
      }
    }
  }

  cfg.has_valid_flag = (cfg.columns.size() > 1);
  return cfg;
}

// ============================================================================
// Helper: 收集单个 asset 的时间序列 (零拷贝访问共享数据)
// ============================================================================

static void collect_asset_series(const TimeSeries::SharedMonthData &shared,
                                 size_t asset_idx,
                                 std::vector<float> &out_series) {
  Trace;
  out_series.clear();
  out_series.reserve(shared.n_days * 14400); // 预估每天最多14400样本

  const size_t A = shared.n_assets;
  const size_t F = shared.F_selected;
  const bool has_valid = shared.has_valid_flag;
  const L2::ValidType vt = shared.valid_type;

  for (const auto &dr : shared.day_ranges) {
    const auto &tensor = shared.months[dr.month_idx];

    for (size_t t = dr.t_start; t < dr.t_end; ++t) {
      const size_t base = t * F * A;
      float val = static_cast<float>(tensor.data[base + asset_idx]);

      if (has_valid) {
        float flag = static_cast<float>(tensor.data[base + A + asset_idx]);
        if (!fmeta::valid(flag, vt))
          continue;
      }

      if (val != val || val > 1e38f || val < -1e38f)
        continue;

      out_series.push_back(val);
    }
  }
}

// ============================================================================
// Stage 0: 平稳性检验 (按月并行)
// ============================================================================

static void compute_stationarity_for_month(
    TimeSeries &ts, size_t month_idx, const TimeSeries::SharedMonthData &shared) {
  TraceN("ComputeStationarityMonth");

  auto &mc = ts.stationarity_cache[month_idx];
  mc.init(shared.n_assets);
  mc.month = shared.months[month_idx].dates.empty()
                 ? ""
                 : shared.months[month_idx].dates[0].substr(0, 6);

  const auto &tensor = shared.months[month_idx];
  const size_t A = shared.n_assets;
  const size_t F = shared.F_selected;
  const bool has_valid = shared.has_valid_flag;
  const L2::ValidType vt = shared.valid_type;

  // 收集每个 asset 的时间序列
  std::vector<std::vector<float>> asset_series(A);
  size_t total_T = tensor.day_start(tensor.dates.size());
  for (auto &v : asset_series)
    v.reserve(total_T);

  for (size_t day_idx = 0; day_idx < tensor.dates.size(); ++day_idx) {
    size_t t_start = tensor.day_start(day_idx);
    // 同上: 哨兵行不进平稳性检验序列
    size_t t_end = t_start + level_valid_rows(static_cast<size_t>(shared.level));

    for (size_t t = t_start; t < t_end; ++t) {
      const size_t base = t * F * A;

      for (size_t a = 0; a < A; ++a) {
        float val = static_cast<float>(tensor.data[base + a]);

        if (has_valid) {
          float flag = static_cast<float>(tensor.data[base + A + a]);
          if (!fmeta::valid(flag, vt))
            continue;
        }

        if (val != val || val > 1e38f || val < -1e38f)
          continue;

        asset_series[a].push_back(val);
      }
    }
  }

  // ADF/KPSS 检验
  math::stationary::ADFWorkspace adf_ws;
  math::stationary::KPSSWorkspace kpss_ws;

  for (size_t a = 0; a < A; ++a) {
    auto &cell = mc.by_asset[a];
    const auto &series = asset_series[a];
    cell.n_samples = series.size();

    if (series.size() < 30) {
      cell.valid = false;
      continue;
    }

    const std::span<const float> s(series.data(), series.size());

    auto adf_result = math::stationary::adf_test(s, 12, adf_ws);
    if (adf_result.valid) {
      cell.adf_statistic = adf_result.statistic;
      cell.adf_pvalue = adf_result.pvalue;
      cell.adf_pass = (adf_result.pvalue < 0.05f);
    }

    auto kpss_result = math::stationary::kpss_test(s, -1, kpss_ws);
    if (kpss_result.valid) {
      cell.kpss_statistic = kpss_result.statistic;
      cell.kpss_pvalue = kpss_result.pvalue;
      cell.kpss_pass = (kpss_result.pvalue > 0.05f);
    }

    cell.valid = adf_result.valid && kpss_result.valid;
  }

  mc.valid = true;
}

// ============================================================================
// Stage 1: PSD 计算 (按 asset 并行)
// ============================================================================

static void compute_psd_for_asset(TimeSeries &ts, size_t asset_idx,
                                  const TimeSeries::SharedMonthData &shared) {
  Trace;
  thread_local math::spectral::MultiResPSDWorkspace<> ws;
  if (!ws.initialized)
    ws.init();

  ws.reset();

  const size_t A = shared.n_assets;
  const size_t F = shared.F_selected;
  const bool has_valid = shared.has_valid_flag;
  const L2::ValidType vt = shared.valid_type;
  const int level = shared.level;

  std::array<float, TimeSeries::PSDHeatmap::N_SCALE_BINS> out_buf;

  for (size_t day_idx = 0; day_idx < shared.n_days; ++day_idx) {
    const auto &dr = shared.day_ranges[day_idx];
    const auto &tensor = shared.months[dr.month_idx];

    for (size_t t = dr.t_start; t < dr.t_end; ++t) {
      const size_t base = t * F * A;
      float val = static_cast<float>(tensor.data[base + asset_idx]);

      if (has_valid) {
        float flag = static_cast<float>(tensor.data[base + A + asset_idx]);
        if (!fmeta::valid(flag, vt))
          continue;
      }

      if (val != val || val > 1e38f || val < -1e38f)
        continue;

      if (level == 0) {
        ws.push_L0(val);
      } else if (level == 1) {
        ws.push_L1(val);
      } else {
        ws.push_L2(val);
      }
    }

    ws.compute_day(out_buf);

    float *dst = ts.psd_cache.asset_day_psd(day_idx, asset_idx);
    std::memcpy(dst, out_buf.data(), sizeof(out_buf));
  }
}

// ============================================================================
// Stage 2: 自相关 ACF/PACF (按 asset 并行)
// ============================================================================

static void compute_acf_for_asset(TimeSeries &ts, size_t asset_idx,
                                  const TimeSeries::SharedMonthData &shared) {
  Trace;
  thread_local std::vector<float> series;
  thread_local math::timeseries::ACFWorkspace ws;

  collect_asset_series(shared, asset_idx, series);

  auto &cell = ts.acf_cache[asset_idx];
  cell.valid = false;

  if (series.size() < 100)
    return;

  constexpr int MAX_LAG = 40;
  auto result = math::timeseries::compute_acf_pacf(
      std::span<const float>(series.data(), series.size()), MAX_LAG, ws);

  if (!result.valid)
    return;

  cell.acf = std::move(result.acf);
  cell.pacf = std::move(result.pacf);
  cell.valid = true;
}

// ============================================================================
// Unified Build Entry
// ============================================================================

void TimeSeries::build_all(const std::vector<std::string> &months,
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
  barriers.reset();
  compute.status = Compute::Status::Loading;
  compute.total = n_assets; // 进度以 assets 为单位

  shared.clear();
  shared.months.resize(n_months);
  shared.n_months = n_months;
  shared.n_assets = n_assets;
  shared.level = feature.selection.selected_level;

  // 获取 feature 配置
  auto cfg = get_feature_config(feature);
  shared.F_selected = cfg.columns.size();
  shared.has_valid_flag = cfg.has_valid_flag;
  shared.valid_type = cfg.valid_type;

  // Worker 数量 = min(core_count, n_months)
  const size_t n_workers = n_months;

  // 预计算每个 worker 负责的范围
  std::vector<WorkerAllocation> allocations(n_workers);
  for (size_t w = 0; w < n_workers; ++w) {
    allocations[w].worker_id = w;
    allocations[w].month_idx = w;
    allocations[w].asset_start = w * n_assets / n_workers;
    allocations[w].asset_end = (w + 1) * n_assets / n_workers;
  }

  // 初始化输出缓存
  stationarity_cache.clear();
  stationarity_cache.resize(n_months);
  for (size_t i = 0; i < n_months; ++i) {
    stationarity_cache[i].month = months[i];
  }

  // 提交 worker 任务
  for (size_t w = 0; w < n_workers; ++w) {
    const auto alloc = allocations[w];
    const std::string month = months[w];
    const auto columns = cfg.columns;
    const int level = shared.level;
    const std::string features_dir_copy = features_dir; // 按值捕获

    submit([this, w, n_workers, month, alloc, columns, level,
            features_dir_copy]() {
      // ========== Phase 1: 加载本 worker 负责的月数据 ==========
      if (compute.cancel.load())
        return;

      {
        TraceN("LoadMonth");
        FeatureRead reader(features_dir_copy);
        std::string year = month.substr(0, 4);
        std::string month_str = month.substr(4, 2);

        auto &tensor = shared.months[w];
        tensor.preallocate(shared.n_assets, 31, columns.size(), level);
        reader.load_month_columns(year, month_str, columns, tensor);
      }

      // Phase 1 完成，等待所有 worker
      barriers.phase1_ready.fetch_add(1);

      while (barriers.phase1_ready.load() < n_workers) {
        if (compute.cancel.load())
          return;
        std::this_thread::yield();
      }

      // ========== Worker 0: 构建共享结构 ==========
      if (w == 0) {
        TraceN("BuildShared");
        build_day_ranges(shared);

        // 初始化所有输出缓存
        psd_cache.init(shared.n_days, shared.n_assets, level);
        for (size_t d = 0; d < shared.n_days; ++d) {
          psd_cache.dates[d] = shared.day_ranges[d].date;
        }

        acf_cache.clear();
        acf_cache.resize(shared.n_assets);

        compute.status = Compute::Status::Building;
        barriers.shared_built.store(true);
      }

      // 等待共享结构构建完成
      while (!barriers.shared_built.load()) {
        if (compute.cancel.load())
          return;
        std::this_thread::yield();
      }

      // ========== Phase 2: 流水线计算 ==========
      if (compute.cancel.load())
        return;

      // Stage 0: 平稳性 (按月)
      {
        TraceN("Stage0_Stationarity");
        compute_stationarity_for_month(*this, alloc.month_idx, shared);
      }

      // Stage 1-2: 按 asset 并行
      for (size_t a = alloc.asset_start; a < alloc.asset_end; ++a) {
        if (compute.cancel.load())
          return;

        {
          TraceN("Stage1_PSD");
          compute_psd_for_asset(*this, a, shared);
        }

        {
          TraceN("Stage2_ACF");
          compute_acf_for_asset(*this, a, shared);
        }

        compute.done.fetch_add(1);
      }
    });
  }
}

// ============================================================================
// Finalize All
// ============================================================================

void TimeSeries::finalize_all() {
  TraceN("FinalizeAll");

  // ========== Step 0: 聚合平稳性结果 ==========
  {
    TraceN("Step0_AggregateStationarity");
    std::vector<float> all_adf_pvalues;
    std::vector<float> all_kpss_pvalues;
    size_t n_total = 0, n_adf_pass = 0, n_kpss_pass = 0;

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
      }
    }

    if (!all_adf_pvalues.empty()) {
      std::sort(all_adf_pvalues.begin(), all_adf_pvalues.end());
      std::sort(all_kpss_pvalues.begin(), all_kpss_pvalues.end());
      size_t mid = all_adf_pvalues.size() / 2;
      step0_stationarity.adf_pvalue = all_adf_pvalues[mid];
      step0_stationarity.kpss_pvalue = all_kpss_pvalues[mid];
      step0_stationarity.adf_pass = (n_adf_pass > n_total / 2);
      step0_stationarity.kpss_pass = (n_kpss_pass > n_total / 2);
      step0_stationarity.valid = true;
    }
  }

  // ========== Step 1: 聚合 PSD 结果 ==========
  {
    TraceN("Step1_AggregatePSD");
    const size_t n_days = psd_cache.n_days;
    const size_t n_assets = psd_cache.n_assets;
    constexpr size_t N_BINS = PSDHeatmap::N_SCALE_BINS;

    if (n_days > 0 && n_assets > 0) {
      // 收集有效天索引
      psd_cache.valid_indices.clear();
      for (size_t d = 0; d < n_days; ++d) {
        if (!psd_cache.dates[d].empty()) {
          psd_cache.valid_indices.push_back(d);
        }
      }

      const size_t valid_days = psd_cache.valid_indices.size();

      if (valid_days > 0) {
        // 找第一个有效天
        psd_cache.first_valid_day = 0;
        for (size_t i = 0; i < valid_days; ++i) {
          const size_t d = psd_cache.valid_indices[i];
          size_t valid_count = 0;
          for (size_t a = 0; a < n_assets; ++a) {
            const float *src = psd_cache.asset_day_psd(d, a);
            for (size_t k = psd_cache.default_y_start; k < N_BINS; ++k) {
              if (src[k] > 0) {
                ++valid_count;
                break;
              }
            }
          }
          if (valid_count > n_assets * 0.8) {
            psd_cache.first_valid_day = i;
            break;
          }
        }

        // 计算 plot_x
        psd_cache.plot_x.resize(N_BINS);
        for (size_t k = 0; k < N_BINS; ++k) {
          psd_cache.plot_x[k] = static_cast<float>(k);
        }

        // 计算 render_data
        psd_cache.render_data.resize(N_BINS * valid_days);
        std::vector<float> day_avg(N_BINS);

        for (size_t i = 0; i < valid_days; ++i) {
          const size_t d = psd_cache.valid_indices[i];
          std::fill(day_avg.begin(), day_avg.end(), 0.0f);
          size_t valid_asset_count = 0;

          for (size_t a = 0; a < n_assets; ++a) {
            const float *src = psd_cache.asset_day_psd(d, a);
            bool has_data = false;
            for (size_t k = 0; k < N_BINS; ++k) {
              if (src[k] > 0) {
                has_data = true;
                break;
              }
            }
            if (has_data) {
              ++valid_asset_count;
              for (size_t k = 0; k < N_BINS; ++k)
                day_avg[k] += src[k];
            }
          }

          if (valid_asset_count > 0) {
            float inv = 1.0f / static_cast<float>(valid_asset_count);
            for (size_t k = 0; k < N_BINS; ++k)
              day_avg[k] *= inv;
          }

          for (size_t k = 0; k < N_BINS; ++k) {
            float val = day_avg[k];
            float log_val = (val > 1e-20f) ? std::log10(val) : -20.0f;
            size_t row = N_BINS - 1 - k;
            psd_cache.render_data[row * valid_days + i] = log_val;
          }
        }

        psd_cache.scale_min = -1.0f;
        psd_cache.scale_max = 3.0f;

        // 轴刻度
        psd_cache.tick_positions.clear();
        psd_cache.tick_labels.clear();
        for (size_t s = 10; s < 60; s += 10) {
          psd_cache.tick_positions.push_back(static_cast<double>(s - 2));
          psd_cache.tick_labels.push_back(std::to_string(s) + "s");
        }
        for (size_t m = 10; m < 60; m += 10) {
          psd_cache.tick_positions.push_back(static_cast<double>(58 + m - 1));
          psd_cache.tick_labels.push_back(std::to_string(m) + "m");
        }
        for (size_t h = 2; h <= 10; h += 2) {
          psd_cache.tick_positions.push_back(static_cast<double>(117 + h - 1));
          psd_cache.tick_labels.push_back(std::to_string(h) + "h");
        }

        psd_cache.selected_day = static_cast<int>(psd_cache.first_valid_day);

        // 平均功率谱
        step1_frequency.avg_power_spectrum.resize(N_BINS);
        std::vector<double> accum(N_BINS, 0.0);

        for (size_t i = 0; i < valid_days; ++i) {
          const size_t d = psd_cache.valid_indices[i];
          std::fill(day_avg.begin(), day_avg.end(), 0.0f);
          size_t valid_asset_count = 0;

          for (size_t a = 0; a < n_assets; ++a) {
            const float *src = psd_cache.asset_day_psd(d, a);
            bool has_data = false;
            for (size_t k = 0; k < N_BINS; ++k) {
              if (src[k] > 0) {
                has_data = true;
                break;
              }
            }
            if (has_data) {
              ++valid_asset_count;
              for (size_t k = 0; k < N_BINS; ++k)
                day_avg[k] += src[k];
            }
          }

          if (valid_asset_count > 0) {
            float inv = 1.0f / static_cast<float>(valid_asset_count);
            for (size_t k = 0; k < N_BINS; ++k)
              accum[k] += day_avg[k] * inv;
          }
        }

        for (size_t k = 0; k < N_BINS; ++k) {
          step1_frequency.avg_power_spectrum[k] =
              static_cast<float>(accum[k] / static_cast<double>(valid_days));
        }

        // 频段能量占比 (在线性能量域计算，保证能量守恒)
        double sec_power = 0, min_power = 0, hour_power = 0, dc_power = 0;
        for (size_t k = 0; k < N_BINS; ++k) {
          float p = step1_frequency.avg_power_spectrum[k];
          if (k < 58)
            sec_power += p; // bin 0-57: 秒级
          else if (k < 117)
            min_power += p; // bin 58-116: 分钟级
          else if (k < 127)
            hour_power += p; // bin 117-126: 小时级
          else
            dc_power += p; // bin 127: DC
        }
        double total_power = sec_power + min_power + hour_power + dc_power;

        if (total_power > 0) {
          step1_frequency.sec_power_ratio = static_cast<float>(sec_power / total_power);
          step1_frequency.min_power_ratio = static_cast<float>(min_power / total_power);
          step1_frequency.hour_power_ratio = static_cast<float>(hour_power / total_power);
          step1_frequency.dc_power_ratio = static_cast<float>(dc_power / total_power);
        }

        psd_cache.valid = true;
        step1_frequency.valid = true;
      }
    }
  }

  // ========== Step 2: 聚合 ACF/PACF (全资产平均) ==========
  {
    TraceN("Step2_AggregateACF");
    std::vector<float> all_acf, all_pacf;
    int max_len = 0;

    for (const auto &cell : acf_cache) {
      if (!cell.valid)
        continue;
      max_len = std::max(max_len, static_cast<int>(cell.acf.size()));
    }

    if (max_len > 0) {
      all_acf.resize(static_cast<size_t>(max_len), 0.0f);
      all_pacf.resize(static_cast<size_t>(max_len), 0.0f);
      size_t count = 0;

      for (const auto &cell : acf_cache) {
        if (!cell.valid)
          continue;
        count++;
        for (size_t k = 0; k < cell.acf.size(); ++k) {
          all_acf[k] += cell.acf[k];
          all_pacf[k] += cell.pacf[k];
        }
      }

      if (count > 0) {
        float inv = 1.0f / static_cast<float>(count);
        for (size_t k = 0; k < all_acf.size(); ++k) {
          all_acf[k] *= inv;
          all_pacf[k] *= inv;
        }

        step2_acf.acf_values = std::move(all_acf);
        step2_acf.pacf_values = std::move(all_pacf);
        step2_acf.max_lag = max_len - 1;

        // 置信带: 理论值 1.96/sqrt(n) 对大数据集太严格, 设实用下限 0.05 (5% 相关性)
        constexpr float PRACTICAL_MIN_THRESHOLD = 0.05f;
        size_t n_samples = shared.n_days > 0 ? shared.n_days * 14400 : 1;
        float theoretical = 1.96f / std::sqrt(static_cast<float>(n_samples));
        step2_acf.confidence_bound = std::max(theoretical, PRACTICAL_MIN_THRESHOLD);

        step2_acf.valid = true;
      }
    }
  }

  // 释放共享数据
  shared.clear();
  compute.status = Compute::Status::Done;
}
