#include "shared/TimeSeries.hpp"
#include "features/backend/FeatureReader.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>

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
    for (size_t a = 0; a < A; ++a) {
      auto &cell = mc.by_asset[a];
      const auto &series = asset_series[a];

      cell.n_samples = series.size();

      // Need minimum samples for meaningful test
      if (series.size() < 30) {
        cell.valid = false;
        continue;
      }

      // ADF test
      auto adf_result = math::stationary::adf_test(series);
      if (adf_result.valid) {
        cell.adf_statistic = adf_result.statistic;
        cell.adf_pvalue = adf_result.pvalue;
        cell.adf_pass = (adf_result.pvalue < 0.05f);
      }

      // KPSS test
      auto kpss_result = math::stationary::kpss_test(series);
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

