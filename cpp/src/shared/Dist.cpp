#include "shared/Dist.hpp"
#include "features/FeaturesDefine.hpp"
#include "features/backend/FeatureReader.hpp"
#include "math/cluster/Ward1D.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <array>
#include <cassert>

// ============================================================================
// Helper: Date parsing
// ============================================================================

namespace {

// Parse "YYYYMMDD" -> (year, month, day)
std::tuple<uint16_t, uint8_t, uint8_t> parse_date(const std::string &date) {
  assert(date.size() == 8);
  uint16_t year = std::stoi(date.substr(0, 4));
  uint8_t month = std::stoi(date.substr(4, 2));
  uint8_t day = std::stoi(date.substr(6, 2));
  return {year, month, day};
}

// Zeller's congruence: weekday (Mon=0, Sun=6)
uint8_t calc_weekday(uint16_t y, uint8_t m, uint8_t d) {
  if (m < 3) {
    m += 12;
    y -= 1;
  }
  int q = d, M = m, K = y % 100, J = y / 100;
  int h = (q + (13 * (M + 1)) / 5 + K + K / 4 + J / 4 - 2 * J) % 7;
  return static_cast<uint8_t>((h + 5) % 7);
}

} // namespace

// ============================================================================
// Build Single Month
// ============================================================================

void Dist::build_month(size_t cache_idx, const std::string &features_dir,
                       const Feature &feature, const Asset &asset) {
  TraceN("BuildMonth");

  assert(cache_idx < cache.size());
  auto &mc = cache[cache_idx];

  if (compute.cancel.load())
    return;

  const int level = feature.selection.selected_level;
  const int primary_idx = feature.selection.primary_feature_idx;
  assert(primary_idx >= 0);
  assert(level >= 0 && level < 3);

  const size_t n_assets = asset.items.size();
  mc.init(n_assets);

  // Get feature metadata for valid_type (constexpr branch elimination)
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
    const char *flag_name = (valid_type == L2::ValidType::DEPTH) ? "_depth_valid" : "_data_valid";
    for (size_t i = 0; i < meta_count; ++i) {
      if (std::strcmp(meta_list[i].code, flag_name) == 0) {
        columns.push_back(i);
        break;
      }
    }
  }

  // Batch load entire month (columnar compressed format)
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

  const size_t A = month_tensor.A;
  assert(A == n_assets);
  const size_t F_selected = columns.size();
  const bool has_valid_flag = (F_selected > 1);

  // Pre-allocate sample buffers
  size_t total_T = month_tensor.day_offsets.back();
  std::vector<float> month_samples;
  std::vector<std::vector<float>> asset_samples(A);
  std::vector<std::vector<float>> hour_samples(24);
  std::vector<std::vector<float>> weekday_samples(7);

  {
    TraceN("PreallocateSamples");
    month_samples.reserve(total_T * A);
    for (auto &v : asset_samples)
      v.reserve(total_T);
    for (auto &v : hour_samples)
      v.reserve((total_T * A) / 24);
    for (auto &v : weekday_samples)
      v.reserve((total_T * A) / 7);
  }

  // Process entire month (zero-copy pointers into month_tensor.data)
  {
    TraceN("ProcessSamples");
    for (size_t day_idx = 0; day_idx < month_tensor.dates.size(); ++day_idx) {
      TraceN("ProcessDay");
      if (compute.cancel.load())
        return;

      // Parse date for weekday (cached)
      auto [year_val, month_val, day] = parse_date(month_tensor.dates[day_idx]);
      uint8_t weekday = calc_weekday(year_val, month_val, day);

      size_t t_start = month_tensor.day_offsets[day_idx];
      size_t t_end = month_tensor.day_offsets[day_idx + 1];

      for (size_t t = t_start; t < t_end; ++t) {
        // Convert time index to clock hour (按小时整数边界分配)
        uint8_t hour;
        if (level == 0) {
          // Level 0 (tick/second): use index2tick
          ClockTime time = index2tick(t - t_start);
          hour = time.hour;
        } else if (level == 1) {
          // Level 1 (minute): use index2minute
          ClockTime time = index2minute(t - t_start);
          hour = time.hour;
        } else {
          // Level 2 (hour): use index2hour (returns hour directly)
          hour = index2hour(t - t_start);
        }

        // Zero-copy pointers into month tensor
        const feature_storage_t *values = &month_tensor.data[t * F_selected * A];
        const feature_storage_t *valid_flags = has_valid_flag ? &month_tensor.data[t * F_selected * A + A] : nullptr;

        for (size_t a = 0; a < A; ++a) {
          float val = static_cast<float>(values[a]);
          mc.integrity.n_total++;

          // Check valid flag
          if (valid_flags && static_cast<float>(valid_flags[a]) <= 0.5f)
            continue;

          // Check NaN/Inf (branchless where possible)
          if (val != val) {
            mc.integrity.n_nan++;
            continue;
          }
          if (val > 1e38f) {
            mc.integrity.n_pos_inf++;
            continue;
          }
          if (val < -1e38f) {
            mc.integrity.n_neg_inf++;
            continue;
          }

          // Count zero
          if (val == 0.0f)
            mc.integrity.n_zero++;

          // Accumulate valid samples
          mc.integrity.n_valid++;
          month_samples.push_back(val);
          asset_samples[a].push_back(val);
          hour_samples[hour].push_back(val);
          weekday_samples[weekday].push_back(val);
        }
      }
    }
  }

  // Batch insert once per month (amortized allocation)
  {
    TraceN("BuildDist");
    {
      TraceN("AddTotal");
      if (!month_samples.empty())
        mc.total.addBatch(month_samples);
    }

    {
      TraceN("AddByAsset");
      for (size_t a = 0; a < A; ++a) {
        if (!asset_samples[a].empty()) {
          TraceN("AddAsset");
          mc.by_asset[a].addBatch(asset_samples[a]);
        }
      }
    }

    {
      TraceN("AddByHour");
      for (size_t h = 0; h < 24; ++h) {
        if (!hour_samples[h].empty()) {
          TraceN("AddHour");
          mc.by_hour[h].addBatch(hour_samples[h]);
        }
      }
    }

    {
      TraceN("AddByWeekday");
      for (size_t wd = 0; wd < 7; ++wd) {
        if (!weekday_samples[wd].empty()) {
          TraceN("AddWeekday");
          mc.by_weekday[wd].addBatch(weekday_samples[wd]);
        }
      }
    }
  }

  mc.valid = true;
}

// ============================================================================
// Build All Months (Parallel)
// ============================================================================

void Dist::build_all(const std::vector<std::string> &months,
                     const std::string &features_dir, const Feature &feature,
                     const Asset &asset,
                     std::function<void(std::function<void()>)> submit) {
  compute.reset();
  compute.status = Compute::Status::Building;
  compute.total = months.size();

  // Initialize cache
  cache.clear();
  cache.resize(months.size());
  for (size_t i = 0; i < months.size(); ++i) {
    cache[i].month = months[i];
  }

  // Dispatch tasks
  for (size_t i = 0; i < months.size(); ++i) {
    submit([this, i, &features_dir, &feature, &asset]() {
      build_month(i, features_dir, feature, asset);
      compute.done.fetch_add(1);
    });
  }
}

// ============================================================================
// Query
// ============================================================================

void Dist::query(Input::GroupBy group_by) {
  const size_t min_samples = kMinSamples;
  result.clear();
  result.integrity.clear();

  if (cache.empty()) {
    result.valid = true;
    return;
  }

  // Aggregate integrity
  for (const auto &mc : cache) {
    result.integrity.add(mc.integrity);
  }

  switch (group_by) {
  case Input::GroupBy::MONTH: {
    // Each month as a bin - pointers directly to cache (already persistent)
    for (const auto &mc : cache) {
      if (mc.total.count() >= min_samples) {
        result.bins.emplace_back();
        result.bins.back().key = mc.month;
        result.bins.back().extract_from(mc.total);
      }
    }
    break;
  }

  case Input::GroupBy::WEEKDAY: {
    // Merge by weekday across all months, store in kll_storage
    result.kll_storage.reserve(7);
    for (size_t i = 0; i < 7; ++i) {
      result.kll_storage.emplace_back(KLL_CAPACITY);
    }
    const char *wd_names[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
    for (const auto &mc : cache) {
      for (size_t wd = 0; wd < mc.by_weekday.size() && wd < 7; ++wd) {
        result.kll_storage[wd].merge(mc.by_weekday[wd]);
      }
    }
    for (size_t wd = 0; wd < 7; ++wd) {
      if (result.kll_storage[wd].count() >= min_samples) {
        result.bins.emplace_back();
        result.bins.back().key = std::string("weekday_") + wd_names[wd];
        result.bins.back().extract_from(result.kll_storage[wd]);
      }
    }
    break;
  }

  case Input::GroupBy::HOUR: {
    // Merge by hour across all months, store in kll_storage
    result.kll_storage.reserve(24);
    for (size_t i = 0; i < 24; ++i) {
      result.kll_storage.emplace_back(KLL_CAPACITY);
    }
    for (const auto &mc : cache) {
      for (size_t h = 0; h < mc.by_hour.size() && h < 24; ++h) {
        result.kll_storage[h].merge(mc.by_hour[h]);
      }
    }
    for (size_t h = 0; h < 24; ++h) {
      if (result.kll_storage[h].count() >= min_samples) {
        result.bins.emplace_back();
        result.bins.back().key = "hour_" + std::to_string(h);
        result.bins.back().extract_from(result.kll_storage[h]);
      }
    }
    break;
  }

  case Input::GroupBy::ASSETS: {
    // Per-asset statistics from global aggregation
    for (size_t a = 0; a < global_by_asset.size(); ++a) {
      if (global_by_asset[a].count() >= min_samples) {
        result.bins.emplace_back();
        result.bins.back().key = "asset_" + std::to_string(a);
        result.bins.back().extract_from(global_by_asset[a]);
      }
    }
    break;
  }
  }

  result.valid = true;
}

// ============================================================================
// Finalize (after build_all completes)
// ============================================================================

void Dist::finalize() {
  if (cache.empty())
    return;

  // 1. Build global hour/weekday aggregations
  {
    global_by_hour.clear();
    global_by_hour.reserve(24);
    for (size_t h = 0; h < 24; ++h) {
      global_by_hour.emplace_back(KLL_CAPACITY);
    }

    global_by_weekday.clear();
    global_by_weekday.reserve(7);
    for (size_t wd = 0; wd < 7; ++wd) {
      global_by_weekday.emplace_back(KLL_CAPACITY);
    }

    for (const auto &mc : cache) {
      if (!mc.valid)
        continue;

      for (size_t h = 0; h < mc.by_hour.size() && h < 24; ++h) {
        if (mc.by_hour[h].count() > 0) {
          global_by_hour[h].merge(mc.by_hour[h]);
        }
      }

      for (size_t wd = 0; wd < mc.by_weekday.size() && wd < 7; ++wd) {
        if (mc.by_weekday[wd].count() > 0) {
          global_by_weekday[wd].merge(mc.by_weekday[wd]);
        }
      }
    }
  }

  // 2. Build global asset aggregations
  const size_t n_assets = cache[0].n_assets;
  {
    global_by_asset.clear();
    global_by_asset.reserve(n_assets);
    for (size_t a = 0; a < n_assets; ++a) {
      global_by_asset.emplace_back(KLL_CAPACITY);
    }

    for (const auto &mc : cache) {
      if (!mc.valid)
        continue;
      for (size_t a = 0; a < mc.by_asset.size() && a < n_assets; ++a) {
        if (mc.by_asset[a].count() > 0) {
          global_by_asset[a].merge(mc.by_asset[a]);
        }
      }
    }
  }

  // 3. Build global_total (merge weekday KLLs - only 7 to merge)
  {
    global_total.clear();
    for (const auto &kll : global_by_weekday) {
      if (kll.count() > 0) {
        global_total.merge(kll);
      }
    }
  }

  // 4. Build stability visualization (only assets with count >= 1000)
  {
    stability.clear();
    constexpr size_t MIN_ASSET_SAMPLES = 1000;

    // Collect valid asset indices
    std::vector<size_t> valid_idx;
    for (size_t a = 0; a < n_assets; ++a) {
      if (global_by_asset[a].count() >= MIN_ASSET_SAMPLES) {
        valid_idx.push_back(a);
      }
    }

    const size_t n_valid = valid_idx.size();
    if (global_total.count() > kMinSamples && n_valid > 1) {
      // Get 19 percentile x-values from global distribution
      constexpr int N_PERCENTILES = 19;
      std::array<float, N_PERCENTILES> ref_x;
      for (int i = 0; i < N_PERCENTILES; ++i) {
        ref_x[i] = static_cast<float>(global_total.quantile(0.05 * (i + 1)));
      }

      // Build 19×n_valid offset matrix
      std::vector<std::vector<float>> offsets(N_PERCENTILES);
      for (int d = 0; d < N_PERCENTILES; ++d) {
        offsets[d].resize(n_valid);
      }

      for (size_t i = 0; i < n_valid; ++i) {
        size_t a = valid_idx[i];
        for (int d = 0; d < N_PERCENTILES; ++d) {
          float cdf_val = static_cast<float>(global_by_asset[a].queryCDF(ref_x[d]));
          float expected = 0.05f * (d + 1);
          offsets[d][i] = cdf_val - expected;
        }
      }

      // Signed square sum for x position
      std::vector<float> scores(n_valid);
      for (size_t i = 0; i < n_valid; ++i) {
        float sum = 0.0f;
        for (int d = 0; d < N_PERCENTILES; ++d) {
          float off = offsets[d][i];
          sum += (off >= 0 ? 1.0f : -1.0f) * off * off;
        }
        scores[i] = sum;
      }

      // Normalize to [0,1] symmetric around zero: [-M, M] -> [0, 1]
      float s_min = scores[0], s_max = scores[0];
      for (float s : scores) {
        if (s < s_min) s_min = s;
        if (s > s_max) s_max = s;
      }
      float M = std::max(std::abs(s_min), std::abs(s_max));
      if (M < 1e-9f) M = 1.0f;

      stability.asset_idx = valid_idx;
      stability.score_min = s_min;
      stability.score_max = s_max;
      stability.x_norm.resize(n_valid);
      for (size_t i = 0; i < n_valid; ++i) {
        stability.x_norm[i] = (scores[i] + M) / (2.0f * M); // 0 maps to 0.5
      }

      // Ward clustering for color
      std::vector<int> leaf_order = ward_leaf_order(offsets);

      // leaf_order[pos] = index in valid array, map to color
      stability.color_t.resize(n_valid);
      for (size_t pos = 0; pos < n_valid; ++pos) {
        int idx = leaf_order[pos];
        stability.color_t[idx] = static_cast<float>(pos) / (n_valid - 1);
      }

      stability.valid = true;
    }
  }

  // 5. Query with MONTH grouping (default)
  query(Input::GroupBy::MONTH);
}
