#include "shared/Dist.hpp"
#include "features/backend/FeatureReader.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"

#include <cassert>
#include <cmath>

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

  L2::ValidType valid_type = L2::ValidType::ALL;
  if (primary_idx >= 0 && static_cast<size_t>(primary_idx) < meta_count) {
    valid_type = meta_list[primary_idx].valid_type;
  }

  // Find all dates in this month
  FeatureReader reader(features_dir);
  std::string year = mc.month.substr(0, 4);
  std::string month_str = mc.month.substr(4, 2);
  auto month_dates = reader.list_dates(year, month_str);

  // Process each date
  for (const auto &date : month_dates) {
    if (compute.cancel.load())
      return;

    // Parse date for hour/weekday
    auto [year, month_val, day] = parse_date(date);
    uint8_t weekday = calc_weekday(year, month_val, day);

    // Load tensor
    FeatureReader::DayTensor tensor;
    if (!reader.load_day_level(date, level, tensor)) {
      continue;
    }

    const size_t T = tensor.T[level];
    const size_t F = tensor.F[level];
    const size_t A = tensor.A;
    assert(A == n_assets);

    if (static_cast<size_t>(primary_idx) >= F) {
      continue;
    }

    // Find valid flag index
    int valid_flag_idx = -1;
    if (valid_type == L2::ValidType::DEPTH) {
      for (size_t i = 0; i < meta_count; ++i) {
        if (std::string(meta_list[i].code) == "_depth_valid") {
          valid_flag_idx = static_cast<int>(i);
          break;
        }
      }
    } else if (valid_type == L2::ValidType::DATA) {
      for (size_t i = 0; i < meta_count; ++i) {
        if (std::string(meta_list[i].code) == "_data_valid") {
          valid_flag_idx = static_cast<int>(i);
          break;
        }
      }
    }

    // Accumulate samples per day
    std::vector<float> day_samples;
    std::vector<std::vector<float>> asset_samples(A);
    std::vector<std::vector<float>> hour_samples(24);
    std::vector<std::vector<float>> weekday_samples(7);

    day_samples.reserve(T * A);
    for (auto &v : asset_samples) v.reserve(T);
    for (auto &v : hour_samples) v.reserve((T * A) / 24);
    for (auto &v : weekday_samples) v.reserve((T * A) / 7);

    // Process all (t, a) samples
    for (size_t t = 0; t < T; ++t) {
      uint8_t hour = static_cast<uint8_t>(t % 24);

      // Get feature values
      const feature_storage_t *values = nullptr;
      if (level == 0) {
        values = tensor.get_all_assets<0>(t, primary_idx);
      } else if (level == 1) {
        values = tensor.get_all_assets<1>(t, primary_idx);
      } else {
        values = tensor.get_all_assets<2>(t, primary_idx);
      }

      // Get valid flags
      const feature_storage_t *valid_flags = nullptr;
      if (valid_flag_idx >= 0) {
        if (level == 0) {
          valid_flags = tensor.get_all_assets<0>(t, valid_flag_idx);
        } else if (level == 1) {
          valid_flags = tensor.get_all_assets<1>(t, valid_flag_idx);
        } else {
          valid_flags = tensor.get_all_assets<2>(t, valid_flag_idx);
        }
      }

      for (size_t a = 0; a < A; ++a) {
        float val = static_cast<float>(values[a]);
        mc.integrity.n_total++;

        // Check valid flag
        if (valid_flags) {
          float vf = static_cast<float>(valid_flags[a]);
          if (vf <= 0.5f) {
            continue; // Skip invalid
          }
        }

        // Check NaN
        if (val != val) {
          mc.integrity.n_nan++;
          continue;
        }

        // Check +Inf
        if (val > 1e38f) {
          mc.integrity.n_pos_inf++;
          continue;
        }

        // Check -Inf
        if (val < -1e38f) {
          mc.integrity.n_neg_inf++;
          continue;
        }

        // Count zero
        if (val == 0.0f) {
          mc.integrity.n_zero++;
        }

        // Accumulate valid samples
        mc.integrity.n_valid++;
        day_samples.push_back(val);
        asset_samples[a].push_back(val);
        hour_samples[hour].push_back(val);
        weekday_samples[weekday].push_back(val);
      }
    }

    // Batch insert once per day
    if (!day_samples.empty()) {
      mc.total.addBatch(day_samples);
    }

    for (size_t a = 0; a < A; ++a) {
      if (!asset_samples[a].empty()) {
        mc.by_asset[a].addBatch(asset_samples[a]);
      }
    }

    for (size_t h = 0; h < 24; ++h) {
      if (!hour_samples[h].empty()) {
        mc.by_hour[h].addBatch(hour_samples[h]);
      }
    }

    for (size_t wd = 0; wd < 7; ++wd) {
      if (!weekday_samples[wd].empty()) {
        mc.by_weekday[wd].addBatch(weekday_samples[wd]);
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
  case Input::GroupBy::NONE: {
    // Merge all months into single bin, store in kll_storage
    result.kll_storage.emplace_back(KLL_CAPACITY);
    auto &merged = result.kll_storage.back();
    for (const auto &mc : cache) {
      merged.merge(mc.total);
    }
    if (merged.count() >= min_samples) {
      result.bins.emplace_back();
      result.bins.back().key = "all";
      result.bins.back().extract_from(merged);
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
  }

  result.valid = true;
}

// ============================================================================
// Build Trajectory
// ============================================================================

void Dist::build_trajectory() {
  trajectory.clear();

  if (cache.empty()) {
    trajectory.valid = true;
    return;
  }

  const size_t n_months = cache.size();
  const size_t n_assets = cache[0].n_assets;

  trajectory.n_assets = n_assets;
  trajectory.months.reserve(n_months);
  for (const auto &mc : cache) {
    trajectory.months.push_back(mc.month);
  }

  // [n_assets][n_months]
  trajectory.paths.resize(n_assets);
  for (size_t a = 0; a < n_assets; ++a) {
    trajectory.paths[a].resize(n_months);
    for (size_t m = 0; m < n_months; ++m) {
      const auto &mc = cache[m];
      if (a < mc.by_asset.size()) {
        trajectory.paths[a][m].extract_from(mc.by_asset[a]);
      }
    }
  }

  trajectory.valid = true;
}
