#include "shared/Dist.hpp"
#include "features/backend/FeatureReader.hpp"
#include "shared/Asset.hpp"
#include "shared/Config.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>

// ============================================================================
// Input - Change Detection
// ============================================================================

bool Dist::Input::has_changes(const Feature &feature,
                              const Config &config) const {
  // Build current state string
  std::string current_date_range = config.start_date + "_" + config.end_date;

  // Detect changes
  return (cached_primary_feature_idx != feature.selection.primary_feature_idx) ||
         (cached_level != feature.selection.selected_level) ||
         (cached_date_range != current_date_range) ||
         (cached_grouping != time_grouping);
}

void Dist::Input::update_cache(const Feature &feature, const Config &config) {
  cached_primary_feature_idx = feature.selection.primary_feature_idx;
  cached_level = feature.selection.selected_level;
  cached_date_range = config.start_date + "_" + config.end_date;
  cached_grouping = time_grouping;
}

// ============================================================================
// RawData - Methods
// ============================================================================

void Dist::RawData::reserve(size_t estimated_samples) {
  values.reserve(estimated_samples);
  day_idx.reserve(estimated_samples);
  time_idx.reserve(estimated_samples);
  asset_idx.reserve(estimated_samples);
  hour.reserve(estimated_samples);
  weekday.reserve(estimated_samples);
  month.reserve(estimated_samples);
  year.reserve(estimated_samples);
}

void Dist::RawData::push(float value, uint32_t day, uint32_t time,
                         uint16_t asset, uint8_t h, uint8_t wd, uint8_t m,
                         uint16_t y) {
  values.push_back(value);
  day_idx.push_back(day);
  time_idx.push_back(time);
  asset_idx.push_back(asset);
  hour.push_back(h);
  weekday.push_back(wd);
  month.push_back(m);
  year.push_back(y);
  ++n_samples;
}

void Dist::RawData::merge_buffers(const std::vector<PerDateBuffer> &buffers,
                                  size_t n_assets_val) {
  // Aggregate statistics from all workers (parallel accumulation)
  size_t total_samples = 0;
  size_t total_invalid = 0;
  size_t total_zero = 0;
  size_t total_nan = 0;
  size_t total_pos_inf = 0;
  size_t total_neg_inf = 0;
  
  for (const auto &buf : buffers) {
    total_samples += buf.n_samples;
    total_invalid += buf.n_invalid;
    total_zero += buf.n_zero;
    total_nan += buf.n_nan;
    total_pos_inf += buf.n_pos_inf;
    total_neg_inf += buf.n_neg_inf;
  }

  // Clear and pre-allocate
  clear();
  reserve(total_samples);
  n_assets = n_assets_val;
  n_invalid = total_invalid;
  n_zero = total_zero;
  n_nan = total_nan;
  n_pos_inf = total_pos_inf;
  n_neg_inf = total_neg_inf;

  // Merge all buffers (sequential, but fast - vectorized memcpy)
  for (const auto &buf : buffers) {
    if (buf.n_samples == 0)
      continue;

    values.insert(values.end(), buf.values.begin(), buf.values.end());
    day_idx.insert(day_idx.end(), buf.day_idx.begin(), buf.day_idx.end());
    time_idx.insert(time_idx.end(), buf.time_idx.begin(), buf.time_idx.end());
    asset_idx.insert(asset_idx.end(), buf.asset_idx.begin(), buf.asset_idx.end());
    hour.insert(hour.end(), buf.hour.begin(), buf.hour.end());
    weekday.insert(weekday.end(), buf.weekday.begin(), buf.weekday.end());
    month.insert(month.end(), buf.month.begin(), buf.month.end());
    year.insert(year.end(), buf.year.begin(), buf.year.end());
  }

  n_samples = values.size();
  loaded = true;
  ++version;
}

void Dist::RawData::clear() {
  values.clear();
  day_idx.clear();
  time_idx.clear();
  asset_idx.clear();
  hour.clear();
  weekday.clear();
  month.clear();
  year.clear();
  n_samples = 0;
  n_invalid = 0;
  n_zero = 0;
  n_nan = 0;
  n_pos_inf = 0;
  n_neg_inf = 0;
  n_assets = 0;
  loaded = false;
  ++version;
}

// ============================================================================
// GroupedData - Methods
// ============================================================================

void Dist::GroupedData::Bin::reserve(size_t n) {
  values.reserve(n);
  asset_indices.reserve(n);
}

void Dist::GroupedData::Bin::clear() {
  key.clear();
  values.clear();
  asset_indices.clear();
  n_samples = 0;
}

const Dist::GroupedData::Bin &Dist::GroupedData::get_bin(size_t idx) const {
  assert(idx < bins.size());
  return bins[idx];
}

void Dist::GroupedData::clear() {
  bins.clear();
  grouping_type = Input::TimeGrouping::NONE;
  valid = false;
  raw_version = 0;
}

// ============================================================================
// Statistics - Methods
// ============================================================================

void Dist::Statistics::clear() {
  integrity = Integrity{};
  moments.clear();
  quantiles.clear();
  pdf.clear();
  trajectory.clear();
  heterogeneity.clear();
  scale_robustness = ScaleRobustness{};
  valid = false;
  grouped_version = 0;
}

// ============================================================================
// VisualizationCache - Methods
// ============================================================================

void Dist::VisualizationCache::clear() {
  pdf_plots.clear();
  trajectory_plots.clear();
  quantile_heatmap = QuantileHeatmap{};
  moments_display.clear();
  valid = false;
  stats_version = 0;
  cached_pdf_sensitivity = -1.0f;
}

// ============================================================================
// Dist - Data Loading
// ============================================================================

void Dist::load_data_async(const FeatureReader::MultiDayCache &cache,
                           const Feature &feature, const Config & /*config*/,
                           const Asset &asset) {
  // Mark as loading
  compute.status = Compute::Status::LoadingData;
  compute.reset();

  // Get primary feature index and level
  int primary_idx = feature.selection.primary_feature_idx;
  int level = feature.selection.selected_level;

  assert(primary_idx >= 0 && "No primary feature selected");
  assert(level >= 0 && level < LEVEL_COUNT && "Invalid level");

  // Clear existing data
  raw.clear();

  // Estimate total samples: n_days × n_time_per_day × n_assets
  size_t n_days = cache.days.size();
  size_t n_assets = asset.items.size();
  size_t estimated_samples = 0;

  if (n_days > 0 && cache.days[0].is_loaded()) {
    size_t time_per_day = cache.days[0].T[level];
    estimated_samples = n_days * time_per_day * n_assets;
  }

  raw.reserve(estimated_samples);
  raw.n_assets = n_assets;

  // TODO: Extract date parsing utility
  auto parse_date = [](const std::string &date_str) -> std::tuple<uint16_t, uint8_t, uint8_t> {
    // Expected format: "YYYYMMDD"
    if (date_str.size() != 8)
      return {0, 0, 0};
    uint16_t year = std::stoi(date_str.substr(0, 4));
    uint8_t month = std::stoi(date_str.substr(4, 2));
    uint8_t day = std::stoi(date_str.substr(6, 2));
    return {year, month, day};
  };

  // TODO: Weekday calculation (0=Mon, 6=Sun)
  auto get_weekday = [](uint16_t year, uint8_t month, uint8_t day) -> uint8_t {
    // Zeller's congruence for Gregorian calendar
    if (month < 3) {
      month += 12;
      year -= 1;
    }
    int q = day;
    int m = month;
    int k = year % 100;
    int j = year / 100;
    int h = (q + (13 * (m + 1)) / 5 + k + k / 4 + j / 4 - 2 * j) % 7;
    // h: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
    // Convert to 0=Mon, 6=Sun
    return (h + 5) % 7;
  };

  compute.progress_total = estimated_samples;
  compute.progress_current = 0;

  // Load data from all days
  for (size_t day_idx = 0; day_idx < n_days; ++day_idx) {
    const auto &day_tensor = cache.days[day_idx];
    if (!day_tensor.is_loaded())
      continue;

    // Parse date
    auto [year, month, day] = parse_date(day_tensor.date);
    uint8_t weekday = get_weekday(year, month, day);

    size_t T = day_tensor.T[level];
    size_t A = day_tensor.A;

    assert(A == n_assets && "Asset count mismatch");

    // Extract feature values for all (t, a) pairs
    // Tensor layout: [T][F][A], access: data[(t * F + f) * A + a]
    for (size_t t = 0; t < T; ++t) {
      // Simple general hour mapping: map time index to [0, 23]
      uint8_t hour = static_cast<uint8_t>(t % 24);

      for (size_t a = 0; a < A; ++a) {
        // Get feature value from tensor
        float value = day_tensor.data[level][(t * day_tensor.F[level] + primary_idx) * A + a];

        // Skip invalid values (NaN, Inf will be counted in integrity check)
        // Use safe check: !(NaN or Inf)
        bool is_valid = (value == value) && (value > -1e38f) && (value < 1e38f);
        if (is_valid) {
          raw.push(value, day_idx, t, a, hour, weekday, month, year);
        }

        compute.progress_current++;
      }
    }
  }

  raw.loaded = true;
  compute.status = Compute::Status::Idle;
}

// ============================================================================
// Dist - Time Grouping
// ============================================================================

void Dist::apply_time_grouping_async() {
  assert(raw.loaded && "Raw data not loaded");

  compute.status = Compute::Status::Grouping;
  compute.reset();

  grouped.clear();
  grouped.grouping_type = input.time_grouping;
  grouped.raw_version = raw.version;

  compute.progress_total = raw.n_samples;
  compute.progress_current = 0;

  // Create bins based on grouping type
  if (input.time_grouping == Input::TimeGrouping::NONE) {
    // Single bin for all data
    grouped.bins.resize(1);
    grouped.bins[0].key = "all";
    grouped.bins[0].reserve(raw.n_samples);

    for (size_t i = 0; i < raw.n_samples; ++i) {
      grouped.bins[0].values.push_back(raw.values[i]);
      grouped.bins[0].asset_indices.push_back(raw.asset_idx[i]);
      ++grouped.bins[0].n_samples;
      compute.progress_current++;
    }
  } else if (input.time_grouping == Input::TimeGrouping::HOUR) {
    // 24 bins for hours (0-23)
    grouped.bins.resize(24);
    for (size_t h = 0; h < 24; ++h) {
      grouped.bins[h].key = "hour_" + std::to_string(h);
      grouped.bins[h].reserve(raw.n_samples / 24);
    }

    for (size_t i = 0; i < raw.n_samples; ++i) {
      uint8_t h = raw.hour[i];
      if (h >= 24) continue; // Skip invalid hour
      grouped.bins[h].values.push_back(raw.values[i]);
      grouped.bins[h].asset_indices.push_back(raw.asset_idx[i]);
      ++grouped.bins[h].n_samples;
      compute.progress_current++;
    }
  } else if (input.time_grouping == Input::TimeGrouping::WEEKDAY) {
    // 7 bins for weekdays (0=Mon, 6=Sun)
    grouped.bins.resize(7);
    const char *weekday_names[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
    for (size_t d = 0; d < 7; ++d) {
      grouped.bins[d].key = std::string("weekday_") + weekday_names[d];
      grouped.bins[d].reserve(raw.n_samples / 7);
    }

    for (size_t i = 0; i < raw.n_samples; ++i) {
      uint8_t wd = raw.weekday[i];
      if (wd >= 7) continue; // Skip invalid weekday
      grouped.bins[wd].values.push_back(raw.values[i]);
      grouped.bins[wd].asset_indices.push_back(raw.asset_idx[i]);
      ++grouped.bins[wd].n_samples;
      compute.progress_current++;
    }
  } else if (input.time_grouping == Input::TimeGrouping::MONTH) {
    // 12 bins for months (1-12)
    grouped.bins.resize(12);
    for (size_t m = 0; m < 12; ++m) {
      grouped.bins[m].key = "month_" + std::to_string(m + 1);
      grouped.bins[m].reserve(raw.n_samples / 12);
    }

    for (size_t i = 0; i < raw.n_samples; ++i) {
      uint8_t m = raw.month[i];
      if (m < 1 || m > 12) continue; // Skip invalid month
      grouped.bins[m - 1].values.push_back(raw.values[i]); // 1-12 -> 0-11
      grouped.bins[m - 1].asset_indices.push_back(raw.asset_idx[i]);
      ++grouped.bins[m - 1].n_samples;
      compute.progress_current++;
    }
  } else if (input.time_grouping == Input::TimeGrouping::YEAR) {
    // Dynamic number of bins based on unique years
    // First pass: collect unique years
    std::vector<uint16_t> unique_years;
    for (uint16_t y : raw.year) {
      if (std::find(unique_years.begin(), unique_years.end(), y) == unique_years.end()) {
        unique_years.push_back(y);
      }
    }
    std::sort(unique_years.begin(), unique_years.end());

    grouped.bins.resize(unique_years.size());
    for (size_t i = 0; i < unique_years.size(); ++i) {
      grouped.bins[i].key = "year_" + std::to_string(unique_years[i]);
      grouped.bins[i].reserve(raw.n_samples / unique_years.size());
    }

    // Second pass: populate bins
    for (size_t i = 0; i < raw.n_samples; ++i) {
      uint16_t y = raw.year[i];
      auto it = std::find(unique_years.begin(), unique_years.end(), y);
      if (it == unique_years.end()) continue; // Skip if year not found (shouldn't happen)
      size_t bin_idx = it - unique_years.begin();
      grouped.bins[bin_idx].values.push_back(raw.values[i]);
      grouped.bins[bin_idx].asset_indices.push_back(raw.asset_idx[i]);
      ++grouped.bins[bin_idx].n_samples;
      compute.progress_current++;
    }
  }

  grouped.valid = true;
  compute.status = Compute::Status::Idle;
}

// ============================================================================
// Dist - Statistics Computation
// ============================================================================

void Dist::compute_integrity() {
  assert(raw.loaded && "Raw data not loaded");

  // Data already filtered during loading, just aggregate statistics
  stats.integrity = Statistics::Integrity{};
  stats.integrity.total_count = raw.n_samples;      // Finite values only
  stats.integrity.valid_count = raw.n_samples;      // Same as total
  stats.integrity.invalid_count = raw.n_invalid;    // Filtered by valid flag
  stats.integrity.zero_count = raw.n_zero;          // Counted during loading
  stats.integrity.nan_count = raw.n_nan;            // Filtered during loading
  stats.integrity.pos_inf_count = raw.n_pos_inf;    // Filtered during loading
  stats.integrity.neg_inf_count = raw.n_neg_inf;    // Filtered during loading
  
  // Calculate percentages
  size_t total_examined = stats.integrity.valid_count + stats.integrity.invalid_count;
  if (total_examined > 0) {
    stats.integrity.valid_pct = 100.0f * stats.integrity.valid_count / total_examined;
  }
  
  size_t total_before_finite_filter = raw.n_samples + raw.n_nan + raw.n_pos_inf + raw.n_neg_inf;
  if (total_before_finite_filter > 0) {
    stats.integrity.zero_pct = 100.0f * stats.integrity.zero_count / total_before_finite_filter;
    stats.integrity.nan_pct = 100.0f * stats.integrity.nan_count / total_before_finite_filter;
    stats.integrity.pos_inf_pct = 100.0f * stats.integrity.pos_inf_count / total_before_finite_filter;
    stats.integrity.neg_inf_pct = 100.0f * stats.integrity.neg_inf_count / total_before_finite_filter;
  }
}

void Dist::compute_moments_parallel() {
  assert(grouped.valid && "Grouped data not valid");

  stats.moments.clear();
  stats.moments.resize(grouped.n_bins());

  // For each bin
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    const auto &bin = grouped.bins[bin_idx];
    stats.moments[bin_idx].per_asset.resize(raw.n_assets);

    // Group values by asset
    std::vector<std::vector<float>> values_per_asset(raw.n_assets);
    for (size_t i = 0; i < bin.n_samples; ++i) {
      uint16_t asset_idx = bin.asset_indices[i];
      values_per_asset[asset_idx].push_back(bin.values[i]);
    }

    // Compute moments for each asset
    for (size_t asset_idx = 0; asset_idx < raw.n_assets; ++asset_idx) {
      const auto &vals = values_per_asset[asset_idx];
      auto &moments = stats.moments[bin_idx].per_asset[asset_idx];

      if (vals.empty())
        continue;

      size_t n = vals.size();

      // Mean (1st raw moment)
      double sum = 0.0;
      for (float v : vals)
        sum += v;
      moments.mean = sum / n;

      // Variance (2nd central moment)
      double sum_sq_diff = 0.0;
      for (float v : vals) {
        double diff = v - moments.mean;
        sum_sq_diff += diff * diff;
      }
      moments.variance = sum_sq_diff / n;

      if (moments.variance > 1e-10) {
        double std_dev = std::sqrt(moments.variance);

        // Skewness (3rd standardized moment)
        double sum_cube_diff = 0.0;
        for (float v : vals) {
          double diff = (v - moments.mean) / std_dev;
          sum_cube_diff += diff * diff * diff;
        }
        moments.skewness = sum_cube_diff / n;

        // Kurtosis (4th standardized moment)
        double sum_quad_diff = 0.0;
        for (float v : vals) {
          double diff = (v - moments.mean) / std_dev;
          sum_quad_diff += diff * diff * diff * diff;
        }
        moments.kurtosis = sum_quad_diff / n;
      }
    }
  }
}

void Dist::compute_quantiles_parallel() {
  assert(grouped.valid && "Grouped data not valid");

  stats.quantiles.clear();
  stats.quantiles.resize(grouped.n_bins());

  // For each bin, compute cross-sectional quantiles
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    const auto &bin = grouped.bins[bin_idx];
    auto &quantiles = stats.quantiles[bin_idx];

    if (bin.values.empty())
      continue;

    // Sort values for quantile calculation
    std::vector<float> sorted_vals = bin.values;
    std::sort(sorted_vals.begin(), sorted_vals.end());

    size_t n = sorted_vals.size();
    auto get_quantile = [&](float q) -> float {
      size_t idx = static_cast<size_t>(q * (n - 1));
      return sorted_vals[idx];
    };

    quantiles.q01 = get_quantile(0.01f);
    quantiles.q05 = get_quantile(0.05f);
    quantiles.q25 = get_quantile(0.25f);
    quantiles.q50 = get_quantile(0.50f);
    quantiles.q75 = get_quantile(0.75f);
    quantiles.q95 = get_quantile(0.95f);
    quantiles.q99 = get_quantile(0.99f);
  }
}

void Dist::compute_pdf_parallel() {
  // TODO: Implement KDE for PDF computation
  // This requires a kernel density estimation implementation
  // For now, create placeholder structure
  assert(grouped.valid && "Grouped data not valid");

  stats.pdf.clear();
  stats.pdf.resize(grouped.n_bins());

  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    stats.pdf[bin_idx].per_asset.resize(raw.n_assets);
    // TODO: Implement actual KDE computation
  }
}

void Dist::compute_trajectory_parallel() {
  assert(grouped.valid && "Grouped data not valid");
  assert(!stats.moments.empty() && "Moments not computed");

  stats.trajectory.clear();
  stats.trajectory.resize(grouped.n_bins());

  // For each bin
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    const auto &bin = grouped.bins[bin_idx];
    stats.trajectory[bin_idx].per_asset.resize(raw.n_assets);

    // Group values by asset
    std::vector<std::vector<float>> values_per_asset(raw.n_assets);
    for (size_t i = 0; i < bin.n_samples; ++i) {
      uint16_t asset_idx = bin.asset_indices[i];
      values_per_asset[asset_idx].push_back(bin.values[i]);
    }

    // Compute trajectory metrics for each asset
    for (size_t asset_idx = 0; asset_idx < raw.n_assets; ++asset_idx) {
      const auto &vals = values_per_asset[asset_idx];
      auto &traj = stats.trajectory[bin_idx].per_asset[asset_idx];
      const auto &moments = stats.moments[bin_idx].per_asset[asset_idx];

      if (vals.empty())
        continue;

      // Robust skewness (using quantiles instead of moments)
      std::vector<float> sorted_vals = vals;
      std::sort(sorted_vals.begin(), sorted_vals.end());
      size_t n = sorted_vals.size();
      if (n >= 3) {
        float q25 = sorted_vals[n / 4];
        float q50 = sorted_vals[n / 2];
        float q75 = sorted_vals[3 * n / 4];
        float iqr = q75 - q25;
        if (iqr > 1e-10f) {
          traj.robust_skewness = (q75 + q25 - 2 * q50) / iqr;
        }
      }

      // Tail thickness (ratio of extreme quantiles to IQR)
      if (n >= 10) {
        float q05 = sorted_vals[n / 20];
        float q95 = sorted_vals[19 * n / 20];
        float q25 = sorted_vals[n / 4];
        float q75 = sorted_vals[3 * n / 4];
        float iqr = q75 - q25;
        if (iqr > 1e-10f) {
          traj.tail_thickness = (q95 - q05) / iqr;
        }
      }

      // Peakedness / Central Concentration Ratio
      // Ratio of values within 1 std dev to total
      if (moments.variance > 1e-10) {
        float std_dev = std::sqrt(moments.variance);
        size_t count_within = 0;
        for (float v : vals) {
          if (std::abs(v - moments.mean) <= std_dev) {
            ++count_within;
          }
        }
        traj.peakedness_ccr = static_cast<float>(count_within) / vals.size();
      }

      // Variance (for point size)
      traj.variance = moments.variance;
    }
  }
}

void Dist::compute_heterogeneity_parallel() {
  assert(grouped.valid && "Grouped data not valid");

  stats.heterogeneity.clear();
  stats.heterogeneity.resize(grouped.n_bins());

  // For each bin, compute cross-sectional heterogeneity
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    const auto &bin = grouped.bins[bin_idx];
    auto &hetero = stats.heterogeneity[bin_idx];

    if (bin.values.empty())
      continue;

    // Compute absolute values for Gini and HHI
    std::vector<float> abs_vals;
    abs_vals.reserve(bin.values.size());
    for (float v : bin.values) {
      abs_vals.push_back(std::abs(v));
    }

    // Gini coefficient
    std::sort(abs_vals.begin(), abs_vals.end());
    size_t n = abs_vals.size();
    double sum_weighted = 0.0;
    double sum_total = 0.0;
    for (size_t i = 0; i < n; ++i) {
      sum_weighted += (2 * i - n + 1) * abs_vals[i];
      sum_total += abs_vals[i];
    }
    if (sum_total > 1e-10 && n > 1) {
      hetero.gini = sum_weighted / (n * sum_total);
    }

    // HHI (Herfindahl-Hirschman Index)
    double sum_sq = 0.0;
    for (float v : abs_vals) {
      double share = v / sum_total;
      sum_sq += share * share;
    }
    hetero.hhi = sum_sq;
  }
}

void Dist::compute_scale_robustness() {
  // TODO: Implement rank correlation computation
  // This requires computing normalized versions of the feature
  // and comparing rank correlations
  stats.scale_robustness = Statistics::ScaleRobustness{};
}

void Dist::compute_all_statistics_async() {
  assert(grouped.valid && "Grouped data not valid");

  compute.status = Compute::Status::Computing;
  compute.reset();

  stats.clear();
  stats.grouped_version = grouped.raw_version;

  // Compute all statistics
  compute_integrity();
  compute_moments_parallel();
  compute_quantiles_parallel();
  compute_pdf_parallel();
  compute_trajectory_parallel();
  compute_heterogeneity_parallel();
  compute_scale_robustness();

  stats.valid = true;
  compute.status = Compute::Status::Idle;
}

// ============================================================================
// Dist - Visualization Cache
// ============================================================================

void Dist::build_visualization_cache() {
  assert(stats.valid && "Statistics not computed");

  compute.status = Compute::Status::BuildingCache;

  vis_cache.clear();
  vis_cache.stats_version = stats.grouped_version;
  vis_cache.cached_pdf_sensitivity = input.pdf_sensitivity;

  // Build PDF plots (TODO: implement when PDF is computed)
  vis_cache.pdf_plots.resize(grouped.n_bins());

  // Build trajectory plots
  vis_cache.trajectory_plots.resize(grouped.n_bins());
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    auto &plot = vis_cache.trajectory_plots[bin_idx];
    const auto &traj_bin = stats.trajectory[bin_idx];

    plot.x.resize(raw.n_assets);
    plot.y.resize(raw.n_assets);
    plot.colors.resize(raw.n_assets);
    plot.sizes.resize(raw.n_assets);

    plot.x_min = std::numeric_limits<float>::max();
    plot.x_max = std::numeric_limits<float>::lowest();
    plot.y_min = std::numeric_limits<float>::max();
    plot.y_max = std::numeric_limits<float>::lowest();

    for (size_t asset_idx = 0; asset_idx < raw.n_assets; ++asset_idx) {
      const auto &traj = traj_bin.per_asset[asset_idx];

      plot.x[asset_idx] = traj.robust_skewness;
      plot.y[asset_idx] = traj.tail_thickness;
      plot.sizes[asset_idx] = std::sqrt(traj.variance);

      // Color from peakedness (0.0-1.0 -> blue to red)
      uint8_t r = static_cast<uint8_t>(255 * traj.peakedness_ccr);
      uint8_t b = static_cast<uint8_t>(255 * (1.0f - traj.peakedness_ccr));
      plot.colors[asset_idx] = 0xFF000000 | (r << 16) | (128 << 8) | b;

      plot.x_min = std::min(plot.x_min, traj.robust_skewness);
      plot.x_max = std::max(plot.x_max, traj.robust_skewness);
      plot.y_min = std::min(plot.y_min, traj.tail_thickness);
      plot.y_max = std::max(plot.y_max, traj.tail_thickness);
    }
  }

  // Build quantile heatmap
  vis_cache.quantile_heatmap.n_cols = grouped.n_bins();
  vis_cache.quantile_heatmap.n_rows = 7;
  vis_cache.quantile_heatmap.matrix.resize(7 * grouped.n_bins());
  vis_cache.quantile_heatmap.col_labels.resize(grouped.n_bins());

  vis_cache.quantile_heatmap.min_val = std::numeric_limits<float>::max();
  vis_cache.quantile_heatmap.max_val = std::numeric_limits<float>::lowest();

  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    const auto &q = stats.quantiles[bin_idx];
    vis_cache.quantile_heatmap.col_labels[bin_idx] = grouped.bins[bin_idx].key;

    float quantiles[7] = {q.q01, q.q05, q.q25, q.q50, q.q75, q.q95, q.q99};
    for (size_t row = 0; row < 7; ++row) {
      float val = quantiles[row];
      vis_cache.quantile_heatmap.matrix[row * grouped.n_bins() + bin_idx] = val;
      vis_cache.quantile_heatmap.min_val = std::min(vis_cache.quantile_heatmap.min_val, val);
      vis_cache.quantile_heatmap.max_val = std::max(vis_cache.quantile_heatmap.max_val, val);
    }
  }

  // Build moments display (pre-formatted strings)
  vis_cache.moments_display.resize(grouped.n_bins());
  for (size_t bin_idx = 0; bin_idx < grouped.n_bins(); ++bin_idx) {
    vis_cache.moments_display[bin_idx].resize(raw.n_assets);

    for (size_t asset_idx = 0; asset_idx < raw.n_assets; ++asset_idx) {
      const auto &m = stats.moments[bin_idx].per_asset[asset_idx];
      auto &display = vis_cache.moments_display[bin_idx][asset_idx];

      // Format strings
      std::ostringstream oss;
      oss.precision(4);
      oss << m.mean;
      display.mean_text = oss.str();

      oss.str("");
      oss << m.variance;
      display.var_text = oss.str();

      oss.str("");
      oss << m.skewness;
      display.skew_text = oss.str();

      oss.str("");
      oss << m.kurtosis;
      display.kurt_text = oss.str();

      // Color for skewness (negative=blue, positive=red)
      if (m.skewness < 0) {
        uint8_t intensity = static_cast<uint8_t>(std::min(255.0f, -m.skewness * 100));
        display.skew_color = 0xFF000000 | intensity; // Blue
      } else {
        uint8_t intensity = static_cast<uint8_t>(std::min(255.0f, m.skewness * 100));
        display.skew_color = 0xFF000000 | (intensity << 16); // Red
      }

      // Color for kurtosis (low=green, high=red)
      float excess = m.kurtosis - 3.0f;
      if (excess < 0) {
        uint8_t intensity = static_cast<uint8_t>(std::min(255.0f, -excess * 50));
        display.kurt_color = 0xFF000000 | (intensity << 8); // Green
      } else {
        uint8_t intensity = static_cast<uint8_t>(std::min(255.0f, excess * 50));
        display.kurt_color = 0xFF000000 | (intensity << 16); // Red
      }
    }
  }

  vis_cache.valid = true;
  compute.status = Compute::Status::Completed;
}

// ============================================================================
// Dist - Control
// ============================================================================

void Dist::start_full_compute(const FeatureReader::MultiDayCache &cache,
                              const Feature &feature, const Config &config,
                              const Asset &asset) {
  // Full pipeline: load -> group -> compute -> cache
  load_data_async(cache, feature, config, asset);
  if (compute.status == Compute::Status::Error)
    return;

  apply_time_grouping_async();
  if (compute.status == Compute::Status::Error)
    return;

  compute_all_statistics_async();
  if (compute.status == Compute::Status::Error)
    return;

  build_visualization_cache();
}

void Dist::cancel_compute() {
  compute.cancel_flag = true;
  compute.status = Compute::Status::Cancelled;
}

void Dist::clear() {
  input = Input{};
  raw.clear();
  grouped.clear();
  stats.clear();
  vis_cache.clear();
  compute.reset();
  compute.status = Compute::Status::Idle;
  compute.error_message.clear();
  version = 0;
}

// ============================================================================
// Parallel Data Loading
// ============================================================================

std::tuple<std::shared_ptr<std::vector<Dist::PerDateBuffer>>,
           std::shared_ptr<std::atomic<size_t>>,
           std::shared_ptr<std::atomic<size_t>>>
Dist::dispatch_parallel_loading(const std::vector<std::string> &available_dates,
                                const std::string &features_dir,
                                const Feature &feature,
                                const Config & /*config*/,
                                const Asset & /*asset*/,
                                std::function<void(std::function<void()>)> submit_task) {
  const size_t n_dates = available_dates.size();
  const int level = feature.selection.selected_level;
  const int primary_idx = feature.selection.primary_feature_idx;
  
  // Get feature metadata to determine valid_type
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

  // Allocate per-date buffers and counters
  auto buffers = std::make_shared<std::vector<PerDateBuffer>>(n_dates);
  auto completed = std::make_shared<std::atomic<size_t>>(0);
  auto failed = std::make_shared<std::atomic<size_t>>(0);

  // Weekday calculation (Zeller's congruence)
  auto calc_weekday = [](uint16_t y, uint8_t m, uint8_t d) -> uint8_t {
    if (m < 3) {
      m += 12;
      y -= 1;
    }
    int q = d, M = m, K = y % 100, J = y / 100;
    int h = (q + (13 * (M + 1)) / 5 + K + K / 4 + J / 4 - 2 * J) % 7;
    return (h + 5) % 7; // Mon=0, Sun=6
  };

  // Dispatch complete date-processing workflows
  for (size_t date_idx = 0; date_idx < n_dates; ++date_idx) {
    submit_task([=]() {
      const std::string &date = available_dates[date_idx];
      PerDateBuffer &buf = (*buffers)[date_idx];

      // Parse date (YYYYMMDD)
      uint16_t year_v = std::stoi(date.substr(0, 4));
      uint8_t month_v = std::stoi(date.substr(4, 2));
      uint8_t day_v = std::stoi(date.substr(6, 2));
      uint8_t weekday_v = calc_weekday(year_v, month_v, day_v);

      // Load tensor
      FeatureReader reader(features_dir);
      FeatureReader::DayTensor tensor;
      if (!reader.load_day_level(date, level, tensor)) {
        std::cerr << "  [Worker] Failed to load " << date << std::endl;
        failed->fetch_add(1);
        return;
      }

      // Extract dimensions
      const size_t T = tensor.T[level];
      const size_t F = tensor.F[level];
      const size_t A = tensor.A;

      if (primary_idx < 0 || static_cast<size_t>(primary_idx) >= F) {
        std::cerr << "  [Worker] Invalid feature idx for " << date << std::endl;
        failed->fetch_add(1);
        return;
      }

      // Find valid flag feature index based on valid_type
      int valid_flag_idx = -1;
      if (valid_type == L2::ValidType::DEPTH) {
        // Look for _depth_valid
        for (size_t i = 0; i < F; ++i) {
          if (meta_list && i < meta_count && std::string(meta_list[i].code) == "_depth_valid") {
            valid_flag_idx = i;
            break;
          }
        }
      } else if (valid_type == L2::ValidType::DATA) {
        // Look for _data_valid
        for (size_t i = 0; i < F; ++i) {
          if (meta_list && i < meta_count && std::string(meta_list[i].code) == "_data_valid") {
            valid_flag_idx = i;
            break;
          }
        }
      }
      // If ALL, we don't filter by valid flag

      // Reserve buffer
      buf.reserve(T * A);

      // Extract feature values: [t][a] → buffer
      for (size_t t = 0; t < T; ++t) {
        // Simple general hour mapping: map time index to [0, 23]
        uint8_t hour_v = static_cast<uint8_t>(t % 24);

        // Get pointer to all assets for this (t, feature)
        const feature_storage_t *all_assets = nullptr;
        if (level == 0) {
          all_assets = tensor.get_all_assets<0>(t, primary_idx);
        } else if (level == 1) {
          all_assets = tensor.get_all_assets<1>(t, primary_idx);
        } else {
          all_assets = tensor.get_all_assets<2>(t, primary_idx);
        }
        
        // Get valid flags if needed
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
          float val = static_cast<float>(all_assets[a]);
          
          // Check valid flag (if applicable)
          if (valid_flags) {
            float valid_flag = static_cast<float>(valid_flags[a]);
            if (__builtin_expect(valid_flag <= 0.5f, 0)) {
              ++buf.n_invalid;
              continue;
            }
          }

          // Statistics + filtering in one pass (unlikely branch prediction)
          
          // Check NaN
          if (__builtin_expect(val != val, 0)) {
            ++buf.n_nan;
            continue;  // Filter out NaN
          }
          
          // Check +Inf
          if (__builtin_expect(val > 1e38f, 0)) {
            ++buf.n_pos_inf;
            continue;
          }
          
          // Check -Inf
          if (__builtin_expect(val < -1e38f, 0)) {
            ++buf.n_neg_inf;
            continue;
          }
          
          // Count Zero (but keep it)
          if (val == 0.0f) {
            ++buf.n_zero;
          }
          
          // Keep finite values only
          buf.values.push_back(val);
          buf.day_idx.push_back(static_cast<uint32_t>(date_idx));
          buf.time_idx.push_back(static_cast<uint32_t>(t));
          buf.asset_idx.push_back(static_cast<uint16_t>(a));
          buf.hour.push_back(hour_v);
          buf.weekday.push_back(weekday_v);
          buf.month.push_back(month_v);
          buf.year.push_back(year_v);
          ++buf.n_samples;
        }
      }

      completed->fetch_add(1);
    });
  }

  return {buffers, completed, failed};
}
