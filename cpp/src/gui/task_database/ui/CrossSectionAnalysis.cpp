// Cross-section analysis implementation

#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "gui/task_database/models/SharedTypes.hpp" // For BoardType
#include <algorithm>
#include <cmath>
#include <numeric>

namespace GUI::Database {

// ============================================================================
// Statistical Analysis Implementation
// ============================================================================

ColumnStats CalculateColumnStats(const std::vector<double> &values) {
  ColumnStats stats;
  stats.total_count = values.size();

  if (values.empty()) {
    return stats;
  }

  // Filter out invalid values (NaN, Inf)
  std::vector<double> valid_values;
  valid_values.reserve(values.size());
  for (double v : values) {
    // Valid if finite: not NaN and within reasonable bounds
    if (v == v && v > -1e300 && v < 1e300) {
      valid_values.push_back(v);
    }
  }

  stats.valid_count = valid_values.size();
  if (valid_values.empty()) {
    return stats;
  }

  // Sort for median and percentiles
  std::vector<double> sorted = valid_values;
  std::sort(sorted.begin(), sorted.end());

  // Min and Max
  stats.min = sorted.front();
  stats.max = sorted.back();

  // Mean
  double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
  stats.mean = sum / sorted.size();

  // Median
  size_t mid = sorted.size() / 2;
  if (sorted.size() % 2 == 0) {
    stats.median = (sorted[mid - 1] + sorted[mid]) / 2.0;
  } else {
    stats.median = sorted[mid];
  }

  // Percentiles
  size_t q25_idx = sorted.size() / 4;
  size_t q75_idx = (sorted.size() * 3) / 4;
  stats.q25 = sorted[q25_idx];
  stats.q75 = sorted[q75_idx];

  // Standard Deviation
  double sq_sum = 0.0;
  for (double v : sorted) {
    sq_sum += (v - stats.mean) * (v - stats.mean);
  }
  stats.std_dev = std::sqrt(sq_sum / sorted.size());

  return stats;
}

std::vector<HistogramBin> GenerateHistogram(const std::vector<double> &values, int num_bins) {
  std::vector<HistogramBin> bins;

  // Filter valid values
  std::vector<double> valid_values;
  for (double v : values) {
    if (v == v && v > -1e300 && v < 1e300) {
      valid_values.push_back(v);
    }
  }

  if (valid_values.empty() || num_bins <= 0) {
    return bins;
  }

  // Find range
  double min_val = *std::min_element(valid_values.begin(), valid_values.end());
  double max_val = *std::max_element(valid_values.begin(), valid_values.end());

  // Handle edge case where all values are the same
  if (min_val == max_val) {
    HistogramBin bin;
    bin.range_start = min_val;
    bin.range_end = max_val;
    bin.count = valid_values.size();
    bins.push_back(bin);
    return bins;
  }

  // Create bins
  double range = max_val - min_val;
  double bin_width = range / num_bins;

  bins.resize(num_bins);
  for (int i = 0; i < num_bins; ++i) {
    bins[i].range_start = min_val + i * bin_width;
    bins[i].range_end = min_val + (i + 1) * bin_width;
    bins[i].count = 0;
  }

  // Count values in each bin
  for (double v : valid_values) {
    int bin_idx = static_cast<int>((v - min_val) / bin_width);
    if (bin_idx >= num_bins) bin_idx = num_bins - 1; // Edge case for max value
    bins[bin_idx].count++;
  }

  return bins;
}

std::vector<std::pair<std::string, double>> GetTopN(
    const std::vector<std::string> &names,
    const std::vector<double> &values,
    int n,
    bool descending) {
  
  std::vector<std::pair<std::string, double>> result;

  if (names.size() != values.size() || names.empty()) {
    return result;
  }

  // Create pairs and filter valid values
  std::vector<std::pair<std::string, double>> pairs;
  pairs.reserve(names.size());
  for (size_t i = 0; i < names.size(); ++i) {
    double v = values[i];
    if (v == v && v > -1e300 && v < 1e300) {
      pairs.emplace_back(names[i], v);
    }
  }

  if (pairs.empty()) {
    return result;
  }

  // Sort
  if (descending) {
    std::sort(pairs.begin(), pairs.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });
  } else {
    std::sort(pairs.begin(), pairs.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });
  }

  // Take top N
  size_t count = std::min(static_cast<size_t>(n), pairs.size());
  result.assign(pairs.begin(), pairs.begin() + count);

  return result;
}

std::map<std::string, double> GroupByBoard(
    const std::vector<std::string> &codes,
    const std::vector<double> &values) {
  
  std::map<std::string, double> result;

  if (codes.size() != values.size()) {
    return result;
  }

  // Group by board
  std::map<std::string, std::vector<double>> board_values;

  for (size_t i = 0; i < codes.size(); ++i) {
    double v = values[i];
    if (v != v || v <= -1e300 || v >= 1e300) continue;

    BoardType board = GetBoardType(codes[i]);
    std::string board_name = GetBoardName(board);

    if (board != BoardType::All && board != BoardType::Unknown) {
      board_values[board_name].push_back(values[i]);
    }
  }

  // Calculate average for each board
  for (const auto &[board_name, vals] : board_values) {
    if (!vals.empty()) {
      double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
      result[board_name] = sum / vals.size();
    }
  }

  return result;
}

std::map<std::string, double> GroupByIndustry(
    const std::vector<std::string> &ind_codes,
    const std::vector<double> &values) {
  
  std::map<std::string, double> result;

  if (ind_codes.size() != values.size()) {
    return result;
  }

  // Group by industry
  std::map<std::string, std::vector<double>> ind_values;

  for (size_t i = 0; i < ind_codes.size(); ++i) {
    double v = values[i];
    if (v != v || v <= -1e300 || v >= 1e300 || ind_codes[i].empty()) continue;
    ind_values[ind_codes[i]].push_back(v);
  }

  // Calculate average for each industry
  for (const auto &[ind_code, vals] : ind_values) {
    if (!vals.empty()) {
      double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
      result[ind_code] = sum / vals.size();
    }
  }

  return result;
}

} // namespace GUI::Database

