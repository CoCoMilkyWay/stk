// Cross-section analysis implementation

#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "gui/task_database/models/SharedTypes.hpp" // For BoardType
#include <algorithm>
#include <cmath>
#include <map>
#include <numeric>

namespace GUI::Database {

// ============================================================================
// Helper: Filter Valid Values
// ============================================================================

static std::vector<double> FilterValidValues(const std::vector<double> &values) {
  std::vector<double> valid;
  valid.reserve(values.size());
  for (double v : values) {
    if (std::isfinite(v) && v > -1e300 && v < 1e300) {
      valid.push_back(v);
    }
  }
  return valid;
}

// ============================================================================
// Numeric Data Analysis Implementation
// ============================================================================

std::vector<double> RemoveOutliers(const std::vector<double> &values, double percentile) {
  auto valid = FilterValidValues(values);
  if (valid.empty())
    return {};

  std::sort(valid.begin(), valid.end());

  size_t lower_idx = static_cast<size_t>(valid.size() * percentile / 100.0);
  size_t upper_idx = static_cast<size_t>(valid.size() * (100.0 - percentile) / 100.0);

  if (upper_idx > lower_idx && upper_idx < valid.size()) {
    return std::vector<double>(valid.begin() + lower_idx, valid.begin() + upper_idx);
  }

  return valid;
}

ColumnStats CalculateRobustStats(const std::vector<double> &values) {
  ColumnStats stats;
  stats.total_count = values.size();

  auto filtered = RemoveOutliers(values, 5.0); // Remove top/bottom 5%
  stats.valid_count = filtered.size();

  if (filtered.empty()) {
    return stats;
  }

  std::vector<double> sorted = filtered;
  std::sort(sorted.begin(), sorted.end());

  stats.min = sorted.front();
  stats.max = sorted.back();

  double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
  stats.mean = sum / sorted.size();

  size_t mid = sorted.size() / 2;
  stats.median = (sorted.size() % 2 == 0)
                     ? (sorted[mid - 1] + sorted[mid]) / 2.0
                     : sorted[mid];

  size_t q25_idx = sorted.size() / 4;
  size_t q75_idx = (sorted.size() * 3) / 4;
  stats.q25 = sorted[q25_idx];
  stats.q75 = sorted[q75_idx];

  double sq_sum = 0.0;
  for (double v : sorted) {
    sq_sum += (v - stats.mean) * (v - stats.mean);
  }
  stats.std_dev = std::sqrt(sq_sum / sorted.size());

  return stats;
}

std::vector<BoardStats> GroupNumericByBoard(
    const std::vector<std::string> &codes,
    const std::vector<double> &values) {

  std::vector<BoardStats> result;

  if (codes.size() != values.size()) {
    return result;
  }

  // Group by board
  std::map<std::string, std::vector<double>> board_values;

  for (size_t i = 0; i < codes.size(); ++i) {
    double v = values[i];
    if (!std::isfinite(v))
      continue;

    BoardType board = GetBoardType(codes[i]);
    if (board == BoardType::All || board == BoardType::Unknown)
      continue;

    std::string board_name = GetBoardName(board);
    board_values[board_name].push_back(v);
  }

  // Calculate stats for each board
  for (const auto &[board_name, vals] : board_values) {
    if (vals.empty())
      continue;

    BoardStats bs;
    bs.board_name = board_name;
    bs.count = vals.size();

    auto sorted = vals;
    std::sort(sorted.begin(), sorted.end());

    double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
    bs.mean = sum / sorted.size();

    size_t mid = sorted.size() / 2;
    bs.median = (sorted.size() % 2 == 0)
                    ? (sorted[mid - 1] + sorted[mid]) / 2.0
                    : sorted[mid];

    double sq_sum = 0.0;
    for (double v : sorted) {
      sq_sum += (v - bs.mean) * (v - bs.mean);
    }
    bs.std_dev = std::sqrt(sq_sum / sorted.size());

    result.push_back(bs);
  }

  return result;
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

// ============================================================================
// Categorical Data Analysis Implementation
// ============================================================================

std::vector<CategoryCount> CountCategories(const std::vector<std::string> &categories) {
  std::map<std::string, size_t> count_map;
  size_t total = 0;

  for (const auto &cat : categories) {
    if (!cat.empty()) {
      count_map[cat]++;
      total++;
    }
  }

  std::vector<CategoryCount> result;
  for (const auto &[label, count] : count_map) {
    CategoryCount cc;
    cc.label = label;
    cc.count = count;
    cc.percentage = (total > 0) ? (count * 100.0 / total) : 0.0;
    result.push_back(cc);
  }

  // Sort by count descending
  std::sort(result.begin(), result.end(),
            [](const auto &a, const auto &b) { return a.count > b.count; });

  return result;
}

std::vector<BoardCategoryBreakdown> GroupCategoricalByBoard(
    const std::vector<std::string> &codes,
    const std::vector<std::string> &categories) {

  std::vector<BoardCategoryBreakdown> result;

  if (codes.size() != categories.size()) {
    return result;
  }

  // Group by board
  std::map<std::string, std::vector<std::string>> board_categories;

  for (size_t i = 0; i < codes.size(); ++i) {
    if (categories[i].empty())
      continue;

    BoardType board = GetBoardType(codes[i]);
    if (board == BoardType::All || board == BoardType::Unknown)
      continue;

    std::string board_name = GetBoardName(board);
    board_categories[board_name].push_back(categories[i]);
  }

  // Count categories for each board
  for (const auto &[board_name, cats] : board_categories) {
    BoardCategoryBreakdown breakdown;
    breakdown.board_name = board_name;
    breakdown.categories = CountCategories(cats);
    result.push_back(breakdown);
  }

  return result;
}

} // namespace GUI::Database
