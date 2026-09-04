// Cross-section analysis utility functions for table columns
// Provides statistical analysis, histograms, rankings, and board grouping

#pragma once

#include <string>
#include <utility>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Column Type Classification
// ============================================================================

enum class ColumnDataType {
  Numeric,    // 有序数据: PE, PB, 市值, 交易日数等
  Categorical // 分类数据: 板块, 行业, ST, DL等
};

// ============================================================================
// Data Structures for Numeric Analysis
// ============================================================================

struct ColumnStats {
  float min = 0.0;
  float max = 0.0;
  float mean = 0.0;
  float median = 0.0;
  float std_dev = 0.0;
  float q25 = 0.0; // 25th percentile
  float q75 = 0.0; // 75th percentile
  size_t valid_count = 0;
  size_t total_count = 0;
};

struct BoardStats {
  std::string board_name;
  float mean = 0.0;
  float median = 0.0;
  float std_dev = 0.0;
  size_t count = 0;
};

// ============================================================================
// Data Structures for Categorical Analysis
// ============================================================================

struct CategoryCount {
  std::string label;
  size_t count = 0;
  float percentage = 0.0;
};

struct BoardCategoryBreakdown {
  std::string board_name;
  std::vector<CategoryCount> categories;
};

// ============================================================================
// Numeric Data Analysis Functions
// ============================================================================

// Remove top/bottom 5% outliers and return filtered data
std::vector<float> RemoveOutliers(const std::vector<float> &values, float percentile = 5.0);

// Calculate statistics with outliers removed
ColumnStats CalculateRobustStats(const std::vector<float> &values);

// Group numeric values by board and calculate stats for each board
std::vector<BoardStats> GroupNumericByBoard(
    const std::vector<std::string> &codes,
    const std::vector<float> &values);

// Get top/bottom N rankings
std::vector<std::pair<std::string, float>> GetTopN(
    const std::vector<std::string> &names,
    const std::vector<float> &values,
    int n,
    bool descending = true);

// ============================================================================
// Categorical Data Analysis Functions
// ============================================================================

// Count occurrences of each category
std::vector<CategoryCount> CountCategories(const std::vector<std::string> &categories);

// Group categorical data by board
std::vector<BoardCategoryBreakdown> GroupCategoricalByBoard(
    const std::vector<std::string> &codes,
    const std::vector<std::string> &categories);

} // namespace GUI::Database
