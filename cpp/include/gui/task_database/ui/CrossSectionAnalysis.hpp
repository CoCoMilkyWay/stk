// Cross-section analysis utility functions for table columns
// Provides statistical analysis, histograms, rankings, and board grouping

#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Data Structures
// ============================================================================

struct ColumnStats {
  double min = 0.0;
  double max = 0.0;
  double mean = 0.0;
  double median = 0.0;
  double std_dev = 0.0;
  double q25 = 0.0; // 25th percentile
  double q75 = 0.0; // 75th percentile
  size_t valid_count = 0;
  size_t total_count = 0;
};

struct HistogramBin {
  double range_start = 0.0;
  double range_end = 0.0;
  size_t count = 0;
};

// ============================================================================
// Statistical Analysis Functions
// ============================================================================

// Calculate basic statistics for a column
ColumnStats CalculateColumnStats(const std::vector<double> &values);

// Generate histogram bins for visualization
std::vector<HistogramBin> GenerateHistogram(const std::vector<double> &values, int num_bins = 15);

// Get top N or bottom N items (returns name-value pairs)
std::vector<std::pair<std::string, double>> GetTopN(
    const std::vector<std::string> &names,
    const std::vector<double> &values,
    int n,
    bool descending = true);

// Group values by board type and calculate average per board
std::map<std::string, double> GroupByBoard(
    const std::vector<std::string> &codes,     // Stock codes (e.g. "000785")
    const std::vector<double> &values);

// Group values by industry code and calculate average per industry
std::map<std::string, double> GroupByIndustry(
    const std::vector<std::string> &ind_codes, // Industry codes (e.g. "C26")
    const std::vector<double> &values);

} // namespace GUI::Database

