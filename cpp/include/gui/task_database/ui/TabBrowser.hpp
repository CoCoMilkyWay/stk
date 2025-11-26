// Tab Browser - Calendar Grid View for Trading Days
// Displays dense calendar grid based on stock_days.json

#pragma once

#include "gui/task_database/models/BaostockData.hpp"
#include "gui/task_database/models/L2AssetData.hpp"
#include <map>
#include <string>
#include <vector>

namespace GUI::Database {

// View mode enum for different data perspectives
enum class BrowserViewMode {
  All,       // Show stocks with both snapshots AND orders
  Snapshots, // Show snapshots coverage
  Orders     // Show orders coverage
};

// Daily statistics aggregated for a single date
struct DailyStats {
  std::string date_str; // YYYYMMDD format
  bool is_trading_day = false;
  bool is_holiday = false; // Non-weekend non-trading day
  bool is_in_backtest_range = false;

  // L2 data coverage
  size_t total_assets = 0; // Total stocks listed on this date
  size_t assets_with_snapshots = 0;
  size_t assets_with_orders = 0;
  size_t assets_with_both = 0; // Both snapshots AND orders

  // Dividend/split events
  size_t dividend_split_count = 0; // Number of stocks with events on this date

  // Completeness metrics (0.0 - 1.0)
  float completeness_all() const {
    return total_assets > 0 ? (float)assets_with_both / total_assets : 0.0f;
  }
  float completeness_snapshots() const {
    return total_assets > 0 ? (float)assets_with_snapshots / total_assets : 0.0f;
  }
  float completeness_orders() const {
    return total_assets > 0 ? (float)assets_with_orders / total_assets : 0.0f;
  }
};

// Browser state
struct BrowserState {
  int selected_year = -1;
  int selected_month = -1;
  int selected_day = -1;
  std::string hover_date;
  BrowserViewMode view_mode = BrowserViewMode::All;
  std::map<std::string, DailyStats> daily_stats_cache; // date -> stats
};

// Render the browser tab showing calendar grid
void RenderTabBrowser(
    const StockDaysVec &stock_days,
    const StockFactorMap &stock_factors,
    const std::vector<AssetInfo> &assets,
    const std::string &backtest_start,
    const std::string &backtest_end,
    BrowserState &browser_state);

} // namespace GUI::Database
