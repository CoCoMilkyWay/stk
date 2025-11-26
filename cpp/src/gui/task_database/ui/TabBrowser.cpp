// Tab Browser - Calendar Grid View Implementation
// Dense calendar display for trading day visualization

#include "gui/task_database/ui/TabBrowser.hpp"
#include "imgui.h"
#include <cmath>
#include <cstdio>
#include <ctime>
#include <map>
#include <set>

namespace GUI::Database {

// Color constants - Fill colors
constexpr ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.2f, 1.0f); // Yellow: Dividend/split
constexpr ImVec4 COLOR_BLUE = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);    // Blue: Has L2 data
constexpr ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);  // Green: Backtest trading day
constexpr ImVec4 COLOR_PURPLE = ImVec4(0.7f, 0.3f, 0.9f, 1.0f);  // Purple: Holiday
constexpr ImVec4 COLOR_GRAY = ImVec4(0.3f, 0.3f, 0.3f, 1.0f);    // Gray: Other
constexpr ImVec4 COLOR_HOVER = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);  // Hover highlight

// Border colors - Completeness
constexpr ImVec4 BORDER_GREEN = ImVec4(0.2f, 0.9f, 0.3f, 1.0f);   // 100%
constexpr ImVec4 BORDER_YELLOW = ImVec4(0.95f, 0.9f, 0.2f, 1.0f); // 95-99%
constexpr ImVec4 BORDER_RED = ImVec4(0.95f, 0.2f, 0.2f, 1.0f);    // <95%

constexpr float CELL_SIZE = 12.0f;
constexpr float CELL_SPACING = 0.5f;   // Dense packing horizontally
constexpr float ROW_SPACING = 0.5f;    // Dense packing vertically
constexpr float BORDER_WIDTH = 2.5f;   // Thick border for visibility
constexpr float MONTH_SPACING = 15.0f; // Space between months

// ============================================================================
// Helper: Date conversion utilities
// ============================================================================

// Convert YYYY-MM-DD to YYYYMMDD
std::string DateToDense(const std::string &date_dashed) {
  if (date_dashed.length() < 10)
    return "";
  return date_dashed.substr(0, 4) + date_dashed.substr(5, 2) + date_dashed.substr(8, 2);
}

// Convert YYYYMMDD to YYYY-MM-DD
std::string DateToDashed(const std::string &date_dense) {
  if (date_dense.length() < 8)
    return "";
  return date_dense.substr(0, 4) + "-" + date_dense.substr(4, 2) + "-" + date_dense.substr(6, 2);
}

// Get day of week (0=Sun, 1=Mon, ..., 6=Sat) from YYYYMMDD
int GetDayOfWeek(const std::string &date_dense) {
  if (date_dense.length() < 8)
    return -1;
  int year = std::stoi(date_dense.substr(0, 4));
  int month = std::stoi(date_dense.substr(4, 2));
  int day = std::stoi(date_dense.substr(6, 2));

  std::tm time_in = {};
  time_in.tm_year = year - 1900;
  time_in.tm_mon = month - 1;
  time_in.tm_mday = day;
  std::mktime(&time_in);
  return time_in.tm_wday;
}

// ============================================================================
// Build Daily Statistics from all data sources
// ============================================================================

std::map<std::string, DailyStats> BuildDailyStats(
    const StockDaysVec &stock_days,
    const StockFactorMap &stock_factors,
    const std::vector<AssetInfo> &assets,
    const std::string &backtest_start,
    const std::string &backtest_end) {

  std::map<std::string, DailyStats> stats_map;
  
  // Convert backtest dates from YYYY-MM-DD to YYYYMMDD for comparison
  std::string backtest_start_dense = DateToDense(backtest_start);
  std::string backtest_end_dense = DateToDense(backtest_end);

  // Step 1: Initialize from stock_days (trading calendar)
  for (const auto &day_info : stock_days) {
    if (day_info.size() < 2)
      continue;

    const std::string &date_dashed = day_info[0]; // YYYY-MM-DD
    const std::string &is_trading = day_info[1];
    std::string date_dense = DateToDense(date_dashed);

    if (date_dense.empty())
      continue;

    DailyStats &stats = stats_map[date_dense];
    stats.date_str = date_dense;
    stats.is_trading_day = (is_trading == "1");

    // Detect holidays: non-trading weekdays (Mon-Fri)
    int dow = GetDayOfWeek(date_dense);
    if (!stats.is_trading_day && dow >= 1 && dow <= 5) {
      stats.is_holiday = true;
    }

    // Mark backtest range
    if (date_dense >= backtest_start_dense && date_dense <= backtest_end_dense) {
      stats.is_in_backtest_range = true;
    }
  }

  // Step 2: Count L2 data availability per date from assets
  for (const auto &asset : assets) {
    for (const auto &[date_dense, date_info] : asset.date_info) {
      auto it = stats_map.find(date_dense);
      if (it == stats_map.end())
        continue;

      DailyStats &stats = it->second;
      stats.total_assets++;

      bool has_snapshots = date_info.snapshots_encoded;
      bool has_orders = date_info.orders_encoded;

      if (has_snapshots)
        stats.assets_with_snapshots++;
      if (has_orders)
        stats.assets_with_orders++;
      if (has_snapshots && has_orders)
        stats.assets_with_both++;
    }
  }

  // Step 3: Detect dividend/split events from stock_factors
  // Compare factor[i] / factor[i-1], if ratio != 1.0, event occurred
  for (const auto &[code, factor_data] : stock_factors) {
    const auto &data = factor_data.data;
    for (size_t i = 1; i < data.size(); ++i) {
      if (data[i].size() < 2 || data[i - 1].size() < 2)
        continue;

      // Convert date from YYYY-MM-DD to YYYYMMDD
      const std::string &date_dashed = data[i][0];
      std::string date_dense = DateToDense(date_dashed);
      
      if (date_dense.empty())
        continue;

      double factor_curr = std::stod(data[i][1]);
      double factor_prev = std::stod(data[i - 1][1]);

      // Check if there's a significant factor change (dividend/split)
      double ratio = std::abs(factor_curr / factor_prev - 1.0);
      if (ratio > 0.0001) { // Threshold for detecting events
        auto it = stats_map.find(date_dense);
        if (it != stats_map.end()) {
          it->second.dividend_split_count++;
        }
      }
    }
  }

  return stats_map;
}

// ============================================================================
// Helper: Parse date string YYYY-MM-DD
// ============================================================================

struct ParsedDate {
  int year = 0;
  int month = 0;
  int day = 0;

  static ParsedDate Parse(const std::string &date_str) {
    ParsedDate info;
    if (date_str.length() >= 10) {
      info.year = std::stoi(date_str.substr(0, 4));
      info.month = std::stoi(date_str.substr(5, 2));
      info.day = std::stoi(date_str.substr(8, 2));
    }
    return info;
  }

  bool operator<(const ParsedDate &other) const {
    if (year != other.year)
      return year < other.year;
    if (month != other.month)
      return month < other.month;
    return day < other.day;
  }
};

// ============================================================================
// Helper: Build calendar structure from stock_days data
// ============================================================================

struct CalendarData {
  std::map<int, std::map<int, std::set<int>>> trading_days; // year -> month -> days
  int start_year = 0;
  int end_year = 0;

  void Build(const std::vector<std::vector<std::string>> &stock_days) {
    trading_days.clear();

    if (stock_days.empty())
      return;

    // Process all dates
    for (const auto &day_info : stock_days) {
      if (day_info.size() < 2)
        continue;

      const std::string &date = day_info[0];
      const std::string &is_trading = day_info[1];

      if (is_trading == "1") { // Only trading days
        ParsedDate info = ParsedDate::Parse(date);
        if (info.year > 0) {
          trading_days[info.year][info.month].insert(info.day);
        }
      }
    }

    // Get year range
    if (!trading_days.empty()) {
      start_year = trading_days.begin()->first;
      end_year = trading_days.rbegin()->first;
    }
  }

  bool IsTradingDay(int year, int month, int day) const {
    auto year_it = trading_days.find(year);
    if (year_it == trading_days.end())
      return false;

    auto month_it = year_it->second.find(month);
    if (month_it == year_it->second.end())
      return false;

    return month_it->second.find(day) != month_it->second.end();
  }
};

// ============================================================================
// Helper: Render a single month calendar grid
// ============================================================================

void RenderMonthGrid(
    int year,
    int month,
    const std::map<std::string, DailyStats> &daily_stats,
    BrowserState &state) {
  const char *month_names[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

  ImGui::BeginGroup();

  // Month label and day headers in one line
  ImGui::Text("%s 一...日", month_names[month - 1]);

  // Get starting position for grid
  ImVec2 grid_start = ImGui::GetCursorScreenPos();
  
  // Day grid (approximately 31 days, arranged in rows of 7)
  for (int day = 1; day <= 31; ++day) {
    int col = (day - 1) % 7;
    int row = (day - 1) / 7;
    
    // Calculate position for dense packing
    ImVec2 cell_pos = ImVec2(
        grid_start.x + col * (CELL_SIZE + CELL_SPACING),
        grid_start.y + row * (CELL_SIZE + ROW_SPACING));
    
    ImGui::SetCursorScreenPos(cell_pos);

    // Build date string YYYYMMDD
    char date_dense[16];
    snprintf(date_dense, sizeof(date_dense), "%04d%02d%02d", year, month, day);
    std::string date_key(date_dense);

    // Get daily stats
    auto stats_it = daily_stats.find(date_key);
    const DailyStats *stats = (stats_it != daily_stats.end()) ? &stats_it->second : nullptr;

    // Determine fill color based on priority (highest to lowest)
    ImVec4 fill_color = COLOR_GRAY; // Default: non-backtest or non-trading

    if (stats) {
      // Priority 4: Has L2 data (lowest)
      bool has_data = false;
      if (state.view_mode == BrowserViewMode::All) {
        has_data = stats->assets_with_both > 0;
      } else if (state.view_mode == BrowserViewMode::Snapshots) {
        has_data = stats->assets_with_snapshots > 0;
      } else { // Orders
        has_data = stats->assets_with_orders > 0;
      }

      if (has_data) {
        fill_color = COLOR_BLUE; // Has L2 data
      }

      // Priority 3: Backtest range trading day (overrides blue)
      if (stats->is_in_backtest_range && stats->is_trading_day) {
        fill_color = COLOR_GREEN;
      }

      // Priority 2: Holiday (overrides green and blue)
      if (stats->is_holiday) {
        fill_color = COLOR_PURPLE;
      }

      // Priority 1: Dividend/split events (HIGHEST, overrides all)
      if (stats->dividend_split_count > 0) {
        fill_color = COLOR_YELLOW;
      }
    }

    // Determine border color based on completeness
    ImVec4 border_color = ImVec4(0.15f, 0.15f, 0.15f, 1.0f); // Very dark gray for non-backtest
    if (stats && stats->is_trading_day) {
      float completeness = 0.0f;
      if (state.view_mode == BrowserViewMode::All) {
        completeness = stats->completeness_all();
      } else if (state.view_mode == BrowserViewMode::Snapshots) {
        completeness = stats->completeness_snapshots();
      } else {
        completeness = stats->completeness_orders();
      }

      // Only show colored borders for backtest range trading days
      if (stats->is_in_backtest_range) {
        if (completeness >= 0.9999f) // 100% (allowing floating point precision)
          border_color = BORDER_GREEN;
        else if (completeness >= 0.95f) // 95-99.9%
          border_color = BORDER_YELLOW;
        else // <95%
          border_color = BORDER_RED;
      } else {
        // Non-backtest trading days: subtle border
        border_color = ImVec4(0.4f, 0.4f, 0.4f, 1.0f);
      }
    }

    // Draw cell
    ImVec2 cursor_pos = ImGui::GetCursorScreenPos();
    ImDrawList *draw_list = ImGui::GetWindowDrawList();

    // Fill
    draw_list->AddRectFilled(
        cursor_pos,
        ImVec2(cursor_pos.x + CELL_SIZE, cursor_pos.y + CELL_SIZE),
        ImGui::GetColorU32(fill_color));

    // Border
    draw_list->AddRect(
        cursor_pos,
        ImVec2(cursor_pos.x + CELL_SIZE, cursor_pos.y + CELL_SIZE),
        ImGui::GetColorU32(border_color),
        0.0f,
        0,
        BORDER_WIDTH);

    // Invisible button for interaction
    ImGui::InvisibleButton(
        ("##day" + date_key).c_str(),
        ImVec2(CELL_SIZE, CELL_SIZE));

    // Handle hover
    if (ImGui::IsItemHovered()) {
      state.hover_date = DateToDashed(date_key);

      // Hover highlight
      draw_list->AddRect(
          cursor_pos,
          ImVec2(cursor_pos.x + CELL_SIZE, cursor_pos.y + CELL_SIZE),
          ImGui::GetColorU32(COLOR_HOVER),
          0.0f,
          0,
          2.0f);

      // Enhanced tooltip
      if (stats) {
        float completeness = 0.0f;
        if (state.view_mode == BrowserViewMode::All) {
          completeness = stats->completeness_all() * 100.0f;
        } else if (state.view_mode == BrowserViewMode::Snapshots) {
          completeness = stats->completeness_snapshots() * 100.0f;
        } else {
          completeness = stats->completeness_orders() * 100.0f;
        }

        const char *dow_names[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
        int dow = GetDayOfWeek(date_key);
        const char *dow_name = (dow >= 0 && dow <= 6) ? dow_names[dow] : "?";

        ImGui::BeginTooltip();
        ImGui::Text("Date: %s (%s)%s", state.hover_date.c_str(), dow_name,
                    stats->is_trading_day ? " [Trading Day]" : "");
        ImGui::Separator();
        ImGui::Text("Backtest Range: %s", stats->is_in_backtest_range ? "YES" : "NO");
        ImGui::Separator();
        ImGui::Text("L2 Data Coverage:");
        ImGui::Text("  Total Stocks: %zu", stats->total_assets);
        ImGui::Text("  With Snapshots: %zu (%.1f%%)", stats->assets_with_snapshots,
                    stats->total_assets > 0 ? stats->completeness_snapshots() * 100.0f : 0.0f);
        ImGui::Text("  With Orders: %zu (%.1f%%)", stats->assets_with_orders,
                    stats->total_assets > 0 ? stats->completeness_orders() * 100.0f : 0.0f);
        ImGui::Text("  With Both: %zu (%.1f%%)", stats->assets_with_both,
                    stats->total_assets > 0 ? stats->completeness_all() * 100.0f : 0.0f);
        ImGui::Separator();
        ImGui::Text("Dividend/Split Events: %zu stocks", stats->dividend_split_count);
        ImGui::Separator();

        const char *view_name = (state.view_mode == BrowserViewMode::All)         ? "All"
                                : (state.view_mode == BrowserViewMode::Snapshots) ? "Snapshots"
                                                                                  : "Orders";
        ImGui::Text("Data Completeness: %.1f%% [View: %s]", completeness, view_name);
        ImGui::EndTooltip();
      } else {
        ImGui::SetTooltip("%s (No data)", state.hover_date.c_str());
      }
    }

    // Handle click
    if (ImGui::IsItemClicked()) {
      state.selected_year = year;
      state.selected_month = month;
      state.selected_day = day;
    }
  }

  // Calculate total height: 5 rows (31 days / 7 = 5 rows with some empty cells)
  float total_height = 5 * (CELL_SIZE + ROW_SPACING) - ROW_SPACING;
  ImGui::SetCursorScreenPos(ImVec2(grid_start.x, grid_start.y + total_height));
  ImGui::Dummy(ImVec2(7 * CELL_SIZE + 6 * CELL_SPACING, 0)); // Reserve horizontal space

  ImGui::EndGroup();
}

// ============================================================================
// Helper: Render year row (12 months)
// ============================================================================

void RenderYearRow(
    int year,
    const std::map<std::string, DailyStats> &daily_stats,
    BrowserState &state) {
  ImGui::Text("%d", year);
  ImGui::SameLine(0, 20);

  ImGui::BeginGroup();
  for (int month = 1; month <= 12; ++month) {
    if (month > 1) {
      ImGui::SameLine(0, MONTH_SPACING);
    }
    RenderMonthGrid(year, month, daily_stats, state);
  }
  ImGui::EndGroup();
}

// ============================================================================
// Main TabBrowser Render Function
// ============================================================================

void RenderTabBrowser(
    const StockDaysVec &stock_days,
    const StockFactorMap &stock_factors,
    const std::vector<AssetInfo> &assets,
    const std::string &backtest_start,
    const std::string &backtest_end,
    BrowserState &browser_state) {

  // Build calendar from stock_days data
  CalendarData calendar;
  calendar.Build(stock_days);

  if (calendar.start_year == 0) {
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f),
                       "No trading calendar data available. Please update stock_days.json first.");
    return;
  }

  // Build or reuse daily statistics cache
  if (browser_state.daily_stats_cache.empty()) {
    browser_state.daily_stats_cache = BuildDailyStats(
        stock_days, stock_factors, assets, backtest_start, backtest_end);
  }

  // View Mode Selector and Refresh Button
  ImGui::Text("View Mode:");
  ImGui::SameLine();
  if (ImGui::RadioButton("All", browser_state.view_mode == BrowserViewMode::All)) {
    browser_state.view_mode = BrowserViewMode::All;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("Snapshots", browser_state.view_mode == BrowserViewMode::Snapshots)) {
    browser_state.view_mode = BrowserViewMode::Snapshots;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("Orders", browser_state.view_mode == BrowserViewMode::Orders)) {
    browser_state.view_mode = BrowserViewMode::Orders;
  }
  ImGui::SameLine();
  ImGui::Spacing();
  ImGui::SameLine();
  if (ImGui::Button("Refresh Data")) {
    browser_state.daily_stats_cache.clear();
    browser_state.daily_stats_cache = BuildDailyStats(
        stock_days, stock_factors, assets, backtest_start, backtest_end);
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Calculate summary statistics
  int total_trading_days = 0;
  int total_dividend_events = 0;
  float avg_completeness = 0.0f;
  int backtest_days = 0;

  for (const auto &[date, stats] : browser_state.daily_stats_cache) {
    if (stats.is_trading_day) {
      total_trading_days++;
    }
    if (stats.is_in_backtest_range && stats.is_trading_day) {
      backtest_days++;
      if (browser_state.view_mode == BrowserViewMode::All) {
        avg_completeness += stats.completeness_all();
      } else if (browser_state.view_mode == BrowserViewMode::Snapshots) {
        avg_completeness += stats.completeness_snapshots();
      } else {
        avg_completeness += stats.completeness_orders();
      }
    }
    total_dividend_events += stats.dividend_split_count;
  }

  if (backtest_days > 0) {
    avg_completeness = (avg_completeness / backtest_days) * 100.0f;
  }

  // Summary Bar
  ImGui::Text("[%d-%d] %d trading days | Backtest: [%s to %s] %d days",
              calendar.start_year, calendar.end_year, total_trading_days,
              DateToDashed(backtest_start).c_str(), DateToDashed(backtest_end).c_str(),
              backtest_days);
  ImGui::SameLine();
  ImGui::TextColored(ImVec4(0.3f, 0.95f, 1.0f, 1.0f),
                     " | Avg Completeness: %.1f%% | Dividend Events: %d",
                     avg_completeness, total_dividend_events);

  ImGui::Spacing();

  // Legend - Fill Colors
  ImGui::Text("Fill:");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_YELLOW, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("Dividend/Split (Priority 1)");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_PURPLE, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("Holiday (Priority 2)");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_GREEN, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("Backtest Trading Day (Priority 3)");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_BLUE, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("Has L2 Data (Priority 4)");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_GRAY, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("Other");

  // Legend - Border Colors
  ImGui::Text("Border:");
  ImGui::SameLine();
  ImGui::TextColored(BORDER_GREEN, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("100%%");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(BORDER_YELLOW, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("95-99%%");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(BORDER_RED, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("<95%%");

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Calendar grid (scrollable)
  ImGui::BeginChild("CalendarScroll", ImVec2(0, 0), true, ImGuiWindowFlags_HorizontalScrollbar);

  // Render each year as a row
  for (int year = calendar.start_year; year <= calendar.end_year; ++year) {
    RenderYearRow(year, browser_state.daily_stats_cache, browser_state);
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
  }

  ImGui::EndChild();
}

} // namespace GUI::Database
