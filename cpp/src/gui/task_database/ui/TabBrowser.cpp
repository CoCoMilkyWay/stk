// Tab Browser - Calendar Grid View Implementation
// Dense calendar display for trading day visualization

#include "gui/task_database/ui/TabBrowser.hpp"
#include "imgui.h"
#include <algorithm>
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

constexpr float CELL_SIZE = 12.0f;       // 12px cell
constexpr float BORDER_THICKNESS = 2.0f; // Border thickness in pixels (1/2/3)
constexpr float MONTH_SPACING = 15.0f;   // Space between months

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
    const StockInfoMap &stock_info,
    const Asset &asset_data,
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

  // Step 2: Copy L2 data statistics from precomputed Asset::date_stats
  for (const auto &[date_dense, date_stat] : asset_data.date_stats) {
    auto it = stats_map.find(date_dense);
    if (it != stats_map.end()) {
      it->second.total_assets = date_stat.total_assets;
      it->second.assets_with_orders = date_stat.assets_with_orders;
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

      float factor_curr = std::stod(data[i][1]);
      float factor_prev = std::stod(data[i - 1][1]);

      // Check if there's a significant factor change (dividend/split)
      float ratio = factor_curr / factor_prev;
      if (std::abs(ratio - 1.0) > 0.0001) { // Threshold for detecting events
        auto it = stats_map.find(date_dense);
        if (it != stats_map.end()) {
          auto info_it = stock_info.find(code);
          it->second.dividend_events.push_back(
              {code,
               info_it != stock_info.end() ? info_it->second.name : std::string(),
               ratio});
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

  // Get the day of week for the 1st day of this month
  char first_date[16];
  snprintf(first_date, sizeof(first_date), "%04d%02d%02d", year, month, 1);
  int first_dow = GetDayOfWeek(std::string(first_date)); // 0=Sunday, 1=Monday, ..., 6=Saturday

  // Convert first_dow to Mon=0 format: Sunday=6, Monday=0
  int first_col = (first_dow + 6) % 7;

  // Get starting position for grid
  ImVec2 grid_start = ImGui::GetCursorScreenPos();

  // Get draw list once (outside loop)
  ImDrawList *draw_list = ImGui::GetWindowDrawList();

  // Pre-convert colors to U32 (avoid repeated conversions)
  const ImU32 color_bg_weekday = ImGui::GetColorU32(COLOR_GRAY);
  const ImU32 color_bg_weekend = ImGui::GetColorU32(ImVec4(0.15f, 0.15f, 0.15f, 1.0f));
  const ImU32 color_yellow = ImGui::GetColorU32(COLOR_YELLOW);
  const ImU32 color_purple = ImGui::GetColorU32(COLOR_PURPLE);
  const ImU32 color_green = ImGui::GetColorU32(COLOR_GREEN);
  const ImU32 color_blue = ImGui::GetColorU32(COLOR_BLUE);
  const ImU32 color_hover = ImGui::GetColorU32(COLOR_HOVER);
  const ImU32 color_border_green = ImGui::GetColorU32(BORDER_GREEN);
  const ImU32 color_border_yellow = ImGui::GetColorU32(BORDER_YELLOW);
  const ImU32 color_border_red = ImGui::GetColorU32(BORDER_RED);
  const ImU32 color_border_dark = ImGui::GetColorU32(ImVec4(0.15f, 0.15f, 0.15f, 1.0f));

  // Track maximum row for height calculation
  int max_row = 0;

  // Day grid (up to 31 days, arranged in rows of 7, aligned by day of week)
  for (int day = 1; day <= 31; ++day) {
    // Build date string YYYYMMDD (only once)
    char date_dense[16];
    snprintf(date_dense, sizeof(date_dense), "%04d%02d%02d", year, month, day);
    std::string date_key(date_dense);

    // Check if date is valid
    int dow = GetDayOfWeek(date_key);
    if (dow < 0)
      break; // Invalid date

    // Calculate grid position
    int total_offset = first_col + day - 1;
    int col = total_offset % 7;
    int row = total_offset / 7;
    if (row > max_row)
      max_row = row;

    // Get daily stats
    auto stats_it = daily_stats.find(date_key);
    const DailyStats *stats = (stats_it != daily_stats.end()) ? &stats_it->second : nullptr;

    // Calculate position for dense packing
    ImVec2 cell_pos = ImVec2(
        grid_start.x + col * CELL_SIZE,
        grid_start.y + row * CELL_SIZE);

    // Determine background color: weekend or weekday
    const ImU32 bg_color = (dow == 0 || dow == 6) ? color_bg_weekend : color_bg_weekday;

    // Pre-calculate pixel-aligned coordinates for reuse
    const float x0 = floorf(cell_pos.x);
    const float y0 = floorf(cell_pos.y);
    const float x1 = floorf(cell_pos.x + CELL_SIZE);
    const float y1 = floorf(cell_pos.y + CELL_SIZE);

    // Step 1: Determine border color (shows completeness only when total_assets > 0)
    ImU32 border_color = color_border_dark; // Default: dark gray

    // For trading days with assets and completeness enabled: show green/yellow/red
    // If total_assets == 0, it means this date is outside database range, keep default gray
    if (state.layers.show_completeness && stats && stats->is_trading_day && stats->total_assets > 0) {
      const float completeness = stats->completeness_orders();

      border_color = (completeness >= 0.9999f) ? color_border_green
                     : (completeness >= 0.95f) ? color_border_yellow
                                               : color_border_red;
    }

    // Step 2: Draw border (entire cell first)
    draw_list->AddRectFilled(ImVec2(x0, y0), ImVec2(x1, y1), border_color);

    // Step 3: Determine fill color based on priority (highest visible layer wins)
    // Priority: Yellow > Purple > Green > Blue > Background
    ImU32 fill_color = bg_color;

    if (stats) {
      // Check from lowest to highest priority (last match wins)
      if (state.layers.show_l2_data && stats->assets_with_orders > 0) {
        fill_color = color_blue;
      }

      if (state.layers.show_backtest_range && stats->is_in_backtest_range && stats->is_trading_day) {
        fill_color = color_green;
      }

      if (state.layers.show_holiday && stats->is_holiday) {
        fill_color = color_purple;
      }

      if (state.layers.show_dividend_split && !stats->dividend_events.empty()) {
        fill_color = color_yellow;
      }
    }

    // Step 4: Draw inner fill (inset by BORDER_THICKNESS from all sides)
    draw_list->AddRectFilled(
        ImVec2(x0 + BORDER_THICKNESS, y0 + BORDER_THICKNESS),
        ImVec2(x1 - BORDER_THICKNESS, y1 - BORDER_THICKNESS),
        fill_color);

    // Step 5: Hover detection and tooltip
    const ImVec2 cell_max(x1, y1);
    if (ImGui::IsMouseHoveringRect(cell_pos, cell_max)) {
      state.hover_date = DateToDashed(date_key);
      draw_list->AddRect(cell_pos, cell_max, color_hover, 0.0f, 0, 2.0f);

      // Tooltip
      if (stats) {
        static const char *dow_names[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
        const char *dow_name = (dow >= 0 && dow <= 6) ? dow_names[dow] : "?";

        const float completeness = stats->completeness_orders() * 100.0f;

        ImGui::BeginTooltip();
        ImGui::Text("Date: %s (%s)%s", state.hover_date.c_str(), dow_name,
                    stats->is_trading_day ? " [Trading Day]" : "");
        ImGui::Separator();
        ImGui::Text("Backtest Range: %s", stats->is_in_backtest_range ? "YES" : "NO");
        ImGui::Separator();
        ImGui::Text("L2 Data Coverage: (BJ / 当日停牌 不计入分母)");
        ImGui::Text("  Total Stocks: %zu", stats->total_assets);
        ImGui::Text("  With Orders: %zu (%.1f%%)", stats->assets_with_orders, completeness);
        ImGui::Separator();
        ImGui::Text("Dividend/Split Events: %zu stocks", stats->dividend_events.size());
        // 明细: 代码 + 简称 + 复权因子比值 (>1 = 除权除息, 因子被上调)
        constexpr size_t kMaxListed = 30;
        const size_t listed = std::min(stats->dividend_events.size(), kMaxListed);
        for (size_t k = 0; k < listed; ++k) {
          const DividendEvent &ev = stats->dividend_events[k];
          ImGui::Text("  %s  %s  x%.6f", ev.code.c_str(),
                      ev.name.empty() ? "-" : ev.name.c_str(), ev.ratio);
        }
        if (stats->dividend_events.size() > listed) {
          ImGui::TextDisabled("  ... +%zu more",
                              stats->dividend_events.size() - listed);
        }
        ImGui::EndTooltip();
      } else {
        ImGui::SetTooltip("%s (No data)", state.hover_date.c_str());
      }

      // Handle click
      if (ImGui::IsMouseClicked(0)) {
        state.selected_year = year;
        state.selected_month = month;
        state.selected_day = day;
      }
    }
  }

  // Reserve space for the grid
  float total_height = (max_row + 1) * CELL_SIZE;
  ImGui::SetCursorScreenPos(ImVec2(grid_start.x, grid_start.y + total_height));
  ImGui::Dummy(ImVec2(7 * CELL_SIZE, 0));

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
    const StockInfoMap &stock_info,
    const Asset &asset_data,
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
        stock_days, stock_factors, stock_info, asset_data, backtest_start, backtest_end);
  }

  if (ImGui::Button("Refresh Data")) {
    browser_state.daily_stats_cache.clear();
    browser_state.daily_stats_cache = BuildDailyStats(
        stock_days, stock_factors, stock_info, asset_data, backtest_start, backtest_end);
  }

  // Layer Toggle Buttons
  ImGui::Text("Layers:");
  ImGui::SameLine();

  // Dividend/Split layer (Yellow)
  ImVec4 yellow_btn = browser_state.layers.show_dividend_split ? COLOR_YELLOW : ImVec4(0.3f, 0.3f, 0.1f, 1.0f);
  ImGui::PushStyleColor(ImGuiCol_Button, yellow_btn);
  if (ImGui::SmallButton("除权除息")) {
    browser_state.layers.show_dividend_split = !browser_state.layers.show_dividend_split;
  }
  ImGui::PopStyleColor();
  ImGui::SameLine();

  // Holiday layer (Purple)
  ImVec4 purple_btn = browser_state.layers.show_holiday ? COLOR_PURPLE : ImVec4(0.2f, 0.1f, 0.3f, 1.0f);
  ImGui::PushStyleColor(ImGuiCol_Button, purple_btn);
  if (ImGui::SmallButton("节假日")) {
    browser_state.layers.show_holiday = !browser_state.layers.show_holiday;
  }
  ImGui::PopStyleColor();
  ImGui::SameLine();

  // Backtest range layer (Green)
  ImVec4 green_btn = browser_state.layers.show_backtest_range ? COLOR_GREEN : ImVec4(0.1f, 0.3f, 0.1f, 1.0f);
  ImGui::PushStyleColor(ImGuiCol_Button, green_btn);
  if (ImGui::SmallButton("回测区间")) {
    browser_state.layers.show_backtest_range = !browser_state.layers.show_backtest_range;
  }
  ImGui::PopStyleColor();
  ImGui::SameLine();

  // L2 data layer (Blue)
  ImVec4 blue_btn = browser_state.layers.show_l2_data ? COLOR_BLUE : ImVec4(0.1f, 0.1f, 0.3f, 1.0f);
  ImGui::PushStyleColor(ImGuiCol_Button, blue_btn);
  if (ImGui::SmallButton("L2数据")) {
    browser_state.layers.show_l2_data = !browser_state.layers.show_l2_data;
  }
  ImGui::PopStyleColor();
  ImGui::SameLine();

  // Completeness border toggle
  if (ImGui::SmallButton(browser_state.layers.show_completeness ? "完整性边框✓" : "完整性边框✗")) {
    browser_state.layers.show_completeness = !browser_state.layers.show_completeness;
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
      avg_completeness += stats.completeness_orders();
    }
    total_dividend_events += static_cast<int>(stats.dividend_events.size());
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

  // Legend - Multi-layer Display (Left to Right)
  ImGui::Text("Layers (Left → Right):");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_YELLOW, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("除权除息");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_PURPLE, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("节假日");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_GREEN, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("回测区间");
  ImGui::SameLine(0, 10);
  ImGui::TextColored(COLOR_BLUE, "■");
  ImGui::SameLine(0, 2);
  ImGui::Text("L2数据");

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
