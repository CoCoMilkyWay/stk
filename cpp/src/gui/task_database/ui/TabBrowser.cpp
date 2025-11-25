// Tab Browser - Calendar Grid View Implementation
// Dense calendar display for trading day visualization

#include "gui/task_database/ui/TabBrowser.hpp"
#include "imgui.h"
#include <cstdio>
#include <map>
#include <set>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_TRADING = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);    // Green: Trading day
constexpr ImVec4 COLOR_NON_TRADING = ImVec4(0.3f, 0.3f, 0.3f, 1.0f); // Dark gray: Non-trading
constexpr ImVec4 COLOR_SELECTED = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);    // Blue: Selected
constexpr ImVec4 COLOR_HOVER = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);      // Yellow: Hover

constexpr float CELL_SIZE = 8.0f;
constexpr float CELL_SPACING = 2.0f;

// ============================================================================
// Helper: Parse date string YYYY-MM-DD
// ============================================================================

struct DateInfo {
  int year = 0;
  int month = 0;
  int day = 0;

  static DateInfo Parse(const std::string &date_str) {
    DateInfo info;
    if (date_str.length() >= 10) {
      info.year = std::stoi(date_str.substr(0, 4));
      info.month = std::stoi(date_str.substr(5, 2));
      info.day = std::stoi(date_str.substr(8, 2));
    }
    return info;
  }

  bool operator<(const DateInfo &other) const {
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
        DateInfo info = DateInfo::Parse(date);
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
    const CalendarData &calendar,
    BrowserState &state) {
  const char *month_names[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

  ImGui::BeginGroup();

  // Month label
  ImGui::Text("%s", month_names[month - 1]);

  // Day headers (一 二 三 四 五 六 日)
  const char *day_headers[] = {"一", "二", "三", "四", "五", "六", "日"};
  for (int i = 0; i < 7; ++i) {
    ImGui::SameLine(0, CELL_SPACING);
    ImGui::Text("%s", day_headers[i]);
  }

  // Day grid (approximately 31 days, arranged in rows of 7)
  for (int day = 1; day <= 31; ++day) {
    if (day == 1) {
      ImGui::NewLine();
    } else if ((day - 1) % 7 == 0) {
      ImGui::NewLine();
    } else {
      ImGui::SameLine(0, CELL_SPACING);
    }

    // Check if this is a valid trading day
    bool is_trading = calendar.IsTradingDay(year, month, day);
    bool is_selected = (state.selected_year == year &&
                        state.selected_month == month &&
                        state.selected_day == day);

    ImVec4 cell_color = is_trading ? COLOR_TRADING : COLOR_NON_TRADING;
    if (is_selected) {
      cell_color = COLOR_SELECTED;
    }

    // Draw small square
    ImVec2 cursor_pos = ImGui::GetCursorScreenPos();
    ImDrawList *draw_list = ImGui::GetWindowDrawList();
    draw_list->AddRectFilled(
        cursor_pos,
        ImVec2(cursor_pos.x + CELL_SIZE, cursor_pos.y + CELL_SIZE),
        ImGui::GetColorU32(cell_color));

    // Invisible button for interaction
    ImGui::InvisibleButton(
        ("##day" + std::to_string(year) + "_" + std::to_string(month) + "_" + std::to_string(day)).c_str(),
        ImVec2(CELL_SIZE, CELL_SIZE));

    // Handle hover
    if (ImGui::IsItemHovered()) {
      char date_str[16];
      snprintf(date_str, sizeof(date_str), "%04d-%02d-%02d", year, month, day);
      state.hover_date = date_str;

      // Draw hover highlight
      draw_list->AddRect(
          cursor_pos,
          ImVec2(cursor_pos.x + CELL_SIZE, cursor_pos.y + CELL_SIZE),
          ImGui::GetColorU32(COLOR_HOVER),
          0.0f,
          0,
          2.0f);

      // Tooltip
      ImGui::SetTooltip("%s%s", date_str, is_trading ? " (Trading Day)" : "");
    }

    // Handle click
    if (ImGui::IsItemClicked()) {
      state.selected_year = year;
      state.selected_month = month;
      state.selected_day = day;
    }
  }

  ImGui::EndGroup();
}

// ============================================================================
// Helper: Render year row (12 months)
// ============================================================================

void RenderYearRow(
    int year,
    const CalendarData &calendar,
    BrowserState &state) {
  ImGui::Text("%d", year);
  ImGui::SameLine(0, 20);

  ImGui::BeginGroup();
  for (int month = 1; month <= 12; ++month) {
    if (month > 1) {
      ImGui::SameLine(0, 15);
    }
    RenderMonthGrid(year, month, calendar, state);
  }
  ImGui::EndGroup();
}

// ============================================================================
// Main TabBrowser Render Function
// ============================================================================

void RenderTabBrowser(
    const StockDaysVec &stock_days,
    BrowserState &browser_state) {
  // Build calendar from stock_days data
  CalendarData calendar;
  calendar.Build(stock_days);

  if (calendar.start_year == 0) {
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f),
                       "No trading calendar data available. Please update stock_days.json first.");
    return;
  }

  // Header
  ImGui::Text("Trading Calendar: %d to %d", calendar.start_year, calendar.end_year);
  int total_days = 0;
  for (const auto &[year, months] : calendar.trading_days) {
    for (const auto &[month, days] : months) {
      total_days += days.size();
    }
  }
  ImGui::SameLine();
  ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f),
                     "  Total: %d trading days", total_days);

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Legend
  ImGui::BeginGroup();
  ImGui::TextColored(COLOR_TRADING, "■");
  ImGui::SameLine();
  ImGui::Text("Trading Day");
  ImGui::SameLine(0, 20);
  ImGui::TextColored(COLOR_NON_TRADING, "■");
  ImGui::SameLine();
  ImGui::Text("Non-trading");
  if (!browser_state.hover_date.empty()) {
    ImGui::SameLine(0, 20);
    ImGui::Text("Hover: %s", browser_state.hover_date.c_str());
  }
  ImGui::EndGroup();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Calendar grid (scrollable)
  ImGui::BeginChild("CalendarScroll", ImVec2(0, 0), true, ImGuiWindowFlags_HorizontalScrollbar);

  // Render each year as a row
  for (int year = calendar.start_year; year <= calendar.end_year; ++year) {
    RenderYearRow(year, calendar, browser_state);
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
  }

  ImGui::EndChild();
}

} // namespace GUI::Database
