// Tab Browser - Calendar Grid View for Trading Days
// Displays dense calendar grid based on stock_days.json

#pragma once

#include "gui/task_database/models/BaostockData.hpp"
#include <string>

namespace GUI::Database {

// Browser state
struct BrowserState {
  int selected_year = -1;
  int selected_month = -1;
  int selected_day = -1;
  std::string hover_date;
};

// Render the browser tab showing calendar grid
void RenderTabBrowser(
    const StockDaysVec &stock_days,
    BrowserState &browser_state);

} // namespace GUI::Database
