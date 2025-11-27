// Tab Table - L2 Database Asset Table View with Enhanced Filtering
// Shows asset table with board classification and ST filtering

#pragma once

#include "gui/task_database/models/BaostockData.hpp"
#include "shared/Asset.hpp"
#include <string>
#include <vector>

namespace GUI::Database {

// Table state
struct TableState {
  // Filters
  bool filter_missing_only = false;
  bool filter_st_only = false;
  bool filter_no_missing = false;      // Only show assets with no missing data
  bool filter_listed_only = false;     // Only show listed stocks (outDate is empty)
  BoardType board_filter = BoardType::All;
  std::string search_query;
  std::string industry_filter;         // Filter by industry code (e.g. "C26")

  // Selection and sorting
  int selected_asset_idx = -1;
  int selected_column_idx_for_highlight = -1;  // Column to highlight (from body click)
  int sort_column = -1;
  bool sort_ascending = true;

  // Cross-section analysis panel
  int selected_column_idx = -1;        // Selected column for analysis (-1 = none)
  bool show_cross_section_panel = true; // Show right panel
  float table_split_ratio = 0.65f;     // Left/right split ratio (0-1)
};

// Render the table tab
void RenderTabTable(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state);

} // namespace GUI::Database
