// Tab Table - L2 Database Asset Table View with Enhanced Filtering
// Shows asset table with board classification and ST filtering

#pragma once

#include "gui/task_database/models/BaostockData.hpp"
#include "gui/task_database/models/L2AssetData.hpp"
#include <string>
#include <vector>

namespace GUI::Database {

// Table state
struct TableState {
  // Filters
  bool filter_missing_only = false;
  bool filter_st_only = false;
  BoardType board_filter = BoardType::All;
  std::string search_query;

  // Selection and sorting
  int selected_asset_idx = -1;
  int sort_column = -1;
  bool sort_ascending = true;
};

// Render the table tab
void RenderTabTable(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state);

} // namespace GUI::Database
