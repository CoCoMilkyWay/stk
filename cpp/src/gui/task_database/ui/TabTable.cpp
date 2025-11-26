// Tab Table - L2 Database Asset Table View Implementation
// 18-column table with enhanced filtering and cross-section analysis panel

#include "gui/task_database/ui/TabTable.hpp"
#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "imgui.h"
#include <chrono>
#include <cmath>
#include <cstdio>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
constexpr ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
constexpr ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_RED = ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_GRAY = ImVec4(0.6f, 0.6f, 0.6f, 1.0f);

// ============================================================================
// Helper: Calculate days since IPO
// ============================================================================

int CalculateDaysSinceIPO(const std::string &ipo_date) {
  if (ipo_date.empty() || ipo_date.length() != 10) return 0;

  // Parse YYYY-MM-DD
  int year = std::stoi(ipo_date.substr(0, 4));
  int month = std::stoi(ipo_date.substr(5, 2));
  int day = std::stoi(ipo_date.substr(8, 2));

  std::tm tm_ipo = {};
  tm_ipo.tm_year = year - 1900;
  tm_ipo.tm_mon = month - 1;
  tm_ipo.tm_mday = day;

  auto ipo_time = std::chrono::system_clock::from_time_t(std::mktime(&tm_ipo));
  auto now = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::hours>(now - ipo_time);
  
  return static_cast<int>(duration.count() / 24);
}

// ============================================================================
// Helper: Calculate market cap (billion yuan)
// ============================================================================

double CalculateMarketCap(const StockInfo &info) {
  // Market cap (billion) = amount (yuan) × 100 / turn (%) / 1e8
  if (info.amount.empty() || info.turn.empty()) return 0.0;
  
  try {
    double amount = std::stod(info.amount);
    double turn = std::stod(info.turn);
    
    if (turn <= 0) return 0.0;
    
    return amount * 100.0 / turn / 1e8;
  } catch (...) {
    return 0.0;
  }
}

// ============================================================================
// Helper: Check if asset should be shown based on filters
// ============================================================================

bool ShouldShowAsset(
    const AssetInfo &asset,
    const TableState &state,
    const StockInfoMap &stock_info) {
  
  // Convert to lowercase format: sh.600000
  std::string exchange_lower = asset.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(),
                 exchange_lower.begin(), ::tolower);
  std::string full_code = exchange_lower + "." + asset.asset_code;
  const StockInfo *info = nullptr;
  auto it = stock_info.find(full_code);
  if (it != stock_info.end()) {
    info = &it->second;
  }

  // Filter: missing only
  if (state.filter_missing_only && asset.get_missing_count() == 0) {
    return false;
  }

  // Filter: no missing (opposite of above)
  if (state.filter_no_missing && asset.get_missing_count() > 0) {
    return false;
  }

  // Filter: ST only
  if (state.filter_st_only) {
    if (!info || info->isST != "1") {
      return false;
    }
  }

  // Filter: listed only (outDate is empty)
  if (state.filter_listed_only) {
    if (!info || !info->outDate.empty()) {
      return false;
    }
  }

  // Filter: board
  if (state.board_filter != BoardType::All) {
    BoardType asset_board = GetBoardType(asset.asset_code);
    if (asset_board != state.board_filter) {
      return false;
    }
  }

  // Filter: industry
  if (!state.industry_filter.empty()) {
    if (!info || info->ind_code != state.industry_filter) {
      return false;
    }
  }

  // Filter: search query
  if (!state.search_query.empty()) {
    if (asset.asset_code.find(state.search_query) != std::string::npos) {
      return true;
    }
    if (info && info->name.find(state.search_query) != std::string::npos) {
      return true;
    }
    return false;
  }

  return true;
}

// ============================================================================
// Helper: Render filter bar
// ============================================================================

void RenderFilterBar(
    TableState &state, 
    size_t visible_count, 
    size_t total_count,
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info) {
  
  // Search box
  static char search_buf[256] = "";
  ImGui::SetNextItemWidth(250.0f);
  if (ImGui::InputTextWithHint("##Search", "Search code/name...",
                               search_buf, sizeof(search_buf))) {
    state.search_query = search_buf;
  }

  ImGui::SameLine();
  ImGui::Checkbox("ST", &state.filter_st_only);

  ImGui::SameLine();
  ImGui::Checkbox("Listed", &state.filter_listed_only);

  ImGui::SameLine();
  ImGui::Checkbox("No Missing", &state.filter_no_missing);

  // Board filter dropdown
  ImGui::SameLine();
  ImGui::SetNextItemWidth(100.0f);
  const char *board_names[] = {"All", "Unknown", "沪主板", "深主板", "科创板", "创业板", "北交所"};
  int current_board = static_cast<int>(state.board_filter);
  if (ImGui::Combo("Board##BoardFilter", &current_board, board_names, 7)) {
    state.board_filter = static_cast<BoardType>(current_board);
  }

  // Industry filter - collect all unique industries
  static std::vector<std::pair<std::string, std::string>> industries; // code, name
  static bool industries_cached = false;
  
  if (!industries_cached) {
    std::map<std::string, std::string> ind_map; // code -> name
    for (const auto &asset : assets) {
      std::string exchange_lower = asset.exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(),
                     exchange_lower.begin(), ::tolower);
      std::string full_code = exchange_lower + "." + asset.asset_code;
      auto it = stock_info.find(full_code);
      if (it != stock_info.end() && !it->second.ind_code.empty()) {
        ind_map[it->second.ind_code] = it->second.ind_name;
      }
    }
    industries.clear();
    industries.emplace_back("", "All Industries");
    for (const auto &[code, name] : ind_map) {
      industries.emplace_back(code, code + " - " + name);
    }
    industries_cached = true;
  }

  ImGui::SameLine();
  ImGui::SetNextItemWidth(150.0f);
  if (ImGui::BeginCombo("Industry##IndFilter", 
      state.industry_filter.empty() ? "All" : state.industry_filter.c_str())) {
    for (const auto &[code, display] : industries) {
      bool is_selected = (state.industry_filter == code);
      if (ImGui::Selectable(display.c_str(), is_selected)) {
        state.industry_filter = code;
      }
      if (is_selected) {
        ImGui::SetItemDefaultFocus();
      }
    }
    ImGui::EndCombo();
  }

  // Count shown
  ImGui::SameLine();
  ImGui::Text("  Showing: %zu / %zu", visible_count, total_count);

  // Panel toggle
  ImGui::SameLine();
  ImGui::SetCursorPosX(ImGui::GetWindowWidth() - 150.0f);
  if (ImGui::Button(state.show_cross_section_panel ? "Hide Panel" : "Show Panel")) {
    state.show_cross_section_panel = !state.show_cross_section_panel;
  }
}

// ============================================================================
// Helper: Render data table (18 columns)
// ============================================================================

void RenderDataTable(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state) {
  
  ImGuiTableFlags flags = ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                          ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollY |
                          ImGuiTableFlags_Resizable | ImGuiTableFlags_Reorderable |
                          ImGuiTableFlags_SizingStretchProp;

  if (!ImGui::BeginTable("AssetsTable", 18, flags)) {
    return;
  }

  // Setup columns (18 columns) - use auto width (default)
  ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending);
  ImGui::TableSetupColumn("Name");
  ImGui::TableSetupColumn("Exch");
  ImGui::TableSetupColumn("Board");
  ImGui::TableSetupColumn("ST");
  ImGui::TableSetupColumn("Listed");
  ImGui::TableSetupColumn("Ind");
  ImGui::TableSetupColumn("PE");
  ImGui::TableSetupColumn("PB");
  ImGui::TableSetupColumn("PS");
  ImGui::TableSetupColumn("PCF");
  ImGui::TableSetupColumn("Cap");
  ImGui::TableSetupColumn("Days");
  ImGui::TableSetupColumn("Snap%");
  ImGui::TableSetupColumn("Order%");
  ImGui::TableSetupColumn("Miss");
  ImGui::TableSetupColumn("Snaps");
  ImGui::TableSetupColumn("Orders");
  
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableHeadersRow();

  // Build filtered asset list for sorting
  struct AssetRow {
    const AssetInfo *asset;
    const StockInfo *info;
    std::string full_code;
  };
  
  std::vector<AssetRow> filtered_rows;
  filtered_rows.reserve(assets.size());
  
  for (const auto &asset : assets) {
    if (!ShouldShowAsset(asset, table_state, stock_info))
      continue;
    
    std::string exchange_lower = asset.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(),
                   exchange_lower.begin(), ::tolower);
    std::string full_code = exchange_lower + "." + asset.asset_code;
    const StockInfo *info = nullptr;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      info = &it->second;
    }
    filtered_rows.push_back({&asset, info, full_code});
  }

  // Safe string to double conversion
  auto safe_stod = [](const std::string &s, double default_val = -1e9) -> double {
    if (s.empty()) return default_val;
    try {
      double val = std::stod(s);
      if (!std::isfinite(val)) return default_val;
      return val;
    } catch (...) {
      return default_val;
    }
  };

  // Apply sorting (restore from table_state if needed)
  if (ImGuiTableSortSpecs* sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty || table_state.sort_column >= 0) {
      int col = table_state.sort_column;
      bool ascending = table_state.sort_ascending;
      
      // Update from ImGui if dirty
      if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
        const auto& spec = sort_specs->Specs[0];
        col = spec.ColumnIndex;
        ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
        table_state.sort_column = col;
        table_state.sort_ascending = ascending;
      }
      
      if (col >= 0) {
        std::sort(filtered_rows.begin(), filtered_rows.end(),
          [col, ascending, &safe_stod](const AssetRow &a, const AssetRow &b) {
            bool result = false;
            
            switch (col) {
              case 0: result = a.asset->asset_code < b.asset->asset_code; break; // Code
              case 1: { // Name
                std::string name_a = a.info && !a.info->name.empty() ? a.info->name : a.asset->asset_code;
                std::string name_b = b.info && !b.info->name.empty() ? b.info->name : b.asset->asset_code;
                result = name_a < name_b;
                break;
              }
              case 2: result = a.asset->exchange < b.asset->exchange; break; // Exchange
              case 3: result = (int)GetBoardType(a.asset->asset_code) < (int)GetBoardType(b.asset->asset_code); break; // Board
              case 4: { // ST
                bool a_st = a.info && a.info->isST == "1";
                bool b_st = b.info && b.info->isST == "1";
                result = a_st < b_st;
                break;
              }
              case 5: { // Listed days
                int a_days = (a.info && !a.info->ipoDate.empty()) ? CalculateDaysSinceIPO(a.info->ipoDate) : 0;
                int b_days = (b.info && !b.info->ipoDate.empty()) ? CalculateDaysSinceIPO(b.info->ipoDate) : 0;
                result = a_days < b_days;
                break;
              }
              case 6: { // Industry
                std::string a_ind = a.info ? a.info->ind_code : "";
                std::string b_ind = b.info ? b.info->ind_code : "";
                result = a_ind < b_ind;
                break;
              }
              case 7: { // PE
                double a_val = a.info ? safe_stod(a.info->peTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->peTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 8: { // PB
                double a_val = a.info ? safe_stod(a.info->pbMRQ) : -1e9;
                double b_val = b.info ? safe_stod(b.info->pbMRQ) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 9: { // PS
                double a_val = a.info ? safe_stod(a.info->psTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->psTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 10: { // PCF
                double a_val = a.info ? safe_stod(a.info->pcfNcfTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->pcfNcfTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 11: { // Market Cap
                double a_cap = a.info ? CalculateMarketCap(*a.info) : 0;
                double b_cap = b.info ? CalculateMarketCap(*b.info) : 0;
                result = a_cap < b_cap;
                break;
              }
              case 12: result = a.asset->get_total_trading_days() < b.asset->get_total_trading_days(); break; // Days
              case 13: { // Snap%
                double a_pct = a.asset->get_total_trading_days() > 0 ?
                  (double)a.asset->get_snapshots_encoded_count() / a.asset->get_total_trading_days() : 0;
                double b_pct = b.asset->get_total_trading_days() > 0 ?
                  (double)b.asset->get_snapshots_encoded_count() / b.asset->get_total_trading_days() : 0;
                result = a_pct < b_pct;
                break;
              }
              case 14: { // Order%
                double a_pct = a.asset->get_total_trading_days() > 0 ?
                  (double)a.asset->get_orders_encoded_count() / a.asset->get_total_trading_days() : 0;
                double b_pct = b.asset->get_total_trading_days() > 0 ?
                  (double)b.asset->get_orders_encoded_count() / b.asset->get_total_trading_days() : 0;
                result = a_pct < b_pct;
                break;
              }
              case 15: result = a.asset->get_missing_count() < b.asset->get_missing_count(); break; // Miss
              case 16: result = a.asset->get_total_snapshot_count() < b.asset->get_total_snapshot_count(); break; // Snaps
              case 17: result = a.asset->get_total_order_count() < b.asset->get_total_order_count(); break; // Orders
            }
            
            return ascending ? result : !result;
          });
      }
      sort_specs->SpecsDirty = false;
    }
  }

  // Helper lambda to handle column highlight and click
  auto handle_column_click = [&table_state](int col_idx) {
    if (ImGui::IsItemClicked(ImGuiMouseButton_Left)) {
      if (table_state.selected_column_idx_for_highlight == col_idx) {
        table_state.selected_column_idx_for_highlight = -1;
        table_state.selected_column_idx = -1;
      } else {
        table_state.selected_column_idx_for_highlight = col_idx;
        table_state.selected_column_idx = col_idx;
        table_state.show_cross_section_panel = true;
      }
    }
  };

  // Render rows
  int row_idx = 0;
  for (const auto &row : filtered_rows) {
    const AssetInfo &asset = *row.asset;
    const StockInfo *info = row.info;

    ImGui::TableNextRow();
    ImGui::PushID(row_idx);
    bool is_row_selected = (table_state.selected_asset_idx == row_idx);

    // Col 0: Code
    ImGui::TableSetColumnIndex(0);
    if (table_state.selected_column_idx_for_highlight == 0) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    bool is_selected = is_row_selected;
    if (ImGui::Selectable(asset.asset_code.c_str(), is_selected,
                          ImGuiSelectableFlags_SpanAllColumns)) {
      table_state.selected_asset_idx = row_idx;
    }
    handle_column_click(0);

    // Col 1: Name
    ImGui::TableSetColumnIndex(1);
    if (table_state.selected_column_idx_for_highlight == 1) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->name.empty()) {
      ImGui::Text("%s", info->name.c_str());
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(1);

    // Col 2: Exchange
    ImGui::TableSetColumnIndex(2);
    if (table_state.selected_column_idx_for_highlight == 2) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    ImGui::TextColored(asset.exchange == "SH" ? COLOR_SH : COLOR_SZ,
                       "%s", asset.exchange.c_str());
    handle_column_click(2);

    // Col 3: Board
    ImGui::TableSetColumnIndex(3);
    if (table_state.selected_column_idx_for_highlight == 3) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    BoardType board = GetBoardType(asset.asset_code);
    ImGui::Text("%s", GetBoardName(board));
    handle_column_click(3);

    // Col 4: ST
    ImGui::TableSetColumnIndex(4);
    if (table_state.selected_column_idx_for_highlight == 4) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && info->isST == "1") {
      ImGui::TextColored(COLOR_RED, "ST");
    } else {
      ImGui::Text("-");
    }
    handle_column_click(4);

    // Col 5: Listed (days)
    ImGui::TableSetColumnIndex(5);
    if (table_state.selected_column_idx_for_highlight == 5) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->ipoDate.empty()) {
      int days = CalculateDaysSinceIPO(info->ipoDate);
      ImGui::Text("%d", days);
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(5);

    // Col 6: Industry
    ImGui::TableSetColumnIndex(6);
    if (table_state.selected_column_idx_for_highlight == 6) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->ind_code.empty()) {
      ImGui::Text("%s", info->ind_code.c_str());
      if (ImGui::IsItemHovered() && !info->ind_name.empty()) {
        ImGui::SetTooltip("%s", info->ind_name.c_str());
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(6);

    // Col 7: PE(TTM)
    ImGui::TableSetColumnIndex(7);
    if (table_state.selected_column_idx_for_highlight == 7) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->peTTM.empty()) {
      try {
        double pe = std::stod(info->peTTM);
        if (std::isfinite(pe)) {
          ImGui::Text("%.1f", pe);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(7);

    // Col 8: PB(MRQ)
    ImGui::TableSetColumnIndex(8);
    if (table_state.selected_column_idx_for_highlight == 8) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->pbMRQ.empty()) {
      try {
        double pb = std::stod(info->pbMRQ);
        if (std::isfinite(pb)) {
          ImGui::Text("%.2f", pb);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(8);

    // Col 9: PS(TTM)
    ImGui::TableSetColumnIndex(9);
    if (table_state.selected_column_idx_for_highlight == 9) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->psTTM.empty()) {
      try {
        double ps = std::stod(info->psTTM);
        if (std::isfinite(ps)) {
          ImGui::Text("%.2f", ps);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(9);

    // Col 10: PCF
    ImGui::TableSetColumnIndex(10);
    if (table_state.selected_column_idx_for_highlight == 10) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->pcfNcfTTM.empty()) {
      try {
        double pcf = std::stod(info->pcfNcfTTM);
        if (std::isfinite(pcf)) {
          ImGui::Text("%.1f", pcf);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(10);

    // Col 11: Market Cap (billion yuan)
    ImGui::TableSetColumnIndex(11);
    if (table_state.selected_column_idx_for_highlight == 11) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info) {
      double cap = CalculateMarketCap(*info);
      if (cap > 0) {
        ImGui::Text("%.1f", cap);
      } else {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(11);

    // Col 12: Trading Days
    ImGui::TableSetColumnIndex(12);
    if (table_state.selected_column_idx_for_highlight == 12) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_days = asset.get_total_trading_days();
    ImGui::Text("%zu", total_days);
    handle_column_click(12);

    // Col 13: Snapshots Encoded %
    ImGui::TableSetColumnIndex(13);
    if (table_state.selected_column_idx_for_highlight == 13) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t snap_encoded = asset.get_snapshots_encoded_count();
    double snap_pct = total_days > 0 ? (double)snap_encoded / total_days * 100.0 : 0.0;
    ImVec4 snap_color = snap_pct >= 95.0 ? COLOR_GREEN : (snap_pct >= 90.0 ? COLOR_YELLOW : COLOR_RED);
    ImGui::TextColored(snap_color, "%.1f%%", snap_pct);
    handle_column_click(13);

    // Col 14: Orders Encoded %
    ImGui::TableSetColumnIndex(14);
    if (table_state.selected_column_idx_for_highlight == 14) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t ord_encoded = asset.get_orders_encoded_count();
    double ord_pct = total_days > 0 ? (double)ord_encoded / total_days * 100.0 : 0.0;
    ImVec4 ord_color = ord_pct >= 95.0 ? COLOR_GREEN : (ord_pct >= 90.0 ? COLOR_YELLOW : COLOR_RED);
    ImGui::TextColored(ord_color, "%.1f%%", ord_pct);
    handle_column_click(14);

    // Col 15: Missing Days
    ImGui::TableSetColumnIndex(15);
    if (table_state.selected_column_idx_for_highlight == 15) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t missing = asset.get_missing_count();
    ImGui::TextColored(missing > 0 ? COLOR_YELLOW : COLOR_GREEN, "%zu", missing);
    handle_column_click(15);

    // Col 16: Total Snapshots
    ImGui::TableSetColumnIndex(16);
    if (table_state.selected_column_idx_for_highlight == 16) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_snaps = asset.get_total_snapshot_count();
    if (total_snaps > 1000000) {
      ImGui::Text("%.2fM", total_snaps / 1000000.0);
    } else if (total_snaps > 1000) {
      ImGui::Text("%.1fK", total_snaps / 1000.0);
    } else {
      ImGui::Text("%zu", total_snaps);
    }
    handle_column_click(16);

    // Col 17: Total Orders
    ImGui::TableSetColumnIndex(17);
    if (table_state.selected_column_idx_for_highlight == 17) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_orders = asset.get_total_order_count();
    if (total_orders > 1000000) {
      ImGui::Text("%.2fM", total_orders / 1000000.0);
    } else if (total_orders > 1000) {
      ImGui::Text("%.1fK", total_orders / 1000.0);
    } else {
      ImGui::Text("%zu", total_orders);
    }
    handle_column_click(17);

    ImGui::PopID();
    row_idx++;
  }

  ImGui::EndTable();
}

// ============================================================================
// Helper: Render cross-section analysis panel
// ============================================================================

void RenderCrossSectionPanel(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state) {
  
  if (table_state.selected_column_idx < 0) {
    ImGui::TextWrapped("Click on a column header to view cross-section analysis for that metric.");
    return;
  }

  // Column names for display
  const char *col_names[] = {
    "Code", "Name", "Exchange", "Board", "ST", "Listed Days", "Industry",
    "PE(TTM)", "PB(MRQ)", "PS(TTM)", "PCF", "Market Cap", "Trading Days",
    "Snapshot %", "Order %", "Missing", "Total Snapshots", "Total Orders"
  };

  int col_idx = table_state.selected_column_idx;
  if (col_idx >= 18) {
    ImGui::Text("Invalid column index");
    return;
  }

  ImGui::Text("Analysis: %s", col_names[col_idx]);
  ImGui::Separator();

  // Extract column data (only for filtered assets)
  std::vector<std::string> names;
  std::vector<double> values;
  std::vector<std::string> codes;
  std::vector<std::string> ind_codes;

  for (const auto &asset : assets) {
    if (!ShouldShowAsset(asset, table_state, stock_info))
      continue;

    // Convert to lowercase format: sh.600000
    std::string exchange_lower = asset.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(),
                   exchange_lower.begin(), ::tolower);
    std::string full_code = exchange_lower + "." + asset.asset_code;
    const StockInfo *info = nullptr;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      info = &it->second;
    }

    std::string display_name = info && !info->name.empty() ? info->name : asset.asset_code;
    double value = -999999.0; // Sentinel value for invalid

    // Extract value based on column index
    switch (col_idx) {
      case 5: // Listed Days
        if (info && !info->ipoDate.empty()) {
          value = CalculateDaysSinceIPO(info->ipoDate);
        }
        break;
      case 7: // PE
        if (info && !info->peTTM.empty()) {
          try { value = std::stod(info->peTTM); } catch (...) {}
        }
        break;
      case 8: // PB
        if (info && !info->pbMRQ.empty()) {
          try { value = std::stod(info->pbMRQ); } catch (...) {}
        }
        break;
      case 9: // PS
        if (info && !info->psTTM.empty()) {
          try { value = std::stod(info->psTTM); } catch (...) {}
        }
        break;
      case 10: // PCF
        if (info && !info->pcfNcfTTM.empty()) {
          try { value = std::stod(info->pcfNcfTTM); } catch (...) {}
        }
        break;
      case 11: // Market Cap
        if (info) {
          value = CalculateMarketCap(*info);
        }
        break;
      case 12: // Trading Days
        value = asset.get_total_trading_days();
        break;
      case 13: // Snapshot %
        value = asset.get_total_trading_days() > 0 ?
                (double)asset.get_snapshots_encoded_count() / asset.get_total_trading_days() * 100.0 : 0.0;
        break;
      case 14: // Order %
        value = asset.get_total_trading_days() > 0 ?
                (double)asset.get_orders_encoded_count() / asset.get_total_trading_days() * 100.0 : 0.0;
        break;
      case 15: // Missing
        value = asset.get_missing_count();
        break;
      case 16: // Total Snapshots
        value = asset.get_total_snapshot_count();
        break;
      case 17: // Total Orders
        value = asset.get_total_order_count();
        break;
      default:
        value = -999999.0;
    }

    if (value != -999999.0 && value == value && value > -1e300 && value < 1e300) {
      names.push_back(display_name);
      values.push_back(value);
      codes.push_back(asset.asset_code);
      if (info) {
        ind_codes.push_back(info->ind_code);
      } else {
        ind_codes.push_back("");
      }
    }
  }

  if (values.empty()) {
    ImGui::Text("No valid data for this column");
    return;
  }

  // 1. Basic Statistics
  if (ImGui::CollapsingHeader("Basic Statistics", ImGuiTreeNodeFlags_DefaultOpen)) {
    auto stats = CalculateColumnStats(values);
    ImGui::Text("Valid Samples: %zu / %zu", stats.valid_count, stats.total_count);
    ImGui::Text("Min:     %.2f", stats.min);
    ImGui::Text("Max:     %.2f", stats.max);
    ImGui::Text("Median:  %.2f", stats.median);
    ImGui::Text("Mean:    %.2f", stats.mean);
    ImGui::Text("Std Dev: %.2f", stats.std_dev);
    ImGui::Text("25%%:     %.2f", stats.q25);
    ImGui::Text("75%%:     %.2f", stats.q75);
  }

  // 2. Distribution Histogram
  if (ImGui::CollapsingHeader("Distribution Histogram")) {
    auto histogram = GenerateHistogram(values, 15);
    if (!histogram.empty()) {
      // Find max count for scaling
      size_t max_count = 0;
      for (const auto &bin : histogram) {
        if (bin.count > max_count) max_count = bin.count;
      }

      // Render histogram as text bars
      for (size_t i = 0; i < histogram.size(); ++i) {
        const auto &bin = histogram[i];
        float pct = max_count > 0 ? (float)bin.count / max_count : 0.0f;
        int bar_len = static_cast<int>(pct * 30);
        std::string bar(bar_len, '=');
        ImGui::Text("[%.1f-%.1f]: %s (%zu)", bin.range_start, bin.range_end, bar.c_str(), bin.count);
      }
    }
  }

  // 3. Top/Bottom Rankings
  if (ImGui::CollapsingHeader("Rankings")) {
    auto top10 = GetTopN(names, values, 10, true);
    auto bottom10 = GetTopN(names, values, 10, false);

    ImGui::Text("Top 10:");
    for (size_t i = 0; i < top10.size(); ++i) {
      ImGui::Text("  %zu. %s: %.2f", i + 1, top10[i].first.c_str(), top10[i].second);
    }

    ImGui::Spacing();
    ImGui::Text("Bottom 10:");
    for (size_t i = 0; i < bottom10.size(); ++i) {
      ImGui::Text("  %zu. %s: %.2f", i + 1, bottom10[i].first.c_str(), bottom10[i].second);
    }
  }

  // 4. Board Statistics
  if (ImGui::CollapsingHeader("Board Statistics")) {
    auto board_stats = GroupByBoard(codes, values);
    for (const auto &[board_name, avg_value] : board_stats) {
      ImGui::Text("%s: %.2f", board_name.c_str(), avg_value);
    }
  }
}

// ============================================================================
// Main TabTable Render Function
// ============================================================================

void RenderTabTable(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state) {
  
  // Count visible assets
  size_t visible_count = 0;
  for (const auto &asset : assets) {
    if (ShouldShowAsset(asset, table_state, stock_info)) {
      visible_count++;
    }
  }

  // Render filter bar
  RenderFilterBar(table_state, visible_count, assets.size(), assets, stock_info);
  ImGui::Spacing();

  // Get window dimensions
  float window_width = ImGui::GetContentRegionAvail().x;
  float window_height = ImGui::GetContentRegionAvail().y;

  // Calculate left table width
  float left_width = table_state.show_cross_section_panel ?
                     window_width * table_state.table_split_ratio :
                     window_width;

  // Left: Data Table
  ImGui::BeginChild("LeftTable", ImVec2(left_width, window_height), true,
                    ImGuiWindowFlags_HorizontalScrollbar);
  RenderDataTable(assets, stock_info, table_state);
  ImGui::EndChild();

  // Right: Cross-section Analysis Panel
  if (table_state.show_cross_section_panel) {
    ImGui::SameLine();
    ImGui::BeginChild("RightPanel", ImVec2(0, window_height), true);
    RenderCrossSectionPanel(assets, stock_info, table_state);
    ImGui::EndChild();
  }
}

} // namespace GUI::Database
