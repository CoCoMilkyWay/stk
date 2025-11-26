// Tab Table - L2 Database Asset Table View Implementation
// 18-column table with enhanced filtering and cross-section analysis panel

#include "gui/task_database/ui/TabTable.hpp"
#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "imgui.h"
#include "implot.h"
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>

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
                          ImGuiTableFlags_SizingFixedFit;

  if (!ImGui::BeginTable("AssetsTable", 19, flags)) {
    return;
  }

  // Setup columns (19 columns) - use auto width (default)
  ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending);
  ImGui::TableSetupColumn("Name");
  ImGui::TableSetupColumn("Exch");
  ImGui::TableSetupColumn("Board");
  ImGui::TableSetupColumn("ST");
  ImGui::TableSetupColumn("DL");
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

  // Safe string to double conversion with extra safety for sorting
  auto safe_stod = [](const std::string &s, double default_val = -1e9) -> double {
    if (s.empty()) return default_val;
    try {
      double val = std::stod(s);
      // Extra safety: check for NaN explicitly before isfinite
      if (val != val) return default_val; // NaN check
      if (!std::isfinite(val)) return default_val;
      return val;
    } catch (const std::invalid_argument&) {
      return default_val;
    } catch (const std::out_of_range&) {
      return default_val;
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
        std::stable_sort(filtered_rows.begin(), filtered_rows.end(),
          [col, ascending, &safe_stod](const AssetRow &a, const AssetRow &b) -> bool {
            try {
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
              case 5: { // DL (Delisted)
                bool a_dl = a.info && a.info->outDate != "" && a.info->outDate != "0";
                bool b_dl = b.info && b.info->outDate != "" && b.info->outDate != "0";
                result = a_dl < b_dl;
                break;
              }
              case 6: { // Listed days
                int a_days = (a.info && !a.info->ipoDate.empty()) ? CalculateDaysSinceIPO(a.info->ipoDate) : 0;
                int b_days = (b.info && !b.info->ipoDate.empty()) ? CalculateDaysSinceIPO(b.info->ipoDate) : 0;
                result = a_days < b_days;
                break;
              }
              case 7: { // Industry
                std::string a_ind = a.info ? a.info->ind_code : "";
                std::string b_ind = b.info ? b.info->ind_code : "";
                result = a_ind < b_ind;
                break;
              }
              case 8: { // PE
                double a_val = a.info ? safe_stod(a.info->peTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->peTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 9: { // PB
                double a_val = a.info ? safe_stod(a.info->pbMRQ) : -1e9;
                double b_val = b.info ? safe_stod(b.info->pbMRQ) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 10: { // PS
                double a_val = a.info ? safe_stod(a.info->psTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->psTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 11: { // PCF
                double a_val = a.info ? safe_stod(a.info->pcfNcfTTM) : -1e9;
                double b_val = b.info ? safe_stod(b.info->pcfNcfTTM) : -1e9;
                result = a_val < b_val;
                break;
              }
              case 12: { // Market Cap
                double a_cap = a.info ? CalculateMarketCap(*a.info) : 0;
                double b_cap = b.info ? CalculateMarketCap(*b.info) : 0;
                result = a_cap < b_cap;
                break;
              }
              case 13: result = a.asset->get_total_trading_days() < b.asset->get_total_trading_days(); break; // Days
              case 14: { // Snap%
                double a_pct = a.asset->get_total_trading_days() > 0 ?
                  (double)a.asset->get_snapshots_encoded_count() / a.asset->get_total_trading_days() : 0;
                double b_pct = b.asset->get_total_trading_days() > 0 ?
                  (double)b.asset->get_snapshots_encoded_count() / b.asset->get_total_trading_days() : 0;
                result = a_pct < b_pct;
                break;
              }
              case 15: { // Order%
                double a_pct = a.asset->get_total_trading_days() > 0 ?
                  (double)a.asset->get_orders_encoded_count() / a.asset->get_total_trading_days() : 0;
                double b_pct = b.asset->get_total_trading_days() > 0 ?
                  (double)b.asset->get_orders_encoded_count() / b.asset->get_total_trading_days() : 0;
                result = a_pct < b_pct;
                break;
              }
              case 16: result = a.asset->get_missing_count() < b.asset->get_missing_count(); break; // Miss
              case 17: result = a.asset->get_total_snapshot_count() < b.asset->get_total_snapshot_count(); break; // Snaps
              case 18: result = a.asset->get_total_order_count() < b.asset->get_total_order_count(); break; // Orders
            }
            
            return ascending ? result : !result;
          } catch (...) {
            // If comparison fails, maintain consistent ordering by comparing addresses
            return &a < &b;
          }
          });
      }
      sort_specs->SpecsDirty = false;
    }
  }

  // Helper lambda to handle column highlight and click (left-click to trigger analysis)
  auto handle_column_click = [&table_state](int col_idx) {
    if (ImGui::IsItemClicked(ImGuiMouseButton_Left)) {
      if (table_state.selected_column_idx == col_idx) {
        table_state.selected_column_idx = -1;
      } else {
        table_state.selected_column_idx = col_idx;
        table_state.show_cross_section_panel = true;
      }
    }
  };

  // Get hovered column for highlight
  int hovered_col = ImGui::TableGetHoveredColumn();

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
    if (hovered_col == 0) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.26f, 0.59f, 0.98f, 0.35f)));
    }
    // Use Text instead of Selectable to allow column click
    if (is_row_selected) {
      ImGui::TextColored(ImVec4(0.3f, 0.8f, 1.0f, 1.0f), "%s", asset.asset_code.c_str());
    } else {
      ImGui::Text("%s", asset.asset_code.c_str());
    }
    handle_column_click(0);

    // Col 1: Name
    ImGui::TableSetColumnIndex(1);
    if (hovered_col == 1) {
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
    if (hovered_col == 2) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    ImGui::TextColored(asset.exchange == "SH" ? COLOR_SH : COLOR_SZ,
                       "%s", asset.exchange.c_str());
    handle_column_click(2);

    // Col 3: Board
    ImGui::TableSetColumnIndex(3);
    if (hovered_col == 3) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    BoardType board = GetBoardType(asset.asset_code);
    ImGui::Text("%s", GetBoardName(board));
    handle_column_click(3);

    // Col 4: ST
    ImGui::TableSetColumnIndex(4);
    if (hovered_col == 4) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && info->isST == "1") {
      ImGui::TextColored(COLOR_RED, "ST");
    } else {
      ImGui::Text("-");
    }
    handle_column_click(4);

    // Col 5: DL (Delisted - 退市)
    ImGui::TableSetColumnIndex(5);
    if (hovered_col == 5) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->outDate.empty() && info->outDate != "0") {
      ImGui::TextColored(COLOR_GRAY, "DL");
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("Delisted: %s", info->outDate.c_str());
      }
    } else {
      ImGui::Text("-");
    }
    handle_column_click(5);

    // Col 6: Listed (days)
    ImGui::TableSetColumnIndex(6);
    if (hovered_col == 6) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->ipoDate.empty()) {
      int days = CalculateDaysSinceIPO(info->ipoDate);
      ImGui::Text("%d", days);
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(6);

    // Col 7: Industry
    ImGui::TableSetColumnIndex(7);
    if (hovered_col == 7) {
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
    handle_column_click(7);

    // Col 8: PE(TTM)
    ImGui::TableSetColumnIndex(8);
    if (hovered_col == 8) {
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
    handle_column_click(8);

    // Col 9: PB(MRQ)
    ImGui::TableSetColumnIndex(9);
    if (hovered_col == 9) {
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
    handle_column_click(9);

    // Col 10: PS(TTM)
    ImGui::TableSetColumnIndex(10);
    if (hovered_col == 10) {
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
    handle_column_click(10);

    // Col 11: PCF
    ImGui::TableSetColumnIndex(11);
    if (hovered_col == 11) {
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
    handle_column_click(11);

    // Col 12: Market Cap (billion yuan)
    ImGui::TableSetColumnIndex(12);
    if (hovered_col == 12) {
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
    handle_column_click(12);

    // Col 13: Trading Days
    ImGui::TableSetColumnIndex(13);
    if (hovered_col == 13) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_days = asset.get_total_trading_days();
    ImGui::Text("%zu", total_days);
    handle_column_click(13);

    // Col 14: Snapshots Encoded %
    ImGui::TableSetColumnIndex(14);
    if (hovered_col == 14) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t snap_encoded = asset.get_snapshots_encoded_count();
    double snap_pct = total_days > 0 ? (double)snap_encoded / total_days * 100.0 : 0.0;
    ImVec4 snap_color = snap_pct >= 95.0 ? COLOR_GREEN : (snap_pct >= 90.0 ? COLOR_YELLOW : COLOR_RED);
    ImGui::TextColored(snap_color, "%.1f%%", snap_pct);
    handle_column_click(14);

    // Col 15: Orders Encoded %
    ImGui::TableSetColumnIndex(15);
    if (hovered_col == 15) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t ord_encoded = asset.get_orders_encoded_count();
    double ord_pct = total_days > 0 ? (double)ord_encoded / total_days * 100.0 : 0.0;
    ImVec4 ord_color = ord_pct >= 95.0 ? COLOR_GREEN : (ord_pct >= 90.0 ? COLOR_YELLOW : COLOR_RED);
    ImGui::TextColored(ord_color, "%.1f%%", ord_pct);
    handle_column_click(15);

    // Col 16: Missing Days
    ImGui::TableSetColumnIndex(16);
    if (hovered_col == 16) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t missing = asset.get_missing_count();
    ImGui::TextColored(missing > 0 ? COLOR_YELLOW : COLOR_GREEN, "%zu", missing);
    handle_column_click(16);

    // Col 17: Total Snapshots
    ImGui::TableSetColumnIndex(17);
    if (hovered_col == 17) {
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
    handle_column_click(17);

    // Col 18: Total Orders
    ImGui::TableSetColumnIndex(18);
    if (hovered_col == 18) {
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
    handle_column_click(18);

    ImGui::PopID();
    row_idx++;
  }

  ImGui::EndTable();
}

// ============================================================================
// Forward declarations
// ============================================================================

static void RenderNumericAnalysis(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name);

static void RenderCategoricalAnalysis(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name);

// ============================================================================
// Helper: Determine column data type
// ============================================================================

static ColumnDataType GetColumnDataType(int col_idx) {
  // Categorical: Board(3), ST(4), DL(5), Industry(7)
  if (col_idx == 3 || col_idx == 4 || col_idx == 5 || col_idx == 7) {
    return ColumnDataType::Categorical;
  }
  // Numeric: Listed Days(6), PE(8), PB(9), PS(10), PCF(11), Market Cap(12),
  //          Trading Days(13), Snapshot%(14), Order%(15), Missing(16),
  //          Total Snapshots(17), Total Orders(18)
  if (col_idx >= 6 && col_idx <= 18) {
    return ColumnDataType::Numeric;
  }
  // Others (Code, Name, Exchange) not analyzable
  return ColumnDataType::Categorical; // Default
}

// ============================================================================
// Helper: Render cross-section analysis panel
// ============================================================================

void RenderCrossSectionPanel(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state) {
  
  if (table_state.selected_column_idx < 0) {
    ImGui::TextWrapped("Click on any cell to view cross-section analysis.");
    return;
  }

  // Column names for display
  const char *col_names[] = {
    "Code", "Name", "Exchange", "Board", "ST", "DL", "Listed Days", "Industry",
    "PE(TTM)", "PB(MRQ)", "PS(TTM)", "PCF", "Market Cap", "Trading Days",
    "Snapshot %", "Order %", "Missing", "Total Snapshots", "Total Orders"
  };

  int col_idx = table_state.selected_column_idx;
  if (col_idx >= 19) {
    ImGui::Text("Invalid column index");
    return;
  }

  ImGui::Text("Column: %s", col_names[col_idx]);
  ImGui::Separator();

  ColumnDataType data_type = GetColumnDataType(col_idx);

  if (data_type == ColumnDataType::Categorical) {
    RenderCategoricalAnalysis(assets, stock_info, table_state, col_idx, col_names[col_idx]);
  } else {
    RenderNumericAnalysis(assets, stock_info, table_state, col_idx, col_names[col_idx]);
  }
}

// ============================================================================
// Numeric Column Analysis
// ============================================================================

static void RenderNumericAnalysis(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused

  // Extract numeric data (only for filtered assets)
  std::vector<std::string> names;
  std::vector<double> values;
  std::vector<std::string> codes;

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

    std::string display_name = info && !info->name.empty() ? info->name : asset.asset_code;
    double value = std::numeric_limits<double>::quiet_NaN();
    bool is_valid = false;

    switch (col_idx) {
      case 6: // Listed Days
        if (info && !info->ipoDate.empty()) {
          value = CalculateDaysSinceIPO(info->ipoDate);
          is_valid = (value > 0);
        }
        break;
      case 8: // PE
        if (info && !info->peTTM.empty()) {
          try { 
            value = std::stod(info->peTTM); 
            is_valid = std::isfinite(value);
          } catch (...) {}
        }
        break;
      case 9: // PB
        if (info && !info->pbMRQ.empty()) {
          try { 
            value = std::stod(info->pbMRQ); 
            is_valid = std::isfinite(value);
          } catch (...) {}
        }
        break;
      case 10: // PS
        if (info && !info->psTTM.empty()) {
          try { 
            value = std::stod(info->psTTM); 
            is_valid = std::isfinite(value);
          } catch (...) {}
        }
        break;
      case 11: // PCF
        if (info && !info->pcfNcfTTM.empty()) {
          try { 
            value = std::stod(info->pcfNcfTTM); 
            is_valid = std::isfinite(value);
          } catch (...) {}
        }
        break;
      case 12: // Market Cap
        if (info) {
          value = CalculateMarketCap(*info);
          is_valid = (value > 0);
        }
        break;
      case 13: // Trading Days
        value = asset.get_total_trading_days();
        is_valid = true;
        break;
      case 14: // Snapshot %
        value = asset.get_total_trading_days() > 0 ?
                (double)asset.get_snapshots_encoded_count() / asset.get_total_trading_days() * 100.0 : 0.0;
        is_valid = true;
        break;
      case 15: // Order %
        value = asset.get_total_trading_days() > 0 ?
                (double)asset.get_orders_encoded_count() / asset.get_total_trading_days() * 100.0 : 0.0;
        is_valid = true;
        break;
      case 16: // Missing
        value = asset.get_missing_count();
        is_valid = true;
        break;
      case 17: // Total Snapshots
        value = asset.get_total_snapshot_count();
        is_valid = true;
        break;
      case 18: // Total Orders
        value = asset.get_total_order_count();
        is_valid = true;
        break;
      default:
        break;
    }

    if (is_valid) {
      names.push_back(display_name);
      values.push_back(value);
      codes.push_back(asset.asset_code);
    }
  }

  if (values.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  // === 1. Board Statistics Table (Compact) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Statistics");
  auto board_stats = GroupNumericByBoard(codes, values);
  
  if (ImGui::BeginTable("BoardStatsTable", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("Board");
    ImGui::TableSetupColumn("Mean");
    ImGui::TableSetupColumn("Median");
    ImGui::TableSetupColumn("StdDev");
    ImGui::TableSetupColumn("Count");
    ImGui::TableHeadersRow();

    for (const auto &bs : board_stats) {
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0); ImGui::Text("%s", bs.board_name.c_str());
      ImGui::TableSetColumnIndex(1); ImGui::Text("%.2f", bs.mean);
      ImGui::TableSetColumnIndex(2); ImGui::Text("%.2f", bs.median);
      ImGui::TableSetColumnIndex(3); ImGui::Text("%.2f", bs.std_dev);
      ImGui::TableSetColumnIndex(4); ImGui::Text("%zu", bs.count);
    }
    ImGui::EndTable();
  }

  ImGui::Spacing();

  // === 2. Distribution Plot (Remove top/bottom 5% outliers) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Distribution (Outliers Removed)");
  auto filtered_values = RemoveOutliers(values, 5.0);
  
  if (!filtered_values.empty() && ImPlot::BeginPlot("##Distribution", ImVec2(-1, 200))) {
    ImPlot::PlotHistogram("##hist", filtered_values.data(), (int)filtered_values.size(), 20);
    ImPlot::EndPlot();
  }

  ImGui::Spacing();

  // === 3. Rankings (Top 10 / Bottom 10) ===
  float half_width = ImGui::GetContentRegionAvail().x * 0.48f;
  
  // Top 10
  ImGui::BeginChild("Top10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(0.4f, 1.0f, 0.4f, 1.0f), "Top 10");
  auto top10 = GetTopN(names, values, 10, true);
  for (size_t i = 0; i < top10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, top10[i].first.c_str(), top10[i].second);
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // Bottom 10
  ImGui::BeginChild("Bottom10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "Bottom 10");
  auto bottom10 = GetTopN(names, values, 10, false);
  for (size_t i = 0; i < bottom10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, bottom10[i].first.c_str(), bottom10[i].second);
  }
  ImGui::EndChild();
}

// ============================================================================
// Categorical Column Analysis
// ============================================================================

static void RenderCategoricalAnalysis(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused
  
  // Extract categorical data
  std::vector<std::string> categories;
  std::vector<std::string> codes;

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

    std::string category;
    switch (col_idx) {
      case 3: // Board
        category = GetBoardName(GetBoardType(asset.asset_code));
        break;
      case 4: // ST
        category = (info && info->isST == "1") ? "ST" : "Normal";
        break;
      case 5: // DL
        category = (info && !info->outDate.empty()) ? "Delisted" : "Active";
        break;
      case 7: // Industry
        category = info ? info->ind_code : "Unknown";
        break;
      default:
        break;
    }

    if (!category.empty()) {
      categories.push_back(category);
      codes.push_back(asset.asset_code);
    }
  }

  if (categories.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  // === 1. Overall Pie Chart ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Overall Distribution");
  auto overall_counts = CountCategories(categories);
  
  if (!overall_counts.empty() && ImPlot::BeginPlot("##OverallPie", ImVec2(-1, 250))) {
    std::vector<const char*> labels;
    std::vector<double> counts;
    for (const auto &cc : overall_counts) {
      labels.push_back(cc.label.c_str());
      counts.push_back((double)cc.count);
    }
    ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.4);
    ImPlot::EndPlot();
  }

  ImGui::Spacing();

  // === 2. Board Breakdown Pie Charts (Multi-column) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Breakdown");
  auto board_breakdown = GroupCategoricalByBoard(codes, categories);

  int charts_per_row = 2;
  float chart_width = ImGui::GetContentRegionAvail().x / charts_per_row - 10;

  for (size_t i = 0; i < board_breakdown.size(); ++i) {
    const auto &breakdown = board_breakdown[i];
    
    if (i % charts_per_row != 0) {
      ImGui::SameLine();
    }

    ImGui::BeginChild(("BoardPie_" + std::to_string(i)).c_str(), ImVec2(chart_width, 220), true);
    ImGui::Text("%s", breakdown.board_name.c_str());
    
    if (!breakdown.categories.empty() && ImPlot::BeginPlot("##BoardPie", ImVec2(-1, 180))) {
      std::vector<const char*> labels;
      std::vector<double> counts;
      for (const auto &cc : breakdown.categories) {
        labels.push_back(cc.label.c_str());
        counts.push_back((double)cc.count);
      }
      ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.35);
      ImPlot::EndPlot();
    }
    
    ImGui::EndChild();
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
