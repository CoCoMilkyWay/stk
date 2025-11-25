// Tab Table - L2 Database Asset Table View Implementation
// Enhanced filtering by board type and ST stocks

#include "gui/task_database/ui/TabTable.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "imgui.h"
#include <cstdio>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
constexpr ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
constexpr ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_RED = ImVec4(0.95f, 0.3f, 0.3f, 1.0f);

// ============================================================================
// Helper: Determine board type from asset code
// ============================================================================

BoardType GetBoardType(const std::string &code) {
  if (code.length() < 3)
    return BoardType::All;

  std::string prefix = code.substr(0, 3);

  // Shanghai Main Board
  if (prefix == "600" || prefix == "601" || prefix == "603" || prefix == "605") {
    return BoardType::SH_Main;
  }

  // Shenzhen Main Board
  if (prefix == "000" || prefix == "001" || prefix == "002" ||
      prefix == "003" || prefix == "004") {
    return BoardType::SZ_Main;
  }

  // STAR Board (科创板)
  if (prefix == "688" || prefix == "689") {
    return BoardType::STAR;
  }

  // ChiNext (创业板)
  if (prefix == "300" || prefix == "301" || prefix == "302" || prefix == "309") {
    return BoardType::ChiNext;
  }

  // Beijing Stock Exchange
  if (code.length() >= 2) {
    std::string prefix2 = code.substr(0, 2);
    if (prefix2 == "87" || prefix2 == "88" || prefix2 == "92") {
      return BoardType::BSE;
    }
  }

  return BoardType::All;
}

// ============================================================================
// Helper: Check if asset should be shown based on filters
// ============================================================================

bool ShouldShowAsset(
    const AssetInfo &asset,
    const TableState &state,
    const StockInfoMap &stock_info) {
  // Missing only filter
  if (state.filter_missing_only && asset.get_missing_count() == 0) {
    return false;
  }

  // Board filter
  if (state.board_filter != BoardType::All) {
    BoardType asset_board = GetBoardType(asset.asset_code);
    if (asset_board != state.board_filter) {
      return false;
    }
  }

  // ST filter - check from stock_info data
  if (state.filter_st_only) {
    std::string full_code = asset.exchange + "." + asset.asset_code;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      if (it->second.isST != "1") {
        return false;
      }
    } else {
      // If no stock_info data, don't show when ST filter is on
      return false;
    }
  }

  // Search query
  if (!state.search_query.empty()) {
    if (asset.asset_code.find(state.search_query) != std::string::npos) {
      return true;
    }
    // Search in stock name if available
    std::string full_code = asset.exchange + "." + asset.asset_code;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end() && it->second.name.find(state.search_query) != std::string::npos) {
      return true;
    }
    return false;
  }

  return true;
}

// ============================================================================
// Helper: Render filter bar
// ============================================================================

void RenderFilterBar(TableState &state, size_t visible_count, size_t total_count) {
  // Search box
  static char search_buf[256] = "";
  ImGui::SetNextItemWidth(300.0f);
  if (ImGui::InputTextWithHint("##Search", "🔍 Search code or name...",
                               search_buf, sizeof(search_buf))) {
    state.search_query = search_buf;
  }

  ImGui::SameLine();
  ImGui::Checkbox("Missing Only", &state.filter_missing_only);

  ImGui::SameLine();
  ImGui::Checkbox("ST Only", &state.filter_st_only);

  // Board filter dropdown
  ImGui::SameLine();
  ImGui::SetNextItemWidth(120.0f);
  const char *board_names[] = {"All", "沪市主板", "深市主板", "科创板", "创业板", "北交所"};
  int current_board = static_cast<int>(state.board_filter);
  if (ImGui::Combo("##Board", &current_board, board_names, 6)) {
    state.board_filter = static_cast<BoardType>(current_board);
  }

  // Count shown
  ImGui::SameLine();
  ImGui::Text("  Showing: %zu / %zu", visible_count, total_count);
}

// ============================================================================
// Main TabTable Render Function
// ============================================================================

void RenderTabTable(
    const std::vector<AssetInfo> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state) {
  // Render filter bar
  size_t visible_count = 0;
  for (const auto &asset : assets) {
    if (ShouldShowAsset(asset, table_state, stock_info)) {
      visible_count++;
    }
  }

  RenderFilterBar(table_state, visible_count, assets.size());
  ImGui::Spacing();

  // Render table
  if (ImGui::BeginTable("AssetsTable", 11,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_Resizable)) {

    // Setup columns
    ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed, 80.0f);
    ImGui::TableSetupColumn("Name", ImGuiTableColumnFlags_WidthFixed, 120.0f);
    ImGui::TableSetupColumn("Exchange", ImGuiTableColumnFlags_WidthFixed, 50.0f);
    ImGui::TableSetupColumn("Board", ImGuiTableColumnFlags_WidthFixed, 80.0f);
    ImGui::TableSetupColumn("ST", ImGuiTableColumnFlags_WidthFixed, 40.0f);
    ImGui::TableSetupColumn("Days", ImGuiTableColumnFlags_WidthFixed, 60.0f);
    ImGui::TableSetupColumn("Encoded", ImGuiTableColumnFlags_WidthFixed, 70.0f);
    ImGui::TableSetupColumn("Missing", ImGuiTableColumnFlags_WidthFixed, 70.0f);
    ImGui::TableSetupColumn("%", ImGuiTableColumnFlags_WidthFixed, 50.0f);
    ImGui::TableSetupColumn("Listed", ImGuiTableColumnFlags_WidthFixed, 90.0f);
    ImGui::TableSetupColumn("PE/PB", ImGuiTableColumnFlags_WidthFixed, 100.0f);
    ImGui::TableSetupScrollFreeze(0, 1);
    ImGui::TableHeadersRow();

    // Render rows
    int row_idx = 0;
    for (const auto &asset : assets) {
      if (!ShouldShowAsset(asset, table_state, stock_info))
        continue;

      ImGui::TableNextRow();

      bool is_selected = (table_state.selected_asset_idx == row_idx);
      ImGui::PushID(row_idx);

      // Get stock info if available
      std::string full_code = asset.exchange + "." + asset.asset_code;
      const StockInfo *info = nullptr;
      auto it = stock_info.find(full_code);
      if (it != stock_info.end()) {
        info = &it->second;
      }

      // Code
      ImGui::TableSetColumnIndex(0);
      if (ImGui::Selectable(asset.asset_code.c_str(), is_selected,
                            ImGuiSelectableFlags_SpanAllColumns)) {
        table_state.selected_asset_idx = row_idx;
      }

      // Name
      ImGui::TableSetColumnIndex(1);
      if (info && !info->name.empty()) {
        ImGui::Text("%s", info->name.c_str());
      } else {
        ImGui::Text("%s", asset.get_display_name().c_str());
      }

      // Exchange
      ImGui::TableSetColumnIndex(2);
      ImGui::TextColored(asset.exchange == "SH" ? COLOR_SH : COLOR_SZ,
                         "%s", asset.exchange.c_str());

      // Board
      ImGui::TableSetColumnIndex(3);
      BoardType board = GetBoardType(asset.asset_code);
      ImGui::Text("%s", GetBoardName(board));

      // ST
      ImGui::TableSetColumnIndex(4);
      if (info && info->isST == "1") {
        ImGui::TextColored(COLOR_RED, "ST");
      } else {
        ImGui::Text("-");
      }

      // Days
      ImGui::TableSetColumnIndex(5);
      ImGui::Text("%zu", asset.get_total_trading_days());

      // Encoded
      ImGui::TableSetColumnIndex(6);
      size_t encoded = asset.get_encoded_count();
      ImGui::TextColored(encoded == asset.get_total_trading_days() ? COLOR_GREEN : COLOR_YELLOW,
                         "%zu", encoded);

      // Missing
      ImGui::TableSetColumnIndex(7);
      size_t missing = asset.get_missing_count();
      ImGui::TextColored(missing > 0 ? COLOR_YELLOW : COLOR_GREEN,
                         "%zu", missing);

      // Coverage %
      ImGui::TableSetColumnIndex(8);
      double coverage = asset.get_total_trading_days() > 0 ? (double)encoded / asset.get_total_trading_days() * 100.0 : 0.0;
      ImVec4 pct_color = coverage >= 95.0 ? COLOR_GREEN : (coverage >= 90.0 ? COLOR_YELLOW : COLOR_RED);
      ImGui::TextColored(pct_color, "%.1f", coverage);

      // Listed Date
      ImGui::TableSetColumnIndex(9);
      if (info && !info->ipoDate.empty()) {
        ImGui::Text("%s", info->ipoDate.c_str());
      } else {
        ImGui::Text("-");
      }

      // PE/PB
      ImGui::TableSetColumnIndex(10);
      if (info && !info->peTTM.empty() && !info->pbMRQ.empty()) {
        char pe_pb_text[64];
        snprintf(pe_pb_text, sizeof(pe_pb_text), "%.1f/%.1f",
                 atof(info->peTTM.c_str()), atof(info->pbMRQ.c_str()));
        ImGui::Text("%s", pe_pb_text);
      } else {
        ImGui::Text("-");
      }

      ImGui::PopID();
      row_idx++;
    }

    ImGui::EndTable();
  }
}

} // namespace GUI::Database
