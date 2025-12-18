// Tab Overview - JSON File Maintenance and Crawler Status
// Compact card layout with stage-based crawler monitoring

#include "gui/task_database/ui/TabOverview.hpp"
#include "imgui.h"
#include <cstdio>
#include <map>
#include <string>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_RED = ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_BLUE = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);
constexpr ImVec4 COLOR_CYAN = ImVec4(0.3f, 0.95f, 0.95f, 1.0f);
constexpr ImVec4 COLOR_GRAY = ImVec4(0.6f, 0.6f, 0.6f, 1.0f);

// ============================================================================
// Helper: Render Compact JSON File Card
// ============================================================================

void RenderCompactJsonCard(
    const char *title,
    const JsonFileState &state,
    bool *force_update_clicked,
    bool *force_remove_clicked,
    bool *view_json_clicked,
    bool show_progress) {

  ImGui::PushID(title);

  // Card border with fixed height
  ImGui::BeginChild("Card", ImVec2(0, 100), true, ImGuiWindowFlags_NoScrollbar);

  // ===== Title Row =====
  ImGui::TextUnformatted(title);

  // Integrity status (inline, compact)
  ImGui::SameLine();
  ImGui::TextDisabled("│");
  ImGui::SameLine();

  ImGui::Text("Integrity:");

  // Hover to show integrity check rules
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::TextColored(COLOR_CYAN, "Integrity Check Rules:");
    ImGui::Separator();

    if (std::string(title) == "stock_days.json") {
      ImGui::BulletText("Date range MUST start from L2 database start date");
      ImGui::BulletText("(or earlier) to cover all binary data");
      ImGui::BulletText("Must contain trading date records");
      ImGui::BulletText("Dates must be sorted chronologically");
      ImGui::BulletText("No duplicate dates allowed");
      ImGui::BulletText("Each record: [date, is_trading_day]");
    } else if (std::string(title) == "stock_factor.json") {
      ImGui::BulletText("Each stock: {last_update, data[]}");
      ImGui::BulletText("Factor data must be sorted by date");
      ImGui::BulletText("No duplicate dates per stock");
      ImGui::BulletText("Each record: [date, adjust_factor]");
      ImGui::TextDisabled("  (Missing stocks can be updated individually)");
    } else if (std::string(title) == "stock_info.json") {
      ImGui::BulletText("Must have ALL configured stocks present");
      ImGui::BulletText("Required weekly: name, ipoDate, industry");
      ImGui::BulletText("Required daily: volume, amount, turn, etc");
      ImGui::BulletText("Delisted stocks (outDate set) skip daily check");
      ImGui::TextDisabled("  (Missing any stock triggers full re-query)");
    }

    ImGui::EndTooltip();
  }

  ImGui::SameLine();

  if (state.integrity_passed) {
    ImGui::TextColored(COLOR_GREEN, "✓");
  } else {
    size_t error_count = state.integrity_errors.size();
    size_t warning_count = state.integrity_warnings.size();

    if (error_count > 0) {
      ImGui::TextColored(COLOR_RED, "✗");
      ImGui::SameLine();
      ImGui::Text("%zu error(s)", error_count);
    } else if (warning_count > 0) {
      ImGui::TextColored(COLOR_YELLOW, "⚠");
      ImGui::SameLine();
      ImGui::Text("%zu warning(s)", warning_count);
    } else {
      ImGui::Text("checking...");
    }

    // Hover for details
    if (ImGui::IsItemHovered() && (error_count > 0 || warning_count > 0)) {
      ImGui::BeginTooltip();
      if (error_count > 0) {
        ImGui::TextColored(COLOR_RED, "Errors:");
        for (const auto &err : state.integrity_errors) {
          ImGui::Text("  • %s", err.c_str());
        }
      }
      if (warning_count > 0) {
        if (error_count > 0)
          ImGui::Spacing();
        ImGui::TextColored(COLOR_YELLOW, "Warnings:");
        for (const auto &warn : state.integrity_warnings) {
          ImGui::Text("  • %s", warn.c_str());
        }
      }
      ImGui::EndTooltip();
    }
  }
  ImGui::Separator();

  // ===== Status Row =====
  ImVec4 status_color = COLOR_GRAY;
  const char *status_text = "Unknown";

  switch (state.status) {
  case JsonFileStatus::Idle:
    status_color = COLOR_GRAY;
    status_text = "[Idle]";
    break;
  case JsonFileStatus::Loading:
    status_color = COLOR_BLUE;
    status_text = "[Loading]";
    break;
  case JsonFileStatus::Ready:
    status_color = COLOR_GREEN;
    status_text = "[Ready]";
    break;
  case JsonFileStatus::Outdated:
    status_color = COLOR_YELLOW;
    status_text = "[Outdated]";
    break;
  case JsonFileStatus::Error:
    status_color = COLOR_RED;
    status_text = "[Error]";
    break;
  case JsonFileStatus::Updating:
    status_color = COLOR_BLUE;
    status_text = "[Updating]";
    break;
  }

  ImGui::TextColored(status_color, "%s", status_text);
  ImGui::SameLine();

  auto get_stage_color = [](UpdateStage stage) {
    switch (stage) {
    case UpdateStage::Networking:
      return ImVec4(1.0f, 0.5f, 0.0f, 1.0f); // Orange for networking
    case UpdateStage::UpdatingStockDays:
    case UpdateStage::UpdatingStockFactor:
    case UpdateStage::UpdatingStockInfoWeekly:
    case UpdateStage::UpdatingStockInfoDaily:
      return COLOR_CYAN;
    case UpdateStage::Fetching:
      return COLOR_BLUE;
    case UpdateStage::Saving:
      return COLOR_YELLOW;
    case UpdateStage::CheckingIntegrity:
      return COLOR_GRAY;
    default:
      return COLOR_BLUE;
    }
  };

  // ===== Info Row (compact) =====
  if (state.status == JsonFileStatus::Updating) {
    ImVec4 stage_color = get_stage_color(state.progress.stage);
    if (show_progress) {
      // Show current item being processed
      if (!state.progress.current_item.empty()) {
        ImGui::TextColored(stage_color, "%s", state.progress.current_item.c_str());
        ImGui::SameLine();
        ImGui::TextDisabled("│");
        ImGui::SameLine();
      }

      // Show progress
      ImGui::Text("%.1f%%", state.progress.percentage * 100.0f);
      ImGui::SameLine();
      if (state.progress.total > 0) {
        ImGui::Text("(%zu/%zu)", state.progress.current_index, state.progress.total);
      }
      ImGui::SameLine();
      ImGui::TextDisabled("│");
      ImGui::SameLine();
      ImGui::TextColored(stage_color, "%s", GetStageName(state.progress.stage));

      // Progress bar (with unique ID to avoid sharing)
      ImGui::PushID("progress_bar");
      ImGui::ProgressBar(state.progress.percentage, ImVec2(-1, 0));
      ImGui::PopID();

      // Speed and ETA
      if (state.progress.speed > 0.001) {
        ImGui::Text("Speed: %.1f/s  │  ETA: %ds", state.progress.speed, state.progress.eta_seconds);
      }
    } else {
      if (!state.progress.current_item.empty()) {
        ImGui::TextColored(stage_color, "%s", state.progress.current_item.c_str());
      }
      ImGui::TextColored(stage_color, "%s", GetStageName(state.progress.stage));
      if (state.progress.total > 0) {
        ImGui::Text("%zu/%zu item(s)", state.progress.current_index, state.progress.total);
      }
    }
  } else {
    // Normal stats
    ImGui::Text("%zu stocks", state.stock_count);

    if (state.record_count > 0) {
      ImGui::SameLine();
      ImGui::Text("│ %zu records", state.record_count);
    }

    if (state.trading_days_count > 0) {
      ImGui::SameLine();
      ImGui::Text("│ %zu days", state.trading_days_count);
    }

    if (!state.last_update_time.empty()) {
      ImGui::SameLine();
      ImGui::Text("│ %s", state.last_update_time.c_str());
    }

    // Date range (if available)
    if (!state.date_range_start.empty() && !state.date_range_end.empty()) {
      ImGui::Text("Range: %s ~ %s", state.date_range_start.c_str(), state.date_range_end.c_str());
    }
  }

  // Error message
  if (state.status == JsonFileStatus::Error && !state.error_message.empty()) {
    ImGui::TextColored(COLOR_RED, "Error: %s", state.error_message.c_str());
  }

  ImGui::Spacing();

  // ===== Action Buttons =====
  bool is_updating = (state.status == JsonFileStatus::Updating);
  ImGui::BeginDisabled(is_updating);

  if (ImGui::Button("Trigger Update")) {
    *force_update_clicked = true;
  }
  ImGui::SameLine();
  if (ImGui::Button("Force Remove")) {
    *force_remove_clicked = true;
  }
  ImGui::SameLine();
  if (ImGui::Button("View JSON")) {
    *view_json_clicked = true;
  }

  ImGui::EndDisabled();

  ImGui::EndChild();
  ImGui::PopID();
}

// ============================================================================
// Helper: Render Baostock Crawler Monitor (Compact single-line)
// ============================================================================

void RenderCrawlerMonitor(const CrawlerState &state) {
  ImGui::BeginChild("CrawlerMonitor", ImVec2(0, 120), true);

  const auto &prog = state.progress;

  // ===== Data Source Label =====
  ImGui::TextColored(COLOR_CYAN, "数据源:");
  ImGui::SameLine();
  ImGui::Text("Baostock(证券宝)");

  // Hover for website URL
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::TextColored(COLOR_CYAN, "Baostock - 证券宝");
    ImGui::Separator();
    ImGui::Text("官网: http://www.baostock.com");
    ImGui::Text("数据类型: A股历史行情、财务数据、复权因子");
    ImGui::Text("免费开源、无需token");
    ImGui::EndTooltip();
  }

  // ===== Session Status (colorful) =====
  ImVec4 session_color = COLOR_GRAY;
  const char *session_text = "idle";

  switch (prog.session_status) {
  case BaostockSessionStatus::Idle:
    session_color = COLOR_GRAY;
    session_text = "idle";
    break;
  case BaostockSessionStatus::LoggingIn:
    session_color = ImVec4(1.0f, 0.65f, 0.0f, 1.0f); // Orange
    session_text = "logging in";
    break;
  case BaostockSessionStatus::Active:
    session_color = COLOR_GREEN;
    session_text = "active";
    break;
  case BaostockSessionStatus::LoggingOut:
    session_color = COLOR_YELLOW;
    session_text = "logging out";
    break;
  }

  ImGui::TextColored(COLOR_CYAN, "Status:");
  ImGui::SameLine();
  ImGui::TextColored(session_color, "%s", session_text);

  // ===== Workers (colorful) =====
  ImGui::TextColored(COLOR_CYAN, "Workers:");
  ImGui::SameLine();
  ImGui::TextColored(prog.active_workers > 0 ? COLOR_CYAN : COLOR_GRAY,
                     "%zu/%zu", prog.active_workers, prog.total_workers);

  // ===== Session Query Count (colorful) =====
  ImGui::TextColored(COLOR_CYAN, "Queries:");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_BLUE, "%zu", prog.session_query_count);

  ImGui::EndChild();
}

// ============================================================================
// Helper: Render L2 Database Summary (Expanded details)
// ============================================================================

void RenderL2DatabaseSummary(const Asset &asset_data) {
  ImGui::BeginChild("L2Summary", ImVec2(0, 0), true);

  ImGui::TextColored(COLOR_CYAN, "L2 Database Summary");
  ImGui::Separator();

  // Database range (from binary)
  ImGui::Text("Database Range:");
  ImGui::SameLine();
  if (!asset_data.binary.min_date.empty() && !asset_data.binary.max_date.empty()) {
    ImGui::TextColored(COLOR_BLUE, "%s ~ %s",
                       asset_data.binary.min_date.c_str(),
                       asset_data.binary.max_date.c_str());
    ImGui::SameLine();
    ImGui::TextDisabled("(%zu binary trade days)", asset_data.binary.dates.size());
  } else {
    ImGui::TextDisabled("N/A");
  }

  // Backtest range (from backtest coverage)
  ImGui::Text("Backtest Range:");
  ImGui::SameLine();
  if (!asset_data.backtest.start.empty() && !asset_data.backtest.end.empty()) {
    ImGui::TextColored(COLOR_BLUE, "%s ~ %s",
                       asset_data.backtest.start.c_str(),
                       asset_data.backtest.end.c_str());
    ImGui::SameLine();
    ImGui::TextDisabled("(%zu required days, %zu covered)",
                        asset_data.backtest.required_dates.size(),
                        asset_data.backtest.covered_dates.size());
  } else {
    ImGui::TextDisabled("N/A");
  }

  // Backtest missing days
  ImGui::Text("Backtest Error Days:");
  ImGui::SameLine();
  size_t missing_count = asset_data.backtest.missing_dates.size();
  if (missing_count > 0) {
    ImGui::TextColored(COLOR_RED, "%zu", missing_count);

    // Tooltip with missing dates
    if (ImGui::IsItemHovered() && !asset_data.backtest.missing_dates.empty()) {
      ImGui::BeginTooltip();
      ImGui::TextColored(COLOR_RED, "Missing dates in backtest range:");
      ImGui::Separator();
      for (const auto &date : asset_data.backtest.missing_dates) {
        ImGui::TextDisabled("  %s", date.c_str());
      }
      ImGui::EndTooltip();
    }
  } else {
    ImGui::TextColored(COLOR_GREEN, "0");
  }

  // In range check (coverage >= 100%)
  ImGui::Text("In Range:");
  ImGui::SameLine();
  if (!asset_data.backtest.start.empty()) {
    if (asset_data.backtest.coverage_percent >= 100.0) {
      ImGui::TextColored(COLOR_GREEN, "Yes");
    } else {
      ImGui::TextColored(COLOR_RED, "No");
    }
  } else {
    ImGui::TextDisabled("N/A");
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Assets count
  ImGui::Text("Assets:");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_GREEN, "%zu", asset_data.binary.total_assets);

  ImGui::Spacing();

  // Snapshots: count + size
  ImGui::Text("Snapshots Encoded:");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_CYAN, "%zu", asset_data.binary.total_snapshots);
  ImGui::SameLine();
  ImGui::TextDisabled("(%.2f GB, whole database)", asset_data.binary.snapshots_size_gb);

  // Orders: count + size
  ImGui::Text("Orders Encoded:");
  ImGui::SameLine();
  ImGui::TextColored(COLOR_CYAN, "%zu", asset_data.binary.total_orders);
  ImGui::SameLine();
  ImGui::TextDisabled("(%.2f GB, whole database)", asset_data.binary.orders_size_gb);

  ImGui::Spacing();

  // Missing data per asset (calculated on-the-fly from asset_data.items)
  size_t assets_missing_snapshots = 0;
  size_t assets_missing_orders = 0;
  std::map<std::string, std::vector<std::string>> missing_snapshots_by_asset;
  std::map<std::string, std::vector<std::string>> missing_orders_by_asset;

  // Only calculate if backtest range is set
  if (!asset_data.backtest.required_dates.empty()) {
    for (const auto &item : asset_data.items) {
      std::vector<std::string> missing_snapshots;
      std::vector<std::string> missing_orders;

      for (const auto &date : asset_data.backtest.required_dates) {
        auto it = item.date_info.find(date);
        if (it != item.date_info.end()) {
          if (!it->second.snapshots_encoded) {
            missing_snapshots.push_back(date);
          }
          if (!it->second.orders_encoded) {
            missing_orders.push_back(date);
          }
        } else {
          // Date not in this asset's date_info
          missing_snapshots.push_back(date);
          missing_orders.push_back(date);
        }
      }

      if (!missing_snapshots.empty()) {
        assets_missing_snapshots++;
        missing_snapshots_by_asset[item.asset_code] = missing_snapshots;
      }
      if (!missing_orders.empty()) {
        assets_missing_orders++;
        missing_orders_by_asset[item.asset_code] = missing_orders;
      }
    }
  }

  // Missing data (always show)
  ImGui::Text("Assets w/ Missing Snapshots:");
  ImGui::SameLine();
  if (assets_missing_snapshots > 0) {
    ImGui::TextColored(COLOR_YELLOW, "%zu", assets_missing_snapshots);
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_DelayNormal | ImGuiHoveredFlags_Stationary) && !missing_snapshots_by_asset.empty()) {
      ImGui::BeginTooltip();
      ImGui::PushTextWrapPos(600.0f);
      ImGui::Text("Missing Snapshots by Asset (in backtest range):");
      ImGui::Separator();

      ImGui::BeginChild("MissingSnapshotsScroll", ImVec2(600, 400), true, ImGuiWindowFlags_AlwaysVerticalScrollbar);
      for (const auto &[asset, dates] : missing_snapshots_by_asset) {
        ImGui::TextColored(COLOR_YELLOW, "\"%s\":", asset.c_str());
        ImGui::SameLine();
        std::string all_dates;
        for (const auto &date : dates) {
          if (!all_dates.empty())
            all_dates += ", ";
          all_dates += date;
        }
        ImGui::TextWrapped("%s", all_dates.c_str());
        ImGui::Spacing();
      }
      ImGui::EndChild();

      ImGui::PopTextWrapPos();
      ImGui::EndTooltip();
    }
  } else {
    ImGui::TextColored(COLOR_GREEN, "0");
  }

  ImGui::Text("Assets w/ Missing Orders:");
  ImGui::SameLine();
  if (assets_missing_orders > 0) {
    ImGui::TextColored(COLOR_YELLOW, "%zu", assets_missing_orders);
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_DelayNormal | ImGuiHoveredFlags_Stationary) && !missing_orders_by_asset.empty()) {
      ImGui::BeginTooltip();
      ImGui::PushTextWrapPos(600.0f);
      ImGui::Text("Missing Orders by Asset (in backtest range):");
      ImGui::Separator();

      ImGui::BeginChild("MissingOrdersScroll", ImVec2(600, 400), true, ImGuiWindowFlags_AlwaysVerticalScrollbar);
      for (const auto &[asset, dates] : missing_orders_by_asset) {
        ImGui::TextColored(COLOR_YELLOW, "\"%s\":", asset.c_str());
        ImGui::SameLine();
        std::string all_dates;
        for (const auto &date : dates) {
          if (!all_dates.empty())
            all_dates += ", ";
          all_dates += date;
        }
        ImGui::TextWrapped("%s", all_dates.c_str());
        ImGui::Spacing();
      }
      ImGui::EndChild();

      ImGui::PopTextWrapPos();
      ImGui::EndTooltip();
    }
  } else {
    ImGui::TextColored(COLOR_GREEN, "0");
  }

  ImGui::EndChild();
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabOverview(
    const JsonFileState &stock_factor_state,
    const JsonFileState &stock_info_state,
    const JsonFileState &stock_days_state,
    const CrawlerState &crawler_state,
    bool *stock_factor_update,
    bool *stock_factor_remove,
    bool *stock_factor_view,
    bool *stock_info_update,
    bool *stock_info_remove,
    bool *stock_info_view,
    bool *stock_days_update,
    bool *stock_days_remove,
    bool *stock_days_view,
    bool *update_all_clicked,
    bool *check_integrity_clicked,
    bool *refresh_scan_clicked,
    bool disable_update_controls,
    bool disable_scan_controls) {

  // Top control buttons
  ImGui::BeginDisabled(disable_update_controls);
  if (ImGui::Button("Update All")) {
    *update_all_clicked = true;
  }
  ImGui::EndDisabled();

  ImGui::SameLine();

  ImGui::BeginDisabled(disable_scan_controls);
  if (ImGui::Button("Scan Assets")) {
    *refresh_scan_clicked = true;
  }
  ImGui::EndDisabled();

  ImGui::SameLine();

  ImGui::BeginDisabled(disable_update_controls);
  if (ImGui::Button("Check Integrity")) {
    *check_integrity_clicked = true;
  }
  ImGui::EndDisabled();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Split layout: 60% left (JSON files), 40% right (crawler + L2)
  ImGui::BeginChild("LeftPanel", ImVec2(ImGui::GetContentRegionAvail().x * 0.6f, 0), false);

  // Render JSON file cards (in update order)
  RenderCompactJsonCard("stock_days.json", stock_days_state, stock_days_update, stock_days_remove, stock_days_view, false);

  ImGui::Spacing();

  RenderCompactJsonCard("stock_factor.json", stock_factor_state, stock_factor_update, stock_factor_remove, stock_factor_view, true);

  ImGui::Spacing();

  RenderCompactJsonCard("stock_info.json", stock_info_state, stock_info_update, stock_info_remove, stock_info_view, true);

  ImGui::EndChild(); // End LeftPanel

  ImGui::SameLine();

  // Right panel: Crawler monitor
  ImGui::BeginChild("RightPanel", ImVec2(0, 0), false);

  // Baostock Crawler (compact single line)
  RenderCrawlerMonitor(crawler_state);

  ImGui::EndChild(); // End RightPanel
}

} // namespace GUI::Database
