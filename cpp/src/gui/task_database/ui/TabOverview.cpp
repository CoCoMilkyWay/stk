// Tab Overview - JSON File Maintenance and Crawler Status
// Compact card layout with stage-based crawler monitoring

#include "gui/task_database/ui/TabOverview.hpp"
#include "imgui.h"
#include <cstdio>

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

  if (ImGui::Button("Force Update")) {
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
// Helper: Render Baostock Crawler Monitor (Stage-based)
// ============================================================================

void RenderCrawlerMonitor(const CrawlerState &state) {
  static bool expanded = true;

  ImGui::BeginChild("CrawlerMonitor", ImVec2(0, expanded ? 250 : 60), true);

  // ===== Title with Baostock link =====
  ImGui::TextColored(COLOR_CYAN, "Baostock Crawler Monitor");
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::TextColored(COLOR_CYAN, "Baostock - 证券宝");
    ImGui::Separator();
    ImGui::Text("官网: http://baostock.com");
    ImGui::Text("数据类型: A股历史行情、财务数据、复权因子");
    ImGui::Text("免费开源、无需token");
    ImGui::EndTooltip();
  }

  ImGui::SameLine();
  ImGui::TextColored(COLOR_GRAY, "  数据源: 证券宝");

  ImGui::SameLine(ImGui::GetContentRegionAvail().x - 120);
  if (ImGui::SmallButton(expanded ? "▼ Hide Details" : "▶ Show Details")) {
    expanded = !expanded;
  }

  ImGui::Separator();

  if (!expanded) {
    // Collapsed view - one line summary
    const auto &prog = state.progress;
    ImGui::Text("%s  │  %zu/%zu active  │  %.1f req/s  │  %zu/%zu (%.1f%%)",
                GetStageName(prog.current_stage),
                prog.active_workers,
                prog.total_workers,
                prog.requests_per_second,
                prog.completed_tasks,
                prog.total_tasks,
                prog.total_tasks > 0 ? (prog.completed_tasks * 100.0 / prog.total_tasks) : 0.0);

    ImGui::EndChild();
    return;
  }

  // ===== Expanded view =====
  const auto &prog = state.progress;

  // Overall Status Section
  ImGui::BeginChild("OverallStatus", ImVec2(0, 90), true);
  ImGui::Text("Current Stage: %s", GetStageName(prog.current_stage));

  ImGui::Text("Workers: %zu/%zu active", prog.active_workers, prog.total_workers);
  ImGui::SameLine(0, 30);
  ImGui::Text("│  Throughput: %.1f req/s", prog.requests_per_second);
  ImGui::SameLine(0, 30);
  ImGui::Text("│  Success: %.1f%%", prog.success_rate * 100.0);
  ImGui::SameLine(0, 30);
  ImGui::Text("│  Errors: %zu", prog.error_count);

  ImGui::Text("Progress: %zu/%zu", prog.completed_tasks, prog.total_tasks);
  ImGui::SameLine(0, 30);
  if (prog.elapsed_seconds > 0.001) {
    ImGui::Text("│  Elapsed: %.0fs", prog.elapsed_seconds);
    ImGui::SameLine(0, 30);
  }
  if (prog.eta_seconds > 0.001) {
    ImGui::Text("│  ETA: %.0fs", prog.eta_seconds);
  }

  // Progress bar
  float progress_pct = prog.total_tasks > 0 ? (float)prog.completed_tasks / prog.total_tasks : 0.0f;
  ImGui::ProgressBar(progress_pct, ImVec2(-1, 0));

  ImGui::EndChild();

  // Stage Activity Log (simplified - just showing current stage status)
  ImGui::BeginChild("StageActivity", ImVec2(0, 80), true);
  ImGui::Text("Stage Activity:");
  ImGui::Separator();

  if (!prog.current_item.empty()) {
    ImGui::Text("Processing: %s", prog.current_item.c_str());
  } else {
    ImGui::TextColored(COLOR_GRAY, "Waiting for tasks...");
  }

  if (prog.error_count > 0) {
    ImGui::TextColored(COLOR_YELLOW, "[Warning] %zu items skipped due to errors", prog.error_count);
  }

  ImGui::EndChild();

  // Control buttons
  ImGui::Spacing();
  bool is_running = (state.status == CrawlerStatus::Running);

  ImGui::BeginDisabled(!is_running);
  if (ImGui::Button("Pause All")) {
    // TODO: Implement pause
  }
  ImGui::SameLine();
  if (ImGui::Button("Stop All")) {
    // TODO: Implement stop
  }
  ImGui::EndDisabled();

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
    bool *refresh_scan_clicked,
    size_t l2_asset_count,
    size_t l2_encoded_count,
    size_t l2_missing_count,
    double l2_coverage_pct,
    double l2_disk_usage_gb,
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
    // TODO: Implement integrity check
  }
  ImGui::EndDisabled();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // Split layout: 60% left (JSON files), 40% right (crawler)
  ImGui::BeginChild("LeftPanel", ImVec2(ImGui::GetContentRegionAvail().x * 0.6f, 0), false);

  // Render JSON file cards (in update order)
  RenderCompactJsonCard("stock_days.json", stock_days_state, stock_days_update, stock_days_remove, stock_days_view, false);

  ImGui::Spacing();

  RenderCompactJsonCard("stock_factor.json", stock_factor_state, stock_factor_update, stock_factor_remove, stock_factor_view, true);

  ImGui::Spacing();

  RenderCompactJsonCard("stock_info.json", stock_info_state, stock_info_update, stock_info_remove, stock_info_view, true);

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // L2 Database Summary (compact)
  ImGui::BeginChild("L2Summary", ImVec2(0, 80), true);
  ImGui::Text("L2 Database Summary:");
  ImGui::Separator();
  ImGui::Text("Assets: %zu  │  Encoded: %zu  │  Missing: %zu  │  Coverage: %.1f%%  │  Disk: %.2f GB",
              l2_asset_count, l2_encoded_count, l2_missing_count, l2_coverage_pct, l2_disk_usage_gb);
  ImGui::EndChild();

  ImGui::EndChild(); // End LeftPanel

  ImGui::SameLine();

  // Right panel: Crawler monitor
  ImGui::BeginChild("RightPanel", ImVec2(0, 0), false);
  RenderCrawlerMonitor(crawler_state); // Real data
  ImGui::EndChild();                   // End RightPanel
}

} // namespace GUI::Database
