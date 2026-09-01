// Tab Encode Implementation
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include "imgui.h"
#include "shared/Asset.hpp"

#include <algorithm>
#include <thread>

namespace GUI::Database {

namespace {

int cmp_size(size_t a, size_t b) {
  return (a > b) - (a < b);
}

// 重建表格视图 (过滤 + 排序). order_tab 决定第 3/4 列与过滤看的是 orders
// 缺口还是 archive 缺口 —— 两个页签除此之外完全同构.
void rebuild_table_view(AssetTableView &view, const Asset &asset, bool order_tab,
                        bool missing_only, ImGuiTableSortSpecs *sort_specs) {
  view.rows.clear();
  view.rows.reserve(asset.asset_stats.size());
  for (size_t id = 0; id < asset.asset_stats.size(); ++id) {
    const Asset::AssetStats &s = asset.asset_stats[id];
    const size_t missing_db = order_tab ? s.order_missing_db : s.archive_missing_db;
    if (missing_only && missing_db == 0)
      continue;
    view.rows.push_back(id);
  }

  if (sort_specs && sort_specs->SpecsCount > 0) {
    std::sort(view.rows.begin(), view.rows.end(), [&](size_t a, size_t b) {
      const Asset::AssetStats &sa = asset.asset_stats[a];
      const Asset::AssetStats &sb = asset.asset_stats[b];
      for (int n = 0; n < sort_specs->SpecsCount; n++) {
        const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[n];
        int delta = 0;
        switch (spec.ColumnIndex) {
        case 0: {
          // 代码定长 6 位, 先比代码再比交易所 == 比 "代码.交易所", 但不拼串
          const AssetItem &ia = asset.items[a];
          const AssetItem &ib = asset.items[b];
          delta = ia.asset_code.compare(ib.asset_code);
          if (delta == 0)
            delta = ia.exchange.compare(ib.exchange);
          break;
        }
        case 1:
          delta = cmp_size(sa.backtest_days, sb.backtest_days);
          break;
        case 2:
          delta = cmp_size(sa.total_days, sb.total_days);
          break;
        case 3:
          delta = order_tab ? cmp_size(sa.order_missing_bt, sb.order_missing_bt)
                            : cmp_size(sa.archive_missing_bt, sb.archive_missing_bt);
          break;
        case 4:
          delta = order_tab ? cmp_size(sa.order_missing_db, sb.order_missing_db)
                            : cmp_size(sa.archive_missing_db, sb.archive_missing_db);
          break;
        }
        if (delta != 0)
          return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
      }
      return false;
    });
  }

  view.built = true;
  view.generation = asset.asset_stats_generation;
  view.missing_only = missing_only;
}

// 视图过期就重建. 排序规则变化由 ImGui 的 SpecsDirty 告知.
void sync_table_view(AssetTableView &view, const Asset &asset, bool order_tab, bool missing_only) {
  ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs();
  const bool specs_dirty = sort_specs && sort_specs->SpecsDirty;
  if (!view.built || view.generation != asset.asset_stats_generation ||
      view.missing_only != missing_only || specs_dirty) {
    rebuild_table_view(view, asset, order_tab, missing_only, sort_specs);
    if (sort_specs)
      sort_specs->SpecsDirty = false;
  }
}

} // namespace

void RenderTabEncode(EncodingService *encoding_service, ScanService *scan_service, EncodeState &state, Asset &asset) {
  if (!encoding_service || !scan_service) {
    ImGui::TextDisabled("Services not initialized");
    return;
  }

  // Auto-detect max cores on first run
  if (state.num_workers <= 0) {
    int max_workers = std::thread::hardware_concurrency();
    if (max_workers <= 0)
      max_workers = 8;
    state.num_workers = max_workers;
  }

  const bool is_running = encoding_service->is_running();
  const auto status = encoding_service->get_status();
  const auto progress = encoding_service->get_progress();
  const auto check_result = scan_service->get_last_check_result();
  const auto file_check_result = encoding_service->get_file_check_result();
  const bool file_check_running = encoding_service->is_file_check_running();

  // ========================================================================
  // File Check (Archive Validation)
  // ========================================================================

  if (ImGui::CollapsingHeader("File Check (Archive Validation)", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    ImGui::Text("Status:");
    ImGui::SameLine(150);

    if (file_check_running) {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 1.0f, 1.0f), "Checking...");
      ImGui::TextDisabled("Running in background, see Terminal for progress");
    } else if (!file_check_result.was_run()) {
      ImGui::TextDisabled("Not checked yet");
    } else if (!file_check_result.archive_dir_exists) {
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Archive dir not found");
      ImGui::TextDisabled("Using built binaries");
    } else if (!file_check_result.commands_available) {
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Missing commands");
      ImGui::TextDisabled("Required: unrar, 7z, rar, gdb");
    } else if (file_check_result.passed) {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Pass");
      ImGui::Text("Valid archives: %zu", file_check_result.valid_archives);
    } else {
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Failed");
      ImGui::Spacing();
      if (file_check_result.naming_errors > 0) {
        ImGui::BulletText("Naming errors: %zu", file_check_result.naming_errors);
      }
      if (file_check_result.format_errors > 0) {
        ImGui::BulletText("Format errors: %zu (7z/solid RAR)", file_check_result.format_errors);
      }
      if (file_check_result.structure_errors > 0) {
        ImGui::BulletText("Structure errors: %zu", file_check_result.structure_errors);
      }
      if (file_check_result.integrity_errors > 0) {
        ImGui::BulletText("Integrity errors: %zu (truncated/corrupt)", file_check_result.integrity_errors);
      }
      if (file_check_result.zip_files > 0) {
        ImGui::BulletText("ZIP files: %zu (need conversion)", file_check_result.zip_files);
      }
      ImGui::Spacing();
      ImGui::TextDisabled("(详细错误信息见下方Terminal)");
    }

    ImGui::Spacing();
    ImGui::BeginDisabled(file_check_running);
    if (ImGui::Button(file_check_running ? "Checking..." : "Run File Check", ImVec2(150, 0))) {
      encoding_service->run_file_check(asset.archive.path);
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    ImGui::TextDisabled("Check archive format, structure and integrity");

    // Binary DB 完整性校验 (修复回路: Verify 删坏 → 增量编码补齐)
    const bool verify_running = encoding_service->is_binary_verify_running();
    ImGui::BeginDisabled(verify_running || is_running);
    if (ImGui::Button(verify_running ? "Verifying..." : "Verify Binary DB", ImVec2(150, 0))) {
      encoding_service->run_binary_verify(state.num_workers);
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    ImGui::TextDisabled("全库强制校验并删除损坏文件, 之后增量编码自动补齐");

    ImGui::Unindent();
  }

  // ========================================================================
  // Database Coverage Check
  // ========================================================================

  if (ImGui::CollapsingHeader("Database Coverage Check", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // Check status
    ImGui::Text("Status:");
    ImGui::SameLine(150);

    if (scan_service->is_scanning()) {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 1.0f, 1.0f), "%s", scan_service->get_status_string());
      ImGui::TextDisabled("Database check in progress...");
    } else if (check_result.status == DatabaseStatus::Unchecked) {
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "Not checked");
      ImGui::TextDisabled("Coverage check runs automatically after fundamental sync");
    } else if (check_result.status == DatabaseStatus::Error) {
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "ERROR");
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "%s", check_result.error_message.c_str());
    } else if (check_result.status == DatabaseStatus::Pass) {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Pass");
      ImGui::TextDisabled("All required dates for backtest period are encoded");
    } else if (check_result.status == DatabaseStatus::Incomplete) {
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Incomplete");
      ImGui::Text("Missing %zu / %zu dates (%.1f%% complete)",
                  check_result.missing_dates.size(),
                  check_result.required_dates,
                  check_result.required_dates > 0 ? 100.0 * check_result.binary_coverage / check_result.required_dates : 0.0);

      // Show missing dates if requested
      if (state.show_missing_details && !check_result.missing_dates.empty()) {
        ImGui::Spacing();
        ImGui::TextDisabled("Missing dates:");
        ImGui::BeginChild("MissingDates", ImVec2(0, 100), true);
        for (const auto &date : check_result.missing_dates) {
          ImGui::BulletText("%s", date.c_str());
        }
        ImGui::EndChild();
      }

      if (!check_result.missing_dates.empty()) {
        ImGui::Checkbox("Show missing dates", &state.show_missing_details);
      }
    } else {
      ImGui::TextDisabled("Not checked yet");
    }

    ImGui::Unindent();
  }

  // ========================================================================
  // Control Panel
  // ========================================================================

  if (ImGui::CollapsingHeader("Control Panel", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // Worker count slider
    ImGui::Text("Worker Threads:");
    ImGui::SetNextItemWidth(200);
    int max_workers = std::thread::hardware_concurrency();
    if (max_workers <= 0)
      max_workers = 8;
    ImGui::SliderInt("##workers", &state.num_workers, 1, max_workers);
    ImGui::SameLine();
    ImGui::TextDisabled("(Max: %d)", max_workers);

    // Skip existing checkbox
    ImGui::Checkbox("Skip existing binaries", &state.skip_existing);

    ImGui::Spacing();

    // Start/Stop button
    if (is_running) {
      if (ImGui::Button("Stop Encoding", ImVec2(150, 0))) {
        // Trigger stop (non-blocking)
        // service->stop_encoding() should be called from coroutine context
      }
      ImGui::SameLine();
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Encoding in progress...");
    } else {
      // File Check 不再硬性挡住入口 — 未通过时在确认弹窗里要求"风险自负"勾选
      bool file_check_ok = file_check_result.was_run() &&
                           (file_check_result.passed || !file_check_result.archive_dir_exists);

      if (ImGui::Button("Start Encoding", ImVec2(150, 0))) {
        state.skip_file_check_ack = false; // 每次打开弹窗都要重新勾
        state.show_confirm_dialog = true;
      }

      ImGui::SameLine();
      if (!file_check_result.was_run()) {
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "[!] File Check Not Run");
      } else if (!file_check_result.archive_dir_exists) {
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[✓] No Archive");
      } else if (!file_check_ok) {
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "[✗] File Check Failed");
      } else {
        ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "[✓] File Check Passed");
      }

      ImGui::SameLine();
      ImGui::TextDisabled("|");
      ImGui::SameLine();

      if (status == EncodingStatus::Completed) {
        ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Completed");
      } else if (status == EncodingStatus::Cancelled) {
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Cancelled");
      } else if (status == EncodingStatus::Error) {
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Error");
      } else {
        ImGui::TextDisabled("Idle");
      }
    }

    ImGui::Unindent();
  }

  // ========================================================================
  // Confirmation Dialog (Fullscreen Modal)
  // ========================================================================

  if (state.show_confirm_dialog) {
    ImGui::OpenPopup("Confirm Encoding");
    ImVec2 center = ImGui::GetMainViewport()->GetCenter();
    ImGui::SetNextWindowPos(center, ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    ImGui::SetNextWindowSize(ImVec2(600, 400));
  }

  if (ImGui::BeginPopupModal("Confirm Encoding", &state.show_confirm_dialog, ImGuiWindowFlags_NoResize)) {
    ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "WARNING");
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 1.0f, 0.0f, 1.0f));
    ImGui::TextWrapped("Encoding may overwrite or corrupt existing database files. Please consider moving your database to a backup location before proceeding.");
    ImGui::Spacing();
    ImGui::TextWrapped("Encoding process takes a long time and cannot be interrupted once started. Please be ready.");
    ImGui::PopStyleColor();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    // Check prerequisites
    bool archive_in_range = false;
    if (!asset.backtest.start.empty() && !asset.backtest.end.empty() &&
        !asset.archive.min_date.empty() && !asset.archive.max_date.empty()) {
      archive_in_range = (asset.archive.min_date <= asset.backtest.start &&
                          asset.backtest.end <= asset.archive.max_date);
    }

    bool file_check_ok = file_check_result.was_run() &&
                         (file_check_result.passed || !file_check_result.archive_dir_exists);

    bool can_encode = asset.archive.exists && archive_in_range &&
                      (file_check_ok || state.skip_file_check_ack);

    ImGui::Text("Prerequisites:");
    ImGui::Spacing();

    ImGui::BulletText("File Check:");
    ImGui::SameLine();
    if (!file_check_result.was_run()) {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "Not Run");
      ImGui::SameLine();
      ImGui::TextDisabled("(请先运行 File Check)");
    } else if (!file_check_result.archive_dir_exists) {
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "N/A (no archive)");
    } else if (file_check_result.passed) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Passed");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "Failed");
      ImGui::SameLine();
      ImGui::TextDisabled("(%zu errors)", file_check_result.naming_errors +
                                              file_check_result.format_errors +
                                              file_check_result.structure_errors +
                                              file_check_result.zip_files);
    }

    ImGui::BulletText("Archive Path Exists:");
    ImGui::SameLine();
    if (asset.archive.exists) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
    }

    ImGui::BulletText("Archive In Range:");
    ImGui::SameLine();
    if (archive_in_range) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
    }

    if (!file_check_ok) {
      ImGui::Spacing();
      ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.95f, 0.3f, 0.3f, 1.0f));
      ImGui::TextWrapped(file_check_result.was_run()
                             ? "File Check 未通过: 损坏/solid/结构错误的包会让编码中途 assert 崩溃或静默产出垃圾数据。"
                             : "File Check 未运行: 无法确认归档格式与完整性, 编码可能中途失败。");
      ImGui::PopStyleColor();
      ImGui::Checkbox("跳过 File Check, 风险自负##skip_fc", &state.skip_file_check_ack);
    }

    if (!can_encode) {
      ImGui::Spacing();
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Cannot encode: Prerequisites not met!");
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::Text("Encoding Parameters:");
    ImGui::Spacing();
    ImGui::BulletText("Worker Threads: %d", state.num_workers);
    ImGui::BulletText("Skip Existing: %s", state.skip_existing ? "Yes" : "No");

    ImGui::Spacing();
    ImGui::Text("Paths:");
    ImGui::Spacing();
    ImGui::BulletText("Archive Path:");
    ImGui::SameLine();
    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.archive.path.c_str());

    ImGui::BulletText("Binary Path:");
    ImGui::SameLine();
    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.binary.path.c_str());

    ImGui::Spacing();
    ImGui::Text("Date Ranges:");
    ImGui::Spacing();
    ImGui::BulletText("Backtest Range: %s ~ %s",
                      asset.backtest.start.c_str(), asset.backtest.end.c_str());
    if (asset.archive.exists) {
      ImGui::BulletText("Archive Range: %s ~ %s",
                        asset.archive.min_date.c_str(), asset.archive.max_date.c_str());
    }
    if (asset.binary.exists) {
      ImGui::BulletText("Binary Range: %s ~ %s",
                        asset.binary.min_date.c_str(), asset.binary.max_date.c_str());
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    float button_width = 120;
    float spacing = ImGui::GetStyle().ItemSpacing.x;
    float total_width = button_width * 2 + spacing;
    float offset = (ImGui::GetContentRegionAvail().x - total_width) * 0.5f;

    ImGui::SetCursorPosX(ImGui::GetCursorPosX() + offset);

    ImGui::BeginDisabled(!can_encode);
    if (ImGui::Button("Confirm and Start", ImVec2(button_width, 40))) {
      state.show_confirm_dialog = false;
      state.show_progress_fullscreen = true;
      state.trigger_start = true; // Signal TaskDatabase to start encoding
    }
    ImGui::EndDisabled();

    ImGui::SameLine();
    if (ImGui::Button("Cancel", ImVec2(button_width, 40))) {
      state.show_confirm_dialog = false;
    }

    ImGui::EndPopup();
  }

  // ========================================================================
  // Fullscreen Progress View (when encoding is running)
  // ========================================================================

  if (state.show_progress_fullscreen && is_running) {
    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(ImGui::GetIO().DisplaySize);
    if (ImGui::Begin("Encoding Progress", nullptr, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse)) {

      ImVec2 window_size = ImGui::GetWindowSize();
      float center_x = window_size.x * 0.5f;
      float center_y = window_size.y * 0.5f;

      ImGui::SetCursorPosY(center_y - 150);

      // Title
      ImGui::SetCursorPosX(center_x - 100);
      ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "Encoding in Progress...");

      ImGui::Spacing();
      ImGui::Spacing();

      // Assets Progress
      float assets_progress = progress.total_assets > 0 ? (float)progress.completed_assets / progress.total_assets : 0.0f;
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::Text("Assets: %zu / %zu (%.1f%%)",
                  progress.completed_assets, progress.total_assets,
                  assets_progress * 100.0f);
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::ProgressBar(assets_progress, ImVec2(400, 30));

      ImGui::Spacing();
      ImGui::Spacing();

      // Dates Progress
      float dates_progress = progress.total_dates > 0 ? (float)progress.encoded_dates / progress.total_dates : 0.0f;
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::Text("Trading Days: %zu / %zu (%.1f%%)",
                  progress.encoded_dates, progress.total_dates,
                  dates_progress * 100.0f);
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::ProgressBar(dates_progress, ImVec2(400, 30));

      ImGui::Spacing();
      ImGui::Spacing();
      ImGui::Spacing();

      // Statistics
      ImGui::SetCursorPosX(center_x - 150);
      ImGui::Text("Total Orders: %zu", progress.total_orders);

      ImGui::SetCursorPosX(center_x - 150);
      ImGui::Text("Elapsed Time: %.1f s", progress.elapsed_seconds);

      ImGui::SetCursorPosX(center_x - 150);
      ImGui::Text("Encoding Rate: %.2f assets/s", progress.encoding_rate);

      ImGui::Spacing();
      ImGui::Spacing();
      ImGui::Spacing();

      // Stop button
      ImGui::SetCursorPosX(center_x - 75);
      if (ImGui::Button("Stop Encoding", ImVec2(150, 40))) {
        // Trigger stop
        // service->stop_encoding() should be called from coroutine context
      }

      ImGui::End();
    }
  } else if (state.show_progress_fullscreen && !is_running) {
    // Encoding finished, close fullscreen view
    state.show_progress_fullscreen = false;
  }

  // Only show Asset Summary when not in fullscreen progress mode
  if (!state.show_progress_fullscreen) {

    // ========================================================================
    // Asset Summary
    // ========================================================================

    if (ImGui::CollapsingHeader("Asset Summary", ImGuiTreeNodeFlags_DefaultOpen)) {
      ImGui::Indent();

      // 每资产统计只在扫描后算一次 — 全库编完后 date_info 是满的 (资产数 ×
      // 交易日数, 五百万量级), 逐帧重算会把帧时间拖到几百毫秒.
      if (asset.asset_stats.empty() && !asset.items.empty())
        asset.compute_asset_statistics();

      ImGui::Checkbox("Show only missing assets", &state.show_missing_assets);

      ImGui::Spacing();

      // Tab bar for Archive and Binary
      if (ImGui::BeginTabBar("AssetSummaryTabs", ImGuiTabBarFlags_None)) {

        // ========================================================================
        // Archive Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Archive")) {
          ImGui::Spacing();

          // Calculate archive days in backtest range
          size_t archive_days_in_backtest = 0;
          if (asset.archive.exists && !asset.backtest.start.empty() && !asset.backtest.end.empty()) {
            for (const auto &date : asset.archive.dates) {
              if (date >= asset.backtest.start && date <= asset.backtest.end) {
                archive_days_in_backtest++;
              }
            }
          }
          size_t total_archive_days = asset.archive.dates.size();

          // In Range check - backtest range is within archive database range
          bool archive_in_range = false;
          if (!asset.backtest.start.empty() && !asset.backtest.end.empty() &&
              !asset.archive.min_date.empty() && !asset.archive.max_date.empty()) {
            archive_in_range = (asset.archive.min_date <= asset.backtest.start &&
                                asset.backtest.end <= asset.archive.max_date);
          }

          // Database Path
          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.archive.path.c_str());

          // In Range (first line)
          ImGui::Text("In Range:");
          ImGui::SameLine();
          if (archive_in_range) {
            ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
          } else {
            ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
          }

          // Backtest Range (Target)
          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (!asset.backtest.start.empty() && !asset.backtest.end.empty()) {
            if (archive_in_range && total_archive_days > 0) {
              float pct = 100.0 * archive_days_in_backtest / total_archive_days;
              ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu days, %.1f%%)",
                                 asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                                 archive_days_in_backtest, total_archive_days, pct);
            } else {
              ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (has to be in range to show days)",
                                 asset.backtest.start.c_str(), asset.backtest.end.c_str());
            }
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.archive.exists) {
            // Archive Range (Scanned) - only show files count
            ImGui::Text("Archive Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu files)",
                               asset.archive.min_date.c_str(), asset.archive.max_date.c_str(),
                               asset.archive.total_files);

            // Archive Size
            ImGui::Text("Archive Size:");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "%.2f GB", asset.archive.total_size_gb);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            // Asset table
            if (ImGui::BeginTable("archive_asset_table", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit, ImVec2(0, 400))) {
              ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Days (BT)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Days (DB)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Arch Miss (BT)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Arch Miss (DB)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupScrollFreeze(0, 1);

              // Custom headers with tooltips
              ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
              for (int column = 0; column < 5; column++) {
                ImGui::TableSetColumnIndex(column);
                const char *label = "";
                const char *tooltip = "";

                switch (column) {
                case 0:
                  label = "Asset";
                  tooltip = "资产代码\n格式: 代码.交易所 (如 600000.SH)";
                  break;
                case 1:
                  label = "Days (BT)";
                  tooltip = "回测区间交易日数\n该资产在binary数据库中,位于回测日期范围内的交易日总数";
                  break;
                case 2:
                  label = "Days (DB)";
                  tooltip = "数据库交易日数\n该资产在binary数据库中的总交易日数";
                  break;
                case 3:
                  label = "Arch Miss (BT)";
                  tooltip = "Archive回测区间缺失\n在回测日期范围内,binary有记录但archive源文件缺失的天数\n= 回测区间天数 - archive中可用天数";
                  break;
                case 4:
                  label = "Arch Miss (DB)";
                  tooltip = "Archive数据库区间缺失\n在整个数据库范围内,binary有记录但archive源文件缺失的天数\n= 数据库总天数 - archive中可用天数";
                  break;
                }

                ImGui::TableHeader(label);
                if (ImGui::IsItemHovered()) {
                  ImGui::BeginTooltip();
                  ImGui::TextUnformatted(tooltip);
                  ImGui::EndTooltip();
                }
              }

              // 行统计取自 Asset::asset_stats, 顺序取自缓存视图 — 两者都只
              // 在扫描/过滤/排序变化时重算 (见 AssetTableView)
              sync_table_view(state.archive_view, asset, false, state.show_missing_assets);

              for (const size_t id : state.archive_view.rows) {
                const AssetItem &item = asset.items[id];
                const Asset::AssetStats &stats = asset.asset_stats[id];

                ImGui::TableNextRow();
                ImGui::TableNextColumn();
                ImGui::Text("%s.%s", item.asset_code.c_str(), item.exchange.c_str());

                ImGui::TableNextColumn();
                ImGui::Text("%zu", stats.backtest_days);

                ImGui::TableNextColumn();
                ImGui::Text("%zu", stats.total_days);

                ImGui::TableNextColumn();
                if (stats.archive_missing_bt > 0) {
                  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", stats.archive_missing_bt);

                  if (ImGui::IsItemHovered()) {
                    ImGui::BeginTooltip();
                    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                    ImGui::TextDisabled("Missing dates in backtest range:");
                    ImGui::Separator();
                    std::vector<std::string> missing_dates_vec;
                    for (const auto &[date, info] : item.date_info) {
                      bool in_backtest = !asset.backtest.start.empty() && !asset.backtest.end.empty() &&
                                         date >= asset.backtest.start && date <= asset.backtest.end;
                      if (in_backtest && !asset.archive.dates.count(date)) {
                        missing_dates_vec.push_back(date);
                      }
                    }
                    std::sort(missing_dates_vec.begin(), missing_dates_vec.end());
                    std::string missing_dates;
                    for (const auto &date : missing_dates_vec) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
                    ImGui::TextWrapped("%s", missing_dates.c_str());
                    ImGui::PopTextWrapPos();
                    ImGui::EndTooltip();
                  }
                } else {
                  ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
                }

                ImGui::TableNextColumn();
                if (stats.archive_missing_db > 0) {
                  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", stats.archive_missing_db);

                  if (ImGui::IsItemHovered()) {
                    ImGui::BeginTooltip();
                    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                    ImGui::TextDisabled("Missing dates in database:");
                    ImGui::Separator();
                    std::vector<std::string> missing_dates_vec;
                    for (const auto &[date, info] : item.date_info) {
                      if (!asset.archive.dates.count(date)) {
                        missing_dates_vec.push_back(date);
                      }
                    }
                    std::sort(missing_dates_vec.begin(), missing_dates_vec.end());
                    std::string missing_dates;
                    for (const auto &date : missing_dates_vec) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
                    ImGui::TextWrapped("%s", missing_dates.c_str());
                    ImGui::PopTextWrapPos();
                    ImGui::EndTooltip();
                  }
                } else {
                  ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
                }
              }

              ImGui::EndTable();
            }

          } else {
            ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "No archive database found");
          }

          ImGui::EndTabItem();
        }

        // ========================================================================
        // Binary Order Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Binary Order")) {
          ImGui::Spacing();

          // Use precomputed statistics
          size_t order_days_in_backtest = asset.binary.backtest_order_days;
          size_t total_days_in_database = asset.binary.database_order_days;

          // In Range check - backtest range is within binary database range
          bool order_in_range = false;
          if (!asset.backtest.start.empty() && !asset.backtest.end.empty() &&
              !asset.binary.min_date.empty() && !asset.binary.max_date.empty()) {
            order_in_range = (asset.binary.min_date <= asset.backtest.start &&
                              asset.backtest.end <= asset.binary.max_date);
          }

          // Database Path
          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.binary.path.c_str());

          // In Range (first line)
          ImGui::Text("In Range:");
          ImGui::SameLine();
          if (order_in_range) {
            ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
          } else {
            ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
          }

          // Backtest Range (Target)
          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (!asset.backtest.start.empty() && !asset.backtest.end.empty()) {
            if (order_in_range && total_days_in_database > 0) {
              float pct = 100.0 * order_days_in_backtest / total_days_in_database;
              ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu days, %.1f%%)",
                                 asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                                 order_days_in_backtest, total_days_in_database, pct);
            } else {
              ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (has to be in range to show days)",
                                 asset.backtest.start.c_str(), asset.backtest.end.c_str());
            }
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.binary.exists) {
            // Binary Range (Scanned) - only show assets count
            ImGui::Text("Binary Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu assets)",
                               asset.binary.min_date.c_str(), asset.binary.max_date.c_str(),
                               asset.binary.encoded_assets);

            // Orders Encoded (backtest / whole database)
            ImGui::Text("Orders Encoded:");
            ImGui::SameLine();
            float order_pct = asset.binary.total_orders > 0 ? 100.0 * asset.binary.backtest_orders / asset.binary.total_orders : 0.0;
            ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.1fM / %.1fM (%.1f%%)",
                               asset.binary.backtest_orders / 1000000.0, asset.binary.total_orders / 1000000.0, order_pct);

            ImGui::Text("Orders Size:");
            ImGui::SameLine();
            float order_size_pct = asset.binary.orders_size_gb > 0 ? 100.0 * asset.binary.backtest_orders_size_gb / asset.binary.orders_size_gb : 0.0;
            ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.2fGB / %.2fGB (%.1f%%)",
                               asset.binary.backtest_orders_size_gb, asset.binary.orders_size_gb, order_size_pct);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            // Asset table
            if (ImGui::BeginTable("order_asset_table", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit, ImVec2(0, 400))) {
              ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Days (BT)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Days (DB)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Order Miss (BT)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupColumn("Order Miss (DB)", ImGuiTableColumnFlags_DefaultSort);
              ImGui::TableSetupScrollFreeze(0, 1);

              // Custom headers with tooltips
              ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
              for (int column = 0; column < 5; column++) {
                ImGui::TableSetColumnIndex(column);
                const char *label = "";
                const char *tooltip = "";

                switch (column) {
                case 0:
                  label = "Asset";
                  tooltip = "资产代码\n格式: 代码.交易所 (如 600000.SH)";
                  break;
                case 1:
                  label = "Days (BT)";
                  tooltip = "回测区间交易日数\n该资产在binary数据库中,位于回测日期范围内的交易日总数";
                  break;
                case 2:
                  label = "Days (DB)";
                  tooltip = "数据库交易日数\n该资产在binary数据库中的总交易日数";
                  break;
                case 3:
                  label = "Order Miss (BT)";
                  tooltip = "订单回测区间缺失\n在回测日期范围内,有目录记录但orders文件缺失的天数\n= 回测区间天数 - 有orders文件的天数";
                  break;
                case 4:
                  label = "Order Miss (DB)";
                  tooltip = "订单数据库区间缺失\n在整个数据库范围内,有目录记录但orders文件缺失的天数\n= 数据库总天数 - 有orders文件的天数";
                  break;
                }

                ImGui::TableHeader(label);
                if (ImGui::IsItemHovered()) {
                  ImGui::BeginTooltip();
                  ImGui::TextUnformatted(tooltip);
                  ImGui::EndTooltip();
                }
              }

              // 同 Archive 页签: 统计与顺序都走缓存 (见 AssetTableView)
              sync_table_view(state.order_view, asset, true, state.show_missing_assets);

              for (const size_t id : state.order_view.rows) {
                const AssetItem &item = asset.items[id];
                const Asset::AssetStats &stats = asset.asset_stats[id];

                ImGui::TableNextRow();
                ImGui::TableNextColumn();
                ImGui::Text("%s.%s", item.asset_code.c_str(), item.exchange.c_str());

                ImGui::TableNextColumn();
                ImGui::Text("%zu", stats.backtest_days);

                ImGui::TableNextColumn();
                ImGui::Text("%zu", stats.total_days);

                ImGui::TableNextColumn();
                if (stats.order_missing_bt > 0) {
                  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", stats.order_missing_bt);

                  if (ImGui::IsItemHovered()) {
                    ImGui::BeginTooltip();
                    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                    ImGui::TextDisabled("Missing dates in backtest range:");
                    ImGui::Separator();
                    std::vector<std::string> missing_dates_vec;
                    for (const auto &[date, info] : item.date_info) {
                      bool in_backtest = !asset.backtest.start.empty() && !asset.backtest.end.empty() &&
                                         date >= asset.backtest.start && date <= asset.backtest.end;
                      if (in_backtest && !info.orders_encoded) {
                        missing_dates_vec.push_back(date);
                      }
                    }
                    std::sort(missing_dates_vec.begin(), missing_dates_vec.end());
                    std::string missing_dates;
                    for (const auto &date : missing_dates_vec) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
                    ImGui::TextWrapped("%s", missing_dates.c_str());
                    ImGui::PopTextWrapPos();
                    ImGui::EndTooltip();
                  }
                } else {
                  ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
                }

                ImGui::TableNextColumn();
                if (stats.order_missing_db > 0) {
                  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", stats.order_missing_db);

                  if (ImGui::IsItemHovered()) {
                    ImGui::BeginTooltip();
                    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                    ImGui::TextDisabled("Missing dates in database:");
                    ImGui::Separator();
                    std::vector<std::string> missing_dates_vec;
                    for (const auto &[date, info] : item.date_info) {
                      if (!info.orders_encoded) {
                        missing_dates_vec.push_back(date);
                      }
                    }
                    std::sort(missing_dates_vec.begin(), missing_dates_vec.end());
                    std::string missing_dates;
                    for (const auto &date : missing_dates_vec) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
                    ImGui::TextWrapped("%s", missing_dates.c_str());
                    ImGui::PopTextWrapPos();
                    ImGui::EndTooltip();
                  }
                } else {
                  ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
                }
              }

              ImGui::EndTable();
            }

          } else {
            ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "No binary database found");
          }

          ImGui::EndTabItem();
        }

        ImGui::EndTabBar();
      }

      ImGui::Unindent();
    }

  } // End: if (!state.show_progress_fullscreen)
}

} // namespace GUI::Database
