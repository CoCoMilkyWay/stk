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

// Archives / Orders 两个页签走同一段渲染, 差别只在取哪一组计数.
struct MissingDim {
  size_t Asset::AssetStats::*count;
  std::vector<std::string> Asset::AssetStats::*sample;
};
constexpr MissingDim kArchiveDim{&Asset::AssetStats::archive_missing, &Asset::AssetStats::archive_missing_sample};
constexpr MissingDim kOrderDim{&Asset::AssetStats::orders_missing, &Asset::AssetStats::orders_missing_sample};

// 重建表格视图: 只留该维度确有缺失的资产, 再排序.
void rebuild_table_view(AssetTableView &view, const Asset &asset, const MissingDim &dim,
                        ImGuiTableSortSpecs *sort_specs) {
  view.rows.clear();
  for (size_t id = 0; id < asset.asset_stats.size(); ++id) {
    if (asset.asset_stats[id].*(dim.count) > 0)
      view.rows.push_back(id);
  }

  // 默认按缺失天数降序 —— 缺得最多的排最前, 那才是要先处理的
  auto by_missing_desc = [&](size_t a, size_t b) {
    const size_t ma = asset.asset_stats[a].*(dim.count);
    const size_t mb = asset.asset_stats[b].*(dim.count);
    if (ma != mb)
      return ma > mb;
    return asset.items[a].asset_code < asset.items[b].asset_code;
  };

  if (sort_specs && sort_specs->SpecsCount > 0) {
    std::sort(view.rows.begin(), view.rows.end(), [&](size_t a, size_t b) {
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
          delta = cmp_size(asset.asset_stats[a].*(dim.count), asset.asset_stats[b].*(dim.count));
          break;
        }
        if (delta != 0)
          return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
      }
      return false;
    });
  } else {
    std::sort(view.rows.begin(), view.rows.end(), by_missing_desc);
  }

  view.built = true;
  view.generation = asset.asset_stats_generation;
}

// 视图过期就重建. 排序规则变化由 ImGui 的 SpecsDirty 告知.
void sync_table_view(AssetTableView &view, const Asset &asset, const MissingDim &dim) {
  ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs();
  const bool specs_dirty = sort_specs && sort_specs->SpecsDirty;
  if (!view.built || view.generation != asset.asset_stats_generation || specs_dirty) {
    rebuild_table_view(view, asset, dim, sort_specs);
    if (sort_specs)
      sort_specs->SpecsDirty = false;
  }
}

// 两个页签共用的缺失表. what = "archive" / "orders", 只用于文案.
void render_missing_table(const char *table_id, const Asset &asset, AssetTableView &view,
                          const MissingDim &dim, const char *what) {
  size_t assets_affected = 0;
  size_t asset_days_missing = 0;
  for (const auto &st : asset.asset_stats) {
    const size_t n = st.*(dim.count);
    if (n > 0) {
      assets_affected++;
      asset_days_missing += n;
    }
  }

  ImGui::Text("Missing:");
  ImGui::SameLine();
  if (assets_affected == 0) {
    ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "none — every listed asset has %s for all its trading days", what);
    return;
  }
  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu assets, %zu asset-days",
                     assets_affected, asset_days_missing);
  ImGui::TextDisabled("Trading days the asset was listed and not suspended, but has no %s", what);

  ImGui::Spacing();

  if (ImGui::BeginTable(table_id, 3,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit,
                        ImVec2(0, 400))) {
    ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
    ImGui::TableSetupColumn("Missing", ImGuiTableColumnFlags_DefaultSort);
    ImGui::TableSetupColumn("Dates", ImGuiTableColumnFlags_NoSort | ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupScrollFreeze(0, 1);

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    for (int column = 0; column < 3; column++) {
      ImGui::TableSetColumnIndex(column);
      const char *label = "";
      const char *tooltip = "";
      switch (column) {
      case 0:
        label = "Asset";
        tooltip = "资产代码\n格式: 代码.交易所 (如 600000.SH)";
        break;
      case 1:
        label = "Missing";
        tooltip = "回测区间内缺失的交易日数\n分母是该资产已上市未退市且未停牌的交易日";
        break;
      case 2:
        label = "Dates";
        tooltip = "缺失日期 (只列前若干个)";
        break;
      }
      ImGui::TableHeader(label);
      if (ImGui::IsItemHovered()) {
        ImGui::BeginTooltip();
        ImGui::TextUnformatted(tooltip);
        ImGui::EndTooltip();
      }
    }

    sync_table_view(view, asset, dim);

    for (const size_t id : view.rows) {
      const AssetItem &item = asset.items[id];
      const Asset::AssetStats &stats = asset.asset_stats[id];
      const size_t missing = stats.*(dim.count);
      const std::vector<std::string> &sample = stats.*(dim.sample);

      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::Text("%s.%s", item.asset_code.c_str(), item.exchange.c_str());

      ImGui::TableNextColumn();
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu / %zu", missing, stats.expected_days);

      ImGui::TableNextColumn();
      std::string dates;
      for (const auto &d : sample) {
        if (!dates.empty())
          dates += "  ";
        dates += d;
      }
      if (missing > sample.size())
        dates += "  +" + std::to_string(missing - sample.size()) + " more";
      ImGui::TextUnformatted(dates.c_str());
    }

    ImGui::EndTable();
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
  // 这两个返回的是引用, 别按值接 —— 里面的缺失日期/错误文件列表是几千条
  // std::string, 逐帧整份复制就是逐帧几千次分配
  const auto &check_result = scan_service->get_last_check_result();
  const auto &file_check_result = encoding_service->get_file_check_result();
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
    } else {
      // 每个 DatabaseStatus 都要有分支 —— 漏掉的会掉进 default 显示成
      // "没检查", 而它其实刚检查完.
      switch (check_result.status) {
      case DatabaseStatus::Unchecked:
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "Not checked");
        ImGui::TextDisabled("Coverage check runs automatically after fundamental sync");
        break;
      case DatabaseStatus::Error:
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "ERROR");
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "%s", check_result.error_message.c_str());
        break;
      case DatabaseStatus::Pass:
        ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Pass");
        ImGui::TextDisabled("All required dates for backtest period are encoded");
        break;
      case DatabaseStatus::Incomplete:
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Incomplete");
        ImGui::TextDisabled("Missing dates all have archives — run encoding to fill them");
        break;
      case DatabaseStatus::NotEncoded:
        ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "NotEncoded");
        ImGui::TextDisabled("Archives cover the backtest period, nothing encoded yet");
        break;
      case DatabaseStatus::NeedArchive:
        ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "NeedArchive");
        ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%s", check_result.error_message.c_str());
        ImGui::TextDisabled("Those days must be downloaded before they can be encoded");
        break;
      case DatabaseStatus::NoData:
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "NoData");
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "%s", check_result.error_message.c_str());
        break;
      }

      // 覆盖进度与缺口明细对所有"检查跑完且有缺口"的状态都适用
      if (!check_result.missing_dates.empty() && check_result.required_dates > 0) {
        ImGui::Spacing();
        ImGui::Text("Covered %zu / %zu trading days (%.1f%%)",
                    check_result.binary_coverage,
                    check_result.required_dates,
                    100.0 * check_result.binary_coverage / check_result.required_dates);
        ImGui::BulletText("Can encode from archive: %zu", check_result.missing_can_encode.size());
        ImGui::BulletText("Need download (no archive): %zu", check_result.missing_no_archive.size());

        ImGui::Checkbox("Show missing dates", &state.show_missing_details);
        if (state.show_missing_details) {
          ImGui::BeginChild("MissingDates", ImVec2(0, 140), true);
          for (const auto &date : check_result.missing_no_archive) {
            ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%s  (no archive)", date.c_str());
          }
          for (const auto &date : check_result.missing_can_encode) {
            ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s  (can encode)", date.c_str());
          }
          ImGui::EndChild();
        }
      }
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
      // 条数只有跑过明细扫描才有值 (见 Asset::DateInfo), 没有就不占一行
      if (progress.total_orders > 0) {
        ImGui::SetCursorPosX(center_x - 150);
        ImGui::Text("Total Orders: %zu", progress.total_orders);
      }

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

      // 统计由扫描末尾一次算好 (见 ScanService Phase 5)
      const bool stats_ready = asset.asset_stats.size() == asset.items.size() && !asset.items.empty();

      ImGui::Spacing();

      if (!stats_ready) {
        ImGui::TextDisabled("Waiting for database scan...");
      } else if (ImGui::BeginTabBar("AssetSummaryTabs", ImGuiTabBarFlags_None)) {

        // 分母都是回测区间内的交易日 (日历为准), 不是各自库扫出来的天数 ——
        // 后者做分母的话, 缺的那些天连同分母一起消失, 永远是 100%.
        const size_t required_days = asset.backtest.required_dates.size();

        // ========================================================================
        // Archives Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Archives")) {
          ImGui::Spacing();

          size_t archive_days_in_backtest = 0;
          for (const auto &date : asset.backtest.required_dates) {
            if (asset.archive.dates.count(date))
              archive_days_in_backtest++;
          }

          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.archive.path.c_str());

          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (required_days > 0) {
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu trading days, %.1f%%)",
                               asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                               archive_days_in_backtest, required_days,
                               100.0 * archive_days_in_backtest / required_days);
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.archive.exists) {
            ImGui::Text("Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu files, %.2f GB)",
                               asset.archive.min_date.c_str(), asset.archive.max_date.c_str(),
                               asset.archive.total_files, asset.archive.total_size_gb);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            render_missing_table("archive_missing_table", asset, state.archive_view, kArchiveDim, "archive");
          } else {
            ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "No archive database found");
          }

          ImGui::EndTabItem();
        }

        // ========================================================================
        // Orders Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Orders")) {
          ImGui::Spacing();

          const size_t order_days_in_backtest = asset.binary.backtest_order_days;

          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.binary.path.c_str());

          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (required_days > 0) {
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu trading days, %.1f%%)",
                               asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                               order_days_in_backtest, required_days,
                               100.0 * order_days_in_backtest / required_days);
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.binary.exists) {
            ImGui::Text("Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu assets, %.2f GB)",
                               asset.binary.min_date.c_str(), asset.binary.max_date.c_str(),
                               asset.binary.encoded_assets, asset.binary.orders_size_gb);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            render_missing_table("order_missing_table", asset, state.order_view, kOrderDim, "orders");
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
