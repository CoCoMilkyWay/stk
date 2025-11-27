// Tab Encode Implementation
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "imgui.h"
#include "shared/Asset.hpp"

#include <thread>

namespace GUI::Database {

void RenderTabEncode(EncodingService *service, EncodeState &state, Asset &asset) {
  if (!service) {
    ImGui::TextDisabled("Service not initialized");
    return;
  }

  const bool is_running = service->is_running();
  const auto status = service->get_status();
  const auto progress = service->get_progress();
  const auto check_result = service->get_last_check_result();

  // ========================================================================
  // Database Coverage Check
  // ========================================================================

  if (ImGui::CollapsingHeader("Database Coverage Check", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // Check status
    ImGui::Text("Status:");
    ImGui::SameLine(150);

    if (check_result.status == DatabaseStatus::Error) {
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
      if (ImGui::Button("Start Encoding", ImVec2(150, 0))) {
        // Trigger start (non-blocking)
        // service->start_encoding() should be called from coroutine context
      }

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
  // Progress Statistics
  // ========================================================================

  if (ImGui::CollapsingHeader("Progress", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // Assets
    ImGui::Text("Assets:");
    ImGui::SameLine(150);
    ImGui::Text("%zu / %zu (%.1f%%)",
                progress.completed_assets,
                progress.total_assets,
                progress.total_assets > 0 ? 100.0 * progress.completed_assets / progress.total_assets : 0.0);

    // Dates
    ImGui::Text("Trading Days:");
    ImGui::SameLine(150);
    ImGui::Text("%zu / %zu (%.1f%%)",
                progress.encoded_dates,
                progress.total_dates,
                progress.total_dates > 0 ? 100.0 * progress.encoded_dates / progress.total_dates : 0.0);

    // Orders
    ImGui::Text("Total Orders:");
    ImGui::SameLine(150);
    ImGui::Text("%zu", progress.total_orders);

    if (is_running) {
      ImGui::Separator();
      ImGui::Text("Elapsed Time:");
      ImGui::SameLine(150);
      ImGui::Text("%.1f s", progress.elapsed_seconds);

      ImGui::Text("Encoding Rate:");
      ImGui::SameLine(150);
      ImGui::Text("%.2f assets/s", progress.encoding_rate);
    }

    ImGui::Unindent();
  }

  // ========================================================================
  // Asset Summary
  // ========================================================================

  if (ImGui::CollapsingHeader("Asset Summary", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

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
            double pct = 100.0 * archive_days_in_backtest / total_archive_days;
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
          if (ImGui::BeginTable("archive_asset_table", 6, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit, ImVec2(0, 400))) {
            ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Available", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupScrollFreeze(0, 1);
            ImGui::TableHeadersRow();

            // Handle sorting
            struct RowData {
              const AssetItem *item;
              size_t backtest_days;
              size_t total_days;
              size_t archive_available;
              size_t archive_missing_bt;
              size_t archive_missing_db;
            };
            std::vector<RowData> rows;
            
            for (const auto &item : asset.items) {
              size_t total_days = item.date_info.size();
              size_t backtest_days = 0;
              size_t archive_available = 0;
              size_t archive_available_bt = 0;

              for (const auto &[date, info] : item.date_info) {
                bool in_backtest = !asset.backtest.start.empty() && !asset.backtest.end.empty() &&
                                   date >= asset.backtest.start && date <= asset.backtest.end;
                if (in_backtest) {
                  backtest_days++;
                }
                if (asset.archive.dates.count(date)) {
                  archive_available++;
                  if (in_backtest) {
                    archive_available_bt++;
                  }
                }
              }
              size_t archive_missing_db = total_days - archive_available;
              size_t archive_missing_bt = backtest_days - archive_available_bt;

              if (state.show_missing_assets && archive_missing_db == 0) {
                continue;
              }

              rows.push_back({&item, backtest_days, total_days, archive_available, archive_missing_bt, archive_missing_db});
            }

            // Sort rows based on table specs (always apply current sort)
            if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
              if (sort_specs->SpecsCount > 0) {
                std::sort(rows.begin(), rows.end(), [&](const RowData &a, const RowData &b) {
                  for (int n = 0; n < sort_specs->SpecsCount; n++) {
                    const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[n];
                    int delta = 0;
                    switch (spec.ColumnIndex) {
                      case 0: delta = strcmp((a.item->asset_code + "." + a.item->exchange).c_str(), (b.item->asset_code + "." + b.item->exchange).c_str()); break;
                      case 1: delta = (a.backtest_days > b.backtest_days) - (a.backtest_days < b.backtest_days); break;
                      case 2: delta = (a.total_days > b.total_days) - (a.total_days < b.total_days); break;
                      case 3: delta = (a.archive_available > b.archive_available) - (a.archive_available < b.archive_available); break;
                      case 4: delta = (a.archive_missing_bt > b.archive_missing_bt) - (a.archive_missing_bt < b.archive_missing_bt); break;
                      case 5: delta = (a.archive_missing_db > b.archive_missing_db) - (a.archive_missing_db < b.archive_missing_db); break;
                    }
                    if (delta != 0)
                      return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
                  }
                  return false;
                });
              }
              sort_specs->SpecsDirty = false;
            }

            // Render sorted rows
            for (const auto &row : rows) {
              ImGui::TableNextRow();
              ImGui::TableNextColumn();
              ImGui::Text("%s.%s", row.item->asset_code.c_str(), row.item->exchange.c_str());

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.backtest_days);

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.total_days);

              ImGui::TableNextColumn();
              double archive_pct = row.total_days > 0 ? 100.0 * row.archive_available / row.total_days : 0.0;
              ImVec4 color = archive_pct >= 100.0  ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                             : archive_pct >= 90.0 ? ImVec4(1.0f, 0.95f, 0.3f, 1.0f)
                                                   : ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
              ImGui::TextColored(color, "%zu (%.1f%%)", row.archive_available, archive_pct);

              ImGui::TableNextColumn();
              if (row.archive_missing_bt > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.archive_missing_bt);
              } else {
                ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
              }

              ImGui::TableNextColumn();
              if (row.archive_missing_db > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.archive_missing_db);

                if (ImGui::IsItemHovered()) {
                  ImGui::BeginTooltip();
                  ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                  ImGui::TextDisabled("Missing dates (DB):");
                  ImGui::Separator();
                  std::string missing_dates;
                  for (const auto &[date, info] : row.item->date_info) {
                    if (!asset.archive.dates.count(date)) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
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
      // Binary Snap Tab
      // ========================================================================
      if (ImGui::BeginTabItem("Binary Snap")) {
        ImGui::Spacing();

        // Use precomputed statistics
        size_t snap_days_in_backtest = asset.binary.backtest_snap_days;
        size_t total_days_in_database = asset.binary.database_snap_days;

        // In Range check - backtest range is within binary database range
        bool snap_in_range = false;
        if (!asset.backtest.start.empty() && !asset.backtest.end.empty() &&
            !asset.binary.min_date.empty() && !asset.binary.max_date.empty()) {
          snap_in_range = (asset.binary.min_date <= asset.backtest.start &&
                           asset.backtest.end <= asset.binary.max_date);
        }

        // In Range (first line)
        ImGui::Text("In Range:");
        ImGui::SameLine();
        if (snap_in_range) {
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
        } else {
          ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
        }

        // Backtest Range (Target)
        ImGui::Text("Backtest Range (Target):");
        ImGui::SameLine();
        if (!asset.backtest.start.empty() && !asset.backtest.end.empty()) {
          if (snap_in_range && total_days_in_database > 0) {
            double pct = 100.0 * snap_days_in_backtest / total_days_in_database;
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu days, %.1f%%)",
                               asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                               snap_days_in_backtest, total_days_in_database, pct);
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

          // Snapshots Encoded (backtest / whole database)
          ImGui::Text("Snapshots Encoded:");
          ImGui::SameLine();
          double snap_pct = asset.binary.total_snapshots > 0 ? 100.0 * asset.binary.backtest_snapshots / asset.binary.total_snapshots : 0.0;
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.1fM / %.1fM (%.1f%%)",
                             asset.binary.backtest_snapshots / 1000000.0, asset.binary.total_snapshots / 1000000.0, snap_pct);

          ImGui::Text("Snapshots Size:");
          ImGui::SameLine();
          double snap_size_pct = asset.binary.snapshots_size_gb > 0 ? 100.0 * asset.binary.backtest_snapshots_size_gb / asset.binary.snapshots_size_gb : 0.0;
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.2fGB / %.2fGB (%.1f%%)",
                             asset.binary.backtest_snapshots_size_gb, asset.binary.snapshots_size_gb, snap_size_pct);

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          // Asset table
          if (ImGui::BeginTable("snap_asset_table", 6, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit, ImVec2(0, 400))) {
            ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Snap Encoded", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupScrollFreeze(0, 1);
            ImGui::TableHeadersRow();

            struct SnapRowData {
              const AssetItem *item;
              size_t backtest_days;
              size_t total_days;
              size_t snap_encoded;
              size_t snap_missing_bt;
              size_t snap_missing_db;
            };
            std::vector<SnapRowData> snap_rows;
            
            for (const auto &item : asset.items) {
              size_t total_days = item.date_info.size();
              size_t backtest_days = 0;
              size_t snap_encoded = 0;
              size_t snap_encoded_bt = 0;

              for (const auto &[date, info] : item.date_info) {
                bool in_backtest = !asset.backtest.start.empty() && !asset.backtest.end.empty() &&
                                   date >= asset.backtest.start && date <= asset.backtest.end;
                if (in_backtest) {
                  backtest_days++;
                }
                if (info.snapshots_encoded) {
                  snap_encoded++;
                  if (in_backtest) {
                    snap_encoded_bt++;
                  }
                }
              }
              size_t snap_missing_db = total_days - snap_encoded;
              size_t snap_missing_bt = backtest_days - snap_encoded_bt;

              if (state.show_missing_assets && snap_missing_db == 0) {
                continue;
              }

              snap_rows.push_back({&item, backtest_days, total_days, snap_encoded, snap_missing_bt, snap_missing_db});
            }

            // Sort rows (always apply current sort)
            if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
              if (sort_specs->SpecsCount > 0) {
                std::sort(snap_rows.begin(), snap_rows.end(), [&](const SnapRowData &a, const SnapRowData &b) {
                  for (int n = 0; n < sort_specs->SpecsCount; n++) {
                    const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[n];
                    int delta = 0;
                    switch (spec.ColumnIndex) {
                      case 0: delta = strcmp((a.item->asset_code + "." + a.item->exchange).c_str(), (b.item->asset_code + "." + b.item->exchange).c_str()); break;
                      case 1: delta = (a.backtest_days > b.backtest_days) - (a.backtest_days < b.backtest_days); break;
                      case 2: delta = (a.total_days > b.total_days) - (a.total_days < b.total_days); break;
                      case 3: delta = (a.snap_encoded > b.snap_encoded) - (a.snap_encoded < b.snap_encoded); break;
                      case 4: delta = (a.snap_missing_bt > b.snap_missing_bt) - (a.snap_missing_bt < b.snap_missing_bt); break;
                      case 5: delta = (a.snap_missing_db > b.snap_missing_db) - (a.snap_missing_db < b.snap_missing_db); break;
                    }
                    if (delta != 0)
                      return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
                  }
                  return false;
                });
              }
              sort_specs->SpecsDirty = false;
            }

            // Render sorted rows
            for (const auto &row : snap_rows) {
              ImGui::TableNextRow();
              ImGui::TableNextColumn();
              ImGui::Text("%s.%s", row.item->asset_code.c_str(), row.item->exchange.c_str());

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.backtest_days);

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.total_days);

              ImGui::TableNextColumn();
              double snap_pct = row.total_days > 0 ? 100.0 * row.snap_encoded / row.total_days : 0.0;
              ImVec4 snap_color = snap_pct >= 100.0  ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                                  : snap_pct >= 90.0 ? ImVec4(1.0f, 0.95f, 0.3f, 1.0f)
                                                     : ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
              ImGui::TextColored(snap_color, "%zu (%.1f%%)", row.snap_encoded, snap_pct);

              ImGui::TableNextColumn();
              if (row.snap_missing_bt > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.snap_missing_bt);
              } else {
                ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
              }

              ImGui::TableNextColumn();
              if (row.snap_missing_db > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.snap_missing_db);

                if (ImGui::IsItemHovered()) {
                  ImGui::BeginTooltip();
                  ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                  ImGui::TextDisabled("Missing dates (DB):");
                  ImGui::Separator();
                  std::string missing_dates;
                  for (const auto &[date, info] : row.item->date_info) {
                    if (!info.snapshots_encoded) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
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
            double pct = 100.0 * order_days_in_backtest / total_days_in_database;
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
          double order_pct = asset.binary.total_orders > 0 ? 100.0 * asset.binary.backtest_orders / asset.binary.total_orders : 0.0;
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.1fM / %.1fM (%.1f%%)",
                             asset.binary.backtest_orders / 1000000.0, asset.binary.total_orders / 1000000.0, order_pct);

          ImGui::Text("Orders Size:");
          ImGui::SameLine();
          double order_size_pct = asset.binary.orders_size_gb > 0 ? 100.0 * asset.binary.backtest_orders_size_gb / asset.binary.orders_size_gb : 0.0;
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.95f, 1.0f), "%.2fGB / %.2fGB (%.1f%%)",
                             asset.binary.backtest_orders_size_gb, asset.binary.orders_size_gb, order_size_pct);

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          // Asset table
          if (ImGui::BeginTable("order_asset_table", 6, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit, ImVec2(0, 400))) {
            ImGui::TableSetupColumn("Asset", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Days (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Order Encoded", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (BT)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupColumn("Miss (DB)", ImGuiTableColumnFlags_DefaultSort);
            ImGui::TableSetupScrollFreeze(0, 1);
            ImGui::TableHeadersRow();

            struct OrderRowData {
              const AssetItem *item;
              size_t backtest_days;
              size_t total_days;
              size_t order_encoded;
              size_t order_missing_bt;
              size_t order_missing_db;
            };
            std::vector<OrderRowData> order_rows;
            
            for (const auto &item : asset.items) {
              size_t total_days = item.date_info.size();
              size_t backtest_days = 0;
              size_t order_encoded = 0;
              size_t order_encoded_bt = 0;

              for (const auto &[date, info] : item.date_info) {
                bool in_backtest = !asset.backtest.start.empty() && !asset.backtest.end.empty() &&
                                   date >= asset.backtest.start && date <= asset.backtest.end;
                if (in_backtest) {
                  backtest_days++;
                }
                if (info.orders_encoded) {
                  order_encoded++;
                  if (in_backtest) {
                    order_encoded_bt++;
                  }
                }
              }
              size_t order_missing_db = total_days - order_encoded;
              size_t order_missing_bt = backtest_days - order_encoded_bt;

              if (state.show_missing_assets && order_missing_db == 0) {
                continue;
              }

              order_rows.push_back({&item, backtest_days, total_days, order_encoded, order_missing_bt, order_missing_db});
            }

            // Sort rows (always apply current sort)
            if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
              if (sort_specs->SpecsCount > 0) {
                std::sort(order_rows.begin(), order_rows.end(), [&](const OrderRowData &a, const OrderRowData &b) {
                  for (int n = 0; n < sort_specs->SpecsCount; n++) {
                    const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[n];
                    int delta = 0;
                    switch (spec.ColumnIndex) {
                      case 0: delta = strcmp((a.item->asset_code + "." + a.item->exchange).c_str(), (b.item->asset_code + "." + b.item->exchange).c_str()); break;
                      case 1: delta = (a.backtest_days > b.backtest_days) - (a.backtest_days < b.backtest_days); break;
                      case 2: delta = (a.total_days > b.total_days) - (a.total_days < b.total_days); break;
                      case 3: delta = (a.order_encoded > b.order_encoded) - (a.order_encoded < b.order_encoded); break;
                      case 4: delta = (a.order_missing_bt > b.order_missing_bt) - (a.order_missing_bt < b.order_missing_bt); break;
                      case 5: delta = (a.order_missing_db > b.order_missing_db) - (a.order_missing_db < b.order_missing_db); break;
                    }
                    if (delta != 0)
                      return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
                  }
                  return false;
                });
              }
              sort_specs->SpecsDirty = false;
            }

            // Render sorted rows
            for (const auto &row : order_rows) {
              ImGui::TableNextRow();
              ImGui::TableNextColumn();
              ImGui::Text("%s.%s", row.item->asset_code.c_str(), row.item->exchange.c_str());

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.backtest_days);

              ImGui::TableNextColumn();
              ImGui::Text("%zu", row.total_days);

              ImGui::TableNextColumn();
              double order_pct = row.total_days > 0 ? 100.0 * row.order_encoded / row.total_days : 0.0;
              ImVec4 order_color = order_pct >= 100.0  ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                                   : order_pct >= 90.0 ? ImVec4(1.0f, 0.95f, 0.3f, 1.0f)
                                                       : ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
              ImGui::TextColored(order_color, "%zu (%.1f%%)", row.order_encoded, order_pct);

              ImGui::TableNextColumn();
              if (row.order_missing_bt > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.order_missing_bt);
              } else {
                ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "0");
              }

              ImGui::TableNextColumn();
              if (row.order_missing_db > 0) {
                ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu", row.order_missing_db);

                if (ImGui::IsItemHovered()) {
                  ImGui::BeginTooltip();
                  ImGui::PushTextWrapPos(ImGui::GetFontSize() * 40.0f);
                  ImGui::TextDisabled("Missing dates (DB):");
                  ImGui::Separator();
                  std::string missing_dates;
                  for (const auto &[date, info] : row.item->date_info) {
                    if (!info.orders_encoded) {
                      if (!missing_dates.empty())
                        missing_dates += ", ";
                      missing_dates += date;
                    }
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
}

} // namespace GUI::Database
