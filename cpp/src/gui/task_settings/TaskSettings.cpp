#include "gui/task_settings/TaskSettings.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "imgui.h"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include <chrono>
#include <ctime>
#include <filesystem>
#include <unistd.h>

namespace GUI::Tasks {
namespace {

// Get current working directory
static std::string GetCWD() {
  return std::filesystem::current_path().string();
}

// Settings task - config management with auto-sync
class SettingsTask {
private:
  bool is_expanded = false;
  bool initial_sync_done = false;
  bool is_writing = false;

  // Date picker state
  struct DatePickerState {
    int year = 2025;
    int month = 1;
    int day = 1;
    bool is_open = false;
  };
  DatePickerState start_picker;
  DatePickerState end_picker;

  // Helper: Draw date picker
  bool DrawDatePicker(const char *label, const char *popup_id, char *date_buf, size_t buf_size, DatePickerState &state) {
    bool changed = false;

    // Input field with button
    ImGui::PushItemWidth(-100);
    if (ImGui::InputText(label, date_buf, buf_size, ImGuiInputTextFlags_CharsDecimal)) {
      changed = true;
    }
    ImGui::PopItemWidth();

    ImGui::SameLine();
    char btn_id[64];
    snprintf(btn_id, sizeof(btn_id), "📅##%s", popup_id);
    if (ImGui::Button(btn_id)) {
      // Parse date when opening popup
      if (strlen(date_buf) >= 10) {
        sscanf(date_buf, "%d-%d-%d", &state.year, &state.month, &state.day);
      }
      state.is_open = true;
      ImGui::OpenPopup(popup_id);
    }

    // Date picker popup
    if (ImGui::BeginPopup(popup_id)) {
      ImGui::Text("选择日期");
      ImGui::Separator();

      // Year and month selectors
      ImGui::SetNextItemWidth(100);
      ImGui::InputInt("年", &state.year, 1, 10);
      state.year = std::max(2000, std::min(2100, state.year));

      ImGui::SameLine();
      ImGui::SetNextItemWidth(80);
      ImGui::InputInt("月", &state.month, 1, 1);
      state.month = std::max(1, std::min(12, state.month));

      ImGui::Separator();

      // Calendar grid
      const char *weekdays[] = {"日", "一", "二", "三", "四", "五", "六"};

      // Days in month
      int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
      bool is_leap = (state.year % 4 == 0 && state.year % 100 != 0) || (state.year % 400 == 0);
      if (is_leap)
        days_in_month[1] = 29;
      int max_days = days_in_month[state.month - 1];

      // Calculate first weekday using Zeller's formula
      int y = state.year;
      int m = state.month;
      if (m < 3) {
        m += 12;
        y--;
      }
      int K = y % 100;
      int J = y / 100;
      int first_weekday = (1 + (13 * (m + 1)) / 5 + K + K / 4 + J / 4 - 2 * J) % 7;
      first_weekday = (first_weekday + 6) % 7; // Adjust: 0=Sun, 6=Sat

      // Draw weekday headers (use Dummy for spacing)
      ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.7f, 0.7f, 0.7f, 1.0f));
      for (int i = 0; i < 7; i++) {
        ImGui::Button(weekdays[i], ImVec2(32, 0));
        if (i < 6)
          ImGui::SameLine(0, 2);
      }
      ImGui::PopStyleColor();

      ImGui::Separator();

      // Draw calendar
      int current_day = 1;
      for (int week = 0; week < 6 && current_day <= max_days; week++) {
        for (int dow = 0; dow < 7; dow++) {
          ImGui::PushID(week * 7 + dow);

          if (week == 0 && dow < first_weekday) {
            ImGui::Dummy(ImVec2(32, 0));
          } else if (current_day <= max_days) {
            char btn_label[16];
            snprintf(btn_label, sizeof(btn_label), "%2d", current_day);

            bool is_selected = (current_day == state.day);
            if (is_selected) {
              ImGui::PushStyleColor(ImGuiCol_Button, ImVec4(0.26f, 0.59f, 0.98f, 0.80f));
              ImGui::PushStyleColor(ImGuiCol_ButtonHovered, ImVec4(0.26f, 0.59f, 0.98f, 1.00f));
              ImGui::PushStyleColor(ImGuiCol_ButtonActive, ImVec4(0.06f, 0.53f, 0.98f, 1.00f));
            }

            if (ImGui::Button(btn_label, ImVec2(32, 0))) {
              state.day = current_day;
              snprintf(date_buf, buf_size, "%04d-%02d-%02d", state.year, state.month, state.day);
              changed = true;
              state.is_open = false;
              ImGui::CloseCurrentPopup();
            }

            if (is_selected) {
              ImGui::PopStyleColor(3);
            }

            current_day++;
          } else {
            ImGui::Dummy(ImVec2(32, 0));
          }

          ImGui::PopID();
          if (dow < 6)
            ImGui::SameLine(0, 2);
        }
      }

      ImGui::Separator();
      if (ImGui::Button("今天", ImVec2(80, 0))) {
        auto now = std::time(nullptr);
        std::tm *tm_now = std::localtime(&now);
        state.year = tm_now->tm_year + 1900;
        state.month = tm_now->tm_mon + 1;
        state.day = tm_now->tm_mday;
        snprintf(date_buf, buf_size, "%04d-%02d-%02d", state.year, state.month, state.day);
        changed = true;
        state.is_open = false;
        ImGui::CloseCurrentPopup();
      }
      ImGui::SameLine();
      if (ImGui::Button("关闭", ImVec2(80, 0))) {
        state.is_open = false;
        ImGui::CloseCurrentPopup();
      }

      ImGui::EndPopup();
    } else {
      state.is_open = false;
    }

    return changed;
  }

  void EnsureConfigReady(SharedData &data, GuiState &gui_state) {
    if (initial_sync_done) {
      return;
    }
    data.config.log_callback = [&gui_state](const std::string &msg) {
      gui_state.terminal->AddLine(msg);
    };
    data.config.Initialize();
    initial_sync_done = true;
  }

  void MaintainAutoSync(Config &cfg) {
    if (!is_expanded) {
      is_writing = false;
      return;
    }

    if (cfg.dirty) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - cfg.last_modified);
      is_writing = (elapsed.count() >= 150 && elapsed.count() < 250);
    } else {
      is_writing = false;
    }

    cfg.AutoSync();
  }

  bool DrawPeriodSection(Config &cfg) {
    bool changed = false;
    if (ImGui::CollapsingHeader("回测/分析周期: YYYY-MM-DD", ImGuiTreeNodeFlags_DefaultOpen)) {
      if (ImGui::BeginTable("period_table", 2, ImGuiTableFlags_SizingFixedFit)) {
        ImGui::TableSetupColumn("Label", ImGuiTableColumnFlags_WidthFixed, 150);
        ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthStretch);

        ImGui::TableNextRow();
        ImGui::TableNextColumn();
        ImGui::AlignTextToFramePadding();
        ImGui::Text("开始日期");
        if (ImGui::IsItemHovered()) {
          ImGui::SetTooltip("回测/分析开始日期 (YYYY-MM-DD)");
        }
        ImGui::TableNextColumn();
        if (DrawDatePicker("##start", "start_date_picker", cfg.start_date_buf, sizeof(cfg.start_date_buf), start_picker)) {
          cfg.start_date = cfg.start_date_buf;
          changed = true;
        }

        ImGui::TableNextRow();
        ImGui::TableNextColumn();
        ImGui::AlignTextToFramePadding();
        ImGui::Text("结束日期");
        if (ImGui::IsItemHovered()) {
          ImGui::SetTooltip("回测/分析结束日期 (YYYY-MM-DD)");
        }
        ImGui::TableNextColumn();
        if (DrawDatePicker("##end", "end_date_picker", cfg.end_date_buf, sizeof(cfg.end_date_buf), end_picker)) {
          cfg.end_date = cfg.end_date_buf;
          changed = true;
        }

        ImGui::EndTable();
      }
    }
    return changed;
  }

  bool DrawPathSection(Config &cfg) {
    bool changed = false;
    if (ImGui::CollapsingHeader("路径", ImGuiTreeNodeFlags_DefaultOpen)) {
      ImGui::TextWrapped("请尽量使用GPT分区 + XFS文件系统 以满足海量小文件的高性能读写需求");
      ImGui::Spacing();

      if (ImGui::BeginTable("path_table", 2, ImGuiTableFlags_SizingFixedFit)) {
        ImGui::TableSetupColumn("Label", ImGuiTableColumnFlags_WidthFixed, 150);
        ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthStretch);

        static std::string cwd = GetCWD();
        ImGui::TableNextRow();
        ImGui::TableNextColumn();
        ImGui::AlignTextToFramePadding();
        ImGui::Text("Working Directory");
        if (ImGui::IsItemHovered()) {
          ImGui::SetTooltip("当前工作目录 (相对路径基准)");
        }
        ImGui::TableNextColumn();
        ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.6f, 0.8f, 1.0f, 1.0f));
        ImGui::TextWrapped("%s", cwd.c_str());
        ImGui::PopStyleColor();

        auto draw_path_row = [&](const char *label, const char *tooltip, const char *input_id, char *buffer, size_t size, std::string &target) {
          ImGui::TableNextRow();
          ImGui::TableNextColumn();
          ImGui::AlignTextToFramePadding();
          ImGui::Text("%s", label);
          if (ImGui::IsItemHovered()) {
            ImGui::SetTooltip("%s", tooltip);
          }
          ImGui::TableNextColumn();
          ImGui::SetNextItemWidth(-1);
          if (ImGui::InputText(input_id, buffer, size)) {
            target = buffer;
            changed = true;
          }
        };

        draw_path_row("Archive Dir",
                      "L2原始CSV压缩包目录: YYYY/YYYYMM/YYYYMMDD.rar",
                      "##archive_dir",
                      cfg.archive_dir_buf,
                      sizeof(cfg.archive_dir_buf),
                      cfg.archive_dir);
        draw_path_row("Database Dir",
                      "L2二进制数据库目录: YYYY/MM/DD/ASSET_CODE.SH|SZ/ASSET_CODE.SH|SZ_orders|snapshots_数量.bin",
                      "##database_dir",
                      cfg.database_dir_buf,
                      sizeof(cfg.database_dir_buf),
                      cfg.database_dir);
        draw_path_row("Feature Dir",
                      "特征张量库目录: YYYY/MM/DD/",
                      "##feature_dir",
                      cfg.feature_dir_buf,
                      sizeof(cfg.feature_dir_buf),
                      cfg.feature_dir);
        draw_path_row("Factor Dir",
                      "因子库目录: YYYY/MM/DD/",
                      "##factor_dir",
                      cfg.factor_dir_buf,
                      sizeof(cfg.factor_dir_buf),
                      cfg.factor_dir);
        draw_path_row("Log Dir",
                      "日志目录",
                      "##log_dir",
                      cfg.log_dir_buf,
                      sizeof(cfg.log_dir_buf),
                      cfg.log_dir);
        draw_path_row("Config Dir",
                      "配置目录",
                      "##config_dir",
                      cfg.config_dir_buf,
                      sizeof(cfg.config_dir_buf),
                      cfg.config_dir);

        ImGui::EndTable();
      }
    }
    return changed;
  }

  bool DrawCsvSection(Config &cfg) {
    bool changed = false;
    if (ImGui::CollapsingHeader("L2原始数据", ImGuiTreeNodeFlags_DefaultOpen)) {
      if (ImGui::BeginTable("csv_table", 2, ImGuiTableFlags_SizingFixedFit)) {
        ImGui::TableSetupColumn("Label", ImGuiTableColumnFlags_WidthFixed, 150);
        ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthStretch);

        auto draw_csv_row = [&](const char *label, const char *tooltip, const char *input_id, char *buffer, size_t size, std::string &target) {
          ImGui::TableNextRow();
          ImGui::TableNextColumn();
          ImGui::AlignTextToFramePadding();
          ImGui::Text("%s", label);
          if (ImGui::IsItemHovered()) {
            ImGui::SetTooltip("%s", tooltip);
          }
          ImGui::TableNextColumn();
          ImGui::SetNextItemWidth(-1);
          if (ImGui::InputText(input_id, buffer, size)) {
            target = buffer;
            changed = true;
          }
        };

        draw_csv_row("Market Data CSV",
                     "3秒快照(tick)文件名",
                     "##csv_market",
                     cfg.csv_market_data_buf,
                     sizeof(cfg.csv_market_data_buf),
                     cfg.csv_market_data);
        draw_csv_row("Market Trade CSV",
                     "逐笔成交(trade)文件名",
                     "##csv_trade",
                     cfg.csv_tick_trade_buf,
                     sizeof(cfg.csv_tick_trade_buf),
                     cfg.csv_market_trade);
        draw_csv_row("Market Order CSV",
                     "逐笔委托(order)文件名",
                     "##csv_order",
                     cfg.csv_tick_order_buf,
                     sizeof(cfg.csv_tick_order_buf),
                     cfg.csv_market_order);

        ImGui::EndTable();
      }
    }
    return changed;
  }

  bool DrawBinarySection(Config &cfg) {
    bool changed = false;
    if (ImGui::CollapsingHeader("L2二进制数据库 解压/编码", ImGuiTreeNodeFlags_DefaultOpen)) {
      if (ImGui::BeginTable("binary_table", 2, ImGuiTableFlags_SizingFixedFit)) {
        ImGui::TableSetupColumn("Label", ImGuiTableColumnFlags_WidthFixed, 150);
        ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthStretch);

        auto draw_binary_row = [&](const char *label, const char *tooltip, const char *input_id, char *buffer, size_t size, std::string &target) {
          ImGui::TableNextRow();
          ImGui::TableNextColumn();
          ImGui::AlignTextToFramePadding();
          ImGui::Text("%s", label);
          if (ImGui::IsItemHovered()) {
            ImGui::SetTooltip("%s", tooltip);
          }
          ImGui::TableNextColumn();
          ImGui::SetNextItemWidth(-1);
          if (ImGui::InputText(input_id, buffer, size)) {
            target = buffer;
            changed = true;
          }
        };

        draw_binary_row("Archive Extension",
                        "压缩文件扩展名 (.rar/.7z/.zip)",
                        "##archive_ext",
                        cfg.archive_extension_buf,
                        sizeof(cfg.archive_extension_buf),
                        cfg.archive_extension);
        draw_binary_row("Archive Tool",
                        "压缩文件解压工具 (unrar/7z/unzip: 支持高效单文件解压(非固实))",
                        "##archive_tool",
                        cfg.archive_tool_buf,
                        sizeof(cfg.archive_tool_buf),
                        cfg.archive_tool);
        draw_binary_row("Archive Extract Cmd",
                        "压缩文件解压选项 (x for unrar, x for 7z)",
                        "##archive_cmd",
                        cfg.archive_extract_cmd_buf,
                        sizeof(cfg.archive_extract_cmd_buf),
                        cfg.archive_extract_cmd);
        draw_binary_row("Binary Extension",
                        "L2 二进制文件扩展名",
                        "##binary_ext",
                        cfg.binary_extension_buf,
                        sizeof(cfg.binary_extension_buf),
                        cfg.binary_extension);

        ImGui::EndTable();
      }
    }
    return changed;
  }

  void DrawStatusFooter(const Config &cfg) const {
    ImGui::Separator();
    ImGui::TextDisabled("File: %s", cfg.filepath.c_str());
    ImGui::SameLine();
    if (cfg.dirty) {
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "[Pending save...]");
    } else {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "[Synced]");
    }
  }

public:
  const char *GetName() const {
    return "Settings";
  }

  const char *GetStatus() const {
    if (!initial_sync_done) {
      return "initializing";
    }
    if (is_writing) {
      return "writing";
    }
    if (is_expanded) {
      return "syncing";
    }
    return "synced";
  }

  void OnExpand() {
    is_expanded = true;
  }

  void OnCollapse() {
    is_expanded = false;
  }

  void DrawPanel(SharedData &data, GuiState &gui_state) {
    EnsureConfigReady(data, gui_state);
    MaintainAutoSync(data.config);

    Config &cfg = data.config;
    bool changed = false;

    ImGui::BeginChild("ConfigPanel", ImVec2(800, 0), false);
    changed |= DrawPeriodSection(cfg);
    changed |= DrawPathSection(cfg);
    changed |= DrawCsvSection(cfg);
    changed |= DrawBinarySection(cfg);

    if (changed) {
      cfg.MarkDirty();
    }

    DrawStatusFooter(cfg);
    ImGui::EndChild();
  }
};

} // namespace

TaskHandle CreateSettingsTask() {
  auto instance = std::make_shared<SettingsTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.GetStatus = [instance]() { return instance->GetStatus(); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data, GuiState &gui) { instance->DrawPanel(data, gui); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
