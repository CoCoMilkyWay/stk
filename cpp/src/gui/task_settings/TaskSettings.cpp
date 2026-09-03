#include "gui/task_settings/TaskSettings.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "imgui.h"
#include "misc/cross_platform.hpp"
#include "shared/SharedData.hpp"
#include <charconv>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <ranges>
#include <string_view>

namespace GUI::Tasks {
namespace {

// Get current working directory
static std::string GetCWD() {
  return std::filesystem::current_path().string();
}

// Settings task - config management with auto-sync
class SettingsTask {
private:
  bool is_expanded_ = false;

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
    snprintf(btn_id, sizeof(btn_id), "\xef\x81\xb3##%s", popup_id); // Nerd Font calendar icon
    if (ImGui::Button(btn_id)) {
      // Parse date when opening popup
      if (strlen(date_buf) >= 10) {
        auto parts = std::string_view(date_buf) | std::views::split('-');
        auto it = parts.begin();
        std::from_chars((*it).data(), (*it).data() + std::ranges::distance(*it), state.year);
        ++it;
        std::from_chars((*it).data(), (*it).data() + std::ranges::distance(*it), state.month);
        ++it;
        std::from_chars((*it).data(), (*it).data() + std::ranges::distance(*it), state.day);
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
        std::tm tm_now = safe_localtime(&now);
        state.year = tm_now.tm_year + 1900;
        state.month = tm_now.tm_mon + 1;
        state.day = tm_now.tm_mday;
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

  void EnsureConfigReady(SharedData &data) {
    auto &ts = data.taskstate.settings;
    if (ts.initialized) {
      return;
    }
    data.config.log_callback = [&data](const std::string &msg) {
      data.terminal.AddLine(msg);
    };
    data.config.Initialize();
    ts.initialized = true;
  }

  void MaintainAutoSync(SharedData &data) {
    auto &ts = data.taskstate.settings;
    Config &cfg = data.config;

    if (!ts.initialized) {
      ts.status = TaskState::Settings::Status::Initializing;
      return;
    }

    if (!is_expanded_) {
      ts.status = TaskState::Settings::Status::Synced;
      cfg.AutoSync();
      return;
    }

    if (cfg.dirty) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - cfg.last_modified);
      if (elapsed.count() >= 150 && elapsed.count() < 250) {
        ts.status = TaskState::Settings::Status::Writing;
      } else {
        ts.status = TaskState::Settings::Status::Syncing;
      }
    } else {
      ts.status = TaskState::Settings::Status::Synced;
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
        draw_path_row("Orders Dir",
                      "L2逐笔二进制目录: YYYY/MM/DD/ASSET_CODE.SH|SZ.bin (委托+成交合并, 快照不落盘)",
                      "##orders_dir",
                      cfg.orders_dir_buf,
                      sizeof(cfg.orders_dir_buf),
                      cfg.orders_dir);
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

  // 只读一行: 与 draw_xxx_row 同构 (Label/Tooltip/Value 三段), 只是 Value 用
  // TextDisabled 而不是 InputText —— 这些是编译期常量 (namespace config, 不
  // 进 config.json), 标灰即表明"能看不能改".
  static void draw_readonly_row(const char *label, const char *tooltip, const std::string &value) {
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::AlignTextToFramePadding();
    ImGui::Text("%s", label);
    if (ImGui::IsItemHovered()) {
      ImGui::SetTooltip("%s", tooltip);
    }
    ImGui::TableNextColumn();
    ImGui::TextDisabled("%s", value.c_str());
  }

  bool DrawDatabaseSection(Config &cfg) {
    bool changed = false;
    if (ImGui::CollapsingHeader("数据库配置", ImGuiTreeNodeFlags_DefaultOpen)) {
      ImGui::TextWrapped("universe 无需配置: encode / features 两级 cache 均为全市场日频,"
                         " A 轴来自基本面股票全量 (注册表 output/fundamental/asset_axis.json)");
      ImGui::Spacing();

      auto readonly_table = [](const char *table_id, auto draw_rows) {
        if (ImGui::BeginTable(table_id, 2, ImGuiTableFlags_SizingFixedFit)) {
          ImGui::TableSetupColumn("Label", ImGuiTableColumnFlags_WidthFixed, 150);
          ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthStretch);
          draw_rows();
          ImGui::EndTable();
        }
      };

      // 数据源凭据 (与官方 CLI / 控制台 token 同源, 永不回传)
      ImGui::SeparatorText("数据源凭据");
      readonly_table("db_credential_table", [] {
        draw_readonly_row("BigQuant AK", "Flight Basic Token 用户名", config::BIGQUANT_AK);
        draw_readonly_row("BigQuant SK", "Flight Basic Token 密码 (永不回传)", config::BIGQUANT_SK);
        draw_readonly_row("Tushare Token", "Tushare pro token (*_vip 接口需 5000+ 积分)", config::TUSHARE_TOKEN);
      });

      // 数据源端点 (host / port / 超时 / 重试)
      ImGui::SeparatorText("数据源端点");
      readonly_table("db_endpoint_table", [] {
        draw_readonly_row("BigQuant Flight URI", "数据面: 明文 gRPC + Arrow IPC RecordBatch, 零拷贝", config::BIGQUANT_FLIGHT_URI);
        draw_readonly_row("gRPC Max Metadata", "SDK 默认 8KB 会被 JWT 撑爆 [bytes]", std::to_string(config::BIGQUANT_FLIGHT_GRPC_MAX_METADATA_SIZE));
        draw_readonly_row("Tushare Host", "明文 JSON POST, 三张事件表", config::TUSHARE_HTTP_HOST);
        draw_readonly_row("Tushare Port", "走 80, 省掉 SSL 依赖", config::TUSHARE_HTTP_PORT);
        draw_readonly_row("Tushare Timeout", "单次连接+读写整体时长 [s]", std::to_string(config::TUSHARE_HTTP_TIMEOUT_SECONDS));
        draw_readonly_row("Tushare Retry Max", "额外重试次数 (共 N+1 次尝试)", std::to_string(config::TUSHARE_HTTP_RETRY_MAX));
        draw_readonly_row("Tushare Retry Interval", "重试间隔 [s], 线性递增", std::to_string(config::TUSHARE_HTTP_RETRY_INTERVAL_SECONDS));
      });

      // 抓取流水线 (落地 output/fundamental/YYYY-MM/*.parquet)
      ImGui::SeparatorText("抓取流水线");
      readonly_table("db_pipeline_table", [] {
        draw_readonly_row("Pipeline Start", "数据同步起点, 与回测窗口语义不同, 不随其收窄", config::PIPELINE_START_DATE);
        draw_readonly_row("Lookback Days", "月末仍在窗口内视为开放月 (兜服务端回填修订)", std::to_string(config::PIPELINE_LOOKBACK_DAYS));
        draw_readonly_row("Dedup Window", "parquet mtime 距今 < 该值则本表跳过 [s]", std::to_string(config::PIPELINE_DEDUP_WINDOW_SECONDS));
      });

      ImGui::Spacing();
      ImGui::TextDisabled("以上均为编译期常量 (shared/Config.hpp namespace config), 不进 config.json");
    }
    return changed;
  }

  void DrawStatusFooter(const Config &cfg) const {
    ImGui::TextWrapped("File:");
    ImGui::TextDisabled("%s", cfg.filepath.c_str());

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::Text("Status:");
    if (cfg.dirty) {
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Pending\nsave...");
    } else {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Synced");
    }
  }

public:
  const char *GetName() const {
    return "Settings";
  }

  // 与 DrawPanel/选中态解耦: 创建后立即落盘配置到内存, 让 Database 等任务的
  // Init 能在第一帧前拿到真实 backtest range (顺序见 Tasks.cpp::CreateAllTasks).
  void Init(SharedData &data) {
    EnsureConfigReady(data);
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
  }

  void DrawPanel(SharedData &data) {
    EnsureConfigReady(data);
    MaintainAutoSync(data);

    Config &cfg = data.config;
    bool changed = false;

    // Left side: Config panel
    ImGui::BeginChild("ConfigPanel", ImVec2(800, 0), false);
    changed |= DrawPeriodSection(cfg);
    changed |= DrawPathSection(cfg);
    changed |= DrawCsvSection(cfg);
    changed |= DrawDatabaseSection(cfg);

    if (changed) {
      cfg.MarkDirty();
    }
    ImGui::EndChild();

    // Right side: Status bar
    ImGui::SameLine();
    ImGui::BeginChild("StatusPanel", ImVec2(200, 0), false);
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
  handle.Init = [instance](SharedData &data) { instance->Init(data); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data) { instance->DrawPanel(data); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
