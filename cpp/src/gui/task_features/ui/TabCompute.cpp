// Tab Compute Implementation
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/affinity.hpp"
#include "shared/Asset.hpp"
#include "shared/Config.hpp"

#include "imgui.h"

namespace GUI::Features {

void RenderTabCompute(ComputeService *service, ComputeState &state, Asset & /*asset*/, Config &config) {
  const auto status = service->get_status();
  const bool is_running = status == ComputeStatus::Running;

  // ========================================================================
  // State Machine Logic
  // ========================================================================
  switch (state.ui_state) {
  case ComputeUIState::Idle:
    // Do nothing, waiting for user action
    break;

  case ComputeUIState::ShowingPopup:
    // Wait for popup to render (at least 1 frame)
    state.popup_frame_count++;
    if (state.popup_frame_count >= 2) {
      // Popup has been rendered, trigger start immediately
      state.trigger_start = true;
      state.ui_state = ComputeUIState::WaitingStart;
    }
    break;

  case ComputeUIState::WaitingStart:
    // Wait for computation to actually start
    if (is_running) {
      state.ui_state = ComputeUIState::Computing;
    }
    break;

  case ComputeUIState::Computing:
    // Wait for computation to finish
    if (!is_running) {
      // Reset to idle
      state.ui_state = ComputeUIState::Idle;
      state.popup_frame_count = 0;
      state.trigger_start = false;
    }
    break;
  }

  ImGui::TextWrapped("Feature Computation - Multi-threaded feature extraction from binary database");
  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 1: Status
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Status");
  ImGui::Spacing();

  const char *status_text = "Idle";
  ImVec4 status_color = ImVec4(0.7f, 0.7f, 0.7f, 1.0f);

  switch (status) {
  case ComputeStatus::Running:
    status_text = "Running";
    status_color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f);
    break;
  case ComputeStatus::Completed:
    status_text = "Completed";
    status_color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f);
    break;
  case ComputeStatus::Cancelled:
    status_text = "Cancelled";
    status_color = ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
    break;
  case ComputeStatus::Error:
    status_text = "Error";
    status_color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f);
    break;
  default:
    break;
  }

  ImGui::TextColored(status_color, "%s", status_text);

  if (is_running) {
    ImGui::SameLine();
    ImGui::TextDisabled("(GUI sleeping, all CPU for computation)");
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 2: Configuration
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Configuration");
  ImGui::Spacing();

  const int max_cores = misc::Affinity::core_count();

  ImGui::Text("Worker Threads:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(150);

  if (state.num_workers == 0) {
    if (ImGui::InputInt("##workers", &state.num_workers, 1, 1)) {
      if (state.num_workers < 0)
        state.num_workers = 0;
      if (state.num_workers > max_cores)
        state.num_workers = max_cores;
    }
    ImGui::SameLine();
    ImGui::TextDisabled("(auto: %d cores)", max_cores);
  } else {
    if (ImGui::InputInt("##workers", &state.num_workers, 1, 1)) {
      if (state.num_workers < 3)
        state.num_workers = 3; // min: 1 TS + 1 CS + 1 IO
      if (state.num_workers > max_cores)
        state.num_workers = max_cores;
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("Auto")) {
      state.num_workers = 0;
    }
  }

  const int actual_workers = (state.num_workers == 0) ? max_cores : state.num_workers;
  const int ts_workers = actual_workers - 2;
  const int cs_workers = 1;
  const int io_workers = 1;

  ImGui::Indent();
  ImGui::TextDisabled("TS workers: %d (time-series, parallel)", ts_workers);
  ImGui::TextDisabled("CS worker: %d (cross-sectional, single-threaded)", cs_workers);
  ImGui::TextDisabled("IO worker: %d (disk flush, single-threaded)", io_workers);
  ImGui::Unindent();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 3: Architecture
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Architecture");
  ImGui::Spacing();

  ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.6f, 0.6f, 0.6f, 1.0f)); // Gray text
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);                      // Use smaller font if available

  ImGui::TextWrapped("按照日期, 切分多频率Tensor: {[T, F, A]_level0(tick), [T, F, A]_level1(minute)}_dayN");
  ImGui::Text("T (Time):    max = 100,000  (100K time indices per day (ms/s))");
  ImGui::Text("A (Asset):   max = 1,000    (1K assets in universe)");
  ImGui::Text("F (Feature): max = 1,000    (all feature types combined)");
  ImGui::Text("特征子类型示例: F_TS: (600 时序特征) F_CS (250 截面特征) F_LB (50 标签) F_SH (50 共享中间值) F_META (50 元数据)");
  ImGui::Text("访问模式分析 (优化目标: 最小化总内存访问时间)");

  // Access pattern table
  if (ImGui::BeginTable("AccessPattern", 7, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("操作");
    ImGui::TableSetupColumn("循环结构");
    ImGui::TableSetupColumn("单次vector访问");
    ImGui::TableSetupColumn("总内存访问量");
    ImGui::TableSetupColumn("权重");
    ImGui::TableSetupColumn("并行度");
    ImGui::TableSetupColumn("描述", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("TS_write_TS");
    ImGui::TableNextColumn();
    ImGui::Text("for a: for t: write[F_TS]");
    ImGui::TableNextColumn();
    ImGui::Text("连续写600个");
    ImGui::TableNextColumn();
    ImGui::Text("TxAxF_TS = 240GB");
    ImGui::TableNextColumn();
    ImGui::Text("39%%");
    ImGui::TableNextColumn();
    ImGui::Text("10 cores");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("每个core处理~100个assets, 对每个asset遍历时间, 在(a,t)处连续写F_TS个features");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("CS_read_TS");
    ImGui::TableNextColumn();
    ImGui::Text("for t: for f: read[A]");
    ImGui::TableNextColumn();
    ImGui::Text("连续读1000个");
    ImGui::TableNextColumn();
    ImGui::Text("Tx(F_TS+F_OT)xA = 280GB");
    ImGui::TableNextColumn();
    ImGui::Text("45%%");
    ImGui::TableNextColumn();
    ImGui::Text("1 core");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("在每个时刻t, 对每个feature f, 连续读取所有A个assets的值(截面计算)");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("CS_write_CS");
    ImGui::TableNextColumn();
    ImGui::Text("for t: for f: write[A]");
    ImGui::TableNextColumn();
    ImGui::Text("连续写1000个");
    ImGui::TableNextColumn();
    ImGui::Text("TxF_CSxA = 100GB");
    ImGui::TableNextColumn();
    ImGui::Text("16%%");
    ImGui::TableNextColumn();
    ImGui::Text("1 core");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("在每个时刻t, 对每个feature f, 连续写入所有A个assets的值(截面结果)");

    ImGui::EndTable();
  }

  // Memory layout table
  if (ImGui::BeginTable("MemoryLayout", 6, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("布局");
    ImGui::TableSetupColumn("地址公式");
    ImGui::TableSetupColumn("TS_write(39%%)");
    ImGui::TableSetupColumn("CS_read(45%%)");
    ImGui::TableSetupColumn("CS_write(16%%)");
    ImGui::TableSetupColumn("推荐度");
    ImGui::TableHeadersRow();

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[T][A][F]");
    ImGui::TableNextColumn();
    ImGui::Text("(t*A+a)*F+f");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("良好");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[T][F][A]");
    ImGui::TableNextColumn();
    ImGui::Text("(t*F+f)*A+a");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("最优 √√√");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[A][T][F]");
    ImGui::TableNextColumn();
    ImGui::Text("(a*T+t)*F+f");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("较差");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[A][F][T]");
    ImGui::TableNextColumn();
    ImGui::Text("(a*F+f)*T+t");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("极差");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[F][T][A]");
    ImGui::TableNextColumn();
    ImGui::Text("(f*T+t)*A+a");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("不推荐");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[F][A][T]");
    ImGui::TableNextColumn();
    ImGui::Text("(f*A+a)*T+t");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("极差");

    ImGui::EndTable();
  }

  ImGui::PopFont();
  ImGui::PopStyleColor();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 4: Actions
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Actions");
  ImGui::Spacing();

  if (state.ui_state == ComputeUIState::Idle) {
    if (ImGui::Button("Start Compute", ImVec2(200, 30))) {
      state.ui_state = ComputeUIState::ShowingPopup;
      state.popup_frame_count = 0;
    }
  } else {
    ImGui::TextWrapped("Computation in progress...");
  }

  // ========================================================================
  // Popup Display (based on state machine)
  // ========================================================================
  const bool should_show_popup = (state.ui_state == ComputeUIState::ShowingPopup ||
                                  state.ui_state == ComputeUIState::WaitingStart ||
                                  state.ui_state == ComputeUIState::Computing);

  if (should_show_popup) {
    // Set popup position to center on first frame
    ImVec2 center = ImGui::GetMainViewport()->GetCenter();
    ImGui::SetNextWindowPos(center, ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    ImGui::OpenPopup("Feature Computation##Compute");
  }

  if (ImGui::BeginPopupModal("Feature Computation##Compute", nullptr,
                             ImGuiWindowFlags_AlwaysAutoResize | ImGuiWindowFlags_NoResize)) {
    // Check if we should close popup (state returned to Idle)
    if (state.ui_state == ComputeUIState::Idle) {
      ImGui::CloseCurrentPopup();
      ImGui::EndPopup();
      return; // Exit early to avoid rendering popup content
    }

    // Fixed width to prevent resize when text changes
    ImGui::PushItemWidth(500.0f);

    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.8f, 0.0f, 1.0f));
    ImGui::Text("Feature Computation Starting");
    ImGui::PopStyleColor();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::Text("Backtest Period:");
    ImGui::Indent();
    ImGui::BulletText("Start: %s", config.start_date.c_str());
    ImGui::BulletText("End: %s", config.end_date.c_str());
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Text("System Status:");
    ImGui::Indent();
    ImGui::BulletText("GUI entering sleep mode");
    ImGui::BulletText("Using %d CPU cores", actual_workers);
    ImGui::BulletText("Processing backtest period only");
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    // Fixed-width text area to prevent popup resize
    ImGui::BeginChild("StatusText", ImVec2(450.0f, 30.0f), false, ImGuiWindowFlags_NoScrollbar);

    // Display status based on state machine
    switch (state.ui_state) {
    case ComputeUIState::ShowingPopup:
      ImGui::Text("Starting computation...");
      break;
    case ComputeUIState::WaitingStart:
    case ComputeUIState::Computing:
      ImGui::Text("Computing... GUI is frozen, please wait.");
      break;
    default:
      break;
    }

    ImGui::EndChild();
    ImGui::PopItemWidth();

    ImGui::EndPopup();
  }
}

} // namespace GUI::Features
