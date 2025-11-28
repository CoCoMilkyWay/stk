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
  // Section 3: Actions
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
