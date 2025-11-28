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
  
  // Close popup only when computation finishes (not running and was triggered)
  if (!is_running && state.show_warning_popup && state.trigger_start) {
    state.show_warning_popup = false;
    state.warning_display_time = 0.0f;
    state.trigger_start = false;
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

  if (!is_running) {
    if (ImGui::Button("Start Compute", ImVec2(200, 30))) {
      state.show_warning_popup = true;
      state.warning_display_time = 0.0f;
    }
  } else {
    ImGui::TextWrapped("Computation running... GUI is in sleep mode.");
  }

  // Info popup (will be displayed throughout computation due to GUI freeze)
  // Only open popup on button click
  if (state.show_warning_popup) {
    // Set popup position to center on first frame
    ImVec2 center = ImGui::GetMainViewport()->GetCenter();
    ImGui::SetNextWindowPos(center, ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    ImGui::OpenPopup("Feature Computation##Compute");
  }

  if (ImGui::BeginPopupModal("Feature Computation##Compute", nullptr, 
                             ImGuiWindowFlags_AlwaysAutoResize)) {
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
    
    // Wait a moment for popup to fully render before starting
    // Only increment timer if not already triggered
    if (!state.trigger_start) {
      const float display_delay = 1.0f; // 1 second
      state.warning_display_time += ImGui::GetIO().DeltaTime;
      
      if (state.warning_display_time >= display_delay) {
        // Trigger start after delay (only once)
        // DON'T close popup - let it stay visible during freeze
        state.trigger_start = true;
      } else {
        float remaining = display_delay - state.warning_display_time;
        ImGui::TextWrapped("Starting in %.1f seconds...", remaining);
      }
    } else {
      // After trigger, show that computation is running
      // This message will be frozen on screen
      ImGui::TextWrapped("Computing... GUI is frozen, please wait.");
    }
    
    ImGui::EndPopup();
  }
}

} // namespace GUI::Features
