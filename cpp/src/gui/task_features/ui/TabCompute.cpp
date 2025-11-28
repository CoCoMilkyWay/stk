// Tab Compute Implementation
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/affinity.hpp"
#include "shared/Asset.hpp"

#include "imgui.h"

namespace GUI::Features {

void RenderTabCompute(ComputeService *service, ComputeState &state, Asset & /*asset*/) {
  const auto status = service->get_status();
  const auto progress = service->get_progress();
  const bool is_running = status == ComputeStatus::Running;

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
  // Section 3: Progress (if running)
  // ========================================================================
  if (is_running) {
    ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Progress");
    ImGui::Spacing();

    ImGui::Text("Dates: %zu / %zu (%.1f%%)",
                progress.completed_dates, progress.total_dates, progress.progress_percent());
    ImGui::ProgressBar(progress.progress_percent() / 100.0f, ImVec2(-1, 0));

    ImGui::Text("Assets: %zu", progress.total_assets);
    ImGui::Text("Elapsed: %.1f s", progress.elapsed_seconds);

    if (progress.compute_rate > 0) {
      ImGui::Text("Rate: %.2f dates/s", progress.compute_rate);
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
  }

  // ========================================================================
  // Section 4: Actions
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Actions");
  ImGui::Spacing();

  if (is_running) {
    if (ImGui::Button("Stop Compute", ImVec2(200, 30))) {
      service->stop_compute();
    }
  } else {
    if (ImGui::Button("Start Compute", ImVec2(200, 30))) {
      state.show_warning_popup = true;
      state.warning_popup_timer = 0.0f;
    }
  }

  // Warning popup with auto-start
  if (state.show_warning_popup) {
    ImGui::OpenPopup("Feature Computation Warning##Compute");
    state.show_warning_popup = false;
  }

  if (ImGui::BeginPopupModal("Feature Computation Warning##Compute", nullptr, 
                             ImGuiWindowFlags_AlwaysAutoResize | ImGuiWindowFlags_NoMove)) {
    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.8f, 0.0f, 1.0f));
    ImGui::TextWrapped("NOTICE: Starting Feature Computation");
    ImGui::PopStyleColor();
    
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    
    ImGui::TextWrapped("The following will happen:");
    ImGui::BulletText("GUI will enter sleep mode (unresponsive)");
    ImGui::BulletText("All %d CPU cores will be used for computation", actual_workers);
    ImGui::BulletText("Extract features from binary database");
    ImGui::BulletText("Process backtest period dates only");
    ImGui::BulletText("This may take several minutes or hours");
    
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    
    // Auto-start countdown
    const float auto_start_delay = 3.0f; // 3 seconds
    state.warning_popup_timer += ImGui::GetIO().DeltaTime;
    
    if (state.warning_popup_timer < auto_start_delay) {
      float remaining = auto_start_delay - state.warning_popup_timer;
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), 
                        "Starting in %.1f seconds...", remaining);
      ImGui::ProgressBar(state.warning_popup_timer / auto_start_delay, ImVec2(-1, 0));
    } else {
      // Auto-start triggered
      state.trigger_start = true;
      ImGui::CloseCurrentPopup();
    }
    
    ImGui::Spacing();
    
    // Manual cancel button
    if (ImGui::Button("Cancel", ImVec2(-1, 0))) {
      state.warning_popup_timer = 0.0f;
      ImGui::CloseCurrentPopup();
    }
    
    ImGui::EndPopup();
  }
}

} // namespace GUI::Features
