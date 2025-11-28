// Task Features - Feature Engineering Task
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/affinity.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

namespace GUI::Tasks {

// ============================================================================
// Task Features State
// ============================================================================

struct TaskFeaturesState {
  // Services
  std::unique_ptr<Features::ComputeService> compute_service;

  // UI State
  int selected_tab = 0;
  Features::ComputeState compute_state;

  // Terminal reference
  TaskTerminal *terminal = nullptr;
};

// ============================================================================
// Task Features Implementation
// ============================================================================

TaskHandle CreateFeaturesTask() {
  auto state = std::make_shared<TaskFeaturesState>();

  TaskHandle handle;
  handle.name = "Features";
  handle.storage = state;
  handle.task_instance = state.get();

  // GetStatus
  handle.GetStatus = [state]() -> const char * {
    if (!state->compute_service)
      return "";

    const auto status = state->compute_service->get_status();
    switch (status) {
    case Features::ComputeStatus::Running:
      return "computing";
    case Features::ComputeStatus::Completed:
      return "complete";
    case Features::ComputeStatus::Cancelled:
      return "cancelled";
    case Features::ComputeStatus::Error:
      return "error";
    default:
      return "";
    }
  };

  // OnExpand
  handle.OnExpand = [state]() {
    // Initialization will happen in DrawPanel (first call)
  };

  // OnCollapse
  handle.OnCollapse = [state]() {
    // Cleanup if needed (but keep state for resume)
  };

  // DrawPanel
  handle.DrawPanel = [state](SharedData &data) {
    // Lazy initialization
    if (!state->compute_service) {
      state->terminal = &data.gui.terminal;
      state->compute_service = std::make_unique<Features::ComputeService>(data);
    }

    // Handle trigger from UI
    if (state->compute_state.trigger_start) {
      state->compute_state.trigger_start = false;
      const int num_workers = (state->compute_state.num_workers == 0)
                                  ? misc::Affinity::core_count()
                                  : state->compute_state.num_workers;
      state->compute_service->start_compute(num_workers);
    }

    // Render tabs
    ImGui::BeginChild("FeaturesTabs", ImVec2(0, 0), false);

    if (ImGui::BeginTabBar("FeaturesTabBar", ImGuiTabBarFlags_None)) {
      // Tab: Compute
      if (ImGui::BeginTabItem("Compute")) {
        ImGui::Spacing();
        Features::RenderTabCompute(state->compute_service.get(), state->compute_state, data.asset);
        ImGui::EndTabItem();
      }

      // Future tabs can be added here (e.g., Analysis, Visualization, etc.)

      ImGui::EndTabBar();
    }

    ImGui::EndChild();
  };

  // Destroy
  handle.Destroy = [state]() {
    if (state->compute_service) {
      if (state->compute_service->is_running()) {
        state->compute_service->stop_compute();
      }
      state->compute_service.reset();
    }
  };

  return handle;
}

} // namespace GUI::Tasks
