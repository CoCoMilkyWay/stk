// Task Features - Feature Engineering Task
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "gui/task_features/services/DataLoader.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "gui/task_features/services/TimeSeriesService.hpp"
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/ui/TabTimeSeries.hpp"
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
  std::unique_ptr<Features::DataLoader> data_loader;
  std::unique_ptr<Features::DistService> dist_service;
  std::unique_ptr<Features::TimeSeriesService> timeseries_service;

  // UI State
  int selected_tab = 0;
  Features::FeatureUIState feature_ui_state;
  Features::ComputeState compute_state;
  Features::DistUIState dist_ui_state;
  Features::TimeSeriesUIState timeseries_ui_state;

  // Tab state
  bool orderflow_tab_was_active = false;
  bool dist_tab_was_active = false;
  bool timeseries_tab_was_active = false;

  // Compute status tracking (to detect completion)
  Features::ComputeStatus prev_compute_status = Features::ComputeStatus::Idle;

  // Auto-compute tracking (Dist)
  int prev_primary_feature_idx = -1; // Track feature selection changes
  int prev_selected_level = 0;       // Track level changes

  // Auto-compute tracking (TimeSeries)
  int timeseries_prev_step = -1;           // Track step changes, -1 = first entry
  int timeseries_prev_feature_idx = -1;    // Track feature changes for timeseries
  int timeseries_prev_level = -1;          // Track level changes for timeseries

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
    if (!state->data_loader) {
      state->data_loader = std::make_unique<Features::DataLoader>(data.config.feature_dir);
    }
    if (!state->dist_service) {
      state->dist_service = std::make_unique<Features::DistService>(data.config.feature_dir);
    }
    if (!state->timeseries_service) {
      state->timeseries_service = std::make_unique<Features::TimeSeriesService>(data.config.feature_dir);
    }

    // Auto-trigger Dist compute on feature selection change
    if (state->dist_service) {
      auto &sel = data.feature.selection;

      // Detect change
      bool feature_changed = (sel.primary_feature_idx != state->prev_primary_feature_idx);
      bool level_changed = (sel.selected_level != state->prev_selected_level);
      bool has_valid_selection = (sel.primary_feature_idx >= 0);

      if ((feature_changed || level_changed) && has_valid_selection) {
        // Cancel old computation if running
        if (data.dist.compute.is_busy()) {
          data.dist.cancel();
        }

        // Trigger new computation
        state->dist_service->RequestCompute();

        // Update tracking
        state->prev_primary_feature_idx = sel.primary_feature_idx;
        state->prev_selected_level = sel.selected_level;
      }

      // Update tracking even if no change (initialization case)
      if (!feature_changed && state->prev_primary_feature_idx == -1) {
        state->prev_primary_feature_idx = sel.primary_feature_idx;
        state->prev_selected_level = sel.selected_level;
      }
    }

    // Handle trigger from UI
    if (state->compute_state.trigger_start) {
      state->compute_state.trigger_start = false;
      const int num_workers = (state->compute_state.num_workers == 0)
                                  ? misc::Affinity::core_count()
                                  : state->compute_state.num_workers;
      state->compute_service->start_compute(num_workers);
    }

    // Detect compute completion and mark L1 for reload
    {
      auto current_status = state->compute_service->get_status();
      if (state->prev_compute_status == Features::ComputeStatus::Running &&
          (current_status == Features::ComputeStatus::Completed ||
           current_status == Features::ComputeStatus::Cancelled)) {
        // Compute just finished - mark OrderFlow L1 cache for reload
        data.orderflow.loader.l1_needs_reload = true;
      }
      state->prev_compute_status = current_status;
    }

    // Render tabs
    ImGui::BeginChild("FeaturesTabs", ImVec2(0, 0), false);

    if (ImGui::BeginTabBar("FeaturesTabBar", ImGuiTabBarFlags_None)) {
      // Tab 1: Feature (NEW - first tab)
      if (ImGui::BeginTabItem("Feature")) {
        ImGui::Spacing();
        Features::RenderTabFeature(data, state->feature_ui_state);
        ImGui::EndTabItem();
      }

      // Tab 2: Compute
      if (ImGui::BeginTabItem("Compute")) {
        ImGui::Spacing();
        Features::RenderTabCompute(state->compute_service.get(), state->compute_state, data.asset, data.config);
        ImGui::EndTabItem();
      }

      // Tab 3: OrderFlow
      bool orderflow_tab_open = ImGui::BeginTabItem("OrderFlow");
      if (orderflow_tab_open) {
        ImGui::Spacing();
        Features::RenderTabOrderFlow(state->data_loader.get(), data);
        ImGui::EndTabItem();
      }

      // Handle OrderFlow tab lifecycle (blocking start/stop)
      if (orderflow_tab_open && !state->orderflow_tab_was_active) {
        // Tab just opened - coroutine started in RenderTabOrderFlow
        state->orderflow_tab_was_active = true;
      } else if (!orderflow_tab_open && state->orderflow_tab_was_active) {
        // Tab just closed - stop coroutine (blocking)
        Features::StopTabOrderFlow(state->data_loader.get(), data);
        state->orderflow_tab_was_active = false;
      }

      // Tab 4: Distribution
      bool dist_tab_open = ImGui::BeginTabItem("Distribution");
      if (dist_tab_open) {
        ImGui::Spacing();
        Features::RenderTabDist(state->dist_service.get(), data, state->dist_ui_state);
        ImGui::EndTabItem();
      }

      // Handle Distribution tab lifecycle (blocking start/stop)
      if (dist_tab_open && !state->dist_tab_was_active) {
        // Tab just opened - coroutine started in RenderTabDist
        state->dist_tab_was_active = true;
      } else if (!dist_tab_open && state->dist_tab_was_active) {
        // Tab just closed - stop coroutine (blocking)
        Features::StopTabDist(state->dist_service.get(), data);
        state->dist_tab_was_active = false;
      }

      // Tab 5: TimeSeries
      bool timeseries_tab_open = ImGui::BeginTabItem("TimeSeries");
      if (timeseries_tab_open) {
        ImGui::Spacing();
        Features::RenderTabTimeSeries(state->timeseries_service.get(), data,
                                      state->timeseries_ui_state);
        ImGui::EndTabItem();
      }

      // Handle TimeSeries tab lifecycle
      if (timeseries_tab_open && !state->timeseries_tab_was_active) {
        state->timeseries_tab_was_active = true;
        state->timeseries_prev_step = -1; // Reset to trigger first compute
      } else if (!timeseries_tab_open && state->timeseries_tab_was_active) {
        Features::StopTabTimeSeries(state->timeseries_service.get(), data);
        state->timeseries_tab_was_active = false;
      }

      // Auto-trigger TimeSeries compute on step/feature change
      // Note: Must check is_running() to ensure coroutine is ready before RequestCompute
      if (timeseries_tab_open && state->timeseries_service &&
          state->timeseries_service->is_running()) {
        auto &ts = data.timeseries;
        auto &sel = data.feature.selection;
        int current_step = state->timeseries_ui_state.selected_step;

        // Check for feature/level change - clear all step data
        bool feature_changed = (sel.primary_feature_idx != state->timeseries_prev_feature_idx);
        bool level_changed = (sel.selected_level != state->timeseries_prev_level);
        if (feature_changed || level_changed) {
          ts.clear();  // Clear all step data
          state->timeseries_prev_feature_idx = sel.primary_feature_idx;
          state->timeseries_prev_level = sel.selected_level;
        }

        bool step_changed = (current_step != state->timeseries_prev_step);
        bool has_valid_selection = (sel.primary_feature_idx >= 0);

        // Check if current step data is already valid (avoid redundant compute)
        bool step_needs_compute = false;
        switch (current_step) {
        case 0: step_needs_compute = !ts.step0_stationarity.valid; break;
        case 1: step_needs_compute = !ts.step1_frequency.valid; break;
        case 2: step_needs_compute = !ts.step2_arma.valid; break;
        case 3: step_needs_compute = !ts.step3_residual.valid; break;
        case 4: step_needs_compute = !ts.step4_temporal_decay.valid; break;
        default: break;
        }

        // Trigger compute if step changed OR feature changed (with invalid data)
        if (step_changed || feature_changed || level_changed) {
          state->timeseries_prev_step = current_step;
          if (step_needs_compute && has_valid_selection && !ts.compute.is_busy()) {
            state->timeseries_service->RequestCompute();
          }
        }
      }

      ImGui::EndTabBar();
    }

    ImGui::EndChild();
  };

  // Destroy
  handle.Destroy = [state]() {
    // Note: OrderFlow and Dist coroutines will auto-cancel on CoroutineHandle destruction

    if (state->compute_service) {
      if (state->compute_service->is_running()) {
        state->compute_service->stop_compute();
      }
      state->compute_service.reset();
    }

    state->data_loader.reset();
    state->dist_service.reset();
    state->timeseries_service.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
