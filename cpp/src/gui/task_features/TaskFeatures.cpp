// Task Features - Feature Engineering Task
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/services/TimeSeriesService.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/ui/TabTimeSeries.hpp"
#include "gui/task_features/ui/TabTransform.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

namespace GUI::Tasks {

// ============================================================================
// Tab Index Enum
// ============================================================================

enum TabIdx {
  TAB_FEATURE = 0,
  TAB_COMPUTE,
  TAB_TRANSFORM,
  TAB_DISTRIBUTION,
  TAB_TIMESERIES,
  TAB_ORDERFLOW,
  TAB_COUNT
};

// ============================================================================
// Task Features State
// ============================================================================

struct TaskFeaturesState {
  // Services
  std::unique_ptr<Features::ComputeService> compute_service;
  std::unique_ptr<Features::OrderFlowService> orderflow_service;
  std::unique_ptr<Features::DistService> dist_service;
  std::unique_ptr<Features::TimeSeriesService> timeseries_service;
  std::unique_ptr<Features::TransformService> transform_service;

  // UI State
  int active_tab = -1; // 当前选中 tab (由 DrawTab 入口写入), -1 = 未选中
  int locked_tab = -1;
  bool tabs_locked = false;
  // IsTabEnabled 缓存: DrawTab 每帧更新, 左栏渲染时读 (一帧延迟不可见)
  bool tab_enabled_cache[TAB_COUNT] = {};
  bool tab_cache_valid = false;
  Features::FeatureUIState feature_ui_state;
  Features::ComputeState compute_state;
  Features::TransformUIState transform_ui_state;
  Features::DistUIState dist_ui_state;
  Features::TimeSeriesUIState timeseries_ui_state;

  // Tab state
  bool dist_tab_was_active = false;
  bool timeseries_tab_was_active = false;
  bool transform_tab_was_active = false;

  // Compute status tracking (to detect completion)
  Features::ComputeStatus prev_compute_status = Features::ComputeStatus::Idle;

  // Auto-compute tracking (Dist)
  int prev_primary_feature_idx = -1; // Track feature selection changes
  int prev_selected_level = 0;       // Track level changes
  bool dist_prewarmed = false;       // 输入就绪后预热一次 (切出任务时回收并复位)

  // Auto-compute tracking (TimeSeries)
  int timeseries_prev_step = -1;        // Track step changes, -1 = first entry
  int timeseries_prev_feature_idx = -1; // Track feature changes for timeseries
  int timeseries_prev_level = -1;       // Track level changes for timeseries

  // Auto-compute tracking (Transform)
  int transform_prev_feature_idx = -1; // Track feature changes for transform
  int transform_prev_level = -1;       // Track level changes for transform
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

  // OnExpand: 切到 Features 任务时由 Gui.cpp 默认 selected_tab=0 (首个子项).
  // Services 延迟到首次 DrawTab 时创建.
  handle.OnExpand = [state]() {
    // no-op
  };

  // OnCollapse: 切出 Features 任务才回收 Dist 的构建内存 (任务内切 tab 不回收)
  handle.OnCollapse = [state]() {
    if (state->dist_service) {
      state->dist_service->Shutdown();
      state->dist_prewarmed = false;      // 重进任务时重新预热
      state->dist_tab_was_active = false; // 数据已清, 重进按"初次进 tab"走自动重算
    }
  };

  // 子项 (叶子) 名字, 顺序与 TabIdx 一致
  handle.tab_names = {"Feature", "Compute", "Transform", "Distribution",
                      "TimeSeries", "OrderFlow"};

  // IsTabEnabled: 读上一帧 DrawTab 缓存的使能状态 (未缓存前全开, 避免阻塞首次选择)
  handle.IsTabEnabled = [state](int i) -> bool {
    if (i < 0 || i >= TAB_COUNT)
      return false;
    if (!state->tab_cache_valid)
      return true;
    return state->tab_enabled_cache[i];
  };

  // DrawTab: 渲染指定 tab 内容 + 每帧状态更新 + 生命周期/自动重算触发
  handle.DrawTab = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    state->active_tab = idx;

    // Lazy initialization
    if (!state->compute_service) {
      state->compute_service = std::make_unique<Features::ComputeService>(data);
    }
    if (!state->orderflow_service) {
      state->orderflow_service = std::make_unique<Features::OrderFlowService>();
    }
    if (!state->dist_service) {
      state->dist_service = std::make_unique<Features::DistService>(data.config.feature_dir);
    }
    if (!state->timeseries_service) {
      state->timeseries_service = std::make_unique<Features::TimeSeriesService>(data.config.feature_dir);
    }
    if (!state->transform_service) {
      state->transform_service = std::make_unique<Features::TransformService>(data.config.feature_dir);
    }

    // Update features task state
    auto &fs = data.taskstate.features;
    const bool feature_inputs_ready =
        data.taskstate.database.all_json_ready &&
        data.taskstate.database.binary_scanned &&
        data.asset.binary.exists &&
        !data.asset.backtest.required_dates.empty() &&
        !data.taskstate.database.l2_scan_inflight;
    const bool has_selection = (data.feature.selection.primary_feature_idx >= 0);
    fs.has_selection = has_selection;

    if (!feature_inputs_ready) {
      fs.status = TaskState::Features::Status::Waiting;
      fs.computing = false;
    } else if (state->compute_service &&
               state->compute_service->get_status() == Features::ComputeStatus::Running) {
      fs.status = TaskState::Features::Status::Computing;
      fs.computing = true;
    } else if (!has_selection) {
      fs.status = TaskState::Features::Status::Selecting;
      fs.computing = false;
    } else {
      fs.status = TaskState::Features::Status::Ready;
      fs.computing = false;
    }

    // Dist 预热: 输入就绪即起 worker 并预分配全部构建内存 (worker 线程做, 不卡帧),
    // 点 Distribution 零额外分配; 内存保留到切出 Features 任务 (OnCollapse 回收).
    // 先入队预热再起线程: worker 首次醒来若已有真实构建请求排队, 会丢弃预热 ——
    // 反序则 worker 可能先抢走请求开跑, 预热落在非 Idle 状态撞断言.
    if (!state->dist_prewarmed && feature_inputs_ready && !data.asset.items.empty()) {
      state->dist_service->RequestPrewarm(data);
      state->dist_service->Start(data);
      state->dist_prewarmed = true;
    }

    // Auto-trigger Dist compute on feature selection change
    if (state->dist_service) {
      auto &sel = data.feature.selection;

      // Detect change
      bool feature_changed = (sel.primary_feature_idx != state->prev_primary_feature_idx);
      bool level_changed = (sel.selected_level != state->prev_selected_level);
      bool has_valid_selection = (sel.primary_feature_idx >= 0);

      if ((feature_changed || level_changed) && has_valid_selection) {
        // 参数快照 + 取消在跑 (RequestCompute 内部完成; 非 L1 选择静默忽略)
        state->dist_service->RequestCompute(data);

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

    // Handle trigger from UI (核布局由 start_compute 内部按机器核数推导)
    if (state->compute_state.trigger_start) {
      state->compute_state.trigger_start = false;
      state->compute_service->start_compute(state->compute_state.config);
    }

    // Detect compute completion and mark L1 for reload
    {
      auto current_status = state->compute_service->get_status();
      if (state->prev_compute_status == Features::ComputeStatus::Running &&
          (current_status == Features::ComputeStatus::Completed ||
           current_status == Features::ComputeStatus::Cancelled)) {
        // Compute just finished - OrderFlow 重扫日期 + 整体重拉
        data.orderflow.needs_rescan.store(true, std::memory_order_relaxed);
      }
      state->prev_compute_status = current_status;
    }

    // Compute busy/lock state + 缓存使能状态 (供左栏 IsTabEnabled 下一帧读)
    const bool compute_busy =
        (state->compute_service &&
         state->compute_service->get_status() == Features::ComputeStatus::Running);
    // Dist 是流式构建 (切走即取消, 随时可中断), 不参与锁 Tab
    const bool timeseries_busy = data.timeseries.compute.is_busy();
    const bool any_busy = compute_busy || timeseries_busy;

    if (any_busy) {
      if (!state->tabs_locked) {
        state->tabs_locked = true;
        state->locked_tab = state->active_tab;
        if (state->locked_tab < 0)
          state->locked_tab = TAB_FEATURE;
      }
    } else {
      state->tabs_locked = false;
      state->locked_tab = -1;
    }

    // Pre-compute all tab disable states (whitelist mechanism)
    // Order must match TabIdx enum: Feature, Compute, Transform, Distribution, TimeSeries, OrderFlow
    auto is_locked = [&](int tab) { return state->tabs_locked && state->locked_tab != tab; };
    const bool disable[TAB_COUNT] = {
        is_locked(TAB_FEATURE),                                                 // Feature: always accessible
        !feature_inputs_ready || is_locked(TAB_COMPUTE),                        // Compute: needs scanned inputs
        !feature_inputs_ready || !has_selection || is_locked(TAB_TRANSFORM),    // Transform: needs inputs + selection
        !feature_inputs_ready || !has_selection || is_locked(TAB_DISTRIBUTION), // Distribution: needs inputs + selection
        !feature_inputs_ready || !has_selection || is_locked(TAB_TIMESERIES),   // TimeSeries: needs inputs + selection
        !feature_inputs_ready || is_locked(TAB_ORDERFLOW),                      // OrderFlow: needs scanned inputs
    };
    for (int k = 0; k < TAB_COUNT; k++)
      state->tab_enabled_cache[k] = !disable[k];
    state->tab_cache_valid = true;

    // 生命周期: 基于 active_tab 判定各 tab 是否 open (同一时刻仅一个 open, 等价旧 tab-bar 语义)
    const bool transform_tab_open = (idx == TAB_TRANSFORM);
    const bool dist_tab_open = (idx == TAB_DISTRIBUTION);
    const bool timeseries_tab_open = (idx == TAB_TIMESERIES);

    // Transform lifecycle
    if (transform_tab_open && !state->transform_tab_was_active) {
      state->transform_tab_was_active = true;
      state->transform_prev_feature_idx = -1; // Reset tracking on tab enter
      state->transform_prev_level = -1;
    } else if (!transform_tab_open && state->transform_tab_was_active) {
      Features::StopTabTransform(state->transform_service.get(), data);
      state->transform_tab_was_active = false;
    }

    // Auto-trigger Transform compute on feature/level change
    if (transform_tab_open && state->transform_service &&
        state->transform_service->is_running()) {
      auto &sel = data.feature.selection;
      bool feature_changed = (sel.primary_feature_idx != state->transform_prev_feature_idx);
      bool level_changed = (sel.selected_level != state->transform_prev_level);

      if (feature_changed || level_changed) {
        // Cancel old computation if running
        if (data.transform.compute.is_busy()) {
          data.transform.cancel();
        }
        // Trigger new computation
        state->transform_service->RequestCompute();
        // Update tracking
        state->transform_prev_feature_idx = sel.primary_feature_idx;
        state->transform_prev_level = sel.selected_level;
      }
    }

    // Distribution lifecycle: 切走只中断在跑构建 (内存与 worker 保留, 任务级回收在
    // OnCollapse); 切回时 Idle/Cancelled 自动重算, Done 的结果直接复用
    if (dist_tab_open && !state->dist_tab_was_active) {
      state->dist_tab_was_active = true;
      const auto st = data.dist.status.load();
      if (st == Dist::Status::Idle || st == Dist::Status::Cancelled) {
        state->dist_service->RequestCompute(data);
      }
    } else if (!dist_tab_open && state->dist_tab_was_active) {
      Features::StopTabDist(state->dist_service.get(), data);
      state->dist_tab_was_active = false;
    }

    // TimeSeries lifecycle
    if (timeseries_tab_open && !state->timeseries_tab_was_active) {
      state->timeseries_tab_was_active = true;
      state->timeseries_prev_step = -1;
    } else if (!timeseries_tab_open && state->timeseries_tab_was_active) {
      Features::StopTabTimeSeries(state->timeseries_service.get(), data);
      state->timeseries_tab_was_active = false;
    }

    // Auto-trigger TimeSeries compute
    if (timeseries_tab_open && state->timeseries_service &&
        state->timeseries_service->is_running()) {
      auto &ts = data.timeseries;
      auto &sel = data.feature.selection;
      int current_step = state->timeseries_ui_state.selected_step;

      bool feature_changed = (sel.primary_feature_idx != state->timeseries_prev_feature_idx);
      bool level_changed = (sel.selected_level != state->timeseries_prev_level);
      if (feature_changed || level_changed) {
        ts.clear();
        state->timeseries_prev_feature_idx = sel.primary_feature_idx;
        state->timeseries_prev_level = sel.selected_level;
      }

      bool step_changed = (current_step != state->timeseries_prev_step);
      bool step_needs_compute = false;
      switch (current_step) {
      case 0:
        step_needs_compute = !ts.step0_stationarity.valid;
        break;
      case 1:
        step_needs_compute = !ts.step1_frequency.valid;
        break;
      case 2:
        step_needs_compute = !ts.step2_arma.valid;
        break;
      case 3:
        step_needs_compute = !ts.step3_residual.valid;
        break;
      case 4:
        step_needs_compute = !ts.step4_temporal_decay.valid;
        break;
      default:
        break;
      }

      if (step_changed || feature_changed || level_changed) {
        state->timeseries_prev_step = current_step;
        if (step_needs_compute && has_selection && !ts.compute.is_busy()) {
          state->timeseries_service->RequestCompute();
        }
      }
    }

    // Render active tab content (OrderFlow 切 tab 不停 worker: 流式后台继续, 切回即全)
    ImGui::BeginChild("FeaturesTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_FEATURE:
      Features::RenderTabFeature(data, state->feature_ui_state);
      break;
    case TAB_COMPUTE:
      Features::RenderTabCompute(state->compute_service.get(), state->compute_state,
                                 data.asset, data.config);
      break;
    case TAB_TRANSFORM:
      Features::RenderTabTransform(state->transform_service.get(), data,
                                   state->transform_ui_state);
      break;
    case TAB_DISTRIBUTION:
      Features::RenderTabDist(state->dist_service.get(), data, state->dist_ui_state);
      break;
    case TAB_TIMESERIES:
      Features::RenderTabTimeSeries(state->timeseries_service.get(), data,
                                    state->timeseries_ui_state);
      break;
    case TAB_ORDERFLOW:
      Features::RenderTabOrderFlow(state->orderflow_service.get(), data);
      break;
    default:
      break;
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

    state->orderflow_service.reset(); // 析构 Stop() join worker
    if (state->dist_service)
      state->dist_service->Shutdown(); // Reinit 复用 SharedData, 构建内存一并释放
    state->dist_service.reset();
    state->timeseries_service.reset();
    state->transform_service.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
