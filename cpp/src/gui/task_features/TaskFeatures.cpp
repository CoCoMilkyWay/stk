// Task Features - Feature Engineering Task
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
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
  std::unique_ptr<Features::TransformService> transform_service;

  // UI State
  int active_tab = -1; // 当前选中 tab (由 Draw 入口写入), -1 = 未选中
  int locked_tab = -1;
  bool tabs_locked = false;
  // 每帧 Update 产出 (帧首写, 左栏/Draw 同帧读)
  bool inputs_ready = false;        // 数据库扫描完 + 回测日历就绪
  bool tab_enabled[TAB_COUNT] = {}; // 左栏子行使能
  Features::FeatureUIState feature_ui_state;
  Features::ComputeState compute_state;
  Features::TransformUIState transform_ui_state;
  Features::DistUIState dist_ui_state;

  // Tab state
  bool dist_tab_was_active = false;
  bool transform_tab_was_active = false;

  // Compute status tracking (to detect completion)
  Features::ComputeStatus prev_compute_status = Features::ComputeStatus::Idle;

  // Auto-compute tracking (Dist / Transform 共用): 特征/层变了即重算 (无论当前在哪个 tab)
  int prev_primary_feature_idx = -1; // Track feature selection changes
  int prev_selected_level = 0;       // Track level changes
  bool dist_prewarmed = false;       // 输入就绪后预热一次 (切出任务时回收并复位)
  bool transform_started = false;    // 输入就绪后起 worker 一次 (切出任务时回收并复位)
};

// ============================================================================
// Task Features Implementation
// ============================================================================

TaskHandle CreateFeaturesTask() {
  auto state = std::make_shared<TaskFeaturesState>();

  TaskHandle handle;
  handle.name = "Features";
  handle.storage = state;

  // OnExpand 不需要: 切到 Features 时 TaskTree::Select 默认 selected_tab=0,
  // Services 延迟到首次 Draw 时创建.

  // OnCollapse: 切出 Features 任务才回收 Dist / Transform 的构建内存 (任务内切 tab 不回收)
  handle.OnCollapse = [state]() {
    if (state->dist_service) {
      state->dist_service->Shutdown();
      state->dist_prewarmed = false;      // 重进任务时重新预热
      state->dist_tab_was_active = false; // 数据已清, 重进按"初次进 tab"走自动重算
    }
    if (state->transform_service) {
      state->transform_service->Shutdown();
      state->transform_started = false;        // 重进任务时重新起 worker
      state->transform_tab_was_active = false; // 数据已清, 重进按"初次进 tab"走自动重算
    }
  };

  // 子项 (叶子) 名字, 顺序与 TabIdx 一致
  handle.tabs = {"Feature", "Compute", "Transform", "Distribution", "OrderFlow"};

  // Update: 每帧 (无论选中) 更新 taskstate.features + tab 锁定/使能 ——
  // 左栏标签/使能同帧读取, 不再依赖 "打开过 Features 页" 的上一帧缓存
  handle.Update = [state](SharedData &data) {
    auto &fs = data.taskstate.features;
    state->inputs_ready =
        data.taskstate.database.all_json_ready &&
        data.taskstate.database.binary_scanned &&
        data.asset.binary.exists &&
        !data.asset.backtest.required_dates.empty() &&
        !data.taskstate.database.l2_scan_inflight;
    const bool has_selection = (data.feature.selection.primary_feature_idx() >= 0);
    fs.has_selection = has_selection;

    // Services 懒创建 (首次 Draw), 未创建 = 必然没在算
    const bool compute_busy =
        (state->compute_service &&
         state->compute_service->get_status() == Features::ComputeStatus::Running);

    if (!state->inputs_ready) {
      fs.status = TaskState::Features::Status::Waiting;
      fs.computing = false;
    } else if (compute_busy) {
      fs.status = TaskState::Features::Status::Computing;
      fs.computing = true;
    } else if (!has_selection) {
      fs.status = TaskState::Features::Status::Selecting;
      fs.computing = false;
    } else {
      fs.status = TaskState::Features::Status::Ready;
      fs.computing = false;
    }

    // Tab 锁定: 计算期间锁住其它 tab (Dist/Transform 是流式构建, 切走即取消, 不参与锁)
    if (compute_busy) {
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

    // 子行使能表 (whitelist), 顺序与 TabIdx 一致
    auto is_locked = [&](int tab) { return state->tabs_locked && state->locked_tab != tab; };
    const bool inputs_ready = state->inputs_ready;
    const bool disable[TAB_COUNT] = {
        is_locked(TAB_FEATURE),                                         // Feature: always accessible
        !inputs_ready || is_locked(TAB_COMPUTE),                        // Compute: needs scanned inputs
        !inputs_ready || !has_selection || is_locked(TAB_TRANSFORM),    // Transform: needs inputs + selection
        !inputs_ready || !has_selection || is_locked(TAB_DISTRIBUTION), // Distribution: needs inputs + selection
        !inputs_ready || is_locked(TAB_ORDERFLOW),                      // OrderFlow: needs scanned inputs
    };
    for (int k = 0; k < TAB_COUNT; k++)
      state->tab_enabled[k] = !disable[k];
  };

  // Enabled: 左栏子行是否可点 (Update 同帧已写 tab_enabled)
  handle.Enabled = [state](const SharedData & /*data*/, int idx) -> bool {
    if (idx < 0)
      return true; // 任务行 (父节点) 恒可展开
    assert(idx < TAB_COUNT);
    return state->tab_enabled[idx];
  };

  // Status: 行状态标签. 任务行 = 总体状态, 子行 = 各 tab 实际进展
  handle.Status = [state](const SharedData &data, int idx) -> TaskStatus {
    switch (idx) {
    case -1: // 任务行: taskstate.features.status
      switch (data.taskstate.features.status) {
      case TaskState::Features::Status::Waiting:
        return {TaskStatus::Kind::Muted, "waiting"};
      case TaskState::Features::Status::Selecting:
        return {TaskStatus::Kind::Warn, "selecting"};
      case TaskState::Features::Status::Computing:
        return {TaskStatus::Kind::Busy, "computing"};
      case TaskState::Features::Status::Ready:
        return {TaskStatus::Kind::Ready, "done"};
      case TaskState::Features::Status::Error:
        return {TaskStatus::Kind::Error, "error"};
      case TaskState::Features::Status::None:
        break;
      }
      return {};

    case TAB_FEATURE: // 主 feature 选没选
      if (!state->inputs_ready)
        return {};
      return data.taskstate.features.has_selection
                 ? TaskStatus{TaskStatus::Kind::Ready, "selected"}
                 : TaskStatus{TaskStatus::Kind::Warn, "selecting"};

    case TAB_COMPUTE: // 全量特征计算
      if (!state->compute_service)
        return {};
      switch (state->compute_service->get_status()) {
      case Features::ComputeStatus::Running:
        return {TaskStatus::Kind::Busy,
                state->compute_service->is_cancelling() ? "cancelling" : "running"};
      case Features::ComputeStatus::Completed:
        return {TaskStatus::Kind::Ready, "done"};
      case Features::ComputeStatus::Cancelled:
        return {TaskStatus::Kind::Warn, "cancelled"};
      case Features::ComputeStatus::Error:
        return {TaskStatus::Kind::Error, "error"};
      case Features::ComputeStatus::Idle:
        break;
      }
      return {};

    case TAB_TRANSFORM: { // 流式构建中显示天数进度, 完成后 done, 取消 cancelled
      const auto st = data.transform.status.load(std::memory_order_relaxed);
      if (st == Transform::Status::Building) {
        const size_t total = data.transform.days_total.load(std::memory_order_relaxed);
        const size_t done = data.transform.days_loaded.load(std::memory_order_relaxed);
        const int pct = total > 0 ? (int)(100 * done / total) : 0;
        return {TaskStatus::Kind::Busy, "building " + std::to_string(pct) + "%"};
      }
      if (st == Transform::Status::Done)
        return {TaskStatus::Kind::Ready, "done"};
      if (st == Transform::Status::Cancelled)
        return {TaskStatus::Kind::Warn, "cancelled"};
      return {};
    }

    case TAB_DISTRIBUTION: { // 流式构建中显示天数进度, 完成后 done, 取消 cancelled
      const auto st = data.dist.status.load(std::memory_order_relaxed);
      if (st == Dist::Status::Building) {
        const size_t total = data.dist.days_total.load(std::memory_order_relaxed);
        const size_t done = data.dist.days_loaded.load(std::memory_order_relaxed);
        const int pct = total > 0 ? (int)(100 * done / total) : 0;
        return {TaskStatus::Kind::Busy, "building " + std::to_string(pct) + "%"};
      }
      if (st == Dist::Status::Done)
        return {TaskStatus::Kind::Ready, "done"};
      if (st == Dist::Status::Cancelled)
        return {TaskStatus::Kind::Warn, "cancelled"};
      return {};
    }

    case TAB_ORDERFLOW: // 后台流式 worker 常驻 (背景常态, 灰色)
      if (state->orderflow_service && state->orderflow_service->is_running())
        return {TaskStatus::Kind::Muted, "streaming"};
      return {};
    }
    return {};
  };

  // Draw: 渲染指定 tab 内容 + tab 级生命周期/自动重算触发 (仅选中 Features 时)
  handle.Draw = [state](SharedData &data, int idx) {
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
      state->dist_service = std::make_unique<Features::DistService>();
    }
    if (!state->transform_service) {
      state->transform_service = std::make_unique<Features::TransformService>();
    }

    const bool feature_inputs_ready = state->inputs_ready; // Update (帧首) 已算

    // Dist 预热: 输入就绪即起 worker 并预分配全部构建内存 (worker 线程做, 不卡帧),
    // 点 Distribution 零额外分配; 内存保留到切出 Features 任务 (OnCollapse 回收).
    // 先入队预热再起线程: worker 首次醒来若已有真实构建请求排队, 会丢弃预热 ——
    // 反序则 worker 可能先抢走请求开跑, 预热落在非 Idle 状态撞断言.
    if (!state->dist_prewarmed && feature_inputs_ready && !data.asset.items.empty()) {
      state->dist_service->RequestPrewarm(data);
      state->dist_service->Start(data);
      state->dist_prewarmed = true;
    }
    // Transform worker 同点起 (无预热: 构建内存按请求参数分配), 与 Dist 对仗
    if (!state->transform_started && feature_inputs_ready) {
      state->transform_service->Start(data);
      state->transform_started = true;
    }

    // Auto-trigger Dist / Transform compute on feature selection change (无论当前在哪个 tab:
    // 选了就算, 切进 tab 直接看结果)
    {
      auto &sel = data.feature.selection;

      // Detect change
      bool feature_changed = (sel.primary_feature_idx() != state->prev_primary_feature_idx);
      bool level_changed = (sel.selected_level != state->prev_selected_level);
      bool has_valid_selection = (sel.primary_feature_idx() >= 0);

      if ((feature_changed || level_changed) && has_valid_selection) {
        // 参数快照 + 取消在跑 (RequestCompute 内部完成; 非 L1 选择静默忽略)
        state->dist_service->RequestCompute(data);
        state->transform_service->RequestCompute(data, state->transform_ui_state.params,
                                                 state->transform_ui_state.focus);

        // Update tracking
        state->prev_primary_feature_idx = sel.primary_feature_idx();
        state->prev_selected_level = sel.selected_level;
      }

      // Update tracking even if no change (initialization case)
      if (!feature_changed && state->prev_primary_feature_idx == -1) {
        state->prev_primary_feature_idx = sel.primary_feature_idx();
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

    // Tab 锁定/使能已移到 Update (帧首, 无论选中都跑), 这里只管渲染与生命周期

    // 生命周期: 基于 active_tab 判定各 tab 是否 open (同一时刻仅一个 open, 等价旧 tab-bar 语义)
    const bool transform_tab_open = (idx == TAB_TRANSFORM);
    const bool dist_tab_open = (idx == TAB_DISTRIBUTION);

    // Transform lifecycle (与 Distribution 完全对仗): 切走只中断在跑构建 (内存与 worker 保留,
    // 任务级回收在 OnCollapse); 切回时 Idle/Cancelled 用 UI 当前参数自动重算, Done 的结果直接复用
    if (transform_tab_open && !state->transform_tab_was_active) {
      state->transform_tab_was_active = true;
      const auto st = data.transform.status.load();
      if (st == Transform::Status::Idle || st == Transform::Status::Cancelled) {
        state->transform_service->RequestCompute(data, state->transform_ui_state.params,
                                                 state->transform_ui_state.focus);
      }
    } else if (!transform_tab_open && state->transform_tab_was_active) {
      Features::StopTabTransform(state->transform_service.get(), data);
      state->transform_tab_was_active = false;
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
    if (state->transform_service)
      state->transform_service->Shutdown();
    state->transform_service.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
