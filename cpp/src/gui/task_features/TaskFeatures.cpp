// Task Features - Feature Engineering Task
#include "gui/task_features/TaskFeatures.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "gui/task_features/services/CorrService.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/services/PreviewService.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/TabCompute.hpp"
#include "gui/task_features/ui/TabCorr.hpp"
#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/ui/TabTransform.hpp"
#include "shared/Analysis.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <algorithm>
#include <string>
#include <string_view>

namespace GUI::Tasks {

// ============================================================================
// Tab Index Enum
// ============================================================================

enum TabIdx {
  TAB_COMPUTE = 0,
  TAB_FEATURE,
  TAB_CORRELATION,
  TAB_DISTRIBUTION,
  TAB_TRANSFORM,
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
  std::unique_ptr<Features::PreviewService> preview_service;
  std::unique_ptr<Features::CorrService> corr_service;
  std::unique_ptr<Features::CorrPairService> corr_pair_service;
  std::unique_ptr<Features::CorrLagService> corr_lag_service;

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
  Features::CorrUIState corr_ui_state;

  // Tab state (Dist / Transform 流式 tab: 切走中断, 切回自动重算)
  bool dist_tab_was_active = false;
  bool transform_tab_was_active = false;
  // Corr: 切走只中断 (矩阵留在内存); 请求由 TabCorr 按"过滤行集变了"自发, 不走 StreamTabLifecycle
  bool corr_tab_was_active = false;

  // Compute 进度面上一帧阶段 (忙 → 非忙 = 一轮收尾)
  Phase prev_compute_phase = Phase::Idle;

  // 流式分析三件 (Dist / Transform / Preview) 同点起 worker: 输入就绪一次 (切出任务时回收并复位)
  bool streams_started = false;
  // Auto-compute tracking (Dist / Transform 共用): 特征/层/库变了即重算 (无论当前在哪个 tab)
  int prev_primary_feature_idx = -1;
  int prev_selected_level = 0;
  std::string prev_lib;
  // Preview (特征表内联 PDF/PSD 迷你图): 不依赖选中特征; 库 / 日期区间变了自动重算 (快照做变更检测)
  std::string preview_lib;
  misc::Ymd preview_start = 0, preview_end = 0;
  // 预览进度检测 (每跑完一轮 + → Done 那一帧把特征表落地 features.json)
  Phase prev_preview_phase = Phase::Idle;
  size_t prev_preview_done = 0;
};

// 流式 tab 生命周期: 切进 (Idle/Cancelled 才重算, Done 直接复用) / 切走 (只中断在跑构建,
// 内存与 worker 保留, 任务级回收在 OnCollapse). request 返回 void, stop 调 Service::RequestCancel
template <class Request, class Stop>
static void StreamTabLifecycle(bool open, bool &was_active, const Progress &st,
                               Request &&request, Stop &&stop) {
  if (open && !was_active) {
    was_active = true;
    const Phase p = st.phase.load(std::memory_order_acquire);
    if (p == Phase::Idle || p == Phase::Cancelled)
      request();
  } else if (!open && was_active) {
    stop();
    was_active = false;
  }
}

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

  // OnCollapse: 切出 Features 任务才回收三个流式分析的构建内存 (任务内切 tab 不回收)
  handle.OnCollapse = [state]() {
    if (state->dist_service)
      state->dist_service->Shutdown();
    if (state->transform_service)
      state->transform_service->Shutdown();
    if (state->preview_service)
      state->preview_service->Shutdown();
    if (state->corr_service)
      state->corr_service->Shutdown();
    if (state->corr_pair_service)
      state->corr_pair_service->Shutdown();
    if (state->corr_lag_service)
      state->corr_lag_service->Shutdown();
    state->streams_started = false; // 重进任务时重新起 worker
    // 数据已清, 重进按"初次进 tab"走自动重算
    state->dist_tab_was_active = false;
    state->transform_tab_was_active = false;
    state->corr_tab_was_active = false;
    state->corr_ui_state = Features::CorrUIState{}; // 行集快照也清, 重进必然重算
  };

  // 子项 (叶子) 名字, 顺序与 TabIdx 一致
  handle.tabs = {"Compute", "Feature", "Correlation", "Distribution", "Transform", "OrderFlow"};

  // Update: 每帧 (无论选中) 更新 taskstate.features + tab 锁定/使能 ——
  // 左栏标签/使能同帧读取, 不再依赖 "打开过 Features 页" 的上一帧缓存
  handle.Update = [state](SharedData &data) {
    auto &fs = data.taskstate.features;
    state->inputs_ready =
        data.taskstate.database.all_json_ready &&
        data.taskstate.database.binary_scanned &&
        data.asset.binary.exists &&
        !data.asset.backtest.days.empty() &&
        !data.taskstate.database.l2_scan_inflight;
    const bool has_selection = (data.feature.selection.primary_feature_idx() >= 0);
    fs.has_selection = has_selection;

    // Services 懒创建 (首次 Draw), 未创建 = 必然没在算
    const bool compute_busy = state->compute_service && state->compute_service->busy();

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
        !inputs_ready || is_locked(TAB_COMPUTE),                        // Compute: needs scanned inputs
        is_locked(TAB_FEATURE),                                         // Feature: always accessible
        !inputs_ready || is_locked(TAB_CORRELATION),                    // Correlation: 矩阵按过滤行集算, 不看选中
        !inputs_ready || !has_selection || is_locked(TAB_DISTRIBUTION), // Distribution: needs inputs + selection
        !inputs_ready || !has_selection || is_locked(TAB_TRANSFORM),    // Transform: needs inputs + selection
        !inputs_ready || is_locked(TAB_ORDERFLOW),                      // OrderFlow: needs scanned inputs
    };
    for (int k = 0; k < TAB_COUNT; k++)
      state->tab_enabled[k] = !disable[k];

    // 预览每跑完一轮 (done 增长) 就把特征表 (元数据 + 账目/值域) 落地
    // <当前库目录>/features.json: 中途取消/崩了也有当前精度的成品, 不必等全部轮次.
    // done 回退 (新请求 begin 清零) 不落, 避免写出空表; Done 那一帧补一次 (空库 0 轮也有文件)
    {
      const Phase pv_phase = data.preview.phase.load(std::memory_order_acquire);
      const size_t pv_done = data.preview.done.load(std::memory_order_acquire);
      if (pv_done > state->prev_preview_done ||
          (pv_phase == Phase::Done && state->prev_preview_phase != Phase::Done))
        Features::SaveFeatureTableJson(data);
      state->prev_preview_done = pv_done;
      state->prev_preview_phase = pv_phase;
    }
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

    case TAB_COMPUTE: { // 全量特征计算
      const bool running = state->compute_service && state->compute_service->busy();
      // 任一 reader (含 Factors 页) 判废删了某个库 → 提示重算 (该库 compute 开跑即撤销登记; 哪个库见 Compute 页库表)
      const auto lib_stale = [&](std::string_view lib) { return FeatureRead::dir_stale(data.config.FeatureLibDir(lib)); };
      if (!running && (lib_stale(Config::kAllTierDir) ||
                       std::any_of(feature_pools().begin(), feature_pools().end(), [&](const PoolDef &p) { return lib_stale(p.name); })))
        return {TaskStatus::Kind::Warn, "stale"};
      if (!state->compute_service)
        return {};
      if (state->compute_service->cancelling())
        return {TaskStatus::Kind::Busy, "cancelling"};
      return ProgressStatus(*state->compute_service);
    }

    case TAB_TRANSFORM:
      return ProgressStatus(data.transform);

    case TAB_DISTRIBUTION:
      return ProgressStatus(data.dist);

    case TAB_CORRELATION:
      return ProgressStatus(data.corr);

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
      state->orderflow_service = std::make_unique<Features::OrderFlowService>(data.feature.selection.pool);
    }
    if (!state->dist_service) {
      state->dist_service = std::make_unique<Features::DistService>();
    }
    if (!state->transform_service) {
      state->transform_service = std::make_unique<Features::TransformService>();
    }
    if (!state->preview_service) {
      state->preview_service = std::make_unique<Features::PreviewService>();
    }
    if (!state->corr_service) {
      state->corr_service = std::make_unique<Features::CorrService>();
    }
    if (!state->corr_pair_service) {
      state->corr_pair_service = std::make_unique<Features::CorrPairService>();
    }
    if (!state->corr_lag_service) {
      state->corr_lag_service = std::make_unique<Features::CorrLagService>();
    }

    // OrderFlow 一代服务绑一个选池库 (worker 常驻 reader 持该库清单): 换池 / 库重算后整体重建 + 重扫
    const auto renew_orderflow = [&] {
      state->orderflow_service = std::make_unique<Features::OrderFlowService>(data.feature.selection.pool);
      data.orderflow.needs_rescan.store(true, std::memory_order_relaxed);
    };
    if (state->orderflow_service->pool() != data.feature.selection.pool)
      renew_orderflow();

    // 流式分析三件同点起 worker: 输入就绪 (Update 帧首已算) 且资产表非空 (库 A 轴要它).
    // 构建内存按请求分配, 保留到切出 Features 任务 (OnCollapse 回收).
    // Preview 起手即自动构建 (全 L1 特征轮训抽样, 不依赖选中); Dist / Transform 等选中特征
    const std::string lib = data.feature.selection.lib();
    if (!state->streams_started && state->inputs_ready && !data.asset.items.empty()) {
      state->dist_service->Start(data);
      state->transform_service->Start(data);
      state->preview_service->Start(data);
      state->corr_service->Start(data);
      state->corr_pair_service->Start(data);
      state->corr_lag_service->Start(data);
      state->preview_service->RequestCompute(data);
      state->streams_started = true;
      state->preview_lib = lib;
      state->preview_start = data.config.start_date;
      state->preview_end = data.config.end_date;
    } else if (state->streams_started &&
               (state->preview_lib != lib ||
                state->preview_start != data.config.start_date ||
                state->preview_end != data.config.end_date)) {
      // 库 / 日期区间变了 → 预览重算 (特征库目录/文件列序都跟着库走)
      state->preview_lib = lib;
      state->preview_start = data.config.start_date;
      state->preview_end = data.config.end_date;
      state->preview_service->RequestCompute(data);
      // 相关矩阵也跟着库 / 区间走: 清掉请求快照, TabCorr 下一帧自发重算
      Features::InvalidateCorrRequests(state->corr_ui_state);
    }

    // Auto-trigger Dist / Transform compute on feature selection change (无论当前在哪个 tab:
    // 选了就算, 切进 tab 直接看结果)
    {
      auto &sel = data.feature.selection;

      // Detect change (同档换选池: 字段表同, 选中下标不变, 但库变了)
      bool feature_changed = (sel.primary_feature_idx() != state->prev_primary_feature_idx);
      bool level_changed = (sel.selected_level != state->prev_selected_level);
      bool lib_changed = (lib != state->prev_lib);
      bool has_valid_selection = (sel.primary_feature_idx() >= 0);

      if ((feature_changed || level_changed || lib_changed) && has_valid_selection) {
        // 参数快照 + 取消在跑 (RequestCompute 内部完成; 非 L1 选择静默忽略)
        state->dist_service->RequestCompute(data);
        state->transform_service->RequestCompute(data, state->transform_ui_state.params,
                                                 state->transform_ui_state.focus);

        // Update tracking
        state->prev_primary_feature_idx = sel.primary_feature_idx();
        state->prev_selected_level = sel.selected_level;
        state->prev_lib = lib;
      }

      // Update tracking even if no change (initialization case)
      if (!feature_changed && state->prev_primary_feature_idx == -1) {
        state->prev_primary_feature_idx = sel.primary_feature_idx();
        state->prev_selected_level = sel.selected_level;
        state->prev_lib = lib;
      }
    }

    // Handle trigger from UI (核布局由 ComputeService::run 按机器核数推导)
    if (state->compute_state.trigger_start) {
      state->compute_state.trigger_start = false;
      state->compute_service->Request(state->compute_state.config);
    }

    // Detect compute completion and mark L1 for reload
    {
      const Phase current_phase = state->compute_service->phase.load(std::memory_order_acquire);
      if ((state->prev_compute_phase == Phase::Running) &&
          (current_phase == Phase::Done || current_phase == Phase::Cancelled)) {
        // Compute just finished - OrderFlow 按新清单重建 + 重扫; 库表重扫; 预览重抽 (新库落盘)
        renew_orderflow();
        state->compute_state.libs_dirty = true;
        if (state->streams_started) {
          state->preview_service->RequestCompute(data);
          Features::InvalidateCorrRequests(state->corr_ui_state); // 新库落盘: 相关矩阵同样作废
        }
      }
      state->prev_compute_phase = current_phase;
    }

    // Tab 锁定/使能已移到 Update (帧首, 无论选中都跑), 这里只管渲染与生命周期

    // 流式 tab 生命周期 (Transform / Distribution 完全对仗): 基于 active_tab 判定 open
    // (同一时刻仅一个 open); 切回时 Transform 用 UI 当前参数重算
    StreamTabLifecycle(
        idx == TAB_TRANSFORM, state->transform_tab_was_active, data.transform,
        [&] { state->transform_service->RequestCompute(data, state->transform_ui_state.params, state->transform_ui_state.focus); },
        [&] { Features::StopTabTransform(state->transform_service.get(), data); });
    StreamTabLifecycle(
        idx == TAB_DISTRIBUTION, state->dist_tab_was_active, data.dist,
        [&] { state->dist_service->RequestCompute(data); },
        [&] { Features::StopTabDist(state->dist_service.get(), data); });
    // Corr 的请求内容 (过滤行集) 只有 TabCorr 算得出 → 切回时清快照, 由它下一帧自发提交
    StreamTabLifecycle(
        idx == TAB_CORRELATION, state->corr_tab_was_active, data.corr,
        [&] { Features::InvalidateCorrRequests(state->corr_ui_state); },
        [&] {
          Features::StopTabCorr(state->corr_service.get(), state->corr_pair_service.get(),
                                state->corr_lag_service.get(), data);
        });

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
    case TAB_CORRELATION:
      Features::RenderTabCorr(state->corr_service.get(), state->corr_pair_service.get(),
                              state->corr_lag_service.get(), data, state->corr_ui_state);
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
    state->compute_service.reset();   // 析构 Stop(): 取消 + join worker
    state->orderflow_service.reset(); // 析构 Stop() join worker
    if (state->dist_service)
      state->dist_service->Shutdown(); // Reinit 复用 SharedData, 构建内存一并释放
    state->dist_service.reset();
    if (state->transform_service)
      state->transform_service->Shutdown();
    state->transform_service.reset();
    if (state->preview_service)
      state->preview_service->Shutdown();
    state->preview_service.reset();
    if (state->corr_service)
      state->corr_service->Shutdown();
    state->corr_service.reset();
    if (state->corr_pair_service)
      state->corr_pair_service->Shutdown();
    state->corr_pair_service.reset();
    if (state->corr_lag_service)
      state->corr_lag_service->Shutdown();
    state->corr_lag_service.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
