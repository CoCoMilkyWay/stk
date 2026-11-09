// Task Features - 特征面板与调试工具: 特征表 / 分布 / 变换 / 相关 / 订单流 / 节点用时.
//   作用域 (池 / 档 / 层) 在 Table 页自选 (Feature::Selection, 与 Alpha 页顶条互不相干); 库由 Beta / Alpha → Compute 算,
//   本任务盯计算面的轮次 (ComputeHub::rounds) 作废预览 / 矩阵 / 订单流清单, 计算在飞时锁住其它页.
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/compute/ui/TabCompute.hpp"
#include "gui/task_features/services/CorrService.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/services/PreviewService.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/TabCorr.hpp"
#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/ui/TabTransform.hpp"
#include "shared/Analysis.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace GUI::Tasks {

// ============================================================================
// Tab Index Enum
// ============================================================================

enum TabIdx {
  TAB_TABLE = 0,
  TAB_DIST,
  TAB_TRANSFORM,
  TAB_CORR,
  TAB_ORDERFLOW,
  TAB_NODETIME,
  TAB_COUNT
};

// ============================================================================
// Task Features State
// ============================================================================

struct TaskFeaturesState {
  std::shared_ptr<Compute::ComputeHub> compute; // 与 Beta / Alpha 共用
  uint64_t rounds = 0;                          // 已处理的计算轮次 (ComputeHub::rounds)

  // Services
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
  bool tab_enabled[TAB_COUNT] = {}; // 左栏子行使能
  Features::FeatureUIState feature_ui_state;
  Features::TransformUIState transform_ui_state;
  Features::DistUIState dist_ui_state;
  Features::CorrUIState corr_ui_state;

  // Tab state (Dist / Transform 流式 tab: 切走中断, 切回自动重算)
  bool dist_tab_was_active = false;
  bool transform_tab_was_active = false;
  // Corr: 切走只中断 (矩阵留在内存); 请求由 TabCorr 按"过滤行集变了"自发, 不走 StreamTabLifecycle
  bool corr_tab_was_active = false;

  // 流式分析三件 (Dist / Transform / Preview) 同点起 worker: 输入就绪一次 (切出任务时回收并复位)
  bool streams_started = false;
  // Auto-compute tracking (Dist / Transform 共用): 主特征 (库/层/列) 变了即重算 (无论当前在哪个 tab)
  std::optional<ColRef> prev_primary;
  // Preview (特征表内联 PDF/PSD 迷你图): 不依赖选中特征; 库变了自动请求 (快照做变更检测; 跑完过的库换回成品), 库重算收尾另起
  LibId preview_lib = kAllBar;
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

TaskHandle CreateFeaturesTask(std::shared_ptr<Compute::ComputeHub> compute) {
  auto state = std::make_shared<TaskFeaturesState>();
  state->compute = std::move(compute);

  TaskHandle handle;
  handle.name = "Features";
  handle.storage = state;

  // OnExpand 不需要: 切到 Features 时 TaskTree::Select 默认 selected_tab=0,
  // Services 延迟到首次 Draw 时创建.

  // OnCollapse: 切出 Features 任务才回收三个流式分析的构建内存 (任务内切 tab 不回收; 预览成品保留, 重进直接换回)
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
  handle.tabs = {"Table", "Distribution", "Transform", "Correlation", "OrderFlow", "NodeTime"};

  // Update: 每帧 (无论选中) 更新 taskstate.features + tab 锁定/使能 ——
  // 左栏标签/使能同帧读取, 不依赖 "打开过 Features 页" 的上一帧缓存 (计算面已由 Beta 的 Update 先推进)
  handle.Update = [state](SharedData &data) {
    const Compute::ComputeHub &hub = *state->compute;
    const bool has_selection = data.feature.selection.primary() != nullptr;
    data.taskstate.features.has_selection = has_selection;

    // 一轮收尾 (池表重扫在 TickCompute): 新库落盘 → OrderFlow 按新清单重建 + 重扫, 预览成品作废并重抽, 相关矩阵作废 (没建 / 没起的首次 Draw 自然按新库)
    if (hub.rounds != state->rounds) {
      state->rounds = hub.rounds;
      data.preview.forget();
      if (state->orderflow_service) {
        state->orderflow_service = std::make_unique<Features::OrderFlowService>();
        data.orderflow.needs_rescan.store(true, std::memory_order_relaxed);
      }
      if (state->streams_started) {
        state->preview_service->RequestCompute(data);
        Features::InvalidateCorrRequests(state->corr_ui_state);
      }
    }
    const bool compute_busy = hub.service && hub.service->busy();

    // Tab 锁定: 计算期间锁住其它 tab (Dist/Transform 是流式构建, 切走即取消, 不参与锁)
    if (compute_busy) {
      if (!state->tabs_locked) {
        state->tabs_locked = true;
        state->locked_tab = state->active_tab;
        if (state->locked_tab < 0)
          state->locked_tab = TAB_TABLE;
      }
    } else {
      state->tabs_locked = false;
      state->locked_tab = -1;
    }

    // 子行使能表 (whitelist), 顺序与 TabIdx 一致
    auto is_locked = [&](int tab) { return state->tabs_locked && state->locked_tab != tab; };
    const bool inputs_ready = hub.inputs_ready;
    const bool disable[TAB_COUNT] = {
        is_locked(TAB_TABLE),                                        // Table: always accessible
        !inputs_ready || !has_selection || is_locked(TAB_DIST),      // Dist: needs inputs + selection
        !inputs_ready || !has_selection || is_locked(TAB_TRANSFORM), // Transform: needs inputs + selection
        !inputs_ready || is_locked(TAB_CORR),                        // Corr: 矩阵按过滤行集算, 不看选中
        !inputs_ready || is_locked(TAB_ORDERFLOW),                   // OrderFlow: needs scanned inputs
        is_locked(TAB_NODETIME),                                     // NodeTime: always accessible
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

  // Status: 子行状态标签 = 各 tab 实际进展 (任务行由子行聚合)
  handle.Status = [state](const SharedData &data, int idx) -> TaskStatus {
    switch (idx) {
    case TAB_TABLE: // 主 feature 选没选
      if (!state->compute->inputs_ready)
        return {};
      return data.taskstate.features.has_selection
                 ? TaskStatus{TaskStatus::Kind::Ready, "selected"}
                 : TaskStatus{TaskStatus::Kind::Warn, "selecting"};

    case TAB_NODETIME:
      return {};

    case TAB_TRANSFORM:
      return ProgressStatus(data.transform);

    case TAB_DIST:
      return ProgressStatus(data.dist);

    case TAB_CORR:
      return ProgressStatus(data.corr);

    case TAB_ORDERFLOW: // 后台流式 worker 常驻 (背景常态, 灰色)
      if (state->orderflow_service && state->orderflow_service->is_running())
        return {TaskStatus::Kind::Muted, "live"};
      return {};
    }
    assert(false && "Features 子行越界");
    return {};
  };

  // Draw: 渲染指定 tab 内容 + tab 级生命周期/自动重算触发 (仅选中 Features 时)
  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    state->active_tab = idx;

    // Lazy initialization
    if (!state->orderflow_service) {
      state->orderflow_service = std::make_unique<Features::OrderFlowService>();
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

    // 流式分析三件同点起 worker: 输入就绪 (Update 帧首已算) 且资产表非空 (池 A 轴要它).
    // 构建内存按请求分配, 保留到切出 Features 任务 (OnCollapse 回收).
    // Preview 起手即自动构建 (全 L_1min 特征轮训抽样, 不依赖选中); Dist / Transform 等选中特征
    const LibId lib = data.feature.selection.lib();
    if (!state->streams_started && state->compute->inputs_ready && !data.asset.items.empty()) {
      state->dist_service->Start(data);
      state->transform_service->Start(data);
      state->preview_service->Start(data);
      state->corr_service->Start(data);
      state->corr_pair_service->Start(data);
      state->corr_lag_service->Start(data);
      state->preview_service->RequestCompute(data);
      state->streams_started = true;
      state->preview_lib = lib;
    } else if (state->streams_started && state->preview_lib != lib) {
      // 库变了 → 预览重算 (目录/文件列序/日期轴都跟着库走)
      state->preview_lib = lib;
      state->preview_service->RequestCompute(data);
      // 相关矩阵也跟着库走: 清掉请求快照, TabCorr 下一帧自发重算
      Features::InvalidateCorrRequests(state->corr_ui_state);
    }

    // Auto-trigger Dist / Transform compute on feature selection change (无论当前在哪个 tab:
    // 选了就算, 切进 tab 直接看结果)
    {
      auto &sel = data.feature.selection;
      const ColRef *feat = sel.primary();

      // Detect change (选中列绑定池: 切池不改既有选中, 换池即换列)
      const bool feature_changed = feat ? (!state->prev_primary || *state->prev_primary != *feat) : state->prev_primary.has_value();

      if (feature_changed && feat) {
        // 参数快照 + 取消在跑 (RequestCompute 内部完成)
        state->dist_service->RequestCompute(data);
        state->transform_service->RequestCompute(data, state->transform_ui_state.params,
                                                 state->transform_ui_state.focus);
      }
      state->prev_primary = feat ? std::optional<ColRef>(*feat) : std::nullopt;
    }

    // Tab 锁定/使能在 Update (帧首, 无论选中都跑), 这里只管渲染与生命周期

    // 流式 tab 生命周期 (Transform / Distribution 完全对仗): 基于 active_tab 判定 open
    // (同一时刻仅一个 open); 切回时 Transform 用 UI 当前参数重算
    StreamTabLifecycle(
        idx == TAB_TRANSFORM, state->transform_tab_was_active, data.transform,
        [&] { state->transform_service->RequestCompute(data, state->transform_ui_state.params, state->transform_ui_state.focus); },
        [&] { Features::StopTabTransform(state->transform_service.get(), data); });
    StreamTabLifecycle(
        idx == TAB_DIST, state->dist_tab_was_active, data.dist,
        [&] { state->dist_service->RequestCompute(data); },
        [&] { Features::StopTabDist(state->dist_service.get(), data); });
    // Corr 的请求内容 (过滤行集) 只有 TabCorr 算得出 → 切回时清快照, 由它下一帧自发提交
    StreamTabLifecycle(
        idx == TAB_CORR, state->corr_tab_was_active, data.corr,
        [&] { Features::InvalidateCorrRequests(state->corr_ui_state); },
        [&] {
          Features::StopTabCorr(state->corr_service.get(), state->corr_pair_service.get(),
                                state->corr_lag_service.get(), data);
        });

    // Render active tab content (OrderFlow 切 tab 不停 worker: 流式后台继续, 切回即全)
    ImGui::BeginChild("FeaturesTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_TABLE:
      Features::RenderTabTable(data, state->feature_ui_state);
      break;
    case TAB_NODETIME:
      Features::RenderTabNodeTime(data);
      break;
    case TAB_TRANSFORM:
      Features::RenderTabTransform(state->transform_service.get(), data,
                                   state->transform_ui_state);
      break;
    case TAB_DIST:
      Features::RenderTabDist(state->dist_service.get(), data, state->dist_ui_state);
      break;
    case TAB_CORR:
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
