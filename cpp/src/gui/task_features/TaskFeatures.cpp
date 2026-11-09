// Task Features - 特征面板与调试工具: 特征表 / 分布 / 变换 / 相关 / 订单流 / 节点用时.
//   作用域 (池 / 档 / 层) 在 Table 页自选 (Feature::Selection, 与 Alpha 页顶条互不相干); 库由 Beta / Alpha → Compute 算,
//   本任务盯计算面的轮次 (ComputeHub::rounds) 作废预览 / 矩阵 / 订单流清单, 计算在飞时锁住其它页.
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/Tasks.hpp"
#include "gui/compute/ui/TabCompute.hpp"
#include "gui/task_features/services/CorrelationService.hpp"
#include "gui/task_features/services/DistributionService.hpp"
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/services/PreviewService.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/TabCorrelation.hpp"
#include "gui/task_features/ui/TabDistribution.hpp"
#include "gui/task_features/ui/TabNodeTime.hpp"
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/ui/TabTable.hpp"
#include "gui/task_features/ui/TabTransform.hpp"
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
  TAB_DISTRIBUTION,
  TAB_TRANSFORM,
  TAB_CORRELATION,
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

  // Services (各自持有发布面: service->target; 随任务建, 线程按需 Start)
  std::unique_ptr<Features::OrderFlowService> orderflow_service;
  std::unique_ptr<Features::DistributionService> distribution_service;
  std::unique_ptr<Features::TransformService> transform_service;
  std::unique_ptr<Features::PreviewService> preview_service;
  std::unique_ptr<Features::CorrelationService> correlation_service;
  std::unique_ptr<Features::CorrelationPairService> correlation_pair_service;
  std::unique_ptr<Features::CorrelationLagService> correlation_lag_service;

  // UI State
  int active_tab = -1; // 当前选中 tab (由 Draw 入口写入), -1 = 未选中
  int locked_tab = -1;
  bool tabs_locked = false;
  // 每帧 Update 产出 (帧首写, 左栏/Draw 同帧读)
  bool tab_enabled[TAB_COUNT] = {}; // 左栏子行使能
  Features::TableUIState table_ui_state;
  Features::TransformUIState transform_ui_state;
  Features::DistributionUIState distribution_ui_state;
  Features::CorrelationUIState correlation_ui_state;

  // Tab state (Distribution / Transform 流式 tab: 切走中断, 切回自动重算)
  bool distribution_tab_was_active = false;
  bool transform_tab_was_active = false;
  // Correlation: 切走只中断 (矩阵留在内存); 请求由 TabCorrelation 按"过滤行集变了"自发, 不走 StreamTabLifecycle
  bool correlation_tab_was_active = false;

  // 流式分析三件 (Distribution / Transform / Preview) 同点起 worker: 输入就绪一次 (切出任务时回收并复位)
  bool streams_started = false;
  // Auto-compute tracking (Distribution / Transform 共用): 主特征 (库/层/列) 变了即重算 (无论当前在哪个 tab)
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
  state->orderflow_service = std::make_unique<Features::OrderFlowService>();
  state->distribution_service = std::make_unique<Features::DistributionService>();
  state->transform_service = std::make_unique<Features::TransformService>();
  state->preview_service = std::make_unique<Features::PreviewService>();
  state->correlation_service = std::make_unique<Features::CorrelationService>();
  state->correlation_pair_service = std::make_unique<Features::CorrelationPairService>();
  state->correlation_lag_service = std::make_unique<Features::CorrelationLagService>();

  TaskHandle handle;
  handle.name = "Features";
  handle.storage = state;

  // OnExpand 不需要: 切到 Features 时 TaskTree::Select 默认 selected_tab=0,
  // worker 延迟到首次 Draw 时起.

  // OnCollapse: 切出 Features 任务才回收三个流式分析的构建内存 (任务内切 tab 不回收; 预览成品保留, 重进直接换回)
  handle.OnCollapse = [state]() {
    state->distribution_service->Shutdown();
    state->transform_service->Shutdown();
    state->preview_service->Shutdown();
    state->correlation_service->Shutdown();
    state->correlation_pair_service->Shutdown();
    state->correlation_lag_service->Shutdown();
    state->streams_started = false; // 重进任务时重新起 worker
    // 数据已清, 重进按"初次进 tab"走自动重算
    state->distribution_tab_was_active = false;
    state->transform_tab_was_active = false;
    state->correlation_tab_was_active = false;
    state->correlation_ui_state = Features::CorrelationUIState{}; // 行集快照也清, 重进必然重算
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
      state->preview_service->target.forget();
      state->orderflow_service->Stop(); // 下帧 Start 按新库清单建 worker
      state->orderflow_service->target.needs_rescan.store(true, std::memory_order_relaxed);
      if (state->streams_started) {
        state->preview_service->RequestCompute(data);
        Features::InvalidateCorrelationRequests(state->correlation_ui_state);
      }
    }
    const bool compute_busy = hub.service && hub.service->busy();

    // Tab 锁定: 计算期间锁住其它 tab (Distribution/Transform 是流式构建, 切走即取消, 不参与锁)
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
        is_locked(TAB_TABLE),                                           // Table: always accessible
        !inputs_ready || !has_selection || is_locked(TAB_DISTRIBUTION), // Distribution: needs inputs + selection
        !inputs_ready || !has_selection || is_locked(TAB_TRANSFORM),    // Transform: needs inputs + selection
        !inputs_ready || is_locked(TAB_CORRELATION),                    // Correlation: 矩阵按过滤行集算, 不看选中
        !inputs_ready || is_locked(TAB_ORDERFLOW),                      // OrderFlow: needs scanned inputs
        is_locked(TAB_NODETIME),                                        // NodeTime: always accessible
    };
    for (int k = 0; k < TAB_COUNT; k++)
      state->tab_enabled[k] = !disable[k];

    // 预览每跑完一轮 (done 增长) 就把特征表 (元数据 + 账目/值域) 落地
    // <当前库目录>/features.json: 中途取消/崩了也有当前精度的成品, 不必等全部轮次.
    // done 回退 (新请求 begin 清零) 不落, 避免写出空表; Done 那一帧补一次 (空库 0 轮也有文件)
    {
      const Features::Preview &pv = state->preview_service->target;
      const Phase pv_phase = pv.phase.load(std::memory_order_acquire);
      const size_t pv_done = pv.done.load(std::memory_order_acquire);
      if (pv_done > state->prev_preview_done ||
          (pv_phase == Phase::Done && state->prev_preview_phase != Phase::Done))
        Features::SaveFeatureTableJson(data, pv);
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
      return ProgressStatus(state->transform_service->target);

    case TAB_DISTRIBUTION:
      return ProgressStatus(state->distribution_service->target);

    case TAB_CORRELATION:
      return ProgressStatus(state->correlation_service->target);

    case TAB_ORDERFLOW: // 后台流式 worker 常驻 (背景常态, 灰色)
      if (state->orderflow_service->is_running())
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

    // 流式分析三件同点起 worker: 输入就绪 (Update 帧首已算) 且资产表非空 (池 A 轴要它).
    // 构建内存按请求分配, 保留到切出 Features 任务 (OnCollapse 回收).
    // Preview 起手即自动构建 (全 L_1min 特征轮训抽样, 不依赖选中); Distribution / Transform 等选中特征
    const LibId lib = data.feature.selection.lib();
    if (!state->streams_started && state->compute->inputs_ready && !data.asset.items.empty()) {
      state->distribution_service->Start();
      state->transform_service->Start();
      state->preview_service->Start();
      state->correlation_service->Start();
      state->correlation_pair_service->Start();
      state->correlation_lag_service->Start();
      state->preview_service->RequestCompute(data);
      state->streams_started = true;
      state->preview_lib = lib;
    } else if (state->streams_started && state->preview_lib != lib) {
      // 库变了 → 预览重算 (目录/文件列序/日期轴都跟着库走)
      state->preview_lib = lib;
      state->preview_service->RequestCompute(data);
      // 相关矩阵也跟着库走: 清掉请求快照, TabCorrelation 下一帧自发重算
      Features::InvalidateCorrelationRequests(state->correlation_ui_state);
    }

    // Auto-trigger Distribution / Transform compute on feature selection change (无论当前在哪个 tab:
    // 选了就算, 切进 tab 直接看结果)
    {
      auto &sel = data.feature.selection;
      const ColRef *feat = sel.primary();

      // Detect change (选中列绑定池: 切池不改既有选中, 换池即换列)
      const bool feature_changed = feat ? (!state->prev_primary || *state->prev_primary != *feat) : state->prev_primary.has_value();

      if (feature_changed && feat) {
        // 参数快照 + 取消在跑 (RequestCompute 内部完成)
        state->distribution_service->RequestCompute(data);
        state->transform_service->RequestCompute(data, state->transform_ui_state.params,
                                                 state->transform_ui_state.focus);
      }
      state->prev_primary = feat ? std::optional<ColRef>(*feat) : std::nullopt;
    }

    // Tab 锁定/使能在 Update (帧首, 无论选中都跑), 这里只管渲染与生命周期

    // 流式 tab 生命周期 (Transform / Distribution 完全对仗): 基于 active_tab 判定 open
    // (同一时刻仅一个 open); 切回时 Transform 用 UI 当前参数重算
    StreamTabLifecycle(
        idx == TAB_TRANSFORM, state->transform_tab_was_active, state->transform_service->target,
        [&] { state->transform_service->RequestCompute(data, state->transform_ui_state.params, state->transform_ui_state.focus); },
        [&] { Features::StopTabTransform(state->transform_service.get()); });
    StreamTabLifecycle(
        idx == TAB_DISTRIBUTION, state->distribution_tab_was_active, state->distribution_service->target,
        [&] { state->distribution_service->RequestCompute(data); },
        [&] { Features::StopTabDistribution(state->distribution_service.get()); });
    // Correlation 的请求内容 (过滤行集) 只有 TabCorrelation 算得出 → 切回时清快照, 由它下一帧自发提交
    StreamTabLifecycle(
        idx == TAB_CORRELATION, state->correlation_tab_was_active, state->correlation_service->target,
        [&] { Features::InvalidateCorrelationRequests(state->correlation_ui_state); },
        [&] {
          Features::StopTabCorrelation(state->correlation_service.get(), state->correlation_pair_service.get(),
                                       state->correlation_lag_service.get());
        });

    // Render active tab content (OrderFlow 切 tab 不停 worker: 流式后台继续, 切回即全)
    ImGui::BeginChild("FeaturesTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_TABLE:
      Features::RenderTabTable(data, state->preview_service->target, state->table_ui_state);
      break;
    case TAB_NODETIME:
      Features::RenderTabNodeTime(data);
      break;
    case TAB_TRANSFORM:
      Features::RenderTabTransform(state->transform_service.get(), data,
                                   state->transform_ui_state);
      break;
    case TAB_DISTRIBUTION:
      Features::RenderTabDistribution(state->distribution_service.get(), data, state->distribution_ui_state);
      break;
    case TAB_CORRELATION:
      Features::RenderTabCorrelation(state->correlation_service.get(), state->correlation_pair_service.get(),
                                     state->correlation_lag_service.get(), data, state->preview_service->target,
                                     state->correlation_ui_state);
      break;
    case TAB_ORDERFLOW:
      Features::RenderTabOrderFlow(state->orderflow_service.get(), data, state->preview_service->target);
      break;
    default:
      break;
    }
    ImGui::EndChild();
  };

  // Destroy: 析构即 Stop (join worker) + 释放发布面与构建内存
  handle.Destroy = [state]() {
    state->orderflow_service.reset();
    state->distribution_service.reset();
    state->transform_service.reset();
    state->preview_service.reset();
    state->correlation_service.reset();
    state->correlation_pair_service.reset();
    state->correlation_lag_service.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
