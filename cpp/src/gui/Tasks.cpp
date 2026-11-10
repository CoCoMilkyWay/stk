#include "gui/Tasks.hpp"
#include "gui/compute/ComputeHub.hpp"
#include "gui/engine/EngineHub.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_agent/TaskAgent.hpp"
#include "gui/task_alpha/TaskAlpha.hpp"
#include "gui/task_beta/TaskBeta.hpp"
#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/task_live/TaskLive.hpp"
#include "gui/task_market/TaskMarket.hpp"
#include "gui/task_settings/TaskSettings.hpp"
#include "gui/task_tools/TaskTools.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

#include <cassert>
#include <memory>
#include <string>
#include <utility>

namespace GUI {

// 全局唯一色表: 颜色只由 Kind 决定, 任务只产出 (Kind, text)
ImVec4 StatusColor(TaskStatus::Kind kind) {
  switch (kind) {
  case TaskStatus::Kind::Muted:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // Gray
  case TaskStatus::Kind::Busy:
    return ImVec4(0.7f, 0.4f, 1.0f, 1.0f); // Purple
  case TaskStatus::Kind::Warn:
    return ImVec4(1.0f, 0.8f, 0.2f, 1.0f); // Yellow
  case TaskStatus::Kind::Error:
    return ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
  case TaskStatus::Kind::Ready:
    return ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
  case TaskStatus::Kind::None:
    break;
  }
  return ImVec4(0.0f, 1.0f, 1.0f, 1.0f); // Cyan (不应到达)
}

TaskStatus::Kind PhaseKind(Phase p) {
  switch (p) {
  case Phase::Idle:
  case Phase::Cancelled:
    return TaskStatus::Kind::Muted;
  case Phase::Scanning:
  case Phase::Loading:
  case Phase::Running:
    return TaskStatus::Kind::Busy;
  case Phase::Done:
    return TaskStatus::Kind::Ready;
  }
  assert(false && "unknown Phase");
  return TaskStatus::Kind::None;
}

TaskStatus ProgressStatus(const Progress &p) {
  const Phase ph = p.phase.load(std::memory_order_acquire);
  if (ph == Phase::Idle)
    return {};
  std::string text = PhaseName(ph);
  const size_t total = p.total.load(std::memory_order_relaxed);
  if (p.busy() && total > 0)
    text += " " + std::to_string(100 * p.done.load(std::memory_order_relaxed) / total) + "%";
  return {PhaseKind(ph), std::move(text)};
}

void TextPhase(Phase p) { ImGui::TextColored(StatusColor(PhaseKind(p)), "%s", PhaseName(p)); }

namespace {
int rank(TaskStatus::Kind k) {
  switch (k) {
  case TaskStatus::Kind::None:
    return 0;
  case TaskStatus::Kind::Muted:
    return 1;
  case TaskStatus::Kind::Ready:
    return 2;
  case TaskStatus::Kind::Warn:
    return 3;
  case TaskStatus::Kind::Error:
    return 4;
  case TaskStatus::Kind::Busy:
    return 5;
  }
  assert(false && "unknown TaskStatus::Kind");
  return 0;
}
} // namespace

TaskStatus RowStatus(const SharedData &data, const TaskHandle &t, int idx) {
  if (!t.Status)
    return {};
  if (idx >= 0 || t.tabs.empty())
    return t.Status(data, idx);
  TaskStatus top;
  for (int j = 0; j < (int)t.tabs.size(); j++) {
    TaskStatus s = t.Status(data, j);
    if (rank(s.kind) > rank(top.kind))
      top = std::move(s);
  }
  if (top.kind == TaskStatus::Kind::Ready)
    top.text = "done";
  return top;
}

// 切换选中叶子: 跨任务时触发 OnCollapse/OnExpand, 同任务内切 tab 不触发
void TaskTree::Select(int task_idx, int tab_idx) {
  if (task_idx < 0 || task_idx >= (int)tasks.size())
    return;
  TaskHandle &t = tasks[task_idx];
  // 无子项任务: tab_idx 必须是 -1
  if (t.tabs.empty())
    tab_idx = -1;
  else if (tab_idx < 0 || tab_idx >= (int)t.tabs.size())
    tab_idx = 0; // 默认首个子项

  if (selected != task_idx) {
    if (selected >= 0 && selected < (int)tasks.size() && tasks[selected].OnCollapse)
      tasks[selected].OnCollapse();
    selected = task_idx;
    if (t.OnExpand)
      t.OnExpand();
  }
  t.selected_tab = tab_idx;
}

TaskTree CreateAllTasks(SharedData &data) {
  TaskTree tree;
  tree.tasks.reserve(9);

  // 跨任务共用的三个 Hub: 行情 (Database 的 Sync 页读状态, Market 各页 / Live 行情图用), 计算 (Beta / Alpha 两个 Compute 页, Features 读轮次),
  // 因子引擎 (Alpha 写因子 / 编辑组合, Market 画)
  const auto quote = std::make_shared<Quote::QuoteHub>();
  const auto compute = std::make_shared<Compute::ComputeHub>();
  const auto engine = std::make_shared<Engine::EngineHub>();
  tree.tasks.push_back(Tasks::CreateSettingsTask());
  tree.tasks.push_back(Tasks::CreateDatabaseTask(quote));
  tree.tasks.push_back(Tasks::CreateBetaTask(compute));
  tree.tasks.push_back(Tasks::CreateAlphaTask(compute, engine));
  tree.tasks.push_back(Tasks::CreateMarketTask(quote, engine));
  tree.tasks.push_back(Tasks::CreateLiveTask(quote));
  tree.tasks.push_back(Tasks::CreateFeaturesTask(compute));
  tree.tasks.push_back(Tasks::CreateAgentTask());
  tree.tasks.push_back(Tasks::CreateToolsTask());

  // 按 push 顺序立即 Init 一遍 (与"选中才 Draw"解耦, 后台检查提前起):
  // Settings 先把 config.json 落到内存, Database 才能在第一帧前拿到真实
  // backtest range 去跑覆盖检查 —— 顺序即依赖, 不需要额外判断.
  // Update 也按此序: Beta 先推进计算面, Features 同帧读到.
  for (auto &handle : tree.tasks) {
    if (handle.Init)
      handle.Init(data);
  }

  // 默认选中首个任务
  tree.selected = 0;
  if (tree.tasks[tree.selected].OnExpand)
    tree.tasks[tree.selected].OnExpand();

  return tree;
}

void CleanupAllTasks(TaskTree &tree) {
  for (auto &handle : tree.tasks) {
    if (handle.Destroy) {
      handle.Destroy();
    }

    handle.Init = {};
    handle.Update = {};
    handle.OnExpand = {};
    handle.OnCollapse = {};
    handle.Destroy = {};
    handle.Status = {};
    handle.Enabled = {};
    handle.Draw = {};
    handle.tabs.clear();
    handle.selected_tab = -1;
    handle.storage.reset();
  }
  tree.tasks.clear();
  tree.selected = 0;
}

void ReinitAllTasks(TaskTree &tree, SharedData &data) {
  // Asset 不保留: 重建后 orders.scanned == false, StateManager 会走 RescanStorage
  // 重扫一遍 —— 一次扫描是几百次读日文件头 + 目录 (见 Asset::scan_orders),
  // 百毫秒量级, 不值得为省它把七个字段搬出来再搬回去 (帧目录与 items 必须同生
  // 同死, 漏一个就是无主下标).

  // Step 1: Cleanup existing tasks
  CleanupAllTasks(tree);

  // Step 2: Completely rebuild SharedData by placement new (destroys all state)
  data.~SharedData();
  new (&data) SharedData();

  // Step 3: Restore config reinit callback
  data.config.reinit_callback = [&data]() {
    data.request_reinit = true;
  };

  // Step 4: Recreate all tasks (Init 按顺序立即触发, 默认选中并展开首个任务)
  tree = CreateAllTasks(data);
}

} // namespace GUI
