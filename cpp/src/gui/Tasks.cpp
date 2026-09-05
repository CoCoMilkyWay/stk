#include "gui/Tasks.hpp"
#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_settings/TaskSettings.hpp"
#include "gui/task_system_info/TaskSystemInfo.hpp"
#include "gui/task_tools/TaskTools.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

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
  tree.tasks.reserve(5);

  tree.tasks.push_back(Tasks::CreateSettingsTask());
  tree.tasks.push_back(Tasks::CreateSystemInfoTask());
  tree.tasks.push_back(Tasks::CreateDatabaseTask());
  tree.tasks.push_back(Tasks::CreateFeaturesTask());
  tree.tasks.push_back(Tasks::CreateToolsTask());

  // 按 push 顺序立即 Init 一遍 (与"选中才 Draw"解耦, 后台检查提前起):
  // Settings 先把 config.json 落到内存, Database 才能在第一帧前拿到真实
  // backtest range 去跑覆盖检查 —— 顺序即依赖, 不需要额外判断.
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
  // Config range 变化只需要重算 coverage, 不该丢掉底层 L2 扫描缓存.
  // 如果 orders/archive 路径变了, StateManager::initialize 会发现 path 不匹配并重扫.
  auto preserved_items = std::move(data.asset.items);
  auto preserved_all_dates = std::move(data.asset.all_dates);
  auto preserved_day_records = std::move(data.asset.day_records);
  auto preserved_binary = std::move(data.asset.binary);
  auto preserved_archive = std::move(data.asset.archive);

  // Step 1: Cleanup existing tasks
  CleanupAllTasks(tree);

  // Step 2: Cleanup icon bar
  TaskIconBar::CleanupIconBar();

  // Step 3: Completely rebuild SharedData by placement new (destroys all state)
  data.~SharedData();
  new (&data) SharedData();

  // Step 4: Restore config reinit callback
  data.config.reinit_callback = [&data]() {
    data.request_reinit = true;
  };
  data.asset.items = std::move(preserved_items);
  data.asset.all_dates = std::move(preserved_all_dates);
  data.asset.day_records = std::move(preserved_day_records);
  data.asset.binary = std::move(preserved_binary);
  data.asset.archive = std::move(preserved_archive);

  // Step 5: Reinitialize icon bar
  TaskIconBar::InitIconBar(data.coromgr);

  // Step 6: Recreate all tasks (Init 按顺序立即触发, 默认选中并展开首个任务)
  tree = CreateAllTasks(data);
}

} // namespace GUI
