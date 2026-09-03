#include "gui/Tasks.hpp"
#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_settings/TaskSettings.hpp"
#include "gui/task_system_info/TaskSystemInfo.hpp"
#include "gui/task_tools/TaskTools.hpp"
#include "shared/SharedData.hpp"

namespace GUI {

std::vector<TaskHandle> CreateAllTasks(SharedData &data) {
  std::vector<TaskHandle> tasks;
  tasks.reserve(5);

  tasks.push_back(Tasks::CreateSettingsTask());
  tasks.push_back(Tasks::CreateSystemInfoTask());
  tasks.push_back(Tasks::CreateDatabaseTask());
  tasks.push_back(Tasks::CreateFeaturesTask());
  tasks.push_back(Tasks::CreateToolsTask());

  // 按 push 顺序立即 Init 一遍 (与"选中才 DrawPanel"解耦, 后台检查提前起):
  // Settings 先把 config.json 落到内存, Database 才能在第一帧前拿到真实
  // backtest range 去跑覆盖检查 —— 顺序即依赖, 不需要额外判断.
  for (auto &handle : tasks) {
    if (handle.Init)
      handle.Init(data);
  }

  return tasks;
}

void CleanupAllTasks(std::vector<TaskHandle> &tasks) {
  for (auto &handle : tasks) {
    if (handle.Destroy) {
      handle.Destroy();
    }

    handle.Init = {};
    handle.OnExpand = {};
    handle.OnCollapse = {};
    handle.DrawPanel = {};
    handle.Destroy = {};
    handle.storage.reset();
    handle.task_instance = nullptr;
  }
  tasks.clear();
}

void ReinitAllTasks(std::vector<TaskHandle> &tasks, int &selected_task, SharedData &data) {
  // Config range 变化只需要重算 coverage, 不该丢掉底层 L2 扫描缓存.
  // 如果 orders/archive 路径变了, StateManager::initialize 会发现 path 不匹配并重扫.
  auto preserved_items = std::move(data.asset.items);
  auto preserved_all_dates = std::move(data.asset.all_dates);
  auto preserved_day_records = std::move(data.asset.day_records);
  auto preserved_binary = std::move(data.asset.binary);
  auto preserved_archive = std::move(data.asset.archive);

  // Step 1: Cleanup existing tasks
  CleanupAllTasks(tasks);

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

  // Step 6: Recreate all tasks (Init 在其中按顺序立即触发)
  tasks = CreateAllTasks(data);

  // Step 7: Expand first task
  selected_task = 0;
  if (!tasks.empty()) {
    tasks[selected_task].OnExpand();
  }
}

} // namespace GUI
