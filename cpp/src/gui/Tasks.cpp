#include "gui/Tasks.hpp"
#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_features/TaskFeatures.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_settings/TaskSettings.hpp"
#include "gui/task_system_info/TaskSystemInfo.hpp"
#include "shared/SharedData.hpp"

namespace GUI {

std::vector<TaskHandle> CreateAllTasks() {
  std::vector<TaskHandle> tasks;
  tasks.reserve(4);

  tasks.push_back(Tasks::CreateSettingsTask());
  tasks.push_back(Tasks::CreateSystemInfoTask());
  tasks.push_back(Tasks::CreateDatabaseTask());
  tasks.push_back(Tasks::CreateFeaturesTask());

  return tasks;
}

void CleanupAllTasks(std::vector<TaskHandle> &tasks) {
  for (auto &handle : tasks) {
    if (handle.Destroy) {
      handle.Destroy();
    }

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

  // Step 5: Reinitialize icon bar
  TaskIconBar::InitIconBar(data.coromgr);

  // Step 6: Recreate all tasks
  tasks = CreateAllTasks();

  // Step 7: Expand first task
  selected_task = 0;
  if (!tasks.empty()) {
    tasks[selected_task].OnExpand();
  }
}

} // namespace GUI
