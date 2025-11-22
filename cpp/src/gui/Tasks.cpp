#include "gui/Tasks.hpp"
#include "gui/task_crawler/CoroCrawler.hpp"
#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_settings/TaskSettings.hpp"
#include "gui/task_system_info/TaskSystemInfo.hpp"

namespace GUI {

std::vector<TaskHandle> CreateAllTasks() {
  std::vector<TaskHandle> tasks;
  tasks.reserve(4);

  tasks.push_back(Tasks::CreateSettingsTask());
  tasks.push_back(Tasks::CreateSystemInfoTask());
  tasks.push_back(Tasks::CreateDatabaseTask());
  tasks.push_back(Tasks::CreateCrawlerTask());

  return tasks;
}

void CleanupAllTasks(std::vector<TaskHandle> &tasks) {
  for (auto &handle : tasks) {
    if (handle.Destroy) {
      handle.Destroy();
    }

    handle.GetStatus = {};
    handle.OnExpand = {};
    handle.OnCollapse = {};
    handle.DrawPanel = {};
    handle.Destroy = {};
    handle.storage.reset();
    handle.task_instance = nullptr;
  }
  tasks.clear();
}

} // namespace GUI
