#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

// Simple task interface without inheritance - function-based
// Note: Task status is now managed via SharedData::taskstate (unified state management)
struct TaskHandle {
  std::string name;
  std::function<void()> OnExpand;
  std::function<void()> OnCollapse;
  std::function<void(SharedData &)> DrawPanel;
  std::function<void()> Destroy;
  std::shared_ptr<void> storage;
  void *task_instance = nullptr; // Optional raw pointer for debugging
};

namespace GUI {

// Create task handles for all tasks
std::vector<TaskHandle> CreateAllTasks();

// Cleanup tasks
void CleanupAllTasks(std::vector<TaskHandle> &tasks);

// Reinitialize all tasks (cleanup + recreate)
void ReinitAllTasks(std::vector<TaskHandle> &tasks, int &selected_task, SharedData &data);

} // namespace GUI
