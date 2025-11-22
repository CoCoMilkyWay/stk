#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct SharedData;
struct GuiState;

// Simple task interface without inheritance - function-based
struct TaskHandle {
  std::string name;
  std::function<const char *()> GetStatus;
  std::function<void()> OnExpand;
  std::function<void()> OnCollapse;
  std::function<void(SharedData &, GuiState &)> DrawPanel;
  std::function<void()> Destroy;
  std::shared_ptr<void> storage;
  void *task_instance = nullptr; // Optional raw pointer for debugging
};

namespace GUI {

// Create task handles for all tasks
std::vector<TaskHandle> CreateAllTasks();

// Cleanup tasks
void CleanupAllTasks(std::vector<TaskHandle> &tasks);

} // namespace GUI
