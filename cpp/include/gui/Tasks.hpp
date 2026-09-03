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
  std::function<void(SharedData &)> Init; // 创建后立即调用一次, 与选中态/DrawPanel 解耦 (后台任务提前起)
  std::function<void()> OnExpand;
  std::function<void()> OnCollapse;
  std::function<void(SharedData &)> DrawPanel;
  std::function<void()> Destroy;
  std::shared_ptr<void> storage;
  void *task_instance = nullptr; // Optional raw pointer for debugging
};

namespace GUI {

// Create task handles for all tasks (Init 按 push 顺序立即触发一遍, 顺序即依赖: Settings 先落盘配置到内存)
std::vector<TaskHandle> CreateAllTasks(SharedData &data);

// Cleanup tasks
void CleanupAllTasks(std::vector<TaskHandle> &tasks);

// Reinitialize all tasks (cleanup + recreate)
void ReinitAllTasks(std::vector<TaskHandle> &tasks, int &selected_task, SharedData &data);

} // namespace GUI
