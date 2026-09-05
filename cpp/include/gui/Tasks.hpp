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
  // 无子项任务: 用 DrawPanel 渲染整个右栏 (selected_tab == -1)
  std::function<void(SharedData &)> DrawPanel;
  // 有子项任务: 用 DrawTab 渲染指定 tab 内容, IsTabEnabled 决定左栏叶子是否可用
  // IsTabEnabled 读上一帧 DrawTab 缓存的使能状态 (立即模式下一帧延迟不可见)
  std::vector<std::string> tab_names;
  std::function<bool(int)> IsTabEnabled;
  std::function<void(SharedData &, int)> DrawTab;
  int selected_tab = -1; // 当前选中的子项下标, -1 = 无子项 (用 DrawPanel)
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
