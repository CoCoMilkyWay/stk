#pragma once
#include <vector>

struct SharedData;
struct TaskHandle;

namespace GUI {

// Main entry point (called by different pipeline implementations)
// Returns 0 on success, non-zero on error
int RunGUI();

// Shared business logic: Draw GUI layout (used by both OpenGL and Vulkan)
void DrawGUILayout(SharedData &data, std::vector<TaskHandle> &tasks, int &selected_task);

} // namespace GUI
