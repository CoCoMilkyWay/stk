#pragma once

struct SharedData;

namespace GUI {

struct TaskTree;

// Main entry point (called by different pipeline implementations)
// Returns 0 on success, non-zero on error
int RunGUI();

// Shared business logic: Draw GUI layout (used by both OpenGL and Vulkan)
void DrawGUILayout(SharedData &data, TaskTree &tree);

} // namespace GUI
