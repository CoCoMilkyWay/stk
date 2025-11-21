#pragma once
#include <vector>

struct IGuiTask;
struct GuiState;
struct SharedData;

namespace GUI {

// Main entry point (called by different pipeline implementations)
void RunGUI();

// Shared business logic: Draw GUI layout (used by both OpenGL and Vulkan)
void DrawGUILayout(SharedData& sharedData, GuiState& guiState, 
                   std::vector<IGuiTask*>& tasks, int& selected_task);

} // namespace GUI
