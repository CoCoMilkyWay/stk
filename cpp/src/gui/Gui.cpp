#include "gui/Gui.h"
#include "gui/GuiState.hpp"
#include "gui/GuiTask.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"
#include <vector>

// Forward declarations for task creation
IGuiTask *CreateSettingsTask();
IGuiTask *CreateSystemInfoTask();

// Forward declarations for icon bar
void InitIconBar(GuiState& gui_state);
void DrawIconBar();
void CleanupIconBar();

namespace GUI {

// Shared business logic: Draw GUI layout (called by both OpenGL and Vulkan pipelines)
void DrawGUILayout(SharedData &sharedData, GuiState &guiState,
                   std::vector<IGuiTask *> &tasks, int &selected_task) {

  // Get window size
  int display_w = (int)ImGui::GetIO().DisplaySize.x;
  int display_h = (int)ImGui::GetIO().DisplaySize.y;

  // Left panel: Tasks list
  ImGui::SetNextWindowPos(ImVec2(0, 0));
  ImGui::SetNextWindowSize(ImVec2(300, display_h));
  ImGui::Begin("Tasks", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Task list
  ImGui::BeginChild("TaskList", ImVec2(0, -ImGui::GetTextLineHeightWithSpacing() * 1.5f), false);

  for (int i = 0; i < (int)tasks.size(); i++) {
    bool is_selected = (selected_task == i);

    // Draw task name
    char name_label[256];
    snprintf(name_label, sizeof(name_label), "> %s", tasks[i]->GetName());

    if (ImGui::Selectable(name_label, is_selected, 0, ImVec2(0, 0))) {
      if (selected_task != i) {
        tasks[selected_task]->OnCollapse();
        selected_task = i;
        tasks[selected_task]->OnExpand();
      }
    }

    // Draw status with color on the same line
    const char *status = tasks[i]->GetStatus();
    if (status && status[0] != '\0') {
      ImGui::SameLine();
      auto color = tasks[i]->GetStatusColor();
      ImGui::TextColored(ImVec4(color.r, color.g, color.b, color.a), "[%s]", status);
    }
  }

  ImGui::EndChild();

  // Icon bar at bottom
  ImGui::Separator();
  DrawIconBar();

  ImGui::End();

  // Right top panel: Selected task content
  ImGui::SetNextWindowPos(ImVec2(300, 0));
  ImGui::SetNextWindowSize(ImVec2(display_w - 300, display_h * 0.7f));
  ImGui::Begin("Panel", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  if (selected_task >= 0 && selected_task < (int)tasks.size()) {
    tasks[selected_task]->DrawPanel(sharedData, guiState);
  }

  ImGui::End();

  // Right bottom panel: Terminal
  ImGui::SetNextWindowPos(ImVec2(300, display_h * 0.7f));
  ImGui::SetNextWindowSize(ImVec2(display_w - 300, display_h * 0.3f));
  ImGui::Begin("Terminal", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Clear button
  if (ImGui::Button("Clear")) {
    guiState.terminal.Clear();
  }
  ImGui::SameLine();
  bool auto_scroll = guiState.terminal.IsAutoScroll();
  if (ImGui::Checkbox("Auto-scroll", &auto_scroll)) {
    guiState.terminal.SetAutoScroll(auto_scroll);
  }

  ImGui::Separator();

  // Terminal output area
  ImGui::BeginChild("TerminalOutput", ImVec2(0, 0), false, ImGuiWindowFlags_HorizontalScrollbar);

  guiState.terminal.ReadLines([](const std::vector<Terminal::Line>& lines) {
    for (const auto& line : lines) {
      ImGui::TextColored(ImVec4(line.color.r, line.color.g, line.color.b, line.color.a), "%s", line.text.c_str());
    }
  });

  // Auto-scroll to bottom
  if (guiState.terminal.IsAutoScroll() && ImGui::GetScrollY() >= ImGui::GetScrollMaxY()) {
    ImGui::SetScrollHereY(1.0f);
  }

  ImGui::EndChild();
  ImGui::End();
}

} // namespace GUI
