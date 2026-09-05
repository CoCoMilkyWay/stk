#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

namespace GUI {

// Shared business logic: Draw GUI layout (called by both OpenGL and Vulkan pipelines)
void DrawGUILayout(SharedData &data, std::vector<TaskHandle> &tasks, int &selected_task) {

  // Get window size
  int display_w = (int)ImGui::GetIO().DisplaySize.x;
  int display_h = (int)ImGui::GetIO().DisplaySize.y;

  // Left panel: Tasks list (compressed width)
  ImGui::SetNextWindowPos(ImVec2(0, 0));
  ImGui::SetNextWindowSize(ImVec2(200, display_h));
  ImGui::Begin("Tasks", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Task list (leave minimal space for compact icon bar)
  ImGui::BeginChild("TaskList", ImVec2(0, -ImGui::GetTextLineHeightWithSpacing() * 1.2f), false);

  for (int i = 0; i < (int)tasks.size(); i++) {
    bool is_selected = (selected_task == i);

    // Draw task name
    char name_label[256];
    snprintf(name_label, sizeof(name_label), "> %s", tasks[i].name.c_str());

    if (ImGui::Selectable(name_label, is_selected, 0, ImVec2(0, 0))) {
      if (selected_task != i) {
        tasks[selected_task].OnCollapse();
        selected_task = i;
        tasks[selected_task].OnExpand();
      }
    }

    // Draw status from unified taskstate
    const char *status = nullptr;
    ImVec4 color;
    switch (i) {
    case 0: // Settings
      status = data.taskstate.settings.status_text();
      color = data.taskstate.settings.status_color();
      break;
    case 1: // SystemInfo - no status
      break;
    case 2: // Database
      status = data.taskstate.database.status_text();
      color = data.taskstate.database.status_color();
      break;
    case 3: // Features
      status = data.taskstate.features.status_text();
      color = data.taskstate.features.status_color();
      break;
    }
    if (status && status[0] != '\0') {
      ImGui::SameLine();
      ImGui::TextColored(color, "[%s]", status);
    }
  }

  ImGui::EndChild();

  // Icon bar at bottom
  ImGui::Separator();
  TaskIconBar::DrawIconBar();

  ImGui::End();

  // Right panel: Selected task content (full height)
  ImGui::SetNextWindowPos(ImVec2(200, 0));
  ImGui::SetNextWindowSize(ImVec2(display_w - 200, display_h));
  char panel_title[64];
  snprintf(panel_title, sizeof(panel_title), "Panel (%dx%d)###Panel", display_w, display_h);
  ImGui::Begin(panel_title, nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  if (selected_task >= 0 && selected_task < (int)tasks.size()) {
    tasks[selected_task].DrawPanel(data);
  }

  ImGui::End();
}

} // namespace GUI
