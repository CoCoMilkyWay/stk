#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "gui/util/Color.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

#include <algorithm>

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

  // Calculate heights
  float terminal_height = data.terminal.visible ? (display_h * data.terminal.height_ratio) : 0.0f;
  float panel_height = display_h - terminal_height;

  // Right top panel: Selected task content
  ImGui::SetNextWindowPos(ImVec2(200, 0));
  ImGui::SetNextWindowSize(ImVec2(display_w - 200, panel_height));
  ImGui::Begin("Panel", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  if (selected_task >= 0 && selected_task < (int)tasks.size()) {
    // Main content area
    const float button_bar_height = data.terminal.visible ? 0.0f : 25.0f;
    ImGui::BeginChild("PanelContent", ImVec2(0, -button_bar_height), false);
    tasks[selected_task].DrawPanel(data);
    ImGui::EndChild();

    // Show "Show Terminal" button at bottom when terminal is hidden
    if (!data.terminal.visible) {
      ImGui::Separator();
      if (ImGui::Button("Show Terminal", ImVec2(150, 0))) {
        data.terminal.visible = true;
      }
    }
  }

  ImGui::End();

  // Right bottom: Terminal (only when visible)
  if (data.terminal.visible) {
    ImGui::SetNextWindowPos(ImVec2(200, panel_height));
    ImGui::SetNextWindowSize(ImVec2(display_w - 200, terminal_height));
    ImGui::Begin("Terminal", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

    // Control buttons
    if (ImGui::Button("Clear")) {
      data.terminal.Clear();
    }
    ImGui::SameLine();
    bool auto_scroll = data.terminal.IsAutoScroll();
    if (ImGui::Checkbox("Auto-scroll", &auto_scroll)) {
      data.terminal.SetAutoScroll(auto_scroll);
    }
    ImGui::SameLine();
    ImGui::Dummy(ImVec2(20, 0));
    ImGui::SameLine();
    if (ImGui::Button("Hide")) {
      data.terminal.visible = false;
    }

    // Drag separator (static colors)
    static const ImVec4 splitter_col = ImVec4(0.3f, 0.3f, 0.3f, 0.5f);
    static const ImVec4 splitter_hover = ImVec4(0.4f, 0.4f, 0.4f, 0.7f);
    static const ImVec4 splitter_active = ImVec4(0.5f, 0.5f, 0.5f, 0.9f);
    
    ImGui::PushStyleColor(ImGuiCol_Button, splitter_col);
    ImGui::PushStyleColor(ImGuiCol_ButtonHovered, splitter_hover);
    ImGui::PushStyleColor(ImGuiCol_ButtonActive, splitter_active);
    ImGui::Button("##splitter", ImVec2(-1, 4));
    const bool is_hovered = ImGui::IsItemHovered();
    const bool is_active = ImGui::IsItemActive();
    ImGui::PopStyleColor(3);
    
    if (is_hovered) {
      ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeNS);
    }
    if (is_active) {
      float delta = ImGui::GetIO().MouseDelta.y;
      data.terminal.height_ratio -= delta / display_h;
      data.terminal.height_ratio = std::max(0.1f, std::min(0.5f, data.terminal.height_ratio));
    }

    // Terminal output
    ImGui::BeginChild("TerminalOutput", ImVec2(0, 0), false, ImGuiWindowFlags_HorizontalScrollbar);
    data.terminal.ReadLines([](const std::vector<TaskTerminal::Line> &lines) {
      for (const auto &line : lines) {
        ImGui::TextColored(ImVec4(line.color.r, line.color.g, line.color.b, line.color.a), "%s", line.text.c_str());
      }
    });
    if (data.terminal.IsAutoScroll() && ImGui::GetScrollY() >= ImGui::GetScrollMaxY()) {
      ImGui::SetScrollHereY(1.0f);
    }
    ImGui::EndChild();

    ImGui::End();
  }
}

} // namespace GUI
