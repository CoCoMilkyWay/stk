#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"
#include <cassert>

namespace GUI {

// 切换选中叶子: 跨任务时触发 OnCollapse/OnExpand, 同任务内切 tab 不触发
static void SelectLeaf(std::vector<TaskHandle> &tasks, int &selected_task,
                       int task_idx, int tab_idx) {
  if (task_idx < 0 || task_idx >= (int)tasks.size())
    return;
  TaskHandle &t = tasks[task_idx];
  // 无子项任务: tab_idx 必须是 -1
  if (t.tab_names.empty())
    tab_idx = -1;
  else if (tab_idx < 0 || tab_idx >= (int)t.tab_names.size())
    tab_idx = 0; // 默认首个子项

  if (selected_task != task_idx) {
    if (selected_task >= 0 && selected_task < (int)tasks.size())
      tasks[selected_task].OnCollapse();
    selected_task = task_idx;
    tasks[task_idx].OnExpand();
  }
  tasks[task_idx].selected_tab = tab_idx;
}

// Shared business logic: Draw GUI layout (called by both OpenGL and Vulkan pipelines)
void DrawGUILayout(SharedData &data, std::vector<TaskHandle> &tasks, int &selected_task) {

  // Get window size
  int display_w = (int)ImGui::GetIO().DisplaySize.x;
  int display_h = (int)ImGui::GetIO().DisplaySize.y;

  // Left panel: 多层选择栏 (任务树, 叶子=可选项)
  ImGui::SetNextWindowPos(ImVec2(0, 0));
  ImGui::SetNextWindowSize(ImVec2(200, display_h));
  ImGui::Begin("Tasks", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Task tree (leave minimal space for compact icon bar)
  ImGui::BeginChild("TaskList", ImVec2(0, -ImGui::GetTextLineHeightWithSpacing() * 1.2f), false);

  for (int i = 0; i < (int)tasks.size(); i++) {
    TaskHandle &t = tasks[i];

    // Draw status from unified taskstate (top-level row only)
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

    if (t.tab_names.empty()) {
      // 无子项: 直接作为叶子
      bool is_selected = (selected_task == i);
      char name_label[256];
      snprintf(name_label, sizeof(name_label), "> %s", t.name.c_str());
      if (ImGui::Selectable(name_label, is_selected)) {
        SelectLeaf(tasks, selected_task, i, -1);
      }
      if (status && status[0] != '\0') {
        ImGui::SameLine();
        ImGui::TextColored(color, "[%s]", status);
      }
    } else {
      // 有子项: 父节点可展开, 子项为叶子
      char header_label[256];
      snprintf(header_label, sizeof(header_label), "%s###TaskHeader_%d", t.name.c_str(), i);
      // 父节点选中态: 任一子项被选中时高亮父节点
      bool parent_active = (selected_task == i);
      ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_DefaultOpen |
                                 ImGuiTreeNodeFlags_FramePadding;
      if (parent_active)
        flags |= ImGuiTreeNodeFlags_Selected;
      bool opened = ImGui::TreeNodeEx(header_label, flags);
      if (status && status[0] != '\0') {
        ImGui::SameLine();
        ImGui::TextColored(color, "[%s]", status);
      }
      if (opened) {
        for (int j = 0; j < (int)t.tab_names.size(); j++) {
          bool enabled = t.IsTabEnabled ? t.IsTabEnabled(j) : true;
          bool is_selected = (selected_task == i && t.selected_tab == j);
          char child_label[256];
          snprintf(child_label, sizeof(child_label), "  > %s###%d_%d", t.tab_names[j].c_str(), i, j);
          if (!enabled)
            ImGui::BeginDisabled();
          if (ImGui::Selectable(child_label, is_selected)) {
            SelectLeaf(tasks, selected_task, i, j);
          }
          if (!enabled) {
            if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
              ImGui::SetTooltip("Tab not ready");
            ImGui::EndDisabled();
          }
        }
        ImGui::TreePop();
      }
    }
  }

  ImGui::EndChild();

  // Icon bar at bottom
  ImGui::Separator();
  TaskIconBar::DrawIconBar();

  ImGui::End();

  // Right panel: Selected leaf content (full height)
  ImGui::SetNextWindowPos(ImVec2(200, 0));
  ImGui::SetNextWindowSize(ImVec2(display_w - 200, display_h));
  char panel_title[64];
  snprintf(panel_title, sizeof(panel_title), "Panel (%dx%d)###Panel", display_w, display_h);
  ImGui::Begin(panel_title, nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  if (selected_task >= 0 && selected_task < (int)tasks.size()) {
    TaskHandle &t = tasks[selected_task];
    if (t.tab_names.empty()) {
      t.DrawPanel(data);
    } else {
      int idx = t.selected_tab;
      if (idx < 0 || idx >= (int)t.tab_names.size())
        idx = 0;
      assert(t.DrawTab && "tabbed task must provide DrawTab");
      t.DrawTab(data, idx);
    }
  }

  ImGui::End();
}

} // namespace GUI
