#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"
#include <cassert>

namespace GUI {

// 行状态标签: 行名后画 [text], 颜色查全局色表 (idx == -1 任务行, >= 0 子行)
static void DrawRowStatus(const SharedData &data, const TaskHandle &t, int idx) {
  if (!t.Status)
    return;
  TaskStatus s = t.Status(data, idx);
  if (s.kind == TaskStatus::Kind::None || s.text.empty())
    return;
  ImGui::SameLine();
  ImGui::TextColored(StatusColor(s.kind), "[%s]", s.text.c_str());
}

// Shared business logic: Draw GUI layout (called by both OpenGL and Vulkan pipelines)
void DrawGUILayout(SharedData &data, TaskTree &tree) {

  // 帧首: 全部任务 Update (无论选中) —— 状态标签/使能同帧就绪,
  // 左栏不再依赖 "打开过页面" 的上一帧缓存
  for (auto &t : tree.tasks)
    if (t.Update)
      t.Update(data);

  // Get window size
  int display_w = (int)ImGui::GetIO().DisplaySize.x;
  int display_h = (int)ImGui::GetIO().DisplaySize.y;

  // Left panel: 多层选择栏 (任务树, 叶子=可选项; 纯渲染, 不认识任何具体任务)
  ImGui::SetNextWindowPos(ImVec2(0, 0));
  ImGui::SetNextWindowSize(ImVec2(200, display_h));
  ImGui::Begin("Tasks", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Task tree (leave minimal space for compact icon bar)
  ImGui::BeginChild("TaskList", ImVec2(0, -ImGui::GetTextLineHeightWithSpacing() * 1.2f), false);

  for (int i = 0; i < (int)tree.tasks.size(); i++) {
    TaskHandle &t = tree.tasks[i];

    if (t.tabs.empty()) {
      // 无子项: 直接作为叶子
      bool enabled = t.Enabled ? t.Enabled(data, -1) : true;
      bool is_selected = (tree.selected == i);
      char name_label[256];
      snprintf(name_label, sizeof(name_label), "> %s###Task_%d", t.name.c_str(), i);
      if (!enabled)
        ImGui::BeginDisabled();
      if (ImGui::Selectable(name_label, is_selected)) {
        tree.Select(i, -1);
      }
      if (!enabled)
        ImGui::EndDisabled();
      DrawRowStatus(data, t, -1);
    } else {
      // 有子项: 父节点可展开 (本身不可选), 子项为叶子
      char header_label[256];
      snprintf(header_label, sizeof(header_label), "%s###TaskHeader_%d", t.name.c_str(), i);
      // 父节点选中态: 任一子项被选中时高亮父节点
      ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_DefaultOpen |
                                 ImGuiTreeNodeFlags_FramePadding;
      if (tree.selected == i)
        flags |= ImGuiTreeNodeFlags_Selected;
      bool opened = ImGui::TreeNodeEx(header_label, flags);
      DrawRowStatus(data, t, -1);
      if (opened) {
        for (int j = 0; j < (int)t.tabs.size(); j++) {
          bool enabled = t.Enabled ? t.Enabled(data, j) : true;
          bool is_selected = (tree.selected == i && t.selected_tab == j);
          char child_label[256];
          snprintf(child_label, sizeof(child_label), "  > %s###%d_%d", t.tabs[j].c_str(), i, j);
          if (!enabled)
            ImGui::BeginDisabled();
          if (ImGui::Selectable(child_label, is_selected)) {
            tree.Select(i, j);
          }
          if (!enabled) {
            if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
              ImGui::SetTooltip("Tab not ready");
            ImGui::EndDisabled();
          }
          DrawRowStatus(data, t, j);
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

  if (tree.selected >= 0 && tree.selected < (int)tree.tasks.size()) {
    TaskHandle &t = tree.tasks[tree.selected];
    int idx = t.selected_tab;
    if (!t.tabs.empty() && (idx < 0 || idx >= (int)t.tabs.size()))
      idx = 0;
    assert(t.Draw && "task must provide Draw");
    t.Draw(data, idx);
  }

  ImGui::End();
}

} // namespace GUI
