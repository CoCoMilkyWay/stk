#include "gui/task_tools/TaskTools.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_tools/ui/TabLatex.hpp"
#include "shared/SharedData.hpp"
#include "imgui.h"

namespace GUI::Tasks {

struct TaskToolsState {
  int selected_tab = 0;
  Tools::LatexEditorState latex_state;
};

TaskHandle CreateToolsTask() {
  auto state = std::make_shared<TaskToolsState>();

  TaskHandle handle;
  handle.name = "Tools";
  handle.storage = state;

  handle.OnExpand = [state]() {
    Tools::InitLatexEditor(state->latex_state);
  };

  handle.OnCollapse = [state]() {
    Tools::CleanupLatexEditor(state->latex_state);
  };

  handle.DrawPanel = [state](SharedData& data) {
    ImGui::BeginChild("ToolsPanel");

    if (ImGui::BeginTabBar("ToolsTabs")) {
      if (ImGui::BeginTabItem("LaTeX")) {
        Tools::DrawLatexEditor(state->latex_state);
        ImGui::EndTabItem();
      }
      ImGui::EndTabBar();
    }

    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    Tools::CleanupLatexEditor(state->latex_state);
  };

  return handle;
}

} // namespace GUI::Tasks

