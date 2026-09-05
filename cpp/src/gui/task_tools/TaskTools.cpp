#include "gui/task_tools/TaskTools.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_tools/ui/TabLatex.hpp"
#include "imgui.h"
#include "latex.h"
#include "shared/SharedData.hpp"

namespace GUI::Tasks {

struct TaskToolsState {
  std::unique_ptr<Tools::LatexEditorState> latex_state;
  bool latex_engine_initialized = false;
};

TaskHandle CreateToolsTask() {
  auto state = std::make_shared<TaskToolsState>();

  TaskHandle handle;
  handle.name = "Tools";
  handle.storage = state;

  handle.OnExpand = [state]() {
    // LaTeX引擎全局初始化一次
    if (!state->latex_engine_initialized) {
      tex::LaTeX::init("res");
      state->latex_engine_initialized = true;
      // 引擎初始化后再创建editor state
      state->latex_state = std::make_unique<Tools::LatexEditorState>();
      state->latex_state->initialized = true;
    }
    // 之后切换回来时什么都不做，保留所有状态
  };

  handle.OnCollapse = [state]() {
    // 什么都不做，保留所有状态
  };

  handle.DrawPanel = [state](SharedData &data) {
    ImGui::BeginChild("ToolsPanel");
    if (state->latex_state) {
      Tools::DrawLatexEditor(*state->latex_state);
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->latex_state.reset();
    // 程序结束时释放LaTeX引擎
    if (state->latex_engine_initialized) {
      tex::LaTeX::release();
      state->latex_engine_initialized = false;
    }
  };

  return handle;
}

} // namespace GUI::Tasks
