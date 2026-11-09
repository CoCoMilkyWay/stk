#include "gui/task_tools/TaskTools.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_tools/ui/TabLatex.hpp"
#include "gui/task_tools/ui/TabSystem.hpp"
#include "imgui.h"
#include "latex.h"
#include "shared/SharedData.hpp"

namespace GUI::Tasks {

enum TabIdx { TAB_LATEX,
              TAB_SYSTEM };

struct TaskToolsState {
  std::unique_ptr<Tools::LatexEditorState> latex_state;
  bool latex_engine_initialized = false;
  Tools::SystemState system;
  int shown = -1; // 上一帧画的子页 (-1 = 任务收起): 切进 System 清曲线开采样, 切走停采样
};

TaskHandle CreateToolsTask() {
  auto state = std::make_shared<TaskToolsState>();

  TaskHandle handle;
  handle.name = "Tools";
  handle.tabs = {"LaTeX", "System"};
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
    state->system.live = false;
    state->shown = -1;
  };

  handle.Status = [state](const SharedData &, int idx) -> TaskStatus {
    if (idx == TAB_SYSTEM && state->system.live)
      return {TaskStatus::Kind::Muted, "live"};
    return {};
  };

  handle.Draw = [state](SharedData &, int idx) {
    if (idx == TAB_SYSTEM && state->shown != TAB_SYSTEM)
      Tools::ShowSystem(state->system);
    if (idx != TAB_SYSTEM)
      state->system.live = false;
    state->shown = idx;

    ImGui::BeginChild("ToolsPanel");
    if (idx == TAB_LATEX && state->latex_state)
      Tools::DrawLatexEditor(*state->latex_state);
    if (idx == TAB_SYSTEM)
      Tools::DrawSystem(state->system);
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
