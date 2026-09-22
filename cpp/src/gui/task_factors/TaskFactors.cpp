// Task Factors - 因子算子库 (factor/) 的 GUI 入口. 首个子页 Operators: OpTable 全部算子对拍 + 计时.
// 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进; worker 首次进页自动起跑, 之后按 Run 重跑.
#include "gui/task_factors/TaskFactors.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_factors/services/OperatorsService.hpp"
#include "gui/task_factors/ui/TabOperators.hpp"
#include "shared/SharedData.hpp" // config.factor_dir

#include "imgui.h"

#include <cassert>
#include <memory>
#include <string>

namespace GUI::Tasks {

enum TabIdx {
  TAB_OPERATORS = 0,
  TAB_COUNT
};

struct TaskFactorsState {
  std::unique_ptr<Factors::OperatorsService> operators_service;
  Factors::OperatorsUIState operators_ui;
  bool operators_started = false; // 首次 Draw 自动 Request 一次
  // 一轮结束检测 (Running → Done / Cancelled 那一帧把算子表落地 operators.json)
  Factors::OperatorsStatus prev_operators_status = Factors::OperatorsStatus::Idle;
};

TaskHandle CreateFactorsTask() {
  auto state = std::make_shared<TaskFactorsState>();

  TaskHandle handle;
  handle.name = "Factors";
  handle.storage = state;
  handle.tabs = {"Operators"};

  // 不设 OnCollapse: 切走任务 worker 继续跑完 (单线程 + ~90 行小结构, 左栏状态标签照常更新, 切回直接看表)

  // 每帧 (无论是否选中): 一轮跑完 / 取消那一帧把算子表落地 <factor_dir>/operators.json
  // (与 universe 无关, 全局一份; 取消也落, 已跑完的行不白算). 切走任务 worker 仍在跑, 故放 Update 不放 Draw
  handle.Update = [state](SharedData &data) {
    if (!state->operators_service)
      return;
    auto &svc = *state->operators_service;
    const auto st = svc.status();
    const bool finished = st == Factors::OperatorsStatus::Done || st == Factors::OperatorsStatus::Cancelled;
    if (finished && state->prev_operators_status == Factors::OperatorsStatus::Running)
      Factors::SaveOperatorTableJson(data.config.factor_dir, svc);
    state->prev_operators_status = st;
  };

  handle.Status = [state](const SharedData & /*data*/, int idx) -> TaskStatus {
    assert(idx == -1 || idx < TAB_COUNT);
    // 任务行与 Operators 行同一状态 (目前只有这一个子页)
    if (!state->operators_service)
      return {};
    const auto &svc = *state->operators_service;
    switch (svc.status()) {
    case Factors::OperatorsStatus::Running: {
      const int total = svc.total();
      const int pct = total > 0 ? 100 * svc.done() / total : 0;
      return {TaskStatus::Kind::Busy, "running " + std::to_string(pct) + "%"};
    }
    case Factors::OperatorsStatus::Done:
      if (svc.failed() > 0)
        return {TaskStatus::Kind::Error, std::to_string(svc.failed()) + " FAIL"};
      return {TaskStatus::Kind::Ready, "all ok"};
    case Factors::OperatorsStatus::Cancelled:
      return {TaskStatus::Kind::Warn, "cancelled"};
    case Factors::OperatorsStatus::Idle:
      break;
    }
    return {};
  };

  handle.Draw = [state](SharedData & /*data*/, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (!state->operators_service)
      state->operators_service = std::make_unique<Factors::OperatorsService>();
    auto &svc = *state->operators_service;

    if (!state->operators_started) {
      svc.Request(state->operators_ui.req);
      state->operators_started = true;
    }

    ImGui::BeginChild("FactorsTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_OPERATORS: {
      const int action = Factors::RenderTabOperators(svc, state->operators_ui);
      if (action == 1)
        svc.Request(state->operators_ui.req);
      else if (action == -1)
        svc.RequestCancel();
      break;
    }
    default:
      break;
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->operators_service.reset(); // 析构 Stop() join worker
    state->operators_started = false;
  };

  return handle;
}

} // namespace GUI::Tasks
