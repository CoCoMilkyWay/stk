// Task Factors - 因子算子库 (factor/) 的 GUI 入口.
//   Operators: OpTable 全部算子对拍 + 计时. 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进.
//     首次进页默认不算: 先载入本地 operators.json 显示 (校验不过则删文件); 只有手动 Run —— 或本地无有效
//     快照时的那一次自动补算 —— 才真跑 worker, 一轮结束落盘覆盖.
//   Factors: <factor_dir>/<universe>/ 一因子一文件的整体面板. 进页只扫描解析 (任何时候可进); Run 才读特征库评估
//     (要资产轴就绪), 结果回写各因子文件.
//   Inspect: Factors 页点行高光的单因子展示 (分层图等). 与 Factors 共用同一个 FactorsService / UI 状态.
#include "gui/task_factors/TaskFactors.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/OperatorsService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp"
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/task_factors/ui/TabOperators.hpp"
#include "shared/SharedData.hpp" // config.factor_dir / universe / 日期区间, feature.metadata, asset.items

#include "imgui.h"

#include <cassert>
#include <memory>
#include <string>

namespace GUI::Tasks {

enum TabIdx {
  TAB_OPERATORS = 0,
  TAB_FACTORS,
  TAB_INSPECT,
  TAB_COUNT
};

struct TaskFactorsState {
  std::unique_ptr<Factors::OperatorsService> operators_service;
  Factors::OperatorsUIState operators_ui;
  bool operators_started = false; // 首次 Draw 载入本地 json, 没有才 Request 一次
  // 一轮结束检测 (Running → Done / Cancelled 那一帧把算子表落地 operators.json)
  Factors::OperatorsStatus prev_operators_status = Factors::OperatorsStatus::Idle;

  std::unique_ptr<Factors::FactorsService> factors_service;
  Factors::FactorsUIState factors_ui;
  std::string factors_scanned_dir; // 上次扫描的目录 (universe 切换 → 自动重扫)
};

// Factors 子页的作用域 (每帧从 config 取)
static Factors::FactorsUIContext factors_context(const SharedData &data) {
  Factors::FactorsUIContext c;
  c.factor_dir = data.config.factor_dir + "/" + data.config.universe;
  c.universe = data.config.universe;
  c.start_date = data.config.start_date;
  c.end_date = data.config.end_date;
  c.axis_ready = !data.asset.items.empty();
  return c;
}

static TaskStatus factors_status(const Factors::FactorsService &svc) {
  switch (svc.status()) {
  case Factors::FactorsStatus::Scanning:
    return {TaskStatus::Kind::Busy, "scanning"};
  case Factors::FactorsStatus::Loading: {
    const int total = svc.total();
    return {TaskStatus::Kind::Busy, "loading " + std::to_string(total > 0 ? 100 * svc.done() / total : 0) + "%"};
  }
  case Factors::FactorsStatus::Running: {
    const int total = svc.total();
    return {TaskStatus::Kind::Busy, "running " + std::to_string(total > 0 ? 100 * svc.done() / total : 0) + "%"};
  }
  case Factors::FactorsStatus::Done:
    if (svc.broken() > 0)
      return {TaskStatus::Kind::Warn, std::to_string(svc.broken()) + " BROKEN"};
    return {TaskStatus::Kind::Ready, "ok"};
  case Factors::FactorsStatus::Cancelled:
    return {TaskStatus::Kind::Warn, "cancelled"};
  case Factors::FactorsStatus::Idle:
    break;
  }
  return {};
}

TaskHandle CreateFactorsTask() {
  auto state = std::make_shared<TaskFactorsState>();

  TaskHandle handle;
  handle.name = "Factors";
  handle.storage = state;
  handle.tabs = {"Operators", "Factors", "Inspect"};

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
    if (idx == TAB_FACTORS || idx == TAB_INSPECT)
      return state->factors_service ? factors_status(*state->factors_service) : TaskStatus{};
    // 任务行与 Operators 行同一状态
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

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (!state->operators_service)
      state->operators_service = std::make_unique<Factors::OperatorsService>();
    auto &svc = *state->operators_service;

    // 默认不自动跑: 先吃本地 operators.json (校验不过它自己会删掉文件), 没有有效快照才起算一轮.
    // 载入的一轮状态直接是 Done 且不经 Running, Update 的落盘判据 (prev == Running) 因此不会触发
    if (!state->operators_started) {
      state->operators_started = true;
      if (!Factors::LoadOperatorTableJson(data.config.factor_dir, svc, state->operators_ui))
        svc.Request(state->operators_ui.req);
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
    case TAB_FACTORS:
    case TAB_INSPECT: {
      // 首次进页: 字段表视图建一次 (编译期字段表, 不随运行变), 只扫描不算; universe 切换 → 目录变 → 自动重扫
      if (!state->factors_service) {
        state->factors_service = std::make_unique<Factors::FactorsService>();
        state->factors_service->SetFeatureTable(Factors::BuildFeatureTable(data.feature.metadata));
      }
      auto &fs = *state->factors_service;
      const Factors::FactorsUIContext ctx = factors_context(data);
      if (state->factors_scanned_dir != ctx.factor_dir) {
        state->factors_scanned_dir = ctx.factor_dir;
        Factors::FactorsRequest req;
        Factors::MakeFactorsRequest(data, /*evaluate=*/false, false, req);
        fs.Request(req);
      }
      if (idx == TAB_INSPECT) {
        Factors::RenderTabInspect(fs, state->factors_ui, ctx);
        break;
      }
      const int action = Factors::RenderTabFactors(fs, state->factors_ui, ctx);
      if (action == 1 || action == 2) {
        Factors::FactorsRequest req;
        if (Factors::MakeFactorsRequest(data, /*evaluate=*/action == 1, state->factors_ui.backend == 1, req))
          fs.Request(req);
      } else if (action == -1) {
        fs.RequestCancel();
      }
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
    state->factors_service.reset();
    state->factors_scanned_dir.clear();
  };

  return handle;
}

} // namespace GUI::Tasks
