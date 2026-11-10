// Task Beta - 风格择优: 先算全市场 all.bar 标签库, 再在其上回测风格篮子的参数网格, 选中的子池落 pools.json.
//   Compute: 库表的 Beta 段 (只 all.bar; 见 ComputeHub.hpp). 计算服务全局一份 (ComputeHub), 输入就绪即建, Update 每帧推进 (自动续算不等进页);
//     Alpha → Compute 管其余各库, Features 盯它的轮次作废缓存.
//   Style: 风格篮子参数优选 (StyleService, 原料直接读 all.bar 库; 首次进页建), 选中落地 = config/pools.json (只本页读; 计算的池表 = 编译期 feature_pools()).
#include "gui/task_beta/TaskBeta.hpp"
#include "gui/Tasks.hpp"
#include "gui/compute/ui/TabCompute.hpp"
#include "gui/task_beta/services/StyleService.hpp"
#include "gui/task_beta/ui/TabStyle.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <cassert>
#include <memory>
#include <utility>

namespace GUI::Tasks {

enum TabIdx {
  TAB_COMPUTE = 0,
  TAB_STYLE,
  TAB_COUNT
};

struct TaskBetaState {
  std::shared_ptr<Compute::ComputeHub> compute;
  std::unique_ptr<Beta::StyleService> style_service;
  Beta::StyleUIState style_ui;
};

TaskHandle CreateBetaTask(std::shared_ptr<Compute::ComputeHub> compute) {
  auto state = std::make_shared<TaskBetaState>();
  state->compute = std::move(compute);

  TaskHandle handle;
  handle.name = "Beta";
  handle.storage = state;
  handle.tabs = {"Compute", "Style"};

  handle.Update = [state](SharedData &data) { Compute::TickCompute(*state->compute, data); };

  handle.Enabled = [state](const SharedData & /*data*/, int idx) { return idx != TAB_COMPUTE || state->compute->service != nullptr; };

  handle.Status = [state](const SharedData &data, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (idx == TAB_COMPUTE)
      return Compute::ComputeStatus(*state->compute, data, Compute::LibSide::Beta);
    return state->style_service ? ProgressStatus(*state->style_service) : TaskStatus{};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    ImGui::BeginChild("BetaTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == TAB_COMPUTE) {
      Compute::RenderTabCompute(*state->compute, data, Compute::LibSide::Beta);
    } else {
      Beta::SyncStylePools(state->style_ui);
      if (!state->style_service)
        state->style_service = std::make_unique<Beta::StyleService>();
      Beta::RenderTabStyle(*state->style_service, state->style_ui);
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->compute->service.reset(); // 析构 Stop(): 取消 + join worker
    state->style_service.reset();    // 析构 Stop() join worker
  };

  return handle;
}

} // namespace GUI::Tasks
