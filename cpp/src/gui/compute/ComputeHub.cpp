#include "gui/compute/ComputeHub.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

namespace GUI::Compute {

void ComputeState::begin(const LibPlan &s, bool resume) {
  config.lib = s.lib.id;
  config.dates = s.plan;
  config.resume = resume;
  ui_state = ComputeUIState::ShowingPopup;
  popup_frame_count = 0;
}

void TickCompute(ComputeHub &hub, SharedData &data) {
  hub.inputs_ready = data.task_state.database.asset_info_ready && data.task_state.database.scanned && data.asset.orders.exists &&
                     !data.asset.backtest.days.empty() && !data.task_state.database.scan_inflight;
  if (hub.inputs_ready && !hub.service)
    hub.service = std::make_unique<ComputeService>(data);
  if (!hub.service)
    return;
  ComputeService &service = *hub.service;
  ComputeState &state = hub.state;
  const bool inputs_ready = hub.inputs_ready;
  const Phase phase = service.phase.load(std::memory_order_acquire);
  if (hub.prev_phase == Phase::Running && (phase == Phase::Done || phase == Phase::Cancelled))
    ++hub.rounds;
  hub.prev_phase = phase;

  const Config &config = data.config;
  const bool is_running = service.busy();

  // ---- 弹窗状态机 ----
  switch (state.ui_state) {
  case ComputeUIState::Idle:
    break;
  case ComputeUIState::ShowingPopup:
    // 弹窗至少画过一帧再开跑 (开跑即低刷新)
    if (++state.popup_frame_count >= 2) {
      service.Request(state.config);
      state.ui_state = ComputeUIState::WaitingStart;
    }
    break;
  case ComputeUIState::WaitingStart:
    if (is_running)
      state.ui_state = ComputeUIState::Computing;
    break;
  case ComputeUIState::Computing:
    if (!is_running) {
      state.ui_state = ComputeUIState::Idle;
      state.popup_frame_count = 0;
      state.libs_dirty = true;
      if (service.phase.load(std::memory_order_acquire) == Phase::Cancelled)
        state.auto_append = false;
    }
    break;
  }
  // 点下 Rebuild / Append 起到一轮收尾 (弹窗状态机非 Idle): 共享数据冻结
  data.task_state.compute.computing = state.ui_state != ComputeUIState::Idle;
  if (!inputs_ready)
    return;

  // ---- 库表: 数据换进 (同步 / 编码收尾都以扫描换进结束) 即重扫, 数据末日 / 未落盘随之前移; 并起一轮自动续算 ----
  if (state.generation != data.asset.generation) {
    state.generation = data.asset.generation;
    state.libs_dirty = true;
    state.auto_append = true;
    state.auto_done.clear();
  }
  if (state.libs_dirty && !is_running) {
    state.libs_dirty = false;
    state.libs = plan_libs(data);
  }

  // ---- 自动续算: 空闲 (无在算 / 无全量确认 / 无编码在飞) 时依次 Append 可续算且未判废的库, 每库一轮至多一次;
  //      规划序上游在前, 它算完重扫, 下游的规划随之前移再轮到; 无可续算 / 取消即止 ----
  if (!state.auto_append || state.ui_state != ComputeUIState::Idle || is_running || state.libs_dirty || state.confirm || state.confirm_remove ||
      data.task_state.database.encode_inflight)
    return;
  assert(state.libs && "库表未扫");
  const PerLib<LibPlan> &libs = *state.libs;
  const std::vector<LibId> order = lib_order();
  const auto it = std::ranges::find_if(order, [&](LibId l) {
    return libs[l].appendable() && !FeatureRead::dir_stale(lib_dir(config.feature_dir, l)) && std::ranges::find(state.auto_done, l) == state.auto_done.end();
  });
  if (it == order.end()) {
    state.auto_append = false;
    return;
  }
  state.auto_done.push_back(*it);
  state.begin(libs[*it], true);
}

TaskStatus ComputeStatus(const ComputeHub &hub, const SharedData &data, LibSide side) {
  if (!hub.inputs_ready)
    return {TaskStatus::Kind::Muted, "waiting"};
  const ComputeService *const service = hub.service.get();
  const bool running = service && service->busy();
  // 任一 reader 判废删了本页某个库 → 待重算 (该库开跑即撤销登记; 哪个库见库表)
  const auto lib_stale = [&](LibId l) { return on_side(side, l) && FeatureRead::dir_stale(lib_dir(data.config.feature_dir, l)); };
  if (!running && std::ranges::any_of(lib_ids(), lib_stale))
    return {TaskStatus::Kind::Warn, "pending"};
  if (!service || !on_side(side, hub.state.config.lib))
    return {};
  if (service->cancelling())
    return {TaskStatus::Kind::Busy, "cancelling"};
  return ProgressStatus(*service);
}

} // namespace GUI::Compute
