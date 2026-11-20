// Task Live - 实盘: 券商无关中枢 LiveHub; 两页对仗:
//   Board  持仓 · 调仓 · 闪电下单 + 右栏行情图
//   Client 客户端窗口复现 (窗口树 + GDI 绘制层 + 控件子项 + 鼠键透传)
#include "gui/task_live/TaskLive.hpp"
#include "gui/Tasks.hpp"
#include "gui/account/AccountHub.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_live/LiveHub.hpp"
#include "gui/task_live/ui/TabBoard.hpp"
#include "gui/task_live/ui/TabClient.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <cassert>
#include <memory>
#include <utility>

namespace GUI::Tasks {

enum TabIdx {
  TAB_BOARD = 0,
  TAB_CLIENT = 1,
  TAB_COUNT
};

struct TaskLiveState {
  std::shared_ptr<Quote::QuoteHub> quote;
  std::shared_ptr<Account::AccountHub> account;
  std::unique_ptr<Live::LiveHub> hub;
  Live::BoardState board;
  Live::ClientState client;
};

TaskHandle CreateLiveTask(std::shared_ptr<Quote::QuoteHub> quote, std::shared_ptr<Account::AccountHub> account) {
  auto state = std::make_shared<TaskLiveState>();
  state->quote = std::move(quote);
  state->account = std::move(account);

  TaskHandle handle;
  handle.name = "Live";
  handle.storage = state;
  handle.tabs = {"Board", "Client"};

  handle.Init = [state](SharedData &) { state->hub = std::make_unique<Live::LiveHub>(); };

  handle.Update = [state](SharedData &) {
    Live::LiveHub &h = *state->hub;
    h.peek();
    h.auto_pull();
    if (!h.connected() || !h.snapshot())
      return;
    Account::AccountHub &a = *state->account;
    a.feed(h.snapshot());
    state->quote->spot(a.spot());
  };

  handle.Status = [state](const SharedData &, [[maybe_unused]] int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    const Live::LiveHub &h = *state->hub;
    if (!h.connected())
      return {};
    const live::Refresh &g = h.refresh();
    const auto &t = h.task();
    if (!g.down.empty())
      return {TaskStatus::Kind::Error, "down"};
    if (h.busy())
      return {TaskStatus::Kind::Busy, "running"};
    if (!g.at.segs.empty())
      return {TaskStatus::Kind::Busy, "refreshing"};
    if (!g.err.empty())
      return {TaskStatus::Kind::Error, "error"};
    if (t && t->st == Live::JobSt::Err)
      return {TaskStatus::Kind::Error, "failed"};
    return {TaskStatus::Kind::Muted, "live"};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    Live::LiveHub &h = *state->hub;
    ImGui::BeginChild("LiveTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == TAB_BOARD) {
      if (data.task_state.database.asset_info_ready)
        state->quote->frame(data);
      h.tick(data.asset_info, *state->quote);
      Live::RenderTabBoard(data, h, *state->quote, state->board);
    } else {
      Live::RenderTabClient(h, state->client);
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->client.stop();
    state->board = Live::BoardState{};
    state->client = Live::ClientState{};
    state->hub.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
