// Task Live - 实盘: 券商客户端的持仓、下单与一键调仓 (中枢 LiveHub 券商无关; 客户端适配在 live/<客户端>/, 登记在 live/Clients.hpp)
//   Board: 连接 + 持仓 · 调仓一页 (控件分组 / 持仓表行内闪电下单与卖出滑杆, 并调仓名单 / 日志) + 右栏选中行的行情图
//     (Market 的个股面板, 与 Database / Market 共用 QuoteHub)
//   行情走新浪 (LiveHub 把要的码订阅给 QuoteHub, 本地资产库就绪才有), 不读客户端的行情; Board 可见时才盯客户端 (见 live/Broker.hpp 的 touch)
//   账户净值 (gui/account): 每帧交信用账户 + 今日成交 (其库外持仓的现价转给价格库回放); 两融条款与资金流水由 LiveHub 每天自动拉
#include "gui/task_live/TaskLive.hpp"
#include "gui/Tasks.hpp"
#include "gui/account/AccountHub.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_live/LiveHub.hpp"
#include "gui/task_live/ui/TabBoard.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <cassert>
#include <memory>
#include <utility>

namespace GUI::Tasks {

enum TabIdx {
  TAB_BOARD = 0,
  TAB_COUNT
};

struct TaskLiveState {
  std::shared_ptr<Quote::QuoteHub> quote;       // 与 Database / Market 任务共用
  std::shared_ptr<Account::AccountHub> account; // 与 Market 任务共用
  std::unique_ptr<Live::LiveHub> hub;
  Live::BoardState board;
};

TaskHandle CreateLiveTask(std::shared_ptr<Quote::QuoteHub> quote, std::shared_ptr<Account::AccountHub> account) {
  auto state = std::make_shared<TaskLiveState>();
  state->quote = std::move(quote);
  state->account = std::move(account);

  TaskHandle handle;
  handle.name = "Live";
  handle.storage = state;
  handle.tabs = {"Board"};

  handle.Init = [state](SharedData &) { state->hub = std::make_unique<Live::LiveHub>(); };

  handle.Update = [state](SharedData &) {
    Live::LiveHub &h = *state->hub;
    h.peek();
    h.auto_pull();
    if (!h.connected() || !h.state()->acct)
      return;
    Account::AccountHub &a = *state->account;
    a.feed(h.state()->acct, h.state()->fills);
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
    if (!g.step.empty())
      return {TaskStatus::Kind::Busy, "refreshing"};
    if (!g.err.empty())
      return {TaskStatus::Kind::Error, "error"};
    if (t && t->st == Live::JobSt::Err)
      return {TaskStatus::Kind::Error, "failed"};
    return {TaskStatus::Kind::Muted, "live"};
  };

  handle.Draw = [state](SharedData &data, [[maybe_unused]] int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    Live::LiveHub &h = *state->hub;
    if (data.task_state.database.asset_info_ready)
      state->quote->frame(data);
    h.tick(data.asset_info, *state->quote);
    ImGui::BeginChild("LiveTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    Live::RenderTabBoard(data, h, *state->quote, state->board);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->board = Live::BoardState{};
    state->hub.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
