// Task Live - 实盘: 券商客户端的持仓、下单与一键调仓 (中枢 LiveHub 券商无关; 客户端适配在 live/<客户端>/, 登记在 live/Clients.hpp)
//   未连接: 两页同一个连接面板 (客户端适配表一行一个按钮)
//   Board: 持仓 · 调仓一页 (控件分组 / 持仓表行内闪电下单与卖出滑杆, 并调仓名单 / 日志) + 右栏选中行的行情图
//     (Market 的个股面板, 与 Database / Market 共用 QuoteHub)
//   Client: 客户端窗口复现 (窗口树 + GDI 绘制层 + 控件子项, 检查器 / 鼠键透传; 只经 live::Mirror); 有事务在跑即停拉、停透传
//   行情走新浪 (LiveHub 把要的码订阅给 QuoteHub, 本地资产库就绪才有), 不读客户端的行情; Board 可见时才盯客户端 (见 live/Broker.hpp 的 touch)
//   账户净值 (gui/account): 每帧交快照 (行情经 AccountHub 取其当日与库外持仓的现价); 两融条款与资金流水由 LiveHub 每天自动拉, 也可人点
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

namespace {

// 未连接: 客户端适配表一行一个连接按钮 (两页同一处)
void connect_panel(Live::LiveHub &h) {
  ImGui::TextUnformatted("接券商客户端:");
  for (size_t i = 0; i < live::clients().size(); ++i) {
    const live::Client &c = live::clients()[i];
    if (ImGui::Button(live::strf("连接%s", c.name).c_str()))
      h.connect(i);
    ImGui::SameLine();
    ImGui::TextWrapped("%s", c.about);
  }
}

} // namespace

struct TaskLiveState {
  std::shared_ptr<Quote::QuoteHub> quote;       // 与 Database / Market 任务共用
  std::shared_ptr<Account::AccountHub> account; // 与 Market 任务共用
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
    state->hub->update();
    state->account->feed(state->hub->snapshot());
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
    }
    if (!h.connected())
      connect_panel(h);
    else if (idx == TAB_BOARD)
      Live::RenderTabBoard(data, h, *state->quote, state->board);
    else
      Live::RenderTabClient(h, state->client);
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
