// Task Live - 实盘: 券商客户端的持仓、下单与一键调仓 (中枢 LiveHub 券商无关; 客户端适配在 live/<客户端>/, 现只有通达信)
//   Board: 连接 + 持仓表 (行内闪电下单) + 右栏选中持仓的行情图 (Market 的个股面板, 与 Database / Market 共用 QuoteHub)
//   Rebal: 名单 / 配置 / 试算 / 执行调仓 / 日志
//   行情走新浪 (LiveHub 一路), 不读客户端的行情; 页面可见时才盯客户端 (见 live/Broker.hpp 的 touch)
#include "gui/task_live/TaskLive.hpp"
#include "gui/Tasks.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_live/LiveHub.hpp"
#include "gui/task_live/ui/TabBoard.hpp"
#include "gui/task_live/ui/TabRebal.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <cassert>
#include <memory>
#include <utility>

namespace GUI::Tasks {

enum TabIdx {
  TAB_BOARD = 0,
  TAB_REBAL,
  TAB_COUNT
};

struct TaskLiveState {
  std::shared_ptr<Quote::QuoteHub> quote; // 与 Database / Market 任务共用
  std::unique_ptr<Live::LiveHub> hub;
  Live::BoardState board;
  Live::RebalState rebal;
};

TaskHandle CreateLiveTask(std::shared_ptr<Quote::QuoteHub> quote) {
  auto state = std::make_shared<TaskLiveState>();
  state->quote = std::move(quote);

  TaskHandle handle;
  handle.name = "Live";
  handle.storage = state;
  handle.tabs = {"Board", "Rebal"};

  handle.Init = [state](SharedData &) { state->hub = std::make_unique<Live::LiveHub>(); };

  handle.Update = [state](SharedData &) { state->hub->peek(); };

  handle.Status = [state](const SharedData &, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    const Live::LiveHub &h = *state->hub;
    if (!h.connected())
      return {};
    if (idx == TAB_BOARD) {
      const live::Refresh &g = h.refresh();
      if (!g.down.empty())
        return {TaskStatus::Kind::Error, "down"};
      if (h.ordering())
        return {TaskStatus::Kind::Busy, "ordering"};
      if (!g.step.empty())
        return {TaskStatus::Kind::Busy, "refreshing"};
      if (!g.err.empty())
        return {TaskStatus::Kind::Error, "error"};
      return {TaskStatus::Kind::Muted, "live"};
    }
    const auto &t = h.task();
    if (!t)
      return {};
    if (t->st == Live::JobSt::Run)
      return {TaskStatus::Kind::Busy, "running"};
    if (t->st == Live::JobSt::Err)
      return {TaskStatus::Kind::Error, "failed"};
    return {};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    Live::LiveHub &h = *state->hub;
    h.tick();
    ImGui::BeginChild("LiveTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == TAB_BOARD)
      Live::RenderTabBoard(data, h, *state->quote, state->board);
    else
      Live::RenderTabRebal(h, state->rebal);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->board = Live::BoardState{};
    state->hub.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
