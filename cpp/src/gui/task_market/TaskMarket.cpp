// Task Market - 行情面: 个股 (ASSET) 与篮子 / 组合的曲线、估值、资金流、单标的回测 (NAV · VAL · FLOW · CTA).
//   ASSET: 资产表 + 右栏 Quotes / Statistics (见 TabAsset.hpp), AssetInfo 建好且扫描过才可进.
//   NAV / VAL / FLOW: 篮子账 (all 标签库建好即算) + 因子 / 策略两组 (Alpha → Factors 页编辑的组合, 无组合时只少这两组),
//     走 EngineHub 的 PortfolioService (见 gui/engine/EngineHub.hpp); 日内栏 / 近况列 / 分钟线取 QuoteHub 行情 (新浪当日 + 价格库), AssetInfo 建好才起.
//   CTA 单标的回测 = CtaService (读 QuoteHub 的价格库; 行情起了, 首次画行情页建).
//   自选首行 = 账户 (行情的账户一侧: 价格库回放 + gui/account 拼的当日; 悬停 / 持仓取自 gui/account).
#include "gui/task_market/TaskMarket.hpp"
#include "gui/Tasks.hpp"
#include "gui/account/AccountHub.hpp"
#include "gui/engine/EngineHub.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_market/services/CtaService.hpp"
#include "gui/task_market/ui/LinesCommon.hpp" // kPageDefs (页名 / 状态的 worker)
#include "gui/task_market/ui/TabAsset.hpp"
#include "gui/task_market/ui/TabLines.hpp"
#include "gui/util/PoolTip.hpp"  // move_pool (右表指数组拖动)
#include "shared/SharedData.hpp" // config, asset, asset_info, task_state.database

#include "imgui.h"

#include <cassert>
#include <memory>
#include <optional>
#include <utility>

namespace GUI::Tasks {

// 左栏子行: ASSET + 行情页 (idx = kTabLines + LinesPage)
constexpr int kTabAsset = 0, kTabLines = 1, kTabs = kTabLines + Market::kPages;

struct TaskMarketState {
  std::shared_ptr<Quote::QuoteHub> quote;          // 与 Database 任务共用 (首次画本任务才起)
  std::shared_ptr<Engine::EngineHub> engine;       // 与 Alpha 任务共用
  std::shared_ptr<Account::AccountHub> account;    // 与 Live 任务共用
  std::unique_ptr<Market::CtaService> cta_service; // 读 quote 的价格库 (先于它析构)
  Market::AssetState asset_state;
  Market::LinesState lines_state; // 缓存键含 portfolio_service 的结果代 / portfolio_ui 的解析代
};

TaskHandle CreateMarketTask(std::shared_ptr<Quote::QuoteHub> quote, std::shared_ptr<Engine::EngineHub> engine,
                            std::shared_ptr<Account::AccountHub> account) {
  auto state = std::make_shared<TaskMarketState>();
  state->quote = std::move(quote);
  state->engine = std::move(engine);
  state->account = std::move(account);

  TaskHandle handle;
  handle.name = "Market";
  handle.storage = state;
  handle.tabs = {"ASSET"};
  for (const Market::PageDef &p : Market::kPageDefs)
    handle.tabs.emplace_back(p.name);

  handle.Enabled = [](const SharedData &data, int idx) {
    if (idx != kTabAsset)
      return true;
    return data.task_state.database.asset_info_ready && data.task_state.database.scanned;
  };

  handle.Status = [state](const SharedData & /*data*/, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < kTabs);
    if (idx == kTabAsset) // 新浪行情起了即常驻 (背景常态, 灰色)
      return state->quote->started() ? TaskStatus{TaskStatus::Kind::Muted, "live"} : TaskStatus{};
    const auto of = [&](Market::PageWork w) -> const Progress * {
      switch (w) {
      case Market::kWorkPortfolio:
        return state->engine->portfolio_service.get();
      case Market::kWorkPrice:
        return state->quote->started() ? &state->quote->progress() : nullptr;
      case Market::kWorkCta:
        return state->cta_service.get();
      }
      assert(false && "未知 worker");
      return nullptr;
    };
    const Market::PageDef &p = Market::kPageDefs[idx - kTabLines];
    const Progress *w = of(p.work);
    const Progress *s = w && w->busy() ? w : of(p.rest);
    return s ? ProgressStatus(*s) : TaskStatus{};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < kTabs);
    ImGui::BeginChild("MarketTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == kTabAsset) {
      state->quote->frame(data);
      Market::RenderTabAsset(data.asset, data.asset_info.stocks, state->asset_state, *state->quote);
      ImGui::EndChild();
      return;
    }
    Engine::EngineHub &e = *state->engine;
    Market::LinesState &ui = state->lines_state;
    Engine::EnsurePortfolio(e);
    Market::SyncLinesWatch(ui);
    Quote::QuoteHub *mk = nullptr;
    if (data.task_state.database.asset_info_ready) {
      state->quote->frame(data);
      mk = state->quote.get();
      if (!state->cta_service)
        state->cta_service = std::make_unique<Market::CtaService>();
    }
    const int action = Market::RenderTabLines(idx - kTabLines, *e.portfolio_service, state->cta_service.get(), e.portfolio_ui, ui, data.config.fixed_impact,
                                              {data.config.commission, data.config.stamp, data.config.fixed_impact}, mk, data.asset_info.stocks,
                                              state->account->note(), state->account->holdings());
    if (action == -1)
      e.portfolio_service->RequestCancel();
    Engine::SyncPortfolio(data, e, action == 1);
    if (const std::optional<Move> m = std::exchange(ui.pool_move, std::nullopt))
      move_pool(data, *m);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->cta_service.reset(); // 析构 Stop() join worker
    state->lines_state = Market::LinesState{};
  };

  return handle;
}

} // namespace GUI::Tasks
