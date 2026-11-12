// Task Market - 行情面: 个股 (ASSET) 与篮子 / 组合的曲线、估值、资金流、单标的回测 (NAV · VAL · FLOW · CTA).
//   ASSET: 资产表 + 右栏 Quotes / Statistics (见 TabAsset.hpp), AssetInfo 建好且扫描过才可进.
//   NAV / VAL / FLOW: 篮子账 (all 标签库建好即算) + 因子 / 策略两组 (Alpha → Factors 页编辑的组合, 无组合时只少这两组),
//     走 EngineHub 的 PortfolioService (见 gui/engine/EngineHub.hpp); 日内栏 / 近况列取 QuoteHub 行情 (新浪当日 + 篮子历史 + 叠加的成分股), AssetInfo 建好才起.
//   分钟价格库 MinuteLib (本任务持有, 由 all.bar 库合成, 不经 QuoteHub) 两个用处各一个 worker (首次画行情页建):
//     NAV 主图的分钟线 = MinuteService, CTA 单标的回测 = CtaService.
#include "gui/task_market/TaskMarket.hpp"
#include "gui/Tasks.hpp"
#include "gui/engine/EngineHub.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_market/services/CtaService.hpp"
#include "gui/task_market/services/MinuteService.hpp"
#include "gui/task_market/ui/LinesCommon.hpp" // kPageDefs (页名)
#include "gui/task_market/ui/QuotesPanel.hpp" // Sync
#include "gui/task_market/ui/TabAsset.hpp"
#include "gui/task_market/ui/TabLines.hpp"
#include "gui/util/PoolTip.hpp"  // move_pool (右表指数组拖动)
#include "shared/Basket.hpp"     // basket_table (篮子键集)
#include "shared/SharedData.hpp" // config, asset, asset_info, task_state.database

#include "imgui.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>
#include <vector>

namespace GUI::Tasks {

// 左栏子行: ASSET + 行情页 (idx = kTabLines + LinesPage)
constexpr int kTabAsset = 0, kTabLines = 1, kTabs = kTabLines + Market::kPages;

struct TaskMarketState {
  std::shared_ptr<Quote::QuoteHub> quote;    // 与 Database 任务共用 (首次画本任务才起)
  std::shared_ptr<Engine::EngineHub> engine; // 与 Alpha 任务共用
  Market::MinuteLib minute_lib;              // 两个 worker 共用 (先于它们构造, 后于它们析构)
  std::unique_ptr<Market::MinuteService> minute_service;
  std::unique_ptr<Market::CtaService> cta_service;
  Market::AssetState asset_state;
  Market::LinesUIState lines_ui; // 缓存键含 portfolio_service 的结果代 / portfolio_ui 的解析代
  std::vector<uint32_t> lines;   // 全部篮子线 (篮子表随进程代变, 本状态每代新建)
};

TaskHandle CreateMarketTask(std::shared_ptr<Quote::QuoteHub> quote, std::shared_ptr<Engine::EngineHub> engine) {
  auto state = std::make_shared<TaskMarketState>();
  state->quote = std::move(quote);
  state->engine = std::move(engine);

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
      return state->quote->quote ? TaskStatus{TaskStatus::Kind::Muted, "live"} : TaskStatus{};
    if (idx - kTabLines == Market::kPageCta)
      return state->cta_service ? ProgressStatus(*state->cta_service) : TaskStatus{};
    if (idx - kTabLines == Market::kPageNav && state->minute_service && state->minute_service->busy()) // 分钟价格库合成中
      return ProgressStatus(*state->minute_service);
    const auto &ps = state->engine->portfolio_service;
    return ps ? ProgressStatus(*ps) : TaskStatus{};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < kTabs);
    ImGui::BeginChild("MarketTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == kTabAsset) {
      state->quote->start(data);
      Market::RenderTabAsset(data.asset, data.asset_info.stocks, state->asset_state, *state->quote);
      ImGui::EndChild();
      return;
    }
    Engine::EngineHub &e = *state->engine;
    Market::LinesUIState &ui = state->lines_ui;
    Engine::EnsurePortfolio(e);
    Market::SyncLinesWatch(ui);
    const Quote::QuoteHub *mk = nullptr;
    if (data.task_state.database.asset_info_ready) {
      state->quote->start(data);
      if (state->lines.empty()) {
        state->lines.resize(basket_table().size());
        std::iota(state->lines.begin(), state->lines.end(), 0u);
      }
      Market::Sync(*state->quote, Quote::kBasket, state->lines, ui.overlay.baskets);
      Market::Sync(*state->quote, Quote::kStock, ui.pick.stocks(Market::kPickMany), ui.overlay.stocks);
      mk = state->quote.get();
    }
    if (!state->cta_service) {
      state->minute_service = std::make_unique<Market::MinuteService>(state->minute_lib);
      state->cta_service = std::make_unique<Market::CtaService>(state->minute_lib);
    }
    const int action = Market::RenderTabLines(idx - kTabLines, *e.portfolio_service, *state->minute_service, *state->cta_service, e.portfolio_ui,
                                              ui, data.config.fixed_impact, mk, data.asset_info.stocks);
    if (action == -1)
      e.portfolio_service->RequestCancel();
    Engine::SyncPortfolio(data, e, action == 1);
    if (const std::optional<Move> m = std::exchange(ui.pool_move, std::nullopt))
      move_pool(data, *m);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->cta_service.reset(); // 析构 Stop() join worker
    state->minute_service.reset();
    state->lines_ui = Market::LinesUIState{};
  };

  return handle;
}

} // namespace GUI::Tasks
