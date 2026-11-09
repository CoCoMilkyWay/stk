// Task Market - 行情面: 个股 (ASSET) 与篮子 / 组合的曲线、估值、资金流、单标的回测 (NAV · VAL · FLOW · CTA).
//   ASSET: 资产表 + 右栏 Quotes / Statistics (见 TabAssets.hpp), AssetInfo 建好且扫描过才可进.
//   NAV / VAL / FLOW: 篮子账 (all 标签库建好即算) + 因子 / 策略两组 (Alpha → Factors 页编辑的组合, 无组合时只少这两组),
//     走 AlphaHub 的 PortfolioService (见 gui/task_alpha/AlphaHub.hpp); 日内栏 / 近况列取 MarketHub 行情 (新浪当日 + 篮子历史 + 叠加的成分股), AssetInfo 建好才起.
//   CTA: 单标的回测走自己的 CtaService (首次进页建; 价格直接由 all.bar 库合成, 不经 MarketHub).
#include "gui/task_market/TaskMarket.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_alpha/AlphaHub.hpp"
#include "gui/task_market/services/CtaService.hpp"
#include "gui/task_market/services/MarketHub.hpp"
#include "gui/task_market/ui/QuotesPanel.hpp" // Sync
#include "gui/task_market/ui/TabAssets.hpp"
#include "shared/Basket.hpp"     // basket_count (篮子键集)
#include "shared/SharedData.hpp" // config, asset, assetinfo, taskstate.database

#include "imgui.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

namespace GUI::Tasks {

enum TabIdx {
  TAB_ASSET = 0,
  TAB_NAV,
  TAB_VAL,
  TAB_FLOW,
  TAB_CTA,
  TAB_COUNT
};
static_assert(TAB_COUNT - TAB_NAV == Market::kPages);

struct TaskMarketState {
  std::shared_ptr<Market::MarketHub> market; // 与 Database 任务共用 (首次画本任务才起)
  std::shared_ptr<Alpha::AlphaHub> alpha;
  std::unique_ptr<Market::CtaService> cta_service;
  Market::AssetsState assets_state;
};

TaskHandle CreateMarketTask(std::shared_ptr<Market::MarketHub> market, std::shared_ptr<Alpha::AlphaHub> alpha) {
  auto state = std::make_shared<TaskMarketState>();
  state->market = std::move(market);
  state->alpha = std::move(alpha);

  TaskHandle handle;
  handle.name = "Market";
  handle.storage = state;
  handle.tabs = {"ASSET", "NAV", "VAL", "FLOW", "CTA"};

  handle.Enabled = [](const SharedData &data, int idx) {
    if (idx != TAB_ASSET)
      return true;
    return data.taskstate.database.all_json_ready && data.taskstate.database.binary_scanned;
  };

  handle.Status = [state](const SharedData & /*data*/, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (idx == TAB_ASSET) // 新浪行情起了即常驻 (背景常态, 灰色)
      return state->market->quote ? TaskStatus{TaskStatus::Kind::Muted, "live"} : TaskStatus{};
    if (idx == TAB_CTA)
      return state->cta_service ? ProgressStatus(*state->cta_service) : TaskStatus{};
    const auto &ps = state->alpha->portfolio_service;
    return ps ? ProgressStatus(*ps) : TaskStatus{};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    ImGui::BeginChild("MarketTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    if (idx == TAB_ASSET) {
      state->market->start(data);
      Market::RenderTabAssets(data.asset, data.assetinfo.stocks, state->assets_state, *state->market);
      ImGui::EndChild();
      return;
    }
    Alpha::AlphaHub &f = *state->alpha;
    Alpha::EnsurePortfolio(data, f);
    const Market::MarketHub *mk = nullptr;
    if (data.taskstate.database.all_json_ready) {
      state->market->start(data);
      static const std::vector<uint32_t> lines = [] { // 全部篮子线
        std::vector<uint32_t> k(basket_count());
        std::iota(k.begin(), k.end(), 0u);
        return k;
      }();
      Market::Sync(*state->market, Market::kBasket, lines, f.portfolio_ui.basket.feed);
      Market::Sync(*state->market, Market::kStock, f.portfolio_ui.pick.stocks(Market::kPickMany), f.portfolio_ui.basket.stocks);
      mk = state->market.get();
    }
    if (!state->cta_service)
      state->cta_service = std::make_unique<Market::CtaService>(data.config);
    const int action = Market::RenderMarketPage(idx - TAB_NAV, *f.portfolio_service, *state->cta_service, f.portfolio_ui,
                                                data.config.fixed_impact, mk, data.assetinfo.stocks);
    if (action == -1)
      f.portfolio_service->RequestCancel();
    Alpha::SyncPortfolio(data, f, action == 1);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->cta_service.reset(); // 析构 Stop() join worker
  };

  return handle;
}

} // namespace GUI::Tasks
