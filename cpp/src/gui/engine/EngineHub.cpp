#include "gui/engine/EngineHub.hpp"

#include "shared/SharedData.hpp"

#include <utility>

namespace GUI::Engine {

void EngineHub::reset() { *this = EngineHub{}; }

void EnsurePortfolio(const SharedData &data, EngineHub &e) {
  if (!e.portfolio_service)
    e.portfolio_service = std::make_unique<PortfolioService>();
  SyncPortfolioDir(e.portfolio_ui, data.config.portfolio_dir);
}

void SyncPortfolio(const SharedData &data, EngineHub &e, bool reload) {
  PortfolioUIState &pui = e.portfolio_ui;
  if (pui.content_gen != e.portfolio_gen || e.files_gen != e.portfolio_files) {
    e.portfolio_gen = pui.content_gen;
    e.portfolio_files = e.files_gen;
    ResolvePortfolio(data, e.tables, pui.entries, pui.items);
    ++pui.items_gen;
    e.portfolio_dirty = true;
  }
  if ((e.portfolio_dirty || reload) && !data.asset.items.empty()) {
    e.portfolio_dirty = false;
    PortfolioRequest req;
    MakePortfolioRequest(data, e.tables, pui.items, reload, e.gpu(), req);
    ++pui.items_gen; // 作用域建不成的条目在这里标失效
    std::string sig = req.sig();
    if (reload || sig != e.portfolio_sig) {
      e.portfolio_sig = std::move(sig);
      e.portfolio_service->Request(std::move(req));
    }
  }
}

} // namespace GUI::Engine
