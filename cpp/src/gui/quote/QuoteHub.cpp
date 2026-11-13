#include "gui/quote/QuoteHub.hpp"

#include "fundamental/AssetAxis.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <vector>

namespace GUI::Quote {

void QuoteHub::frame(const SharedData &data) {
  assert(data.task_state.database.asset_info_ready);
  if (frame_ == ImGui::GetFrameCount())
    return;
  frame_ = ImGui::GetFrameCount();
  const misc::Ymd mark = data.asset_info.quote_end;
  if (!quote_) {
    std::vector<uint32_t> ids; // 在市股的全局 id (升序)
    const StockInfos &stocks = data.asset_info.stocks;
    for (uint32_t id = 0; id < stocks.size(); ++id)
      if (stocks[id].known && !stocks[id].out_date)
        ids.push_back(id);
    std::vector<misc::Ymd> trading;
    for (const CalendarDay &d : data.asset_info.days)
      if (d.trading)
        trading.push_back(d.date);
    quote_ = std::make_unique<QuoteService>(std::make_shared<const PoolAxis>(pool_axis(std::move(ids))), mark, trading);
    price_ = std::make_unique<PriceLib>(std::move(trading), [q = quote_.get()](const Handoff &h) { q->bind(h); });
    mark_ = mark;
  }
  if (mark != mark_) {
    mark_ = mark;
    quote_->set_fund_end(mark);
  }
  status = quote_->status();
  live = quote_->live();
  std::ranges::sort(want_.stocks);
  want_.stocks.erase(std::ranges::unique(want_.stocks).begin(), want_.stocks.end());
  std::ranges::sort(want_.minutes, {}, [](Ref r) { return std::pair(r.side, r.key); });
  want_.minutes.erase(std::ranges::unique(want_.minutes).begin(), want_.minutes.end());
  want_.mark = mark;
  want_.live = status.date;
  if (!sent_ || !(*sent_ == want_)) {
    sent_ = want_;
    price_->Request(want_);
  }
  want_.stocks.clear(), want_.minutes.clear();
  view = latest(*price_);
}

void QuoteHub::want_kline(uint32_t id) { want_.stocks.push_back(id); }

void QuoteHub::want_minutes(Ref r) {
  assert(r.ok());
  want_.minutes.push_back(r);
}

} // namespace GUI::Quote
