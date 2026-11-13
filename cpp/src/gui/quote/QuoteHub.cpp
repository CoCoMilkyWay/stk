#include "gui/quote/QuoteHub.hpp"

#include "fundamental/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <vector>

namespace GUI::Quote {

void QuoteHub::frame(const SharedData &data) {
  assert(data.task_state.database.asset_info_ready);
  if (!quote) {
    std::vector<uint32_t> ids; // 在市股的全局 id (升序)
    const StockInfos &stocks = data.asset_info.stocks;
    for (uint32_t id = 0; id < stocks.size(); ++id)
      if (stocks[id].known && !stocks[id].out_date)
        ids.push_back(id);
    std::vector<misc::Ymd> trading;
    for (const CalendarDay &d : data.asset_info.days)
      if (d.trading)
        trading.push_back(d.date);
    quote = std::make_unique<QuoteService>(pool_axis(std::move(ids)), data.asset_info.bar1m_end, trading);
    price = std::make_unique<PriceLib>(std::move(trading), [q = quote.get()](const Handoff &h) { q->bind(h); });
    for (const Side s : {kStock, kBasket})
      hist[s] = std::make_unique<History>(s, *price);
  }
  status = quote->status();
}

} // namespace GUI::Quote
