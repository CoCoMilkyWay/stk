#include "gui/quote/QuoteHub.hpp"

#include "fundamental/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <vector>

namespace GUI::Quote {

void QuoteHub::start(const SharedData &data) {
  assert(data.task_state.database.asset_info_ready);
  if (quote)
    return;
  std::vector<uint32_t> ids; // 在市股的全局 id (升序)
  const StockInfos &stocks = data.asset_info.stocks;
  for (uint32_t id = 0; id < stocks.size(); ++id)
    if (stocks[id].known && !stocks[id].out_date)
      ids.push_back(id);
  std::vector<misc::Ymd> trading;
  for (const CalendarDay &d : data.asset_info.days)
    if (d.trading)
      trading.push_back(d.date);
  quote = std::make_unique<QuoteService>(pool_axis(std::move(ids)), data.asset_info.bar1m_end, std::move(trading));
  stock = std::make_unique<StockHistory>();
  basket = std::make_unique<BasketHistory>(data.config, *quote);
}

} // namespace GUI::Quote