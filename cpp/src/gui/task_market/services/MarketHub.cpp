#include "gui/task_market/services/MarketHub.hpp"

#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <vector>

namespace GUI::Market {

void MarketHub::start(const SharedData &data) {
  assert(data.taskstate.database.all_json_ready);
  if (quote)
    return;
  std::vector<uint32_t> ids; // 在市股的全局 id (升序)
  const StockInfos &stocks = data.assetinfo.stocks;
  for (uint32_t id = 0; id < stocks.size(); ++id)
    if (stocks[id].known && !stocks[id].out_date)
      ids.push_back(id);
  std::vector<misc::Ymd> trading;
  for (const CalendarDay &d : data.assetinfo.days)
    if (d.trading)
      trading.push_back(d.date);
  quote = std::make_unique<QuoteService>(pool_axis(std::move(ids)), data.assetinfo.bar1m_end, std::move(trading));
  stock = std::make_unique<StockHistory>();
  basket = std::make_unique<BasketHistory>(data.config, *quote);
}

} // namespace GUI::Market