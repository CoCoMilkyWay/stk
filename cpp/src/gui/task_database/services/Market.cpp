#include "gui/task_database/services/Market.hpp"

#include "gui/task_database/services/AssetLoader.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

namespace GUI::Database {

void Market::start(const SharedData &data) {
  assert(data.taskstate.database.all_json_ready);
  if (quote)
    return;
  std::vector<uint32_t> ids; // 键 "sh.600000" → 全局轴 (AssetLoader 与基本面同拍 intern 过)
  for (const auto &[key, info] : data.assetinfo.get_stock_info())
    if (!info.out_date) {
      std::string code, exchange;
      AssetLoader::split_stock_key(key, code, exchange);
      const size_t id = asset_axis().find(code + "." + exchange);
      assert(id < asset_axis().size() && "在市股不在 A 轴");
      ids.push_back(static_cast<uint32_t>(id));
    }
  std::sort(ids.begin(), ids.end());
  std::vector<misc::Ymd> trading;
  for (const CalendarDay &d : data.assetinfo.get_stock_days())
    if (d.trading)
      trading.push_back(d.date);
  quote = std::make_unique<QuoteService>(pool_axis(std::move(ids)), data.assetinfo.bar1m_end, std::move(trading));
  stock = std::make_unique<StockHistory>();
  basket = std::make_unique<BasketHistory>(*quote);
}

} // namespace GUI::Database
