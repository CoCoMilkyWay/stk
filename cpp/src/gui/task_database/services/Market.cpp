#include "gui/task_database/services/Market.hpp"

#include "shared/SharedData.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace GUI::Database {

void Market::start(const SharedData &data) {
  assert(data.taskstate.database.all_json_ready);
  if (quote)
    return;
  std::vector<std::string> syms; // 键 "sh.600000" → 新浪 "sh600000"
  for (const auto &[key, info] : data.assetinfo.get_stock_info())
    if (!info.out_date) {
      const size_t dot = key.find('.');
      assert(dot != std::string::npos);
      syms.push_back(key.substr(0, dot) + key.substr(dot + 1));
    }
  std::vector<misc::Ymd> trading;
  for (const CalendarDay &d : data.assetinfo.get_stock_days())
    if (d.trading)
      trading.push_back(d.date);
  quote = std::make_unique<QuoteService>(std::move(syms), data.assetinfo.bar1m_end, std::move(trading));
  basket = std::make_unique<BasketHistory>(*quote);
}

} // namespace GUI::Database
