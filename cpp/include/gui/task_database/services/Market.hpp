// 行情三服务: 新浪当日 + 个股历史 + 篮子历史, Assets (个股) / Portfolio (指数 / 行业) 共用一份, 两任务各持一个 shared_ptr (CreateAllTasks 建).
//   首次用到才起 (之前不碰网络); 篮子历史向 quote bind 成员表 ⇒ quote 先建, 历史先拆 (成员序即析构序)
//   两路历史同形 (HistoryRequest → HistoryPub), 按种类分派: request / history
#pragma once

#include "gui/task_database/services/BasketHistory.hpp"
#include "gui/task_database/services/QuoteService.hpp"
#include "gui/task_database/services/StockHistory.hpp"

#include <memory>
#include <utility>

struct SharedData;

namespace GUI::Database {

struct Market {
  std::unique_ptr<QuoteService> quote;
  std::unique_ptr<StockHistory> stock;
  std::unique_ptr<BasketHistory> basket;
  // AssetInfo 已建 (taskstate.database.all_json_ready) 后调; 首次起三个服务, 全市场 = 在市股
  void start(const SharedData &data);

  void request(Side s, HistoryRequest r) { s == kStock ? stock->Request(std::move(r)) : basket->Request(std::move(r)); }
  const HistoryPub &history(Side s) const { return s == kStock ? stock->published() : basket->published(); }
};

} // namespace GUI::Database
