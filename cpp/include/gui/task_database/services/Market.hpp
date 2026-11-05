// 新浪当日行情 + 篮子历史: Assets (个股) / Portfolio (指数 / 行业) 共用一份, 两任务各持一个 shared_ptr (CreateAllTasks 建).
//   首次用到才起 (之前不碰网络); 篮子历史读 quote 的码表并向它 bind 成员表 ⇒ quote 先建, basket 先拆 (成员序即析构序)
#pragma once

#include "gui/task_database/services/BasketHistory.hpp"
#include "gui/task_database/services/QuoteService.hpp"

#include <memory>

struct SharedData;

namespace GUI::Database {

struct Market {
  std::unique_ptr<QuoteService> quote;
  std::unique_ptr<BasketHistory> basket;
  // AssetInfo 已建 (taskstate.database.all_json_ready) 后调; 首次起两个服务, 全市场 = 在市股
  void start(const SharedData &data);
};

} // namespace GUI::Database
