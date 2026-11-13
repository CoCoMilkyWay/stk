// 行情: 新浪当日 + 历史价格库 + 两路历史 (个股 / 篮子), Market 任务各页 (ASSET 个股 / NAV · VAL · FLOW · CTA 指数 / 行业) 与 Live 持仓页共用一份;
//   Database 的 Sync 页只读新浪状态. CreateAllTasks 建, 各任务各持一个 shared_ptr.
//   首次用到才起 (之前不碰网络); 价格库 / 两路历史读写 quote 与 price ⇒ 先建者后拆 (成员序即析构序)
//   两路历史同形 (HistoryRequest → HistoryPub), 按种类分派: request / history
//   一路历史可有多个 Feed (个股: ASSET 选中 / NAV 叠加的成分股): 请求代各路一个计数, 发布代新于某 Feed 的末次请求 = 它被覆盖了
#pragma once

#include "gui/quote/PriceLib.hpp"
#include "gui/quote/services/History.hpp"
#include "gui/quote/services/QuoteService.hpp"

#include <memory>
#include <utility>

struct SharedData;

namespace GUI::Quote {

struct QuoteHub {
  std::unique_ptr<QuoteService> quote;
  std::unique_ptr<PriceLib> price; // 两路历史 + Market 的分钟线 / CTA 同一份
  PerSide<std::unique_ptr<History>> hist;
  PerSide<uint32_t> gen = {}; // 末次请求代 (GUI 线程)
  // AssetInfo 已建 (task_state.database.asset_info_ready) 后调; 首次起全部服务, 全市场 = 在市股
  void start(const SharedData &data);

  void request(Side s, HistoryRequest r) { hist[s]->Request(std::move(r)); }
  const HistoryPub &history(Side s) const { return hist[s]->published(); }
};

} // namespace GUI::Quote
