// Assets 右栏行情: 个股 / 篮子同一套两图 (日内 / 日线, 布局与配色同 py/app/tdx 行情页), 数据 = 历史 Kline + 当日 Day (见 Kline.hpp)
//   个股 = StockHistory + QuoteService::today(i), 另有盘口 (当日最新快照五档);
//   篮子 = BasketHistory + QuoteService::today(Members), 左栏另有篮子列表, 量柱 = 成交额.
//   两图各一份视野: 换标的 / 换档 / 根数变 → 复位到该档窗口, 其间可缩放平移; y 随视野内的根贴合
// 每帧先 Sync (发请求 / 收发布 / 拷出或合成当日), 再画
#pragma once

#include "gui/task_database/services/BasketHistory.hpp"
#include "gui/task_database/services/QuoteService.hpp"
#include "gui/task_database/services/StockHistory.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

struct Config;

namespace GUI::Database {

struct QuotesView {
  double lo = 0, hi = 0;      // 上帧 x 视野
  uint64_t key = 0;           // 复位键 (代 / 档 / 根数)
  float rows[2] = {3.f, 1.f}; // 价 / 量 两格的高比
};

// 一个标的的两张图: 档位 + 视野
struct ChartState {
  int intraday = 0;  // 日内档 (日内 / 2日 / 3日 / 4日)
  int daily = 1;     // 日线档 (周 / 月 / 年)
  QuotesView iv, dv; // 日内 / 日线 视野
};

struct StockState {
  std::string code;                  // 已请求的标的 ("600000.SH"); 空 = 未选
  uint32_t i = ~0u;                  // 全市场下标; ~0u = 不在新浪全市场
  uint32_t gen = 0;                  // StockHistory 请求代
  std::shared_ptr<const Kline> data; // 已收的发布
  Day today;                         // 逐帧拷出的当日
  sina::Quote quote;                 // 逐帧拷出的最新快照
  ChartState chart;
};

struct BasketState {
  bool want = true;                                        // 待发请求 (首次进页 / 重载)
  bool missing = false;                                    // all Bar 库未建
  uint32_t gen = 0;                                        // BasketHistory 请求代
  std::shared_ptr<const Baskets> data;                     // 已收的发布
  std::vector<Day> today;                                  // [篮子] 当日合成
  std::tuple<misc::Ymd, int, const Baskets *> today_key{}; // 合成键 (行情日 / 快照时刻 / 发布)
  int sel[2] = {-1, -1};                                   // 指数 / 行业 页选中的篮子 (Baskets::list 下标)
  ChartState chart;
};

void SyncStock(QuoteService &live, StockHistory &hist, const AssetItem *asset, StockState &st);
void SyncBaskets(QuoteService &live, BasketHistory &hist, const Config &cfg, BasketState &st);

void RenderStockPanel(const QuoteService &live, const StockInfo *info, StockState &st);
void RenderBasketPanel(BasketState &st, int page);                           // page 0 = 指数, 1 = 行业
bool RenderBasketList(const BasketHistory &hist, BasketState &st, int page); // 点中一行 → true

} // namespace GUI::Database
