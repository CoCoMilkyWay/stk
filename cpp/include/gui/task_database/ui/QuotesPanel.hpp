// Assets 右栏 Quotes: 选中标的的 日内 (分钟 K) / 盘口 / 日线 (布局与配色同 py/app/tdx 行情页)
//   日内 = 基本面 bar1m 最近几日 + 当日 (QuoteService); 日线 = 基本面 cn_stock_real_bar1d + 当日一根 (由快照拼)
//   盘口 = 当日最新快照五档. 两图各一份视野: 换标的 / 换档 / 根数变 → 复位到该档窗口, 其间可缩放平移; y 随视野内的根贴合
// 指数 / 行业 (IndexService 篮子): 左栏列表 + 右栏 日内 / 日线 (无盘口, 量柱 = 成交额); 库末日之后的行情日由 QuoteService::compose 现合成
#pragma once

#include "gui/task_database/services/IndexService.hpp"
#include "gui/task_database/services/QuoteService.hpp"
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

struct QuotesState {
  int intraday = 0;  // 日内档 (日内 / 2日 / 3日 / 4日)
  int daily = 1;     // 日线档 (周 / 月 / 年)
  std::string code;  // 已请求的标的 ("600000.SH"); 空 = 未选
  uint32_t gen = 0;  // HistoryService 请求代
  QuotesView iv, dv; // 日内 / 日线 视野
  LiveDay live;      // 逐帧拷出的当日 (复用缓冲)
};

struct BasketsState {
  bool want = true;                      // 待发请求 (首次进页 / 重载)
  bool missing = false;                  // all Bar 库未建或不足 3 日
  uint32_t gen = 0;                      // IndexService 请求代
  std::shared_ptr<const IndexData> data; // 已收的发布
  int sel[2] = {-1, -1};                 // 指数 / 行业 页选中的篮子 (IndexData::baskets 下标)
  int intraday = 0, daily = 1;           // 同 QuotesState
  QuotesView iv, dv;
  std::vector<IndexDay> today;                                    // [篮子] 库末日之后那天的合成 (date 0 = 无); 选中篮子连分钟根
  std::tuple<misc::Ymd, int, int, const IndexData *> today_key{}; // 合成键 (行情日 / 快照时刻 / 选中 / 发布)
};

void RenderQuotesPanel(QuoteService &live, HistoryService &hist, const AssetItem *asset, const StockInfo *info,
                       QuotesState &st);

// 每帧先 Sync (发请求 / 收发布 / 刷当日合成), 再画左栏列表与右栏面板; page 0 = 指数, 1 = 行业
void SyncBaskets(QuoteService &live, IndexService &idx, const Config &cfg, BasketsState &st, int page);
bool RenderBasketList(const IndexService &idx, BasketsState &st, int page); // 点中一行 → true
void RenderBasketPanel(BasketsState &st, int page);

} // namespace GUI::Database
