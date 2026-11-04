// Assets 右栏 Quotes: 选中标的的 日内 (分钟 K) / 盘口 / 日线 (布局与配色同 py/app/tdx 行情页)
//   日内 = 基本面 bar1m 最近几日 + 当日 (QuoteService); 日线 = 基本面 cn_stock_real_bar1d + 当日一根 (由快照拼)
//   盘口 = 当日最新快照五档. 两图各一份视野: 换标的 / 换档 / 根数变 → 复位到该档窗口, 其间可缩放平移; y 随视野内的根贴合
#pragma once

#include "gui/task_database/services/QuoteService.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"

#include <cstdint>
#include <string>

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

void RenderQuotesPanel(QuoteService &live, HistoryService &hist, const AssetItem *asset, const StockInfo *info,
                       QuotesState &st);

} // namespace GUI::Database
