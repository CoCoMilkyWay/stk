// 行情图, 数据 = 历史 Kline + 当日 Day (见 Kline.hpp)
//   个股 = Assets 右栏: StockHistory + QuoteService::today(id), 日内 (右挂盘口: 当日最新快照五档) + 日线 (布局与配色同 py/app/tdx 行情页);
//   篮子 = Portfolio NAV 日内栏: BasketHistory + QuoteService::today(篮子), 多篮子叠加收盘线 (对首日前收的涨跌幅, 刻度区全空: 横网格涨跌幅小字内标在右壁; 不画量, 读数看十字光标);
//     另给 NAV 表的近况三列 (Recent). 以下为个股图:
//   日内价 = 高 / 低单调插值围出的带, 按涨跌染色; 日线价 = 蜡烛 + 均线, 默认 10 年档; 悬停提示只在量图
//     十字光标随鼠标: 竖线吸附最近一根 (底轴标时刻), 横线在鼠标所在格 (价图左价 / 右涨跌幅, 量图标量 / 额)
//   两图各一份视野: 换标的 / 换档 / 根数变 / 双击 → 复位到该档窗口; 左键拖动 = 框选 x 段放大; y 恒随视野内的根贴合
//     日线滚轮 = 只缩放 x, 右端恒锚在最新一根; 日内不滚. 现价线左标价 / 右标当日涨跌幅, 日内另有前收线 (左价 / 右 0%)
//   版面恒定: 标题 / 注恒两行, 各轴刻度区按最大标签框定尺 (右轴无基准也留, 标签随出随没不撑), 图框只随窗口变; 换标的 = 新历史到齐才整体换上, 其间照画旧的
// 每帧先 Sync (发请求 / 收发布 / 当日版本变了才拷), 再画; 个股 / 篮子同一个 Feed + pull
#pragma once

#include "gui/task_database/services/Market.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"

#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

struct Config;

namespace GUI::Database {

// 一张图的 x 视野 (唯一真相: 每帧以 Always 设给 ImPlot, 其自带的拖动 / 滚轮 / 双击全失效, 交互只改这里)
struct QuotesView {
  double lo = 0, hi = 0;                                  // x 视野
  uint64_t key = 0;                                       // 复位键 (代 / 档 / 根数)
  float rows[2] = {3.f, 1.f};                             // 价 / 量 两格的高比
  double drag = std::numeric_limits<double>::quiet_NaN(); // 框选起点 x; NaN = 未在框选
  float drag_px = 0;                                      // 框选起点屏幕 x
  bool cross = false;                                     // 上帧鼠标在价 / 量任一格 (十字竖线两格同画)
};

// 一个标的的两张图: 档位 + 视野
struct ChartState {
  int intraday = 0;  // 日内档 (日内 / 2日 / 3日 / 4日)
  int daily = 3;     // 日线档 (周 / 月 / 年 / 10年)
  QuotesView iv, dv; // 日内 / 日线 视野
};

// 历史发布 + 当日版本 (个股 / 篮子同形)
template <class T>
struct Feed {
  uint32_t gen = 0;              // 末次历史请求代
  uint32_t shown = 0;            // 在画的发布的请求代 (视野复位键)
  std::shared_ptr<const T> data; // 在画的历史发布 (末次请求的到齐才换)
  uint64_t ver = ~0ull;          // 已拷的当日版本 (QuoteStatus); ~0 = 待拷
};

struct StockPick {
  std::string code;  // "600000.SH"; 空 = 未选
  std::string title; // 简称 + 代码
  uint32_t id = ~0u; // 全局 id
};

struct StockState {
  StockPick want;    // 末次请求的标的
  StockPick shown;   // 在画的标的 (= feed.data 的标的)
  Feed<Kline> feed;  // StockHistory
  Day today;         // shown 的当日
  sina::Quote quote; // shown 的最新快照
  ChartState chart;
};

struct BasketState {
  bool want = true;       // 待发请求 (首次进页 / 重载)
  bool missing = false;   // all Bar 库未建
  Feed<Baskets> feed;     // BasketHistory
  std::vector<Day> today; // [篮子] 当日
  int intraday = 0;       // 日内档 (日内 / 2日 / 3日 / 4日)
};

// 日内叠加的一条: 篮子 (Baskets::list 下标 = basket_line) + 线色 (ImU32)
struct BasketPick {
  size_t b;
  uint32_t col;
};

// 近况: 最新一根对前收 / 最新收对 MA5 (含最新一根) / 最新一根额对此前 5 根均额, 各 − 1; 不可算 = NaN
struct Recent {
  double chg = std::numeric_limits<double>::quiet_NaN();
  double ma5 = std::numeric_limits<double>::quiet_NaN();
  double amt5 = std::numeric_limits<double>::quiet_NaN();
};
// 近况的一日: 收 / 前收 / 额 (NaN = 无)
struct Mark {
  double close, ref, amount;
};
// 近况核 (篮子 / 净值线同一份): m = 新 → 旧, 至多用前 6 日
Recent RecentOf(std::span<const Mark> m);

void SyncStock(Market &mk, const AssetItem *asset, const StockInfo *info, StockState &st);
void SyncBaskets(Market &mk, const Config &cfg, BasketState &st);

void RenderStockPanel(const Market &mk, StockState &st);
// picks 各篮子的日内叠加图, 占满当前区域; 库未建 / 读取中 → 提示
void RenderBasketIntraday(const Market &mk, BasketState &st, const std::vector<BasketPick> &picks);
Recent BasketRecent(const BasketState &st, size_t b); // 当日接上取当日, 否则日线末根起; 历史未到 → 全 NaN

} // namespace GUI::Database
