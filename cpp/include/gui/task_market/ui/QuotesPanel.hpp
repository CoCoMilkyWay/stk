// 行情图, 数据 = 历史 Kline + 当日 Day (见 Quote.hpp); 个股 / 篮子同一个 Feed + Sync (键集 → 历史发布 + 当日)
//   个股 = ASSET 右栏 (Live 持仓页右栏同一面板): 键集 {选中标的}, 日内 (右挂盘口: 当日最新快照五档) + 日线 (布局与配色同 py/app/tdx 行情页);
//   篮子 = NAV 日内栏: 键集 = 全部篮子线, 叠加图 (对首日前收的涨跌幅, 刻度区全空: 横网格 (含 0 轴) 涨跌幅小字内标在右壁, 线名小字贴线尾;
//     不画量, 读数看十字光标); 多篮子 = 收盘线, 单篮子 = 分钟蜡烛 (窄了按 kSlots 的因子并根, 同 py/app/tdx) + 现价十字 (虚线, 标涨跌幅 / 时刻), 版面不变;
//     FLOW 页 = 同一叠加图, y = 自日轴首日开盘起逐分钟累计的净流入 (Bars::net; 占近 5 日日均成交额 % / 金额 亿元);
//     叠加的条可含个股 (NAV 右表点中的成分股: 个股历史 + 新浪当日, 与篮子同一画法);
//   近况 (Recent): 一个标的的近况两列 (NAV 右表 / ASSET 表). 以下为个股图:
//   日内多日 (个股 / 篮子同一份) 按前收链前复权到末日 (末日真价; 篮子点位本已接续);
//   价 = 实心蜡烛 (实体 = 上一根收 → 本根收, 首根起于前收, 根根首尾相接; 量柱同色), 日内按视野宽并根 (同篮子);
//     日线另叠均线, 默认 10 年档; 悬停提示只在量图
//     十字光标随鼠标: 竖线吸附最近一根 (底轴标时刻), 横线在鼠标所在格 (价图左价 / 右涨跌幅, 量图标量 / 额)
//   两图各一份视野: 换标的 / 换档 / 根数变 / 双击 → 复位到该档窗口; 左键拖动 = 框选 x 段放大; y 恒随视野内的根贴合
//     日线滚轮 = 只缩放 x, 右端恒锚在最新一根; 日内不滚. 现价线左标价 / 右标当日涨跌幅, 日内另有前收线 (左价 / 右 0%)
//   版面恒定: 标题 / 注恒两行, 各轴刻度区按最大标签框定尺 (右轴无基准也留, 标签随出随没不撑), 图框只随窗口变; 换标的 = 新历史到齐才整体换上, 其间照画旧的
// 每帧先 Sync (键集变 → 发请求 / 收发布 / 当日版本变了才拷), 再画
#pragma once

#include "gui/quote/QuoteHub.hpp"
#include "shared/AssetInfo.hpp" // StockInfo (个股近况)

#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace GUI::Market {

using namespace Quote;

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
  int intraday = 0;  // 日内档 (日内 / 2日 … 5日)
  int daily = 3;     // 日线档 (周 / 月 / 年 / 10年)
  QuotesView iv, dv; // 日内 / 日线 视野
};

// 一路行情的 UI 端 (个股 / 篮子同形): 键集 → 历史发布 + 当日
struct Feed {
  std::vector<uint32_t> want;                    // 末次请求的键集
  bool reload = false;                           // 下帧照原键集重发
  uint32_t gen = 0;                              // 末次请求代
  uint32_t shown = 0;                            // 在画的发布的请求代 (视野复位键)
  std::shared_ptr<const Tape> data;              // 在画的历史 (末次请求的到齐才换; keys = 在画的键集)
  std::shared_ptr<const std::vector<Day>> today; // [data->keys] 当日 (只读; 篮子 = 与服务共享的那份)
  uint64_t ver = ~0ull;                          // 已取的当日版本 (QuoteStatus::ver); ~0 = 待取
};
// 每帧: 键集变 / reload / 被同路别的 Feed 覆盖 → 发请求 (个股另 select 给新浪); 末次请求的发布到齐 → 整体换上; 当日版本变 → 取
void Sync(QuoteHub &mk, Side s, std::span<const uint32_t> want, Feed &f);

struct StockState {
  Feed feed; // 键集 = {选中标的} / 空
  ChartState chart;
};

struct BasketState {
  Feed feed;              // 键集 = 全部篮子线 (下标 = basket_line)
  Feed stocks;            // 键集 = 叠加的个股 (全局 id; 与 ASSET 同一路个股历史)
  int intraday = 0;       // 日内档 (日内 / 2日 … 5日)
  int flow = 0;           // 资金流档 (同日内档)
  int flow_unit = 0;      // 资金流 y: 0 = 占近 5 日日均成交额 [%], 1 = 金额 [亿元]
  bool flow_ends = false; // 资金流计入首根 (开盘集合竞价并入) / 末根 (收盘集合竞价)
};

// 叠加的一条: 篮子 (key = basket_line, 源 = BasketState::feed) / 个股 (key = 全局 id, 源 = BasketState::stocks) + 线色 (ImU32) + 线名
struct OverlayPick {
  Side side;
  uint32_t key;
  uint32_t col;
  std::string name;
};

// 近况: 最新一根对前收 / 最新收对 MA5 (含最新一根; 各日收按前收链前复权到最新一日), 各 − 1; 不可算 = NaN
struct Recent {
  double chg = std::numeric_limits<double>::quiet_NaN();
  double ma5 = std::numeric_limits<double>::quiet_NaN();
};
// 近况的一日: 收 / 前收 (NaN = 无)
struct Mark {
  double close, ref;
};
inline constexpr size_t kMarks = 5;
// 近况核 (标的 / 净值线同一份): m = 新 → 旧, 至多用前 kMarks 日
Recent RecentOf(std::span<const Mark> m);
// 键集第 k 个标的的近况: 当日接上取当日, 再接日线末根起; 历史未到 / 源未就绪 → 全 NaN
Recent RecentOf(const Feed &f, size_t k);
// 个股近况 (不读历史): 新浪当日日根接得上 (其前一交易日 prev = 本只 bar1d 水位) 取当日, 再接 bar1d 最近几个有成交日 (StockInfo::closes)
Recent RecentOf(const StockInfo &info, const Candle &today, misc::Ymd prev);

// title = 在画标的的标题 (简称 + 代码)
void RenderStockPanel(const QuoteHub &mk, StockState &st, const std::string &title);
// picks 各条的日内叠加图 (一条 = 蜡烛), 占满当前区域 (日轴止于行情日, 当日未接上 → 当日空 + 提示); 篮子源未就绪 / 读取中 → 提示; 个股历史未到的条不画
void RenderBasketIntraday(const QuoteHub &mk, BasketState &st, const std::vector<OverlayPick> &picks);
// picks 各条的累计净流入叠加图 (日轴 = FlowDays, 跨日接续; 档位 / 口径 / 开收盘开关叠在左上角), 其余同 RenderBasketIntraday
void RenderBasketFlow(const QuoteHub &mk, BasketState &st, const std::vector<OverlayPick> &picks);
// 资金流档的日轴: 行情日及其前 n − 1 个交易日 (升序); 行情日未知 (首轮快照前) → 空
std::vector<misc::Ymd> FlowDays(const QuoteHub &mk, const BasketState &st);
// 一段的资金流 [元]: 净流入 (分钟涨跌法, 见 Fold) / 成交额; amount 0 = 无根
struct Flow {
  double net = 0, amount = 0;
};
// 一日分钟根的资金流 (首 / 末根按 ends)
Flow FlowOf(const Bars &b, bool ends);
// 键集第 k 个篮子在 days 各日的资金流 (当日接上取当日, 否则历史分钟根; 首 / 末根按 st.flow_ends); 历史未到 / 源未就绪 → 空
std::vector<Flow> FlowOf(const BasketState &st, size_t k, std::span<const misc::Ymd> days);

} // namespace GUI::Market