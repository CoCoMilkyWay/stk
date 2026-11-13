// 行情图, 数据 = 全史 Series + 当日 Day (见 Quote.hpp); 一个标的的源 Source 只经 QuoteHub::of 取 (叠加图 / Lines 右表 / ASSET 表同一查法),
//   其上 RecentOf / Source::flow_of 两侧通用
//   个股 = ASSET 右栏 (Live 持仓页右栏同一面板): 选中标的 (Want), 日内 (右挂盘口: 当日最新快照五档) + K 线;
//   叠加图 (OverlayState; 两种 OverlayKind 同一入口 RenderOverlay, 同一份选中 / 档位条 / 工具角): 条 = 篮子线 / 个股 (右表点中的成分股: 个股历史 + 新浪当日);
//     刻度区全空: 横网格 (含 0 轴) y 值小字内标在右壁, 线名小字贴线尾; 不画量, 读数看十字光标;
//     涨跌幅 (NAV 日内栏) = 对首日前收的涨跌幅, y 以 0 轴居中 (上下对称); 恒画收盘线, 单条另带现价十字 (虚线, 标涨跌幅 / 时刻);
//       0 轴上叠每分钟净流入 (Bars::net) 半透明红绿柱 (日轴各日, 柱高自成比例), 当日此刻所在根随快照动, 标的 = 只一条时它, 否则全A.等;
//     累计净流入 (FLOW 页) = 自日轴首日首根起逐槽累计的净流入 (Bars::net; 占近 5 日日均成交额 % / 金额 亿元);
//   近况 (Recent): 一个标的的近况两列 (NAV 右表 / ASSET 表). 以下为个股图:
//   日内多日 (个股 / 篮子同一份) 按 Source::adj 前复权到末日 (末日真价; 篮子点位本已接续);
//   价 = 实心蜡烛 (实体 = 上一根收 → 本根收, 首根起于前收, 根根首尾相接; 额柱同色; 两侧同只画额: 库里不存量), 两图同按视野宽并根;
//     K 线 = 全史分钟 K (原价, x = 日 · kSlots + 槽), 视野宽到一根不足一日 → 日根, 再宽几日一根; 另叠均线 (按并后的根), 默认 10 年档; 悬停提示只在额图
//     十字光标随鼠标: 竖线吸附最近一根 (底轴标时刻), 横线在鼠标所在格 (价图左价 / 右涨跌幅, 额图标额)
//   注行 = 来源 (同一行: 库段 all.bar 末日 · 补段末日 · 新浪时刻 / 未接当日); 个股在库轴外 = 只画当日
//   两图各一份视野: 换标的 / 换档 / 日数变 / 双击 → 复位到该档窗口; 左键拖动 = 框选 x 段放大; y 恒随视野内的根贴合
//     K 线滚轮 = 只缩放 x, 右端恒锚在最新一根; 日内不滚. 现价线左标价 / 右标当日涨跌幅, 日内另有前收线 (左价 / 右 0%)
//   版面恒定: 标题 / 注恒两行, 各轴刻度区按最大标签框定尺 (右轴无基准也留, 标签随出随没不撑), 图框只随窗口变; 换标的 = 新历史到齐才整体换上, 其间照画旧的
// 图的根 / 叠加图的线按源缓存 (历史 / 当日 / 选中 / 档 变才重建; K 线视野的根另随视野 / 图宽), 每帧只画
#pragma once

#include "gui/quote/QuoteHub.hpp"
#include "gui/util/Chart.hpp" // XZoom

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
  double lo = 0, hi = 0;      // x 视野
  uint64_t key = 0;           // 复位键 (代 / 档 / 根数)
  float rows[2] = {3.f, 1.f}; // 价 / 量 两格的高比
  XZoom zoom;                 // 左键横向框选 (两格同画)
  bool cross = false;         // 上帧鼠标在价 / 量任一格 (十字竖线两格同画)
};

// 一个标的的两张图: 档位 + 视野
struct ChartState {
  int intraday = 0;  // 日内档 (日内 / 2日 … 5日)
  int kline = 3;     // K 线档 (周 / 月 / 年 / 10年)
  QuotesView iv, kv; // 日内 / K 线 视野
};

struct StockSeries; // 图的根 (QuotesPanel.cpp)
struct StockState {
  uint32_t want = Ref::kNone;  // 要画的标的 (全局 id)
  uint32_t shown = Ref::kNone; // 在画的: want 的全历史到齐 (价格库用不了 = 只画当日) 才换上
  ChartState chart;
  std::shared_ptr<StockSeries> series; // 在画的根
};
// 每帧: 要画 id (Ref::kNone = 无) → 登记连分钟全史 (QuoteHub::want), 换了即告诉新浪 (选中标的优先补分钟); 到齐才换上, 其间照画旧的
void Want(QuoteHub &mk, StockState &st, uint32_t id);

// 叠加图两种
enum OverlayKind : int { kOverlayNone = -1,
                         kOverlayIntra, // 涨跌幅 (NAV 日内栏)
                         kOverlayFlow,  // 累计净流入 (FLOW 页)
                         kOverlays };

struct OverlayCache; // 在画的线 (QuotesPanel.cpp)
// 叠加图两种的档位 (Lines 右表的资金流段同一份)
struct OverlayState {
  int span[kOverlays] = {}; // [OverlayKind] 日轴档 (日内 / 2日 … 5日)
  int flow_unit = 0;        // 资金流 y: 0 = 占近 5 日日均成交额 [%], 1 = 金额 [亿元]
  bool flow_ends = false;   // 资金流计入首根 (开盘集合竞价并入) / 末根 (收盘集合竞价)
  std::shared_ptr<OverlayCache> cache[kOverlays];
};

// 叠加的一条: 标的 + 线色 (ImU32) + 线名
struct OverlayPick {
  Ref ref;
  uint32_t col;
  std::string name;
  bool operator==(const OverlayPick &) const = default;
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
static_assert(kMarks <= kTailDays, "近况: 未接当日时日脊末段也须够");
// 近况核 (标的 / 净值线同一份): m = 新 → 旧, 至多用前 kMarks 日
Recent RecentOf(std::span<const Mark> m);
// 源的近况: 当日接上取当日, 再接日线末根起 (个股没留全历史 = 日脊末段; 不足的列 NaN)
Recent RecentOf(const Source &src);

// title = 在画标的的标题 (简称 + 代码)
void RenderStockPanel(const QuoteHub &mk, StockState &st, const std::string &title);
// picks 各条的 kind 叠加图, 占满当前区域 (日轴 = OverlayDays, 当日未接上 → 当日空 + 提示; 档位条等工具叠在左上角); 篮子源未就绪 / 读取中 → 提示; 无历史又无当日的条不画;
//   各条 (与 0 轴净流入柱的标的) 登记连分钟全史, 到了才有历史日.
// hot = 白粗线画在最上的条 (picks 下标, −1 = 无; gui/util/HoverSync.hpp). 返回图上悬停的条 (鼠标 x 处纵向最近; −1 = 无)
int RenderOverlay(OverlayKind kind, QuoteHub &mk, OverlayState &st, const std::vector<OverlayPick> &picks, int hot);
// kind 档的日轴: 行情日及其前 n − 1 个交易日 (升序); 当日未到 (首份 Live 前) → 空
std::vector<misc::Ymd> OverlayDays(OverlayKind kind, const QuoteHub &mk, const OverlayState &st);
} // namespace GUI::Market