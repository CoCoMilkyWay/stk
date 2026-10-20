// AssetEvent — 个股历史事件 (全市场, 基本面派生): 单点 / 区间两形, 一张种类表
// 由 FundamentalService 随 AssetInfo 同一次 fundamental::Scan 构建; 消费端: OrderFlow 图2 时间轴标注.
// 一律原生类型: 日期 Ymd (0 = 无), 数值 float (NaN = 无, 源单位), 文本 = 字符串池下标 (0 = 无); 格式化只在 UI.
#pragma once

#include "misc/date.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

enum class EventShape : std::uint8_t {
  Point, // 单日: d1 == d0
  Span,  // 区间: [d0, d1] 交易日闭区间
};

// 组 = 图上一行
#define ASSET_EVENT_GROUPS(X) \
  X(State, "状态")            \
  X(Corp, "公司")             \
  X(Trade, "交易")            \
  X(Disc, "披露")

// 种类表 X(枚举, 形态, 组, 字, 名). 载荷:
//   List      cn_stock_basic_info.list_date
//   Delist    cn_stock_basic_info.delist_date
//   Suspend   cn_stock_status.suspended 连续交易日   v0 = 交易日数, text = 停牌原因 (cn_stock_suspend, 区间内首条)
//   Risk      cn_stock_status 4 态 ≠ 0 连续交易日     v0 = 交易日数, v1 = 等级 (1=ST 2=*ST 3=退市整理期; 口径同 Fund st_level)
//   Rename    cn_stock_name_change.start_date        text = 新简称 (首段 = 上市简称, 不记)
//   Industry  cn_stock_industry_change (sw2021 一级进入)  text = 新行业名
//   ExDiv     cn_stock_dividend.ex_date              v0 = 每股派现 (税前, 元), v1 = 每股送股, v2 = 每股转增
//   Allot     cn_stock_allotment.exright_date        v0 = 配股价 (元), v1 = 每股配股
//   Capital   cn_stock_capital.change_date           v0 = 总股本 (股), v1 = 流通 A 股 (股), text = 变动原因码 (源码表未知)
//   LimitUp   cn_stock_status.price_limit_status = 3 (收盘涨停)
//   LimitDn   cn_stock_status.price_limit_status = 1 (收盘跌停)
//   Dragon    cn_stock_dragon_list.date              v0 = 净买额 (元), v1 = 当日涨跌幅 (%), text = 上榜原因
//   Report    cn_stock_financial_income_general_pit.date   ref = 报告期
//   Forecast  forecast.ann_date                      ref = 报告期, v0 / v1 = 净利变动幅度下限 / 上限 (%), text = 类型
//   Express   express.ann_date                       ref = 报告期, v0 = 营收 (元), v1 = 净利 (元), v2 = 上年同期净利 (元)
//   Holder    cn_stock_shareholder.publish_date      ref = 截止日, v0 = 股东户数, v1 = 环比 (比例)
#define ASSET_EVENT_KINDS(X)                  \
  X(List, Point, State, "上", "上市")         \
  X(Delist, Point, State, "退", "退市")       \
  X(Suspend, Span, State, "停", "停牌")       \
  X(Risk, Span, State, "警", "风险警示")      \
  X(Rename, Point, State, "名", "改名")       \
  X(Industry, Point, State, "行", "行业变更") \
  X(ExDiv, Point, Corp, "除", "除权除息")     \
  X(Allot, Point, Corp, "配", "配股")         \
  X(Capital, Point, Corp, "股", "股本变动")   \
  X(LimitUp, Point, Trade, "涨", "涨停")      \
  X(LimitDn, Point, Trade, "跌", "跌停")      \
  X(Dragon, Point, Trade, "龙", "龙虎榜")     \
  X(Report, Point, Disc, "报", "财报")        \
  X(Forecast, Point, Disc, "预", "业绩预告")  \
  X(Express, Point, Disc, "快", "业绩快报")   \
  X(Holder, Point, Disc, "户", "股东户数")

enum class EventGroup : std::uint8_t {
#define X(e, name) e,
  ASSET_EVENT_GROUPS(X)
#undef X
};
inline constexpr const char *kEventGroupNames[] = {
#define X(e, name) name,
    ASSET_EVENT_GROUPS(X)
#undef X
};
inline constexpr std::size_t kEventGroups = std::size(kEventGroupNames);

enum class EventKind : std::uint8_t {
#define X(e, shape, group, glyph, name) e,
  ASSET_EVENT_KINDS(X)
#undef X
};

struct EventKindDef {
  EventShape shape;
  EventGroup group;
  const char *glyph; // 单个汉字 (UTF-8)
  const char *name;
};
inline constexpr EventKindDef kEventKindDefs[] = {
#define X(e, shape, group, glyph, name) {EventShape::shape, EventGroup::group, glyph, name},
    ASSET_EVENT_KINDS(X)
#undef X
};
inline constexpr std::size_t kEventKinds = std::size(kEventKindDefs);
constexpr const EventKindDef &event_def(EventKind k) { return kEventKindDefs[static_cast<std::size_t>(k)]; }

struct AssetEvent {
  static constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  misc::Ymd d0 = 0, d1 = 0; // Point: d1 == d0
  misc::Ymd ref = 0;        // 相关日 (报告期 / 截止日)
  std::uint32_t text = 0;   // AssetEvents::texts 下标
  std::array<float, 3> v{kNaN, kNaN, kNaN};
  EventKind kind{};
};
static_assert(sizeof(AssetEvent) == 32, "AssetEvent: 全市场百万条量级, 保持 32 字节");

struct AssetEvents {
  std::uint64_t seq = 0;                                  // 构建代 (FundamentalRequest::seq, 0 = 未建): GUI 缓存失效键
  std::vector<std::string> texts{""};                     // 字符串池 ([0] = 无)
  std::map<std::string, std::vector<AssetEvent>> by_code; // "sz.000001" → 事件 (d0 升序)

  // 不存在 → nullptr
  const std::vector<AssetEvent> *find(const std::string &code) const {
    const auto it = by_code.find(code);
    return it == by_code.end() ? nullptr : &it->second;
  }
};
