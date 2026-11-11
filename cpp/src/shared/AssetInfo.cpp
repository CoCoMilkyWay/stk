// AssetInfo 构建 - output/data/fundamental/ parquet → AssetInfo (Sync 每轮同步之后调)
//
// 构建: 扫描 (并行, 表主序) → 落地 (串行, 产物主序)
//   扫描: 一张源表 = 一个 Part, 全部登记进同一个 fundamental::Scan. id 空间 = A 轴 (+ 档案新见代码), 开扫前钉死,
//         所以每段的累加器都是定长 PerId —— 段间零共享、零锁, 归约只是按段序逐个 merge. 逐行路径上没有哈希也没有
//         日期转换 (Scan 整列预转键与 vd), 千万级的因子排序也在文件内预压缩后消失.
//   落地: 一个产物 (或 StockInfo 的一组字段) = 一个 land_*, 写集互不相交:
//     land_basic      known / name / ipo / out / ind_*  ← cn_stock_basic_info (_meta) + cn_stock_instruments 最新日 (PIT 简称)
//                                                         + cn_stock_industry_component 最新日
//     land_quote      update_date / volume / amount / turn / closes ← cn_stock_real_bar1d 最新行 + 最近几个有效报价行
//     land_status     st / suspended                    ← cn_stock_status 最新行
//     land_valuation  mcap / pe / pb / ps / pcf / dy    ← close × total_shares / 财务分母 (cn_stock_shares / financial_ttm_shift /
//                                                         balance_general_pit) + cn_stock_dividend 近 1/3/5 年
//     land_factors    factors   ← cn_stock_real_bar1d.adjust_factor 变点
//     land_events     events    ← 种类表各源表 (shared/AssetEvent.hpp): 单点 = 档案 + 涨跌停 + kPoints 逐行; 区间 = 逐日标记按交易日连段
//     land_suspended  suspended ← cn_stock_status 逐日停牌标记
//   days ← all_trading_days 全日历; bar1m_end ← bar1m 末个分片日; pit_end ← fund::last_day (Bar 档源末日 = min(两者))
#include "shared/AssetInfo.hpp"

#include "define/Csr.hpp"
#include "features/Panel/Fund.hpp"
#include "fundamental/AssetAxis.hpp"
#include "fundamental/Scan.hpp"
#include "fundamental/Store.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/parquet.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

namespace pq = misc::pq;
using fundamental::Announces;
using fundamental::Append;
using fundamental::Catalog;
using fundamental::Ids;
using fundamental::Latest;
using fundamental::Part;
using fundamental::PerId;
using fundamental::Pick;
using fundamental::report_period;
using fundamental::Rows;
using fundamental::Sink;
using fundamental::spec;
using misc::Ymd;

// 全角 ASCII 拉回半角. 源里 cn_stock_instruments 的 A 股简称尾巴用的是全角 Ａ (U+FF21) ——
// 深振业Ａ / 京东方Ａ / 张裕Ａ 一类, 共二十余只; 而 cn_stock_basic_info 同一批股票写的是半角 A.
// 界面字体 (MapleMono-NF-CN) 的全角块只覆盖标点, 全角字母一律落到豆腐块, 所以源文本一律取半角:
// 既能显示, 也让搜索框敲 "深振业A" 命中, 顺带抹平两张表的口径差.
std::string halfwidth_ascii(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (std::size_t i = 0; i < text.size();) {
    // 全角 ASCII (U+FF01..U+FF5E) 的 UTF-8 一律是 EF BC/BD xx
    const unsigned char c0 = static_cast<unsigned char>(text[i]);
    if (c0 == 0xEF && i + 2 < text.size()) {
      const unsigned char c1 = static_cast<unsigned char>(text[i + 1]);
      const unsigned char c2 = static_cast<unsigned char>(text[i + 2]);
      const unsigned int cp = 0xF000u | ((c1 & 0x3Fu) << 6) | (c2 & 0x3Fu);
      if (cp >= 0xFF01u && cp <= 0xFF5Eu) {
        out.push_back(static_cast<char>(cp - 0xFF01u + '!'));
        i += 3;
        continue;
      }
    }
    out.push_back(text[i]);
    ++i;
  }
  return out;
}

// ===========================================================================
// 构建 · 扫描: 各表的累加器 (fundamental::Scan 的 Acc)
//
// 两条贯穿全表的约定:
//  1) "未出现" 用显式日期 (Latest 的 key, 0 = 从无) 表示, 不拿 NaN 当哨兵. 缺失判据集中在扫描处
//     (Col::null 直读 bitmap), 归约和估值段只看 has(). 本文件在 CMake 的
//     MATH_PRECISE 列表里 (-fno-fast-math), isfinite 可用, 但缺失语义不依赖它
//     —— 不受编译期浮点选项左右.
//  2) 快照按字段各自取 "最后一个有效值", 而不是整行取 max(date). 源里最新那行常
//     常是半空的 —— 停牌日 close/amount 为 null, 停止披露的公司最后几期财报字段
//     为空 —— 整行取会把该股的估值成片打掉, 而更早的期次里是有值的.
// 单段表 (Meta / Pick::Last) 无归约: fold 逐 id 直写本表的列 (Sink), 落地再合进 StockInfo.
// ===========================================================================

// cn_stock_basic_info (_meta, 全市场含退市)
struct Basic {
  bool known = false;
  std::string name; // 过期快照 (ST 摘牌/更名后不回填), 只给 instruments 里没有的退市股兜底
  Ymd list = 0, delist = 0;
};

// cn_stock_industry_component 最新日 (申万一级)
struct Industry {
  std::string code, name;
};

// cn_stock_real_bar1d: 每股最新行 + 最新有效报价行 + 复权因子变点
//
// date / qdate 必须分开: 停牌日源里也落行, 但 close 与 amount 是 null. 退市前的
// 长停和长期停牌股尾部连着几十行全是这样, 拿最新行当报价快照就把估值整块打掉
// (实测 167 只受影响: 160 退市 + 7 在市). 惯例是停牌股按停牌前收盘价计市值, 故
// date 只当数据水位, 价量四项一律取自最后一个有真实成交价的行 (同日, 不跨日混搭).
// qdate == 0 ⇒ 全区间无成交 ⇒ 市值算不出 ⇒ 该股所有估值列一并留空 (吸收合并 /
// 转板退市的老代码就是这样, 8 只; 这种"要么全有要么全无"是刻意的一致性).
struct Quote {
  float close = 0, volume = 0, amount = 0, turn = 0;
};

// 最近 kCloses 个有效报价行, 日降序 (同日只一行; 段间 merge 与段内 offer 同一判据)
struct Closes {
  std::array<StockInfo::DayClose, StockInfo::kCloses> v{};
  void offer(const StockInfo::DayClose &c) {
    if (c.date <= v.back().date)
      return;
    auto at = std::find_if(v.begin(), v.end(), [&](const StockInfo::DayClose &x) { return x.date <= c.date; });
    if (at->date == c.date)
      return;
    std::move_backward(at, v.end() - 1, v.end());
    *at = c;
  }
  void merge(Closes &&o) {
    for (const StockInfo::DayClose &c : o.v)
      if (c.date)
        offer(c);
  }
};

struct BarId {
  Ymd date = 0;    // 最新行, 含停牌日 → update_date
  Latest<Quote> q; // 最新有效报价行 (key = qdate); !has() ⇒ 全区间无成交
  Closes closes;   // 最近几个有效报价行的收 / 前收
  // date 与 qdate 各自独立取最大 —— 最新行和最新有效报价行可能落在不同段
  // (长停股尾部整月全是停牌行), 整体替换会把靠后段的空报价盖上去.
  void merge(BarId &&o) {
    date = std::max(date, o.date);
    q.merge(std::move(o.q));
    closes.merge(std::move(o.closes));
  }
};

struct AdjPoint {
  std::uint32_t id;
  Ymd date;
  float f;
};

struct BarAcc {
  explicit BarAcc(std::uint32_t n) : latest(n) {}
  PerId<BarId> latest;
  Append<AdjPoint> points; // 文件内已压过变点; 段序归约后 = (ym, id, date) 升序
  // 逐文件复用的临时缓冲 (不参与 merge)
  std::vector<Ymd> days;
  std::vector<float> grid;
  std::vector<std::uint8_t> filled;
  void merge(BarAcc &&o) {
    latest.merge(std::move(o.latest));
    points.merge(std::move(o.points));
  }
};

// 事件 (shared/AssetEvent.hpp) 的扫描期形态: 文本先留原串, 落地时入池
struct EventRow {
  std::uint32_t id;
  AssetEvent ev;
  std::string text;
};
using EventAcc = Append<EventRow>;

EventRow point(std::uint32_t id, EventKind kind, Ymd d) {
  EventRow e{id, {}, {}};
  e.ev.kind = kind;
  e.ev.d0 = e.ev.d1 = d;
  return e;
}

// 区间事件的原料: 逐日标记 (同资产 / 同值 / 交易日相连 = 一段)
struct DayMark {
  std::uint32_t id;
  Ymd d;
  int val;
};
using Marks = Append<DayMark>;

// cn_stock_status: 每股最新行 + 两路逐日标记 (停牌 / 风险警示) + 涨跌停
struct Status {
  int st = 0;
  bool suspended = false;
};

struct StatusAcc {
  explicit StatusAcc(std::uint32_t n) : latest(n) {}
  PerId<Latest<Status>> latest;
  Marks halts;     // 停牌日 (val = 1)
  Marks risks;     // 4 态 ≠ 0 的日 (val = 等级)
  EventAcc limits; // 收盘涨停 / 跌停
  void merge(StatusAcc &&o) {
    latest.merge(std::move(o.latest));
    halts.merge(std::move(o.halts));
    risks.merge(std::move(o.risks));
    limits.merge(std::move(o.limits));
  }
};

// cn_stock_suspend: 逐日停牌原因 (挂到停牌区间上)
struct DayText {
  std::uint32_t id;
  Ymd d;
  std::string text;
};

// cn_stock_shares: 每股最新总股本 (<= 0 ⇒ 源脏值, 估值段跳过)
using SharesAcc = PerId<Latest<float>>;

// cn_stock_financial_ttm_shift: 各次公告的主记录 (Announces), 三个分母各自的最后一个有效期次
// 有效判据就地对齐估值段的门 (np/cf != 0, rev > 0), 故下游只需看 has().
struct Ttm {
  Latest<float> np, rev, cf;
  void merge(Ttm &&o) {
    np.merge(std::move(o.np));
    rev.merge(std::move(o.rev));
    cf.merge(std::move(o.cf));
  }
};
using TtmAcc = PerId<Ttm>;

// cn_stock_financial_balance_general_pit: eq 有效的行里 (report_date, date) 最大者
// key = report_date << 32 | date
using BalAcc = PerId<Latest<float, std::int64_t>>;

// cn_stock_dividend: 最长窗口内的原始分红行. 全表才 3.5 万行, 留到落地里串行
// 累加 —— 段序归约 ⇒ 求和顺序与逐文件单线程完全一致, dy 结果逐位相同.
struct DivRow {
  std::uint32_t id;
  Ymd date;   // publish_date
  float cash; // 税前每股分红
};
using DivAcc = Append<DivRow>;

// 三窗股息率的窗口长度 [日历日] 与年化除数 [年]
constexpr int kDyWindows = 3;
constexpr int kDyDays[kDyWindows] = {365, 1095, 1825};
constexpr double kDyYears[kDyWindows] = {1.0, 3.0, 5.0};

// ---------------------------------------------------------------------------
// 单点事件表: 一表一种, 每行一条 —— 载荷按列声明, 与 shared/AssetEvent.hpp 种类表的载荷说明逐项对应
//   at   = 日期列 (nullptr = vd); 日空 → 跳过
//   ref  = 相关日列 → ev.ref;  v = 数值列 → ev.v[k];  text = 文本列 → text (半角化); nullptr = 无
//   when / keep = 过滤列 + 判据 (nullptr = 全收)
// ---------------------------------------------------------------------------
using Cols = std::span<const pq::Col>;

struct PointSrc {
  const char *table;
  EventKind kind;
  const char *at = nullptr;
  const char *ref = nullptr;
  std::array<const char *, 3> v{};
  const char *text = nullptr;
  std::array<const char *, 3> when{};
  bool (*keep)(Cols w, std::int64_t i) = nullptr;
};

// clang-format off
constexpr PointSrc kPoints[] = {
    // 首段 (起始日 ≤ 上市日) = 上市简称, 不是改名: 落地时挂到上市事件 text 后剔除
    {.table = "cn_stock_name_change",                  .kind = EventKind::Rename,   .at = "start_date",                                                                       .text = "name"},
    {.table = "cn_stock_industry_change",              .kind = EventKind::Industry,                                                                                           .text = "industry_name",
     .when = {"industry", "industry_level", "change_flag"},
     .keep = [](Cols w, std::int64_t i) { return w[0].str(i) == "sw2021" && w[1].i32(i, 0) == 1 && w[2].i32(i, -1) == 1; }},
    {.table = "cn_stock_dividend",                     .kind = EventKind::ExDiv,    .at = "ex_date",                .v = {"cash_before_tax", "bonus_rate", "conversed_rate"}},
    {.table = "cn_stock_allotment",                    .kind = EventKind::Allot,    .at = "exright_date",           .v = {"allotment_price", "allotment_rate"}},
    {.table = "cn_stock_capital",                      .kind = EventKind::Capital,  .at = "change_date",            .v = {"total_shares", "float_a"},                              .text = "reason"},
    {.table = "cn_stock_dragon_list",                  .kind = EventKind::Dragon,                                   .v = {"net_buy_amount", "price_change"},                       .text = "reason"},
    {.table = "cn_stock_financial_income_general_pit", .kind = EventKind::Report,   .ref = "report_date"},
    {.table = "forecast",                              .kind = EventKind::Forecast, .ref = "end_date",              .v = {"p_change_min", "p_change_max"},                         .text = "type"},
    {.table = "express",                               .kind = EventKind::Express,  .ref = "end_date",              .v = {"revenue", "n_income", "yoy_net_profit"}},
    {.table = "cn_stock_shareholder",                  .kind = EventKind::Holder,   .ref = "end_date",              .v = {"total_shareholder", "total_shareholder_chg"}},
};
// clang-format on

void add_points(fundamental::Scan &scan, Part<EventAcc> &out, const PointSrc &p) {
  std::vector<std::string> cols;
  for (const char *c : {p.at, p.ref, p.v[0], p.v[1], p.v[2], p.text, p.when[0], p.when[1], p.when[2]})
    if (c)
      cols.emplace_back(c);
  scan.add(out, spec(p.table), std::move(cols), EventAcc{}, [&p](const Rows &r, EventAcc &a) {
    const auto col = [&r](const char *c) { return c ? r.col(c) : pq::Col{}; };
    const pq::Col at = col(p.at), ref = col(p.ref), text = col(p.text);
    const std::array<pq::Col, 3> v{col(p.v[0]), col(p.v[1]), col(p.v[2])};
    const std::array<pq::Col, 3> w{col(p.when[0]), col(p.when[1]), col(p.when[2])};
    r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
      EventRow e = point(id, p.kind, p.at ? at.yyyymmdd(i) : d);
      if (e.ev.d0 == 0 || (p.keep && !p.keep(w, i)))
        return;
      if (p.ref)
        e.ev.ref = ref.yyyymmdd(i);
      for (std::size_t k = 0; k < v.size(); ++k)
        if (p.v[k])
          e.ev.v[k] = v[k].f32(i);
      if (p.text)
        e.text = halfwidth_ascii(text.str(i));
      a.push(e);
    });
  });
}

// ===========================================================================
// 构建 · 落地
// ===========================================================================

// id 空间 = A 轴 + cn_stock_basic_info 里新见的代码 (升序追加在尾)
std::vector<std::string> codes_of(const Catalog &cat) {
  const AssetAxis &axis = asset_axis();
  std::vector<std::string> codes;
  codes.reserve(axis.size());
  for (std::size_t g = 0; g < axis.size(); ++g)
    codes.push_back(axis.code(g));
  std::vector<std::string> fresh;
  for (std::string &c : fundamental::keys(cat, spec("cn_stock_basic_info")))
    if (axis.find(c) == axis.size())
      fresh.push_back(std::move(c));
  std::sort(fresh.begin(), fresh.end());
  codes.insert(codes.end(), std::make_move_iterator(fresh.begin()), std::make_move_iterator(fresh.end()));
  return codes;
}

// 交易日 → 全日历 (含周末/节假日 trading=false 行; Calendar 页的节假日标注依赖非交易日行)
std::vector<CalendarDay> calendar_of(const std::vector<Ymd> &trading) {
  std::vector<CalendarDay> days;
  auto t = trading.begin();
  for (Ymd d = trading.front(); d <= trading.back(); d = misc::add_days(d, 1)) {
    const bool on = *t == d;
    t += on;
    days.push_back({d, on});
  }
  return days;
}

// 档案 + PIT 简称 + 行业. instruments 的逐日 name 与 status.st_status 严格一致 (ST/*ST 前缀 ↔ 1/2);
// basic_info.name 是过期快照, 只给 instruments 里没有的退市股兜底
void land_basic(StockInfos &stocks, const std::vector<Basic> &basic, std::vector<std::string> &pit_name,
                std::vector<Industry> &industry) {
  for (std::size_t id = 0; id < stocks.size(); ++id) {
    StockInfo &s = stocks[id];
    const Basic &b = basic[id];
    s.known = b.known;
    s.name = pit_name[id].empty() ? b.name : std::move(pit_name[id]);
    s.ipo_date = b.list;
    s.out_date = b.delist;
    s.ind_code = std::move(industry[id].code);
    s.ind_name = std::move(industry[id].name);
  }
}

void land_quote(StockInfos &stocks, const PerId<BarId> &bar) {
  for (std::size_t id = 0; id < stocks.size(); ++id) {
    const BarId &b = bar.v[id];
    StockInfo &s = stocks[id];
    s.update_date = b.date;
    if (!b.q.has())
      continue; // 全区间无成交 (5 只早年退市股) → 价量留 NaN, 只留数据水位
    s.volume = b.q.v.volume;
    s.amount = b.q.v.amount;
    s.turn = b.q.v.turn;
    s.closes = b.closes.v;
  }
}

void land_status(StockInfos &stocks, const PerId<Latest<Status>> &status) {
  for (std::size_t id = 0; id < stocks.size(); ++id) {
    const Latest<Status> &st = status.v[id];
    if (!st.has())
      continue;
    // st_status 原值直传: 0=正常, 1=ST, 2=*ST (退市风险警示)
    stocks[id].st = st.v.st;
    stocks[id].suspended = st.v.suspended;
  }
}

// 估值快照 (MCAP/PE/PB/PS/PCF/DY) ← 最新 close × total_shares / 各分母
//   口径与 L_5min 估值特征一致: 分子统一为总市值 mcap = close × total_shares
//   (不复权真价); PE=mcap/归母净利TTM, PB=mcap/归母普通股权益MRQ (扣其他权益工具), PS=mcap/营业总收入TTM
//   (≤0脏值→空), PCF=mcap/经营现金流净额TTM; 亏损/负权益/烧钱保留负值.
//   DY1/3/5 = 近 1/3/5 年税前分红总额年化后 / mcap; 静态快照的股本恒为最新
//   快照, 故等价于 Σ每股分红/年数/close (L_1min dy_raw 用公告日当时股本, 窗口内
//   有增发时会有微差). 无效 → 留 NaN (Table 显示 "-").
//
//   缺失语义 (与 L_1min 一致: 只标记, 不横截面填充 —— 展示层不放非该股真实值):
//     · mcap 算不出 (无有效成交价 / 无股本) → 该股估值列整片留空, 不留半成品;
//     · 各分母独立回退到最后一个有效期次, 所以停止披露只影响到期次而非有无;
//     · 仍为空的只剩两类结构性缺口 —— 2015-01 前退市 (整体在 kSyncStart
//       之外, 源里无任何行) 和上市未满一个报告期的新股 (尚未披露 TTM/权益).
//   分红窗口: publish_date ∈ (today-N日, today] (锚定公告日, 非除权日 — 与 L_1min dy_raw 同口径)
void land_valuation(StockInfos &stocks, const PerId<BarId> &bar, const SharesAcc &shares, const TtmAcc &ttm,
                    const BalAcc &bal, const DivAcc &div, Ymd today) {
  Ymd div_lo[kDyWindows];
  for (int w = 0; w < kDyWindows; ++w)
    div_lo[w] = misc::add_days(today, -kDyDays[w]);
  // 近 1/3/5 年税前每股分红求和 (无分红 = 确知的 0, 非缺失). 段序归约 ⇒
  // 求和顺序与单线程逐文件累加逐位一致.
  std::vector<std::array<double, kDyWindows>> dps(stocks.size());
  for (const DivRow &r : div.v)
    for (int w = 0; w < kDyWindows; ++w)
      if (r.date > div_lo[w])
        dps[r.id][w] += static_cast<double>(r.cash);

  for (std::uint32_t id = 0; id < stocks.size(); ++id) {
    // q.has() 已保证 close 是非 null 正数 (见 BarId); 市值算不出的股票
    // 整只跳过, 估值列全 NaN —— 不留"一半有一半没有"的半成品.
    const Latest<Quote> &q = bar[id].q;
    if (!q.has() || shares[id].v <= 0.0f)
      continue;
    const double close = static_cast<double>(q.v.close);
    const double mcap = close * static_cast<double>(shares[id].v);
    StockInfo &s = stocks[id];
    s.mcap = static_cast<float>(mcap / 1e8); // [亿元]
    // 分母有效性已在扫描处定完 (非 null 且过了 !=0 / >0 的门), 这里只除.
    // 亏损/负权益/烧钱保留负值 —— 那是真实信息, 不是缺失.
    auto ratio = [mcap](const auto &den) {
      return den.has() ? static_cast<float>(mcap / static_cast<double>(den.v)) : StockInfo::kNaN;
    };
    s.pe_ttm = ratio(ttm[id].np);
    s.ps_ttm = ratio(ttm[id].rev);
    s.pcf_ttm = ratio(ttm[id].cf);
    s.pb_mrq = ratio(bal[id]);

    // 年化股息率: Σ每股分红 / 年数 / close (等价于 Σ(分红×股本)/年数/mcap).
    // 年数取 min(窗长, 上市年数) — 否则次新股会被窗长系统性摊薄.
    // ipo_date 缺失 ⇒ 视为已满窗 (退回固定除数).
    const double listed_years = s.ipo_date ? (misc::to_days(today) - misc::to_days(s.ipo_date)).count() / 365.0
                                           : std::numeric_limits<double>::infinity();
    float *dy[kDyWindows] = {&s.dy1y, &s.dy3y, &s.dy5y};
    for (int w = 0; w < kDyWindows; ++w) {
      const double years = std::min(kDyYears[w], listed_years);
      if (years < 0.25)
        continue; // 上市不足一季度, 年化无意义 → 留 NaN
      // 无分红是"确知的 0", 不是缺失 → 显式落 0 而非 NaN
      *dy[w] = static_cast<float>(dps[id][w] / years / close * 100.0); // [%]
    }
  }
}

// 复权因子变点: 段按 ym 升序连续切分且按段序归约, 文件内按 (id, date) 升序 ⇒ 每个 id 的点在 points 里
// 已是 date 全局升序 (下面的 assert 兜住这个不变量), 一遍过滤非变点再分桶, 无需排序
Csr<FactorPoint> land_factors(const std::vector<AdjPoint> &points, std::size_t n) {
  std::vector<AdjPoint> last(n, {0, 0, 0.0f}); // 每 id 末个保留点; f == 0 = 尚无
  std::vector<AdjPoint> kept;
  for (const AdjPoint &p : points) {
    AdjPoint &l = last[p.id];
    assert(p.date > l.date && "adjust_factor 序列非 date 升序 — 月分片的段序被破坏");
    l.date = p.date;
    if (l.f != 0.0f && std::abs(p.f / l.f - 1.0f) < 1e-5f)
      continue; // 非变点
    l.f = p.f;
    kept.push_back(p);
  }
  return bucket(n, kept, [](const AdjPoint &p) { return p.id; }, [](const AdjPoint &p) { return FactorPoint{p.date, p.f}; });
}

void sort_marks(std::vector<DayMark> &marks) {
  std::sort(marks.begin(), marks.end(), [](const DayMark &a, const DayMark &b) { return std::tie(a.id, a.d) < std::tie(b.id, b.d); });
}

// 逐日标记 (取自表 src, 按 (id, d) 升序) → 区间事件 (v0 = 交易日数, v1 = 值); 标记日须是交易日, (资产, 日) 不重复
void to_spans(const std::vector<DayMark> &marks, EventKind kind, const fundamental::Spec &src, const Ids &ids,
              const std::vector<Ymd> &trading, std::vector<EventRow> &out) {
  const auto day_of = [&](const DayMark &m) {
    const auto it = std::lower_bound(trading.begin(), trading.end(), m.d);
    fundamental::expect(it != trading.end() && *it == m.d, src, {.d = m.d, .code = ids.code(m.id)}, "逐日标记不在交易日历");
    return it - trading.begin();
  };
  for (std::size_t i = 0; i < marks.size();) {
    const DayMark &m = marks[i];
    auto t = day_of(m);
    std::size_t j = i + 1;
    for (; j < marks.size() && marks[j].id == m.id && marks[j].val == m.val; ++j) {
      const auto tj = day_of(marks[j]);
      fundamental::expect(tj > t, src, {.d = marks[j].d, .code = ids.code(m.id)}, "逐日标记 (资产, 日) 重复");
      if (tj != t + 1)
        break;
      t = tj;
    }
    EventRow e = point(m.id, kind, m.d);
    e.ev.d1 = marks[j - 1].d;
    e.ev.v[0] = static_cast<float>(j - i);
    e.ev.v[1] = static_cast<float>(m.val);
    out.push_back(std::move(e));
    i = j;
  }
}

// 事件: 单点 (档案上市 / 退市 + points) 与区间 (halts / risks 连段) 汇总 → 附文本 → 文本入池 → 去重 → 按资产分桶
//   points = 涨跌停 + kPoints 各表; halts / risks 按 (id, d) 升序; why = 逐日停牌原因
AssetEvents land_events(const StockInfos &stocks, std::vector<EventAcc> points, const std::vector<DayMark> &halts,
                        const std::vector<DayMark> &risks, std::vector<DayText> why, const Ids &ids,
                        const std::vector<Ymd> &trading) {
  const std::size_t n = stocks.size();
  std::vector<EventRow> rows;
  std::vector<std::size_t> list_row(n, SIZE_MAX); // 上市事件在 rows 的下标
  for (std::uint32_t id = 0; id < n; ++id) {
    if (stocks[id].ipo_date) {
      list_row[id] = rows.size();
      rows.push_back(point(id, EventKind::List, stocks[id].ipo_date));
    }
    if (stocks[id].out_date)
      rows.push_back(point(id, EventKind::Delist, stocks[id].out_date));
  }
  for (EventAcc &a : points)
    rows.insert(rows.end(), std::make_move_iterator(a.v.begin()), std::make_move_iterator(a.v.end()));

  // 上市简称 = 起始日 ≤ 上市日的末段 → 上市事件 text; 这些段不是改名, 剔除
  {
    const auto listing_seg = [&stocks](const EventRow &e) {
      return e.ev.kind == EventKind::Rename && e.ev.d0 <= stocks[e.id].ipo_date;
    };
    std::vector<Ymd> seg_d0(n, 0);
    for (std::size_t k = 0; k < rows.size(); ++k) {
      const EventRow &e = rows[k];
      if (!listing_seg(e) || e.ev.d0 < seg_d0[e.id])
        continue;
      assert(list_row[e.id] < k && "上市事件须先于改名段入 rows");
      seg_d0[e.id] = e.ev.d0;
      rows[list_row[e.id]].text = e.text;
    }
    std::erase_if(rows, listing_seg);
  }

  // 停牌区间: 原因 = 区间内首条停复牌明细
  const fundamental::Spec &status = spec("cn_stock_status");
  const std::size_t s0 = rows.size();
  to_spans(halts, EventKind::Suspend, status, ids, trading, rows);
  const auto by_day = [](const DayText &a, const DayText &b) { return std::tie(a.id, a.d) < std::tie(b.id, b.d); };
  std::sort(why.begin(), why.end(), by_day);
  for (std::size_t k = s0; k < rows.size(); ++k) {
    EventRow &e = rows[k];
    const auto it = std::lower_bound(why.begin(), why.end(), DayText{e.id, e.ev.d0, {}}, by_day);
    if (it != why.end() && it->id == e.id && it->d <= e.ev.d1)
      e.text = it->text;
  }
  to_spans(risks, EventKind::Risk, status, ids, trading, rows);

  AssetEvents out;
  std::unordered_map<std::string, std::uint32_t> pool{{"", 0}};
  std::vector<std::pair<std::uint32_t, AssetEvent>> evs;
  evs.reserve(rows.size());
  for (EventRow &e : rows) {
    const auto [it, fresh] = pool.try_emplace(std::move(e.text), static_cast<std::uint32_t>(out.texts.size()));
    if (fresh)
      out.texts.push_back(it->first);
    e.ev.text = it->second;
    evs.emplace_back(e.id, e.ev);
  }
  rows = {};

  // 去重键 (资产, d0, 种类, ref, 文本): 同键留末条 (段序 = 源 ym 序, 后发布覆盖先发布); 键序即桶内 d0 升序
  const auto key = [](const std::pair<std::uint32_t, AssetEvent> &p) {
    return std::tie(p.first, p.second.d0, p.second.kind, p.second.ref, p.second.text);
  };
  std::stable_sort(evs.begin(), evs.end(), [&key](const auto &a, const auto &b) { return key(a) < key(b); });
  std::size_t m = 0;
  for (std::size_t i = 0; i < evs.size(); ++i)
    if (i + 1 == evs.size() || key(evs[i]) != key(evs[i + 1]))
      evs[m++] = evs[i];
  evs.resize(m);
  out.of = bucket(n, evs, [](const auto &p) { return p.first; }, [](const auto &p) { return p.second; });
  return out;
}

// 逐日停牌标记 (按 (id, d) 升序) → [日历日] 停牌 id (日内 id 升序). Calendar 完整性把停牌股从当日分母里剔掉 —— 全天停牌本就无逐笔可编码
Csr<std::uint32_t> land_suspended(const std::vector<DayMark> &halts, const std::vector<CalendarDay> &days) {
  const std::chrono::sys_days d0 = misc::to_days(days.front().date);
  return bucket(days.size(), halts, [d0](const DayMark &m) { return static_cast<std::size_t>((misc::to_days(m.d) - d0).count()); }, [](const DayMark &m) { return m.id; });
}

} // namespace

// 结构性问题 (缺列/类型不符) 由 TableView 内部 assert fail fast.
bool AssetInfo::build(const Catalog &cat, Progress &p, const std::function<void(std::string)> &stage) {
  const Ymd today = misc::today_ymd();
  if (cat.shards("all_trading_days").empty() || !cat.find("cn_stock_basic_info", 0))
    return false;
  AssetInfo &out = *this;

  // ---- id 空间 + 日历: 先于扫描 (累加器按 id 空间定长; 区间事件按交易日连段) ----
  //   Scan 跳过 id 空间外的 instrument (尚未收录的老代码)
  stage("构建 id 空间 (A 轴 + cn_stock_basic_info) / 交易日历");
  out.codes = codes_of(cat);
  Ids ids(Ids::Cover::Full);
  for (const std::string &c : out.codes)
    ids.add(c);
  const std::uint32_t nid = ids.size();
  const std::vector<Ymd> trading = fundamental::calendar(cat, today);
  out.days = calendar_of(trading);
  const std::vector<fundamental::Shard> &bar1m = cat.shards("bar1m");
  out.bar1m_end = bar1m.empty() ? 0 : bar1m.back().day;
  out.pit_end = fund::last_day(cat, trading);

  // ======================== 扫描 (并行) ========================
  // 留 2 核给 io 线程 + GUI 渲染 (实测 8 线程已拿到 96% 的并行收益)
  const int hw = misc::exec::hw();
  fundamental::Scan scan(cat, ids, static_cast<size_t>(hw > 3 ? hw - 2 : 1));

  // ---- 档案 / PIT 简称 / 行业 (单段, 直写) ----
  std::vector<Basic> basic(nid);
  Part<Sink> basic_p;
  scan.add(basic_p, spec("cn_stock_basic_info"), {"name", "list_date", "delist_date"}, Sink{},
           [&basic](const Rows &r, Sink &) {
             const pq::Col name = r.col("name"), ld = r.col("list_date"), dd = r.col("delist_date");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd) {
               basic[id] = {true, halfwidth_ascii(name.str(i)), ld.yyyymmdd(i), dd.yyyymmdd(i)};
             });
           });
  std::vector<std::string> pit_name(nid);
  Part<Sink> name_p;
  scan.add(name_p, spec("cn_stock_instruments"), {"name"}, Sink{}, [&pit_name](const Rows &r, Sink &) {
    const pq::Col name = r.col("name");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd) { pit_name[id] = halfwidth_ascii(name.str(i)); }); }, Pick::Last);
  std::vector<Industry> industry(nid);
  Part<Sink> ind_p;
  scan.add(ind_p, spec("cn_stock_industry_component"), {"industry_level1_code", "industry_level1_name"}, Sink{}, [&industry](const Rows &r, Sink &) {
             const pq::Col code = r.col("industry_level1_code"), name = r.col("industry_level1_name");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd) { industry[id] = {std::string(code.str(i)), std::string(name.str(i))}; }); }, Pick::Last);

  // ---- 日频行情 + 复权因子 ← cn_stock_real_bar1d ----
  //   最新行 → update_date; 最新有效报价行 → volume/amount/turn/close; 最近几个有效报价行 → closes;
  //   adjust_factor → 文件内变点压缩 (分红/拆分事件日, TabCalendar 用)
  Part<BarAcc> bar_p;
  scan.add(bar_p, spec("cn_stock_real_bar1d"), {"adjust_factor", "volume", "amount", "turn", "close", "pre_close"}, BarAcc(nid),
           [nid](const Rows &r, BarAcc &a) {
             const pq::Col af = r.col("adjust_factor");
             const pq::Col vol = r.col("volume");
             const pq::Col amt = r.col("amount");
             const pq::Col turn = r.col("turn");
             const pq::Col close = r.col("close");
             const pq::Col pre = r.col("pre_close");

             // 该文件的不同 date. 行按日成块 (DAI 返回顺序, 块间不保证升序), 所以只在
             // 块首做一次线性查重 ⇒ 全文件只走 K 次 find.
             a.days.clear();
             Ymd seen_d = -1;
             r.each([&](std::int64_t, std::uint32_t, Ymd d) {
               if (d == seen_d)
                 return;
               seen_d = d;
               if (std::find(a.days.begin(), a.days.end(), d) == a.days.end())
                 a.days.push_back(d);
             });
             std::sort(a.days.begin(), a.days.end());
             const std::size_t K = a.days.size();
             fundamental::expect(K > 0 && K <= 64, r.spec(), {.d = K > 0 ? a.days.front() : 0},
                                 "月分片内不同 date 数 = " + std::to_string(K));

             // [id × 该月 date] 小网格 (5894×≤31×5B ≈ 0.9MB, 贴 L2): 先把因子铺进去,
             // 再按 date 升序逐 id 压变点.
             const std::size_t cells = static_cast<std::size_t>(nid) * K;
             a.grid.assign(cells, 0.0f);
             a.filled.assign(cells, 0);

             Ymd prev_d = -1;
             std::size_t rank = 0;
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               if (d != prev_d) {
                 prev_d = d;
                 rank = static_cast<std::size_t>(
                     std::lower_bound(a.days.begin(), a.days.end(), d) - a.days.begin());
               }
               BarId &b = a.latest[id];
               b.date = std::max(b.date, d);
               // amount 在有效成交行上仍有极少数 null (67/1158万), 落 0 而不是让 NaN 流进估值.
               if (d > b.closes.v.back().date && !close.null(i)) {
                 const float c = close.f32(i);
                 if (c > 0.0f) {
                   b.q.offer(d, {c, vol.null(i) ? 0.0f : vol.f32(i), amt.null(i) ? 0.0f : amt.f32(i),
                                 turn.null(i) ? 0.0f : turn.f32(i)});
                   b.closes.offer({d, c, pre.null(i) ? 0.0f : pre.f32(i)});
                 }
               }
               const float f = af.f32(i);
               if (!af.null(i) && f > 0.0f) {
                 const std::size_t c = static_cast<std::size_t>(id) * K + rank;
                 a.grid[c] = f;
                 a.filled[c] = 1;
               }
             });

             // 文件内变点压缩. 这里只 drop "与前一保留值精确相等" 的点, 而全局那步的
             // 判据是 |f/prev-1| < 1e-5 —— 精确相等必然满足它, 所以两级压缩与单级的
             // 结果完全一致: 被 drop 的点等于其前一局部保留值 p, 若 p 被全局保留则
             // 比值为 1, 若 p 被全局 drop 则该点对全局 prev 的比值与 p 的相同.
             for (std::uint32_t id = 0; id < nid; ++id) {
               const std::size_t base = static_cast<std::size_t>(id) * K;
               float prev_f = 0.0f;
               bool have = false;
               for (std::size_t k = 0; k < K; ++k) {
                 if (!a.filled[base + k])
                   continue;
                 const float f = a.grid[base + k];
                 if (have && f == prev_f)
                   continue;
                 a.points.push({id, a.days[k], f});
                 prev_f = f;
                 have = true;
               }
             }
           });

  // ---- 状态 ← cn_stock_status ----
  //   每股最新行 → st / suspended; 两路逐日标记 → 停牌区间 / 风险警示区间 (+ 逐日停牌名单)
  //   风险警示 4 态 (口径同 Fund st_level): st 1/2 优先; 否则 risk_warning → 3 (退市整理期)
  //   price_limit_status: 1 = 收盘跌停 / 2 = 正常 / 3 = 收盘涨停 (实测对照 cn_stock_limit_price)
  Part<StatusAcc> status_p;
  scan.add(status_p, spec("cn_stock_status"), {"st_status", "suspended", "is_risk_warning", "price_limit_status"},
           StatusAcc(nid), [](const Rows &r, StatusAcc &a) {
             const pq::Col st = r.col("st_status");
             const pq::Col sp = r.col("suspended");
             const pq::Col rw = r.col("is_risk_warning");
             const pq::Col lim = r.col("price_limit_status");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               const bool halted = sp.i32(i, 0) != 0;
               const int s = st.i32(i, 0);
               a.latest[id].offer(d, {s, halted});
               if (d == 0)
                 return;
               if (halted)
                 a.halts.push({id, d, 1});
               const int level = (s == 1 || s == 2) ? s : (rw.i32(i, 0) != 0 ? 3 : 0);
               if (level != 0)
                 a.risks.push({id, d, level});
               const int pl = lim.i32(i, 2);
               if (pl == 1 || pl == 3)
                 a.limits.push(point(id, pl == 3 ? EventKind::LimitUp : EventKind::LimitDn, d));
             });
           });

  // ---- 事件: 停牌原因 ← cn_stock_suspend; 单点事件表 kPoints ----
  Part<Append<DayText>> why_p;
  scan.add(why_p, spec("cn_stock_suspend"), {"suspend_reason"}, Append<DayText>{},
           [](const Rows &r, Append<DayText> &a) {
             const pq::Col why = r.col("suspend_reason");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               if (!why.null(i))
                 a.push({id, d, halfwidth_ascii(why.str(i))});
             });
           });
  std::array<Part<EventAcc>, std::size(kPoints)> point_p;
  for (std::size_t k = 0; k < point_p.size(); ++k)
    add_points(scan, point_p[k], kPoints[k]);

  // ---- 股本 ← cn_stock_shares 每股最新行 ----
  Part<SharesAcc> shares_p;
  scan.add(shares_p, spec("cn_stock_shares"), {"total_shares"}, SharesAcc(nid), [](const Rows &r, SharesAcc &a) {
    const pq::Col ts = r.col("total_shares");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd d) { a[id].offer(d, ts.null(i) ? 0.0f : ts.f32(i)); });
  });

  // ---- TTM 财务 ← cn_stock_financial_ttm_shift, 主记录每股最新行 ----
  Part<TtmAcc> ttm_p;
  scan.add(ttm_p, spec("cn_stock_financial_ttm_shift"),
           {"report_date", "net_profit_to_parent_shareholders_ttm", "total_operating_revenue_ttm", "net_cffoa_ttm"},
           TtmAcc(nid), [](const Rows &r, TtmAcc &a) {
             const pq::Col rd = r.col("report_date");
             const pq::Col np = r.col("net_profit_to_parent_shareholders_ttm");
             const pq::Col rev = r.col("total_operating_revenue_ttm");
             const pq::Col cf = r.col("net_cffoa_ttm");
             const Announces an(r, rd);
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               const Ymd p = report_period(rd, i, d);
               if (!p || p != an.latest(Announces::key(d, id)))
                 return;
               Ttm &c = a[id];
               // 三个分母各自推进: 源里这三列 null 各约 10 万行, 且停止披露的公司最后几期
               // 是空的. 整行取 max(date) 会让 PE/PS/PCF 成片留空 (窗口内退市股 pe 缺 15、
               // ps 31、pcf 26); 各自回退到最后一个有效期次后, 三项一起收敛到 8 只 —— 那
               // 8 只是市值本身算不出的, 属于"整只全空"的一致情形.
               if (!np.null(i) && np.f32(i) != 0.0f)
                 c.np.offer(d, np.f32(i));
               if (!rev.null(i) && rev.f32(i) > 0.0f) // 负营收 = 源脏值
                 c.rev.offer(d, rev.f32(i));
               if (!cf.null(i) && cf.f32(i) != 0.0f)
                 c.cf.offer(d, cf.f32(i));
             });
           });

  // ---- 归母普通股权益 MRQ (归母权益 − 其他权益工具) ← cn_stock_financial_balance_general_pit ----
  //   max(report_date) 的最新可见行 (同 report_date 取 date 最大者)
  Part<BalAcc> bal_p;
  scan.add(bal_p, spec("cn_stock_financial_balance_general_pit"),
           {"report_date", "total_equity_to_parent_shareholders", "other_equity_instruments"}, BalAcc(nid), [](const Rows &r, BalAcc &a) {
             const pq::Col rd = r.col("report_date");
             const pq::Col eq = r.col("total_equity_to_parent_shareholders"), oei = r.col("other_equity_instruments");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               // 先筛有效再定位: 无效行不参与 (report_date, date) 竞争, 否则停止披露公司
               // 最新那份空权益会盖掉上一期的真实值 —— 同 TTM 三列的处理.
               const Ymd p = report_period(rd, i, d);
               if (!p || eq.null(i) || eq.f32(i) == 0.0f)
                 return;
               const std::int64_t key = static_cast<std::int64_t>(p) << 32 | d;
               a[id].offer(key, eq.f32(i) - (oei.null(i) ? 0.0f : oei.f32(i)));
             });
           });

  // ---- 分红 ← cn_stock_dividend, 最长窗口内的税前每股分红行 (vd = publish_date) ----
  Part<DivAcc> div_p;
  const Ymd div_lo = misc::add_days(today, -kDyDays[kDyWindows - 1]);
  scan.add(div_p, spec("cn_stock_dividend"), {"cash_before_tax"}, DivAcc{}, [div_lo, today](const Rows &r, DivAcc &a) {
    const pq::Col cash = r.col("cash_before_tax");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
      if (d <= div_lo || d > today)
        return; // 最长窗口都不覆盖 → 三个窗口都用不上
      // 源里 625 行 cash_before_tax 是 null, 漏一个进求和就把该股 dy1y/3y/5y 全
      // 打成 NaN —— 判空统一走 bitmap (见上方约定 1).
      if (cash.null(i) || cash.f32(i) <= 0.0f)
        return;
      a.push({id, d, cash.f32(i)});
    });
  });

  p.begin(Phase::Running, scan.files());
  stage("构建 AssetInfo (并行扫描分片)");
  scan.run(p.done);

  // ======================== 落地 (串行) ========================
  stage("构建 AssetInfo (落地: 档案 / 行情 / 估值 / 因子 / 事件)");
  const BarAcc bar = std::move(bar_p).take();
  StatusAcc status = std::move(status_p).take();
  sort_marks(status.halts.v);
  sort_marks(status.risks.v);
  std::vector<EventAcc> points;
  points.push_back(std::move(status.limits));
  for (Part<EventAcc> &part : point_p)
    points.push_back(std::move(part).take());

  out.stocks.assign(nid, {});
  land_basic(out.stocks, basic, pit_name, industry);
  land_quote(out.stocks, bar.latest);
  land_status(out.stocks, status.latest);
  land_valuation(out.stocks, bar.latest, std::move(shares_p).take(), std::move(ttm_p).take(), std::move(bal_p).take(),
                 std::move(div_p).take(), today);
  out.factors = land_factors(bar.points.v, nid);
  out.events = land_events(out.stocks, std::move(points), status.halts.v, status.risks.v, std::move(why_p).take().v, ids, trading);
  out.suspended = land_suspended(status.halts.v, out.days);
  return true;
}
