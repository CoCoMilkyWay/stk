// Fundamental Service - 基本面数据 sync + AssetInfo 构建
// 抓取: fundamental::sync::plan<Src>(Round) → 三条同步道逐任务 sync::run<Src> (月度 parquet, 水位增量, 槽位状态见 fundamental/Store.hpp)
//   与 AuditService 的 audit::plan<Src> → 逐单元 audit::run<Src> 同形: 循环 / 进度 / 取消 / 闸门同走 drive (Drive.hpp)
// 构建: parquet → AssetInfo{stock_info, stock_factor, stock_days}
//   stock_days   ← all_trading_days (fundamental::calendar)
//   stock_info   ← cn_stock_basic_info (_meta) + cn_stock_instruments (PIT 简称)
//                  + cn_stock_industry_component (最新快照)
//                  + cn_stock_real_bar1d (最新行 + 最新有效报价行) + cn_stock_status
//   stock_factor ← cn_stock_real_bar1d.adjust_factor 变点序列 (分红/拆分事件日)
//   mcap/pe_ttm/pb_mrq/ps_ttm/pcf_ttm/dy{1,3,5}y ← close × total_shares / 财务分母
//                  (最新可见快照, 口径与估值特征一致; 分钟实时版在特征表阶段算)
//                  dy{1,3,5}y 另取 cn_stock_dividend 近 1/3/5 年公告, 年化
//   events       ← 种类表各源表 (shared/AssetEvent.hpp): 单点逐行 / 区间由逐日标记按交易日连段
//
// 扫描架构: 六张月度大表 (real_bar1d / status / shares / financial_ttm_shift /
//   balance_general_pit / dividend) 共 800+ 个分片, 全部登记进一个 fundamental::Scan.
//   id 空间开扫前就由 cn_stock_basic_info 钉死, 所以每段的累加器都是定长 PerId ——
//   段间零共享、零锁, 归约只是按段序逐个 merge. 逐行路径上没有哈希也没有日期转换
//   (Scan 整列预转键与 vd), 千万级的因子排序也在文件内预压缩后消失.
#include "gui/task_database/services/FundamentalService.hpp"

#include "api/bigquant/client.hpp"
#include "api/tushare/client.hpp"
#include "fundamental/Scan.hpp"
#include "fundamental/Sync.hpp"
#include "gui/task_database/services/Drive.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

namespace pq = misc::pq;
using fundamental::Append;
using fundamental::Catalog;
using fundamental::Ids;
using fundamental::Latest;
using fundamental::Part;
using fundamental::PerId;
using fundamental::Rows;
using misc::Ymd;

// "000001.SZ" → "sz.000001" (与 L2 assets / GUI 查询 key 一致)
std::string to_asset_code(std::string_view instrument) {
  auto dot = instrument.rfind('.');
  assert(dot != std::string_view::npos && "instrument 无交易所后缀");
  std::string ex(instrument.substr(dot + 1));
  std::transform(ex.begin(), ex.end(), ex.begin(), ::tolower);
  return ex + "." + std::string(instrument.substr(0, dot));
}

// 简称里的全角 ASCII 拉回半角. 源里 cn_stock_instruments 的 A 股简称尾巴用
// 的是全角 Ａ (U+FF21) —— 深振业Ａ / 京东方Ａ / 张裕Ａ 一类, 共二十余只; 而
// cn_stock_basic_info 同一批股票写的是半角 A. 界面字体 (MapleMono-NF-CN) 的
// 全角块只覆盖标点, 全角字母一律落到豆腐块, 所以统一取半角: 既能显示, 也让
// 搜索框敲 "深振业A" 命中, 顺带抹平两张表的口径差.
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

std::string now_str() {
  std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[20];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
  return std::string(buf);
}

// Catalog → 逐表落盘状态 (Overview 逐表列出用). _meta 单文件表: months 恒 0, last_month 恒 0.
std::vector<TableFileStat> collect_table_stats(const Catalog &cat) {
  std::vector<TableFileStat> out;
  out.reserve(cat.all().size());
  for (const auto &[name, shards] : cat.all()) {
    TableFileStat st;
    st.name = name;
    auto newest = shards.front().mtime;
    for (const fundamental::Shard &sh : shards) {
      st.bytes += sh.bytes;
      if (sh.ym != 0) {
        ++st.months;
        st.last_month = sh.ym; // (ym, 日) 升序 ⇒ 末个即最新
        st.last_day = sh.day;
      }
      newest = std::max(newest, sh.mtime);
    }
    const std::time_t t = std::chrono::system_clock::to_time_t(
        std::chrono::clock_cast<std::chrono::system_clock>(newest));
    std::tm tm_buf{};
    localtime_r(&t, &tm_buf);
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%02d-%02d %02d:%02d", tm_buf.tm_mon + 1,
                  tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min);
    st.mtime = buf;
    out.push_back(std::move(st));
  }
  return out;
}

// worker 一轮的写口 + 产物: 计步走进度面 (svc 即 Published), 阶段说明持 svc.mutex 写进 svc.state
struct Job {
  FundamentalService &svc;
  const std::atomic<bool> &cancel; // = Service::cancel_ (UI 取消 / 新请求 / Stop)
  bigquant::Quota quota;
  AssetInfo assetinfo; // 构建产物
  FundamentalState st; // 构建统计 + 配额 (worker 私有, 收尾持锁写回)

  void set_message(std::string m) {
    std::lock_guard lk(svc.mutex);
    svc.state.message = std::move(m);
    svc.publish();
  }
};

// 外来写者的 parquet 以本库原样重写 (每轮开头, 先于 sync / 构建; 见 misc::pq::renative):
//   Catalog 全部分片 (含 Daily 分钟线) 经 fundamental::renative. 判定只读 footer; 并行, 重的先跑, 取消在文件间生效
void renative_all(Job &job) {
  struct File {
    std::uint64_t bytes;
    std::function<bool()> renative;
  };
  std::vector<File> files;
  const Catalog cat;
  for (const auto &[name, shards] : cat.all())
    for (const fundamental::Shard &sh : shards)
      files.push_back({sh.bytes, [&sh] { return fundamental::renative(sh); }});
  std::sort(files.begin(), files.end(), [](const File &a, const File &b) { return a.bytes > b.bytes; });

  job.svc.begin(Phase::Scanning, files.size());
  job.set_message("重写外来写者的 parquet");
  std::atomic<std::size_t> rewritten{0};
  misc::exec::ForkJoin ex(misc::exec::fit(files.size()));
  ex.tasks(static_cast<int>(files.size()), [&](int i, int) {
    if (job.cancel.load(std::memory_order_relaxed))
      return;
    if (files[static_cast<std::size_t>(i)].renative())
      rewritten.fetch_add(1, std::memory_order_relaxed);
    job.svc.publish(1);
  });
  std::cout << "[renative] " << rewritten << "/" << files.size() << " file(s) rewritten" << std::endl;
}

// 三窗股息率的窗口长度 [日历日] 与年化除数 [年]
constexpr int kDyWindows = 3;
constexpr int kDyDays[kDyWindows] = {365, 1095, 1825};
constexpr double kDyYears[kDyWindows] = {1.0, 3.0, 5.0};

// ---------------------------------------------------------------------------
// 各表的分段累加器 (fundamental::Scan 的 Acc).
//
// 两条贯穿全表的约定:
//  1) "未出现" 用显式日期 (Latest 的 key, 0 = 从无) 表示, 不拿 NaN 当哨兵. 缺失判据集中在扫描处
//     (Col::null 直读 bitmap), 归约和估值段只看 has(). 本文件在 CMake 的
//     PRECISE_MATH 列表里 (-fno-fast-math), isfinite 本可用, 但缺失语义不依赖它
//     —— 少一层浮点约定, 将来移出列表也不会静默出错.
//  2) 快照按字段各自取 "最后一个有效值", 而不是整行取 max(date). 源里最新那行常
//     常是半空的 —— 停牌日 close/amount 为 null, 停止披露的公司最后几期财报字段
//     为空 —— 整行取会把该股的估值成片打掉, 而更早的期次里是有值的.
// ---------------------------------------------------------------------------

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

struct BarId {
  Ymd date = 0;    // 最新行, 含停牌日 → update_date
  Latest<Quote> q; // 最新有效报价行 (key = qdate); !has() ⇒ 全区间无成交
  // date 与 qdate 各自独立取最大 —— 最新行和最新有效报价行可能落在不同段
  // (长停股尾部整月全是停牌行), 整体替换会把靠后段的空报价盖上去.
  void merge(BarId &&o) {
    date = std::max(date, o.date);
    q.merge(std::move(o.q));
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

// 事件 (shared/AssetEvent.hpp) 的扫描期形态: 文本先留原串, 归约时入池
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

// cn_stock_status: 每股最新行 + 逐日停牌名单 + 风险警示日 + 涨跌停
struct Status {
  int st = 0;
  bool suspended = false;
};

struct StatusAcc {
  explicit StatusAcc(std::uint32_t n) : latest(n) {}
  PerId<Latest<Status>> latest;
  Append<std::pair<Ymd, std::uint32_t>> halts; // (date, id)
  Append<DayMark> risks;                       // 4 态 ≠ 0 的日 (val = 等级)
  EventAcc limits;                             // 收盘涨停 / 跌停
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

// cn_stock_financial_ttm_shift: shift==0, 三个分母各自的最后一个有效期次
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

// cn_stock_dividend: 最长窗口内的原始分红行. 全表才 3.5 万行, 留到归约里串行
// 累加 —— 段序归约 ⇒ 求和顺序与逐文件单线程完全一致, dy 结果逐位相同.
struct DivRow {
  std::uint32_t id;
  Ymd date;   // publish_date
  float cash; // 税前每股分红
};
using DivAcc = Append<DivRow>;

// 单点事件表 → 每行一条 kind (日 = 日期列 at, nullptr = vd; 日空 → 跳过).
// payload(r) 每分片取一次列, 返回逐行 fill(i, id, AssetEvent &, std::string &text); false = 该行不记
template <class Payload>
void add_points(fundamental::Scan &scan, Part<EventAcc> &out, const char *table, EventKind kind, const char *at,
                std::vector<std::string> cols, Payload payload) {
  if (at)
    cols.emplace_back(at);
  scan.add(out, fundamental::spec(table), std::move(cols), EventAcc{}, [kind, at, payload](const Rows &r, EventAcc &a) {
    const pq::Col day = at ? r.col(at) : pq::Col{};
    const auto fill = payload(r);
    r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
      EventRow e = point(id, kind, at ? day.yyyymmdd(i) : d);
      if (e.ev.d0 != 0 && fill(i, id, e.ev, e.text))
        a.push(e);
    });
  });
}

// 逐日标记 (取自表 src) → 区间事件 (v0 = 交易日数, v1 = 值); 标记日须是交易日, (资产, 日) 不重复
void to_spans(std::vector<DayMark> &marks, EventKind kind, const fundamental::Spec &src, const Ids &ids,
              const std::vector<Ymd> &trading, std::vector<EventRow> &out) {
  std::sort(marks.begin(), marks.end(), [](const DayMark &a, const DayMark &b) { return std::tie(a.id, a.d) < std::tie(b.id, b.d); });
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

// parquet → AssetInfo. 返回 false = 本地数据缺失 (首跑需先联网同步);
// 结构性问题 (缺列/类型不符) 由 TableView 内部 assert fail fast.
bool build_asset_info(Job &job, const Catalog &cat) {
  const Ymd today = misc::today_ymd();

  // ---- 首跑本地无数据 → 直接退 ----
  if (cat.shards("all_trading_days").empty() || !cat.find("cn_stock_basic_info", 0))
    return false;

  // ---- 元数据 + id 空间 ← cn_stock_basic_info (_meta 单文件, 全市场含退市) ----
  // 必须先于并行扫描: 后面每段的累加器都按这里定下的 id 空间定长分配.
  // 月度表里它之外的 instrument (换号老代码 / 尚未收录的新股) → Rows::each 跳过;
  // id → StockInfo* 直连 (std::map 节点地址稳定), 归约不逐股查表.
  job.set_message("构建股票元数据 (cn_stock_basic_info)");
  auto &stock_info = job.assetinfo.mutable_stock_info();
  stock_info.clear();
  using fundamental::spec;
  const fundamental::Spec &basic = spec("cn_stock_basic_info");
  const Ids ids(cat, basic);
  const std::uint32_t nid = ids.size();
  std::vector<std::string> codes(nid); // id → "sz.000001"
  std::vector<StockInfo *> infos(nid); // id → &stock_info[code]
  for (std::uint32_t id = 0; id < nid; ++id) {
    codes[id] = to_asset_code(ids.code(id));
    infos[id] = &stock_info[codes[id]];
  }
  fundamental::meta(cat, ids, basic, {"name", "list_date", "delist_date"}, [&](const Rows &r) {
    const pq::Col name = r.col("name"), ld = r.col("list_date"), dd = r.col("delist_date");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd) {
      StockInfo &info = *infos[id];
      info.name = halfwidth_ascii(name.str(i));
      info.ipo_date = ld.yyyymmdd(i);
      info.out_date = dd.yyyymmdd(i);
    });
  });

  // ---- 交易日历 ← all_trading_days → 全日历展开 (含周末/节假日 trading=false 行) ----
  // Browser 日历的节假日标注依赖非交易日行; trading 另作区间事件的交易日相连判据
  const std::vector<Ymd> trading = fundamental::calendar(cat, today);
  {
    auto &stock_days = job.assetinfo.mutable_stock_days();
    stock_days.clear();
    auto t = trading.begin();
    for (Ymd d = trading.front(); d <= trading.back(); d = misc::add_days(d, 1)) {
      const bool on = *t == d;
      t += on;
      stock_days.push_back({d, on});
    }
    job.st.trading_days_count = trading.size();
  }

  // ---- 简称 ← cn_stock_instruments 最新快照 (逐日 PIT 简称) ----
  // basic_info.name 是过期快照 (ST 摘牌/更名后不回填), 与 status.st_status
  // 当日口径对不上; instruments 的逐日 name 与 st_status 严格一致 (ST/*ST
  // 前缀 ↔ 1/2). 退市股不在 instruments 里, 保留 basic_info 的最后简称.
  fundamental::snapshot(cat, ids, spec("cn_stock_instruments"), {"name"}, [&](const Rows &r) {
    const pq::Col name = r.col("name");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd) { infos[id]->name = halfwidth_ascii(name.str(i)); });
  });

  // ---- 行业 ← cn_stock_industry_component 最新快照 (申万一级) ----
  fundamental::snapshot(cat, ids, spec("cn_stock_industry_component"), {"industry_level1_code", "industry_level1_name"},
                        [&](const Rows &r) {
                          const pq::Col code = r.col("industry_level1_code"), name = r.col("industry_level1_name");
                          r.each([&](std::int64_t i, std::uint32_t id, Ymd) {
                            StockInfo &info = *infos[id];
                            info.ind_code = code.str(i);
                            info.ind_name = name.str(i);
                          });
                        });

  // ======================== 并行扫描阶段 ========================
  // 留 2 核给 io 线程 + GUI 渲染 (实测 8 线程已拿到 96% 的并行收益)
  const int hw = misc::exec::hw();
  fundamental::Scan scan(cat, ids, static_cast<size_t>(hw > 3 ? hw - 2 : 1));

  // 分红窗口: publish_date ∈ (today-N日, today] (锚定公告日, 非除权日 —
  // 与 L_1min dy_raw 同口径)
  Ymd div_lo[kDyWindows];
  for (int w = 0; w < kDyWindows; ++w)
    div_lo[w] = misc::add_days(today, -kDyDays[w]);
  const Ymd div_lo_min = div_lo[kDyWindows - 1];

  // ---- 日频行情 + 复权因子 ← cn_stock_real_bar1d ----
  //   最新行 → update_date; 最新有效报价行 → volume/amount/turn/close;
  //   adjust_factor → 文件内变点压缩 (分红/拆分事件日, TabBrowser 用)
  Part<BarAcc> bar_p;
  scan.add(bar_p, spec("cn_stock_real_bar1d"), {"adjust_factor", "volume", "amount", "turn", "close"}, BarAcc(nid),
           [nid](const Rows &r, BarAcc &a) {
             const pq::Col af = r.col("adjust_factor");
             const pq::Col vol = r.col("volume");
             const pq::Col amt = r.col("amount");
             const pq::Col turn = r.col("turn");
             const pq::Col close = r.col("close");

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
               if (d > b.q.key && !close.null(i)) {
                 const float c = close.f32(i);
                 if (c > 0.0f)
                   b.q.offer(d, {c, vol.null(i) ? 0.0f : vol.f32(i), amt.null(i) ? 0.0f : amt.f32(i),
                                 turn.null(i) ? 0.0f : turn.f32(i)});
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
  //   每股最新行 → st / suspended;
  //   suspended≠0 的 (date, id) 全量 → suspended_ (逐日停牌名单) + 停牌区间事件.
  //   Browser 完整性把停牌股从当日分母里剔掉 — 全天停牌本就无逐笔可编码.
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
                 a.halts.push({d, id});
               const int level = (s == 1 || s == 2) ? s : (rw.i32(i, 0) != 0 ? 3 : 0);
               if (level != 0)
                 a.risks.push({id, d, level});
               const int pl = lim.i32(i, 2);
               if (pl == 1 || pl == 3)
                 a.limits.push(point(id, pl == 3 ? EventKind::LimitUp : EventKind::LimitDn, d));
             });
           });

  // ---- 事件: 停牌原因 ← cn_stock_suspend; 单点事件表 (一表一种, 载荷见 shared/AssetEvent.hpp) ----
  Part<Append<DayText>> susp_p;
  scan.add(susp_p, spec("cn_stock_suspend"), {"suspend_reason"}, Append<DayText>{},
           [](const Rows &r, Append<DayText> &a) {
             const pq::Col why = r.col("suspend_reason");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               if (!why.null(i))
                 a.push({id, d, std::string(why.str(i))});
             });
           });
  std::array<Part<EventAcc>, 10> point_p;
  std::size_t np = 0;
  // 首段 (起始日 ≤ 上市日) = 上市简称, 不是改名: 汇总时挂到上市事件 text 后剔除
  add_points(scan, point_p[np++], "cn_stock_name_change", EventKind::Rename, "start_date", {"name"}, [](const Rows &r) { return [name = r.col("name")](std::int64_t i, std::uint32_t, AssetEvent &, std::string &text) {
                                                                                                                           text = halfwidth_ascii(name.str(i));
                                                                                                                           return true;
                                                                                                                         }; });
  add_points(scan, point_p[np++], "cn_stock_industry_change", EventKind::Industry, nullptr,
             {"industry", "industry_level", "change_flag", "industry_name"}, [](const Rows &r) {
               return [ind = r.col("industry"), lvl = r.col("industry_level"), flag = r.col("change_flag"),
                       name = r.col("industry_name")](std::int64_t i, std::uint32_t, AssetEvent &, std::string &text) {
                 if (ind.str(i) != "sw2021" || lvl.i32(i, 0) != 1 || flag.i32(i, -1) != 1)
                   return false;
                 text = name.str(i);
                 return true;
               };
             });
  add_points(scan, point_p[np++], "cn_stock_dividend", EventKind::ExDiv, "ex_date",
             {"cash_before_tax", "bonus_rate", "conversed_rate"}, [](const Rows &r) {
               return [cash = r.col("cash_before_tax"), bonus = r.col("bonus_rate"),
                       conv = r.col("conversed_rate")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &) {
                 ev.v = {cash.f32(i), bonus.f32(i), conv.f32(i)};
                 return true;
               };
             });
  add_points(scan, point_p[np++], "cn_stock_allotment", EventKind::Allot, "exright_date",
             {"allotment_price", "allotment_rate"}, [](const Rows &r) {
               return [price = r.col("allotment_price"),
                       rate = r.col("allotment_rate")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &) {
                 ev.v[0] = price.f32(i);
                 ev.v[1] = rate.f32(i);
                 return true;
               };
             });
  add_points(scan, point_p[np++], "cn_stock_capital", EventKind::Capital, "change_date", {"reason", "total_shares", "float_a"}, [](const Rows &r) { return [why = r.col("reason"), total = r.col("total_shares"),
                                                                                                                                                            fl = r.col("float_a")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &text) {
                                                                                                                                                      ev.v[0] = total.f32(i);
                                                                                                                                                      ev.v[1] = fl.f32(i);
                                                                                                                                                      text = why.str(i);
                                                                                                                                                      return true;
                                                                                                                                                    }; });
  add_points(scan, point_p[np++], "cn_stock_dragon_list", EventKind::Dragon, nullptr, {"reason", "net_buy_amount", "price_change"}, [](const Rows &r) { return [why = r.col("reason"), net = r.col("net_buy_amount"),
                                                                                                                                                                chg = r.col("price_change")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &text) {
                                                                                                                                                          ev.v[0] = net.f32(i);
                                                                                                                                                          ev.v[1] = chg.f32(i);
                                                                                                                                                          text = why.str(i);
                                                                                                                                                          return true;
                                                                                                                                                        }; });
  add_points(scan, point_p[np++], "cn_stock_financial_income_general_pit", EventKind::Report, nullptr, {"report_date"},
             [](const Rows &r) {
               return [rd = r.col("report_date")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &) {
                 ev.ref = rd.yyyymmdd(i);
                 return true;
               };
             });
  add_points(scan, point_p[np++], "forecast", EventKind::Forecast, nullptr, {"end_date", "type", "p_change_min", "p_change_max"}, [](const Rows &r) { return [ed = r.col("end_date"), type = r.col("type"), lo = r.col("p_change_min"),
                                                                                                                                                              hi = r.col("p_change_max")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &text) {
                                                                                                                                                        ev.ref = ed.yyyymmdd(i);
                                                                                                                                                        ev.v[0] = lo.f32(i);
                                                                                                                                                        ev.v[1] = hi.f32(i);
                                                                                                                                                        text = type.str(i);
                                                                                                                                                        return true;
                                                                                                                                                      }; });
  add_points(scan, point_p[np++], "express", EventKind::Express, nullptr, {"end_date", "revenue", "n_income", "yoy_net_profit"}, [](const Rows &r) { return [ed = r.col("end_date"), rev = r.col("revenue"), ni = r.col("n_income"),
                                                                                                                                                             ni0 = r.col("yoy_net_profit")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &) {
                                                                                                                                                       ev.ref = ed.yyyymmdd(i);
                                                                                                                                                       ev.v = {rev.f32(i), ni.f32(i), ni0.f32(i)};
                                                                                                                                                       return true;
                                                                                                                                                     }; });
  add_points(scan, point_p[np++], "cn_stock_shareholder", EventKind::Holder, nullptr,
             {"end_date", "total_shareholder", "total_shareholder_chg"}, [](const Rows &r) {
               return [ed = r.col("end_date"), n = r.col("total_shareholder"),
                       chg = r.col("total_shareholder_chg")](std::int64_t i, std::uint32_t, AssetEvent &ev, std::string &) {
                 ev.ref = ed.yyyymmdd(i);
                 ev.v[0] = n.f32(i);
                 ev.v[1] = chg.f32(i);
                 return true;
               };
             });
  assert(np == point_p.size() && "单点事件表数与 point_p 不符");

  // ---- 股本 ← cn_stock_shares 每股最新行 ----
  Part<SharesAcc> shares_p;
  scan.add(shares_p, spec("cn_stock_shares"), {"total_shares"}, SharesAcc(nid), [](const Rows &r, SharesAcc &a) {
    const pq::Col ts = r.col("total_shares");
    r.each([&](std::int64_t i, std::uint32_t id, Ymd d) { a[id].offer(d, ts.null(i) ? 0.0f : ts.f32(i)); });
  });

  // ---- TTM 财务 ← cn_stock_financial_ttm_shift, shift==0 每股最新行 ----
  Part<TtmAcc> ttm_p;
  scan.add(ttm_p, spec("cn_stock_financial_ttm_shift"),
           {"shift", "net_profit_to_parent_shareholders_ttm", "total_operating_revenue_ttm", "net_cffoa_ttm"},
           TtmAcc(nid), [](const Rows &r, TtmAcc &a) {
             const pq::Col shift = r.col("shift");
             const pq::Col np = r.col("net_profit_to_parent_shareholders_ttm");
             const pq::Col rev = r.col("total_operating_revenue_ttm");
             const pq::Col cf = r.col("net_cffoa_ttm");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               if (shift.i32(i, -1) != 0)
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

  // ---- 权益 MRQ ← cn_stock_financial_balance_general_pit ----
  //   max(report_date) 的最新可见行 (同 report_date 取 date 最大者)
  Part<BalAcc> bal_p;
  scan.add(bal_p, spec("cn_stock_financial_balance_general_pit"),
           {"report_date", "total_equity_to_parent_shareholders"}, BalAcc(nid), [](const Rows &r, BalAcc &a) {
             const pq::Col rd = r.col("report_date");
             const pq::Col eq = r.col("total_equity_to_parent_shareholders");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               // 先筛有效再定位: 无效行不参与 (report_date, date) 竞争, 否则停止披露公司
               // 最新那份空权益会盖掉上一期的真实值 —— 同 TTM 三列的处理.
               if (eq.null(i) || eq.f32(i) == 0.0f)
                 return;
               const std::int64_t key = static_cast<std::int64_t>(rd.yyyymmdd(i)) << 32 | d;
               a[id].offer(key, eq.f32(i));
             });
           });

  // ---- 分红 ← cn_stock_dividend, 最长窗口内的税前每股分红行 (vd = publish_date) ----
  Part<DivAcc> div_p;
  scan.add(div_p, spec("cn_stock_dividend"), {"cash_before_tax"}, DivAcc{},
           [div_lo_min, today](const Rows &r, DivAcc &a) {
             const pq::Col cash = r.col("cash_before_tax");
             r.each([&](std::int64_t i, std::uint32_t id, Ymd d) {
               if (d <= div_lo_min || d > today)
                 return; // 最长窗口都不覆盖 → 三个窗口都用不上
               // 源里 625 行 cash_before_tax 是 null, 漏一个进求和就把该股 dy1y/3y/5y 全
               // 打成 NaN —— 判空统一走 bitmap (见文件上方约定 1).
               if (cash.null(i) || cash.f32(i) <= 0.0f)
                 return;
               a.push({id, d, cash.f32(i)});
             });
           });

  job.svc.begin(Phase::Running, scan.files());
  job.set_message("构建 AssetInfo (并行扫描月度分片)");
  scan.run(job.svc.done);
  job.set_message("构建 AssetInfo (归约 / 估值 / 因子)");

  // ======================== 归约阶段 (单线程) ========================
  const BarAcc bar = std::move(bar_p).take();
  StatusAcc status = std::move(status_p).take();
  const SharesAcc shares = std::move(shares_p).take();
  const TtmAcc ttm = std::move(ttm_p).take();
  const BalAcc bal = std::move(bal_p).take();
  const DivAcc div = std::move(div_p).take();
  std::vector<DayText> why = std::move(susp_p).take().v;

  // ---- 日频行情: 每股最新行 + 最新有效报价行 ----
  for (std::uint32_t id = 0; id < nid; ++id) {
    const BarId &b = bar.latest[id];
    StockInfo &info = *infos[id];
    info.update_date = b.date;
    if (!b.q.has())
      continue; // 全区间无成交 (5 只早年退市股) → 价量留 NaN, 只留数据水位
    info.volume = b.q.v.volume;
    info.amount = b.q.v.amount;
    info.turn = b.q.v.turn;
  }

  // ---- 估值快照 (MCAP/PE/PB/PS/PCF/DY) ← 最新 close × total_shares / 各分母 ----
  //   口径与 L_5min 估值特征一致 (qmt 移植): 分子统一为总市值 mcap = close × total_shares
  //   (不复权真价); PE=mcap/归母净利TTM, PB=mcap/归母权益MRQ, PS=mcap/营业总收入TTM
  //   (≤0脏值→空), PCF=mcap/经营现金流净额TTM; 亏损/负权益/烧钱保留负值.
  //   DY1/3/5 = 近 1/3/5 年税前分红总额年化后 / mcap; 静态快照的股本恒为最新
  //   快照, 故等价于 Σ每股分红/年数/close (L_1min dy_raw 用公告日当时股本, 窗口内
  //   有增发时会有微差). 无效 → 留 NaN (Table 显示 "-").
  //
  //   缺失语义 (与 L_1min 一致: 只标记, 不横截面填充 —— 展示层不放非该股真实值):
  //     · mcap 算不出 (无有效成交价 / 无股本) → 该股估值列整片留空, 不留半成品;
  //     · 各分母独立回退到最后一个有效期次, 所以停止披露只影响到期次而非有无;
  //     · 仍为空的只剩两类结构性缺口 —— 2015-01 前退市 (整体在 PIPELINE_START_DATE
  //       之外, 源里无任何行) 和上市未满一个报告期的新股 (尚未披露 TTM/权益).
  {
    // 近 1/3/5 年税前每股分红求和 (无分红 = 确知的 0, 非缺失). 段序归约 ⇒
    // 求和顺序与单线程逐文件累加逐位一致.
    std::vector<std::array<double, kDyWindows>> dps(nid);
    for (const DivRow &r : div.v)
      for (int w = 0; w < kDyWindows; ++w)
        if (r.date > div_lo[w])
          dps[r.id][w] += static_cast<double>(r.cash);

    for (std::uint32_t id = 0; id < nid; ++id) {
      // q.has() 已保证 close 是非 null 正数 (见 BarId); 市值算不出的股票
      // 整只跳过, 估值列全 NaN —— 不留"一半有一半没有"的半成品.
      const Latest<Quote> &q = bar.latest[id].q;
      if (!q.has() || shares[id].v <= 0.0f)
        continue;
      const double close = static_cast<double>(q.v.close);
      const double mcap = close * static_cast<double>(shares[id].v);
      StockInfo &info = *infos[id];
      info.mcap = static_cast<float>(mcap / 1e8); // [亿元]
      // 分母有效性已在扫描处定完 (非 null 且过了 !=0 / >0 的门), 这里只除.
      // 亏损/负权益/烧钱保留负值 —— 那是真实信息, 不是缺失.
      auto ratio = [mcap](const auto &den) {
        return den.has() ? static_cast<float>(mcap / static_cast<double>(den.v)) : StockInfo::kNaN;
      };
      info.pe_ttm = ratio(ttm[id].np);
      info.ps_ttm = ratio(ttm[id].rev);
      info.pcf_ttm = ratio(ttm[id].cf);
      info.pb_mrq = ratio(bal[id]);

      // 年化股息率: Σ每股分红 / 年数 / close (等价于 Σ(分红×股本)/年数/mcap).
      // 年数取 min(窗长, 上市年数) — 否则次新股会被窗长系统性摊薄.
      // ipo_date 缺失 ⇒ 视为已满窗 (退回固定除数).
      const double listed_years =
          info.ipo_date ? (misc::to_days(today) - misc::to_days(info.ipo_date)).count() / 365.0
                        : std::numeric_limits<double>::infinity();
      float *dy[kDyWindows] = {&info.dy1y, &info.dy3y, &info.dy5y};
      for (int w = 0; w < kDyWindows; ++w) {
        const double years = std::min(kDyYears[w], listed_years);
        if (years < 0.25)
          continue; // 上市不足一季度, 年化无意义 → 留 NaN
        // 无分红是"确知的 0", 不是缺失 → 显式落 0 而非 NaN
        *dy[w] = static_cast<float>(dps[id][w] / years / close * 100.0); // [%]
      }
    }
  }

  // ---- 复权因子变点序列 ----
  auto &stock_factor = job.assetinfo.mutable_stock_factor();
  stock_factor.clear();
  {
    // 段按 ym 升序连续切分且按段序归约, 文件内按 (id, date) 升序 ⇒ 桶式
    // 分发后每个 id 的序列已是 date 全局升序, 无需排序 (下面的 assert 兜住这个不变量).
    std::vector<std::size_t> counts(nid, 0);
    for (const AdjPoint &p : bar.points.v)
      ++counts[p.id];
    std::vector<std::vector<FactorPoint>> seqs(nid);
    for (std::uint32_t id = 0; id < nid; ++id)
      seqs[id].reserve(counts[id]);
    for (const AdjPoint &p : bar.points.v)
      seqs[p.id].push_back({p.date, p.f});

    for (std::uint32_t id = 0; id < nid; ++id) {
      const std::vector<FactorPoint> &seq = seqs[id];
      if (seq.empty())
        continue; // 该股无有限 adjust_factor
      assert(std::is_sorted(seq.begin(), seq.end(),
                            [](const FactorPoint &a, const FactorPoint &b) { return a.date < b.date; }) &&
             "adjust_factor 序列非 date 升序 — 月分片的段序被破坏");
      std::vector<FactorPoint> &out = stock_factor[codes[id]];
      for (const FactorPoint &p : seq) {
        if (!out.empty() && std::abs(p.factor / out.back().factor - 1.0f) < 1e-5f)
          continue; // 非变点
        out.push_back(p);
      }
    }
  }

  // ---- 事件: 汇总 → 文本入池 → 去重 → 按资产分桶 (先于下段: 停牌区间要读 status.halts) ----
  {
    std::vector<EventRow> rows;
    const auto absorb = [&rows](EventAcc &&a) {
      rows.insert(rows.end(), std::make_move_iterator(a.v.begin()), std::make_move_iterator(a.v.end()));
    };
    std::vector<std::size_t> list_row(nid, SIZE_MAX); // 上市事件在 rows 的下标
    for (std::uint32_t id = 0; id < nid; ++id) {
      if (infos[id]->ipo_date) {
        list_row[id] = rows.size();
        rows.push_back(point(id, EventKind::List, infos[id]->ipo_date));
      }
      if (infos[id]->out_date)
        rows.push_back(point(id, EventKind::Delist, infos[id]->out_date));
    }
    absorb(std::move(status.limits));
    for (Part<EventAcc> &p : point_p)
      absorb(std::move(p).take());

    // 上市简称 = 起始日 ≤ 上市日的末段 → 上市事件 text; 这些段不是改名, 剔除
    {
      const auto listing_seg = [&infos](const EventRow &e) {
        return e.ev.kind == EventKind::Rename && e.ev.d0 <= infos[e.id]->ipo_date;
      };
      std::vector<Ymd> seg_d0(nid, 0);
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
    std::vector<DayMark> halts;
    halts.reserve(status.halts.v.size());
    for (const auto &[d, id] : status.halts.v)
      halts.push_back({id, d, 1});
    const std::size_t s0 = rows.size();
    to_spans(halts, EventKind::Suspend, spec("cn_stock_status"), ids, trading, rows);
    const auto by_day = [](const DayText &a, const DayText &b) { return std::tie(a.id, a.d) < std::tie(b.id, b.d); };
    std::sort(why.begin(), why.end(), by_day);
    for (std::size_t k = s0; k < rows.size(); ++k) {
      EventRow &e = rows[k];
      const auto it = std::lower_bound(why.begin(), why.end(), DayText{e.id, e.ev.d0, {}}, by_day);
      if (it != why.end() && it->id == e.id && it->d <= e.ev.d1)
        e.text = it->text;
    }
    to_spans(status.risks.v, EventKind::Risk, spec("cn_stock_status"), ids, trading, rows);

    AssetEvents &events = job.assetinfo.mutable_events();
    events = AssetEvents{};
    std::unordered_map<std::string, std::uint32_t> pool{{"", 0}};
    std::vector<std::pair<std::uint32_t, AssetEvent>> evs;
    evs.reserve(rows.size());
    for (EventRow &e : rows) {
      const auto [it, fresh] = pool.try_emplace(std::move(e.text), static_cast<std::uint32_t>(events.texts.size()));
      if (fresh)
        events.texts.push_back(it->first);
      e.ev.text = it->second;
      evs.emplace_back(e.id, e.ev);
    }
    rows = {};

    // 去重键 (资产, d0, 种类, ref, 文本): 同键留末条 (段序 = 源 ym 序, 后发布覆盖先发布); 键序即桶内 d0 升序
    const auto key = [](const std::pair<std::uint32_t, AssetEvent> &p) {
      return std::tie(p.first, p.second.d0, p.second.kind, p.second.ref, p.second.text);
    };
    std::stable_sort(evs.begin(), evs.end(), [&key](const auto &a, const auto &b) { return key(a) < key(b); });
    std::vector<AssetEvent> *bucket = nullptr;
    std::uint32_t cur = Ids::kNone;
    for (std::size_t i = 0; i < evs.size(); ++i) {
      if (i + 1 < evs.size() && key(evs[i]) == key(evs[i + 1]))
        continue;
      if (evs[i].first != cur) {
        cur = evs[i].first;
        bucket = &events.by_code[codes[cur]];
      }
      bucket->push_back(evs[i].second);
    }
  }

  // ---- 状态: 每股最新行 + 逐日停牌名单 ----
  {
    for (std::uint32_t id = 0; id < nid; ++id) {
      const Latest<Status> &s = status.latest[id];
      if (!s.has())
        continue;
      // st_status 原值直传: 0=正常, 1=ST, 2=*ST (退市风险警示)
      infos[id]->st = s.v.st;
      infos[id]->suspended = s.v.suspended;
    }

    // 28 万条 (date, id): 先按 date 排序, 每个日期只建一次 map 键、只对
    // 目标 set 做一次 reserve —— 逐条查 map + 无 reserve 的 rehash 是可见开销.
    std::vector<std::pair<Ymd, std::uint32_t>> halts = std::move(status.halts.v);
    std::sort(halts.begin(), halts.end());

    auto &suspended = job.assetinfo.mutable_suspended();
    suspended.clear();
    for (std::size_t i = 0; i < halts.size();) {
      std::size_t j = i;
      while (j < halts.size() && halts[j].first == halts[i].first)
        ++j;
      auto &names_at_date = suspended[halts[i].first];
      names_at_date.reserve(j - i);
      for (std::size_t k = i; k < j; ++k)
        names_at_date.insert(codes[halts[k].second]);
      i = j;
    }
  }

  // ---- 统计 (trading_days_count 已在日历段填好) ----
  const auto &stock_days = job.assetinfo.get_stock_days();
  job.st.stock_count = stock_info.size();
  job.st.factor_stock_count = stock_factor.size();
  job.st.date_range_start = stock_days.front().date;
  job.st.date_range_end = stock_days.back().date;
  return true;
}

} // namespace

const TableFileStat *FundamentalState::find_table(const std::string &name) const {
  for (const TableFileStat &t : tables)
    if (t.name == name)
      return &t;
  return nullptr;
}

FundamentalService::FundamentalService() { Start(); }

// 异常不捕获 (fail fast). 取消只跳过同步, 构建照常 ⇒ 每轮必 post 一件交接
bool FundamentalService::run(FundamentalRequest &req) {
  Job job{*this, cancel_};
  renative_all(job);
  if (req.with_network) {
    const fundamental::Round round; // 本轮开头的本地视图 (plan 只算这一次, sync 照单执行)
    bigquant::Client bq;
    // 配额读数 → state (探针不耗配额; Overview 常显本周余额, 未联网的轮次沿用上次读数)
    auto show_quota = [this, &job]() {
      std::lock_guard lk(mutex);
      state.quota_weekly = job.quota.weekly;
      state.quota_used = job.quota.used;
      state.quota_time = now_str();
      publish();
    };
    auto left = [&job]() {
      return 1.0 - static_cast<double>(job.quota.used) / static_cast<double>(job.quota.weekly);
    };
    job.set_message("preflight: BigQuant 周配额");
    job.quota = bigquant::preflight(bq);
    show_quota();
    // plan 纯本地判定: 全部表在 dedup 窗口内 / 已到水位 ⇒ 空计划, 不拉数据
    job.set_message("plan 判定 (水位/dedup 窗口)");
    // Daily (分钟线) 单拆一组放到最后, 用普通表剩下的配额
    std::vector<fundamental::Task> bq_tasks, daily_tasks;
    for (fundamental::Task &t : fundamental::sync::plan<fundamental::Bq>(round))
      (t.spec->shape == fundamental::Shape::Daily ? daily_tasks : bq_tasks).push_back(std::move(t));
    const std::vector<fundamental::Task> ts_tasks = fundamental::sync::plan<fundamental::Tu>(round);
    bool need_bq = !bq_tasks.empty();
    bool declined = false; // 弹窗拒绝 ⇒ 本轮 BigQuant 一律不拉 (含分钟线)
    if (need_bq) {
      // 剩余 < 阈值 ⇒ 挂起等弹窗 (GUI 线程 confirm); 取消 (新请求 / Stop) 视同跳过
      if (left() < config::BIGQUANT_QUOTA_CONFIRM_RATIO) {
        job.set_message("等待确认: BigQuant 周配额不足");
        decision_.store(0, std::memory_order_relaxed);
        confirm_pending.store(true, std::memory_order_release);
        while (decision_.load(std::memory_order_acquire) == 0 && !cancel_.load(std::memory_order_relaxed))
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        confirm_pending.store(false, std::memory_order_release);
        need_bq = decision_.load(std::memory_order_acquire) == 1;
        declined = !need_bq;
      }
    }
    // 三条同步道, 道序即执行序 (同一 drive; 与 AuditService 逐单元 audit::run 同形):
    //   on = 本道是否开跑; BigQuant 每任务后重取配额 (不耗配额); gate 每任务开跑前判
    //   (分钟线一天约 1200 万 cell: 剩余 < 阈值即跳过余下各天)
    std::optional<tushare::Client> ts;
    auto run_bq = [&](const fundamental::Task &t) {
      const std::int64_t rows = fundamental::sync::run<fundamental::Bq>(bq, t);
      job.quota = bq.quota();
      show_quota();
      return std::to_string(rows) + " rows";
    };
    auto run_tu = [&](const fundamental::Task &t) {
      if (!ts)
        ts.emplace();
      return std::to_string(fundamental::sync::run<fundamental::Tu>(*ts, t)) + " rows";
    };
    struct Lane {
      const char *tag;
      const std::vector<fundamental::Task> &tasks;
      bool on;
      std::function<std::string(const fundamental::Task &)> step;
      std::function<bool()> gate;
    };
    const Lane lanes[] = {
        {"bigquant", bq_tasks, need_bq, run_bq, {}},
        {"tushare", ts_tasks, true, run_tu, {}},
        {"bigquant.daily", daily_tasks, config::BIGQUANT_SYNC_DAILY && !declined, run_bq,
         [&left]() { return left() >= config::BIGQUANT_QUOTA_DAILY_RATIO; }},
    };
    std::cout << "[sync] from " << config::PIPELINE_START_DATE << " (lookback=" << config::PIPELINE_LOOKBACK_DAYS
              << "d, dedup=" << config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)" << std::endl;
    for (const Lane &l : lanes) {
      if (!l.on || l.tasks.empty())
        continue;
      begin(Phase::Loading, l.tasks.size());
      const std::string tag = l.tag;
      const Outcome stop = drive(*this, cancel_, tag + ".sync", l.tasks, [&job, &tag](const std::string &s) { job.set_message("同步 " + tag + ": " + s); }, l.step, l.gate);
      if (stop == Outcome::Cancelled)
        break;
    }
  }
  // 同步之后再扫盘, 数字反映本轮落盘结果 (构建失败也清点 — 正是要看清哪张表缺)
  const Catalog cat;
  FundamentalResult out;
  out.seq = req.seq;
  out.ok = build_asset_info(job, cat);
  {
    std::lock_guard lk(mutex);
    state.tables = collect_table_stats(cat);
    if (out.ok) {
      state.stock_count = job.st.stock_count;
      state.factor_stock_count = job.st.factor_stock_count;
      state.trading_days_count = job.st.trading_days_count;
      state.date_range_start = job.st.date_range_start;
      state.date_range_end = job.st.date_range_end;
      state.last_update = now_str();
      state.message.clear();
    } else {
      state.message = "本地 parquet 缺失 — 点击 Update 联网同步";
    }
    publish();
  }
  if (out.ok) {
    job.assetinfo.mutable_events().seq = req.seq;
    out.assetinfo = std::move(job.assetinfo);
  }
  handoff.post(std::move(out));
  return true;
}

void FundamentalService::confirm(bool proceed) {
  assert(confirm_pending.load(std::memory_order_acquire) && decision_.load() == 0 && "confirm: 不在待确认 / 本轮已回答");
  decision_.store(proceed ? 1 : 2, std::memory_order_release);
  confirm_pending.store(false, std::memory_order_release); // 同帧退出待确认, 弹窗不重开
}

} // namespace GUI::Database
