// Fundamental Service - 基本面数据 sync + AssetInfo 构建
// 抓取: fundamental::sync::plan<Src> → 逐任务 sync::run<Src> (月度 parquet, 水位增量, 槽位状态见 fundamental/Store.hpp)
//   与 AuditService 的 audit::plan → 逐单元 audit::run 同形: 循环 / 进度 / 取消都在本层
// 构建: parquet → AssetInfo{stock_info, stock_factor, stock_days}
//   stock_days   ← all_trading_days (fundamental::calendar)
//   stock_info   ← cn_stock_basic_info (_meta) + cn_stock_instruments (PIT 简称)
//                  + cn_stock_industry_component (最新快照)
//                  + cn_stock_real_bar1d (最新行 + 最新有效报价行) + cn_stock_status
//   stock_factor ← cn_stock_real_bar1d.adjust_factor 变点序列 (分红/拆分事件日)
//   mcap/pe_ttm/pb_mrq/ps_ttm/pcf_ttm/dy{1,3,5}y ← close × total_shares / 财务分母
//                  (最新可见快照, 口径与 L1 特征表一致; 分钟实时版在特征表阶段算)
//                  dy{1,3,5}y 另取 cn_stock_dividend 近 1/3/5 年公告, 年化
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
#include "misc/date.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
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
        st.last_month = sh.ym; // ym 升序 ⇒ 末个即最新
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

// 工作线程 → 协程的进度/结果通道 (shared_ptr 持有, detached 线程安全退出)
struct Job {
  std::atomic<bool> done = false;
  std::atomic<std::size_t> steps_done{0};  // 同步: 已跑任务数; 构建: 并行扫描已完成的月分片数
  std::atomic<std::size_t> steps_total{0}; // 0 = 当前不在计步阶段
  // BigQuant 周配额 preflight: quota 先写, confirm_needed 后置 (release) ⇒ io 线程
  // 看到 confirm_needed 即可读 quota; 工作线程随后挂起等 decision
  bigquant::Quota quota;
  std::atomic<bool> confirm_needed = false;
  std::mutex mu;
  std::string message; // 阶段说明 (worker 写, io 线程轮询读)
  bool ok = false;     // 构建是否成功 (false = 本地 parquet 缺失)
  AssetInfo assetinfo; // 构建产物
  FundamentalState st; // 构建统计

  void set_message(std::string m) {
    std::lock_guard lk(mu);
    message = std::move(m);
  }
  std::string get_message() {
    std::lock_guard lk(mu);
    return message;
  }
};

// 逐任务 sync::run (与 AuditService 逐单元 audit::run 同形): 计步进 job.steps_*, 取消在任务间生效
// (在飞的查询不可中断)
template <class Src>
void sync_all(Job &job, const std::atomic<bool> &cancel, typename Src::Client &client,
              const std::vector<fundamental::sync::Task> &tasks, const char *tag) {
  std::cout << "[" << tag << ".sync] " << tasks.size() << " task(s) from " << config::PIPELINE_START_DATE
            << " (lookback=" << config::PIPELINE_LOOKBACK_DAYS
            << "d, dedup=" << config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)" << std::endl;
  job.steps_done = 0;
  job.steps_total = tasks.size();
  for (const fundamental::sync::Task &t : tasks) {
    if (cancel.load(std::memory_order_relaxed))
      break;
    const std::string label = fundamental::sync::label(t);
    job.set_message(std::string("同步 ") + tag + ": " + label);
    std::cout << "[" << tag << ".sync] " << label << " ... " << std::flush;
    const std::int64_t rows = fundamental::sync::run<Src>(client, t);
    std::cout << rows << " rows" << std::endl;
    job.steps_done.fetch_add(1, std::memory_order_relaxed);
  }
  job.steps_total = 0;
  std::cout << "[" << tag << ".sync] " << (cancel.load(std::memory_order_relaxed) ? "cancelled" : "done") << std::endl;
}

// 三档股息率的窗口长度 [日历日] 与年化除数 [年]
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

// cn_stock_status: 每股最新行 + 逐日停牌名单
struct Status {
  int st = 0;
  bool suspended = false;
};

struct StatusAcc {
  explicit StatusAcc(std::uint32_t n) : latest(n) {}
  PerId<Latest<Status>> latest;
  Append<std::pair<Ymd, std::uint32_t>> halts; // (date, id)
  void merge(StatusAcc &&o) {
    latest.merge(std::move(o.latest));
    halts.merge(std::move(o.halts));
  }
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

// parquet → AssetInfo. 返回 false = 本地数据缺失 (首跑需先联网同步);
// 结构性问题 (缺列/类型不符) 由 TableView 内部 assert fail fast.
bool build_asset_info(Job &job, const Catalog &cat) {
  const Ymd today = misc::today_ymd();

  // ---- 首跑本地无数据 → 直接退 ----
  if (cat.shards("all_trading_days").empty() || !cat.find("cn_stock_basic_info", 0))
    return false;

  // ---- 元数据 + id 空间 ← cn_stock_basic_info (_meta 单文件, 全市场含退市) ----
  // 必须先于并行扫描: 后面每段的累加器都按这里定下的 id 空间定长分配.
  // 月度表的 instrument 一律是它的子集 (七张表实测 0 例外; 破例则 assert);
  // id → StockInfo* 直连 (std::map 节点地址稳定), 归约不再逐股查表.
  job.set_message("构建股票元数据 (cn_stock_basic_info)");
  auto &stock_info = job.assetinfo.mutable_stock_info();
  stock_info.clear();
  Ids ids(Ids::Miss::Assert);
  std::vector<std::string> codes; // id → "sz.000001"
  std::vector<StockInfo *> infos; // id → &stock_info[code]
  {
    const pq::TableView bi = fundamental::meta(cat, "cn_stock_basic_info", {"instrument", "name", "list_date", "delist_date"});
    assert(bi.rows() > 0);
    pq::Col ins = bi.col("instrument");
    pq::Col name = bi.col("name");
    pq::Col ld = bi.col("list_date");
    pq::Col dd = bi.col("delist_date");
    for (std::int64_t i = 0, n = bi.rows(); i < n; ++i) {
      std::string_view s = ins.str(i);
      if (s.empty())
        continue;
      std::string code = to_asset_code(s);
      StockInfo &info = stock_info[code];
      info.name = halfwidth_ascii(name.str(i));
      info.ipo_date = ld.yyyymmdd(i);
      info.out_date = dd.yyyymmdd(i);
      ids.add(s);
      codes.push_back(std::move(code));
      infos.push_back(&info);
    }
  }
  const std::uint32_t nid = ids.size();
  assert(nid > 0 && "cn_stock_basic_info 无有效 instrument");

  // ---- 交易日历 ← all_trading_days → 全日历展开 (含周末/节假日 trading=false 行) ----
  // Browser 日历的节假日标注依赖非交易日行
  {
    const std::vector<Ymd> trading = fundamental::calendar(cat, today);
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
  fundamental::snapshot(cat, "cn_stock_instruments", {"date", "instrument", "name"},
                        [&](const pq::TableView &v, const std::vector<std::int64_t> &rows) {
                          const pq::Col ins = v.col("instrument"), name = v.col("name");
                          for (std::int64_t i : rows)
                            infos[ids.find(ins.str(i))]->name = halfwidth_ascii(name.str(i));
                        });

  // ---- 行业 ← cn_stock_industry_component 最新快照 (申万一级) ----
  fundamental::snapshot(cat, "cn_stock_industry_component",
                        {"date", "instrument", "industry_level1_code", "industry_level1_name"},
                        [&](const pq::TableView &v, const std::vector<std::int64_t> &rows) {
                          const pq::Col ins = v.col("instrument"), code = v.col("industry_level1_code"),
                                        name = v.col("industry_level1_name");
                          for (std::int64_t i : rows) {
                            StockInfo &info = *infos[ids.find(ins.str(i))];
                            info.ind_code = code.str(i);
                            info.ind_name = name.str(i);
                          }
                        });

  // ======================== 并行扫描阶段 ========================
  // 留 2 核给 io 线程 + GUI 渲染 (实测 8 线程已拿到 96% 的并行收益)
  const unsigned hw = std::thread::hardware_concurrency();
  fundamental::Scan scan(cat, ids, hw > 3 ? hw - 2 : 1);
  using fundamental::spec;

  // 分红窗口: publish_date ∈ (today-N日, today] (锚定公告日, 非除权日 —
  // 与 L1 dy_raw 同口径)
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
             const std::int64_t n = r.size();
             const pq::Col af = r.col("adjust_factor");
             const pq::Col vol = r.col("volume");
             const pq::Col amt = r.col("amount");
             const pq::Col turn = r.col("turn");
             const pq::Col close = r.col("close");

             // 该文件的不同 date. 行按日成块 (DAI 返回顺序, 块间不保证升序), 所以只在
             // 块首做一次线性查重 ⇒ 全文件只走 K 次 find.
             a.days.clear();
             Ymd seen_d = -1;
             for (std::int64_t i = 0; i < n; ++i) {
               const Ymd d = r.date(i);
               if (d == seen_d)
                 continue;
               seen_d = d;
               if (std::find(a.days.begin(), a.days.end(), d) == a.days.end())
                 a.days.push_back(d);
             }
             std::sort(a.days.begin(), a.days.end());
             const std::size_t K = a.days.size();
             assert(K > 0 && K <= 64 && "月分片内不同 date 数异常");

             // [id × 该月 date] 小网格 (5894×≤31×5B ≈ 0.9MB, 贴 L2): 先把因子铺进去,
             // 再按 date 升序逐 id 压变点.
             const std::size_t cells = static_cast<std::size_t>(nid) * K;
             a.grid.assign(cells, 0.0f);
             a.filled.assign(cells, 0);

             Ymd prev_d = -1;
             std::size_t rank = 0;
             for (std::int64_t i = 0; i < n; ++i) {
               const std::uint32_t id = r.id(i);
               const Ymd d = r.date(i);
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
             }

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
  //   suspended≠0 的 (date, id) 全量 → suspended_ (逐日停牌名单).
  //   Browser 完整性把停牌股从当日分母里剔掉 — 全天停牌本就无逐笔可编码.
  Part<StatusAcc> status_p;
  scan.add(status_p, spec("cn_stock_status"), {"st_status", "suspended"}, StatusAcc(nid),
           [](const Rows &r, StatusAcc &a) {
             const pq::Col st = r.col("st_status");
             const pq::Col sp = r.col("suspended");
             for (std::int64_t i = 0; i < r.size(); ++i) {
               const std::uint32_t id = r.id(i);
               const Ymd d = r.date(i);
               const bool halted = sp.i32(i, 0) != 0;
               a.latest[id].offer(d, {st.i32(i, 0), halted});
               if (halted && d > 0)
                 a.halts.push({d, id});
             }
           });

  // ---- 股本 ← cn_stock_shares 每股最新行 ----
  Part<SharesAcc> shares_p;
  scan.add(shares_p, spec("cn_stock_shares"), {"total_shares"}, SharesAcc(nid), [](const Rows &r, SharesAcc &a) {
    const pq::Col ts = r.col("total_shares");
    for (std::int64_t i = 0; i < r.size(); ++i)
      a[r.id(i)].offer(r.date(i), ts.null(i) ? 0.0f : ts.f32(i));
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
             for (std::int64_t i = 0; i < r.size(); ++i) {
               if (shift.i32(i, -1) != 0)
                 continue;
               Ttm &c = a[r.id(i)];
               const Ymd d = r.date(i);
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
             }
           });

  // ---- 权益 MRQ ← cn_stock_financial_balance_general_pit ----
  //   max(report_date) 的最新可见行 (同 report_date 取 date 最大者)
  Part<BalAcc> bal_p;
  scan.add(bal_p, spec("cn_stock_financial_balance_general_pit"),
           {"report_date", "total_equity_to_parent_shareholders"}, BalAcc(nid), [](const Rows &r, BalAcc &a) {
             const pq::Col rd = r.col("report_date");
             const pq::Col eq = r.col("total_equity_to_parent_shareholders");
             for (std::int64_t i = 0; i < r.size(); ++i) {
               // 先筛有效再定位: 无效行不参与 (report_date, date) 竞争, 否则停止披露公司
               // 最新那份空权益会盖掉上一期的真实值 —— 同 TTM 三列的处理.
               if (eq.null(i) || eq.f32(i) == 0.0f)
                 continue;
               const std::int64_t key = static_cast<std::int64_t>(rd.yyyymmdd(i)) << 32 | r.date(i);
               a[r.id(i)].offer(key, eq.f32(i));
             }
           });

  // ---- 分红 ← cn_stock_dividend, 最长窗口内的税前每股分红行 (vd = publish_date) ----
  Part<DivAcc> div_p;
  scan.add(div_p, spec("cn_stock_dividend"), {"cash_before_tax"}, DivAcc{},
           [div_lo_min, today](const Rows &r, DivAcc &a) {
             const pq::Col cash = r.col("cash_before_tax");
             for (std::int64_t i = 0; i < r.size(); ++i) {
               const Ymd d = r.date(i);
               if (d <= div_lo_min || d > today)
                 continue; // 最长窗口都不覆盖 → 三档都用不上
               // 源里 625 行 cash_before_tax 是 null, 漏一个进求和就把该股 dy1y/3y/5y 全
               // 打成 NaN —— 判空统一走 bitmap (见文件上方约定 1).
               if (cash.null(i) || cash.f32(i) <= 0.0f)
                 continue;
               a.push({r.id(i), d, cash.f32(i)});
             }
           });

  job.steps_done = 0;
  job.steps_total = scan.files();
  job.set_message("构建 AssetInfo (并行扫描月度分片)");
  scan.run(job.steps_done);
  job.steps_total = 0;
  job.set_message("构建 AssetInfo (归约 / 估值 / 因子)");

  // ======================== 归约阶段 (单线程) ========================
  const BarAcc bar = std::move(bar_p).take();
  StatusAcc status = std::move(status_p).take();
  const SharesAcc shares = std::move(shares_p).take();
  const TtmAcc ttm = std::move(ttm_p).take();
  const BalAcc bal = std::move(bal_p).take();
  const DivAcc div = std::move(div_p).take();

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
  //   口径与 L1 特征表一致 (qmt 移植): 分子统一为总市值 mcap = close × total_shares
  //   (不复权真价); PE=mcap/归母净利TTM, PB=mcap/归母权益MRQ, PS=mcap/营业总收入TTM
  //   (≤0脏值→空), PCF=mcap/经营现金流净额TTM; 亏损/负权益/烧钱保留负值.
  //   DY1/3/5 = 近 1/3/5 年税前分红总额年化后 / mcap; 静态快照的股本恒为最新
  //   快照, 故等价于 Σ每股分红/年数/close (L1 dy_raw 用公告日当时股本, 窗口内
  //   有增发时会有微差). 无效 → 留 NaN (Table 显示 "-").
  //
  //   缺失语义 (与 L1 一致: 只标记, 不横截面填充 —— 展示层不放非该股真实值):
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
    // 分发后每个 id 的序列已是 date 全局升序, 原先千万级元素的 std::sort 因此
    // 完全消失 (下面的 assert 兜住这个不变量).
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

awaitable<void> FundamentalService::update_all() {
  co_await run(/*with_network=*/data_.config.network_sync);
}

awaitable<void> FundamentalService::run(bool with_network) {
  if (busy_.exchange(true))
    co_return; // 已有一轮在跑

  auto job = std::make_shared<Job>();
  decision_ = std::make_shared<std::atomic<int>>(0);
  cancel_ = std::make_shared<std::atomic<bool>>(false);
  auto decision = decision_;
  auto cancel = cancel_;
  state_.status =
      with_network ? FundamentalStatus::Updating : FundamentalStatus::Building;

  // 阻塞网络 + parquet IO 全在工作线程; 协程只轮询. 异常不捕获 (fail fast).
  std::thread([job, decision, cancel, with_network]() {
    if (with_network) {
      const fundamental::Now now = fundamental::now();
      const Catalog cat; // 本轮开头的清单 (plan 只算这一次, sync 照单执行)
      bigquant::Client bq;
      // 配额探针不耗配额: 每轮都取, Overview 常显本周余额
      auto take_quota = [&job, &bq]() {
        job->quota = bigquant::preflight(bq);
        job->st.quota_weekly = job->quota.weekly;
        job->st.quota_used = job->quota.used;
        job->st.quota_time = now_str();
      };
      job->set_message("preflight: BigQuant 周配额");
      take_quota();
      // plan 纯本地判定: 全部表在 dedup 窗口内 / 已到水位 ⇒ 空计划, 不拉数据
      job->set_message("plan 判定 (水位/dedup 窗口)");
      const std::vector<fundamental::sync::Task> bq_tasks = fundamental::sync::plan<fundamental::Bq>(cat, now);
      const std::vector<fundamental::sync::Task> ts_tasks = fundamental::sync::plan<fundamental::Tu>(cat, now);
      bool need_bq = !bq_tasks.empty();
      if (need_bq) {
        // 剩余 < 阈值 ⇒ 挂起等弹窗 (io 线程 confirm)
        const double left = 1.0 - static_cast<double>(job->quota.used) /
                                      static_cast<double>(job->quota.weekly);
        if (left < config::BIGQUANT_QUOTA_CONFIRM_RATIO) {
          job->set_message("等待确认: BigQuant 周配额不足");
          job->confirm_needed.store(true, std::memory_order_release);
          while (decision->load(std::memory_order_acquire) == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
          need_bq = decision->load(std::memory_order_acquire) == 1;
        }
      }
      if (need_bq) {
        sync_all<fundamental::Bq>(*job, *cancel, bq, bq_tasks, "bigquant");
        take_quota();
      }
      if (!ts_tasks.empty() && !cancel->load(std::memory_order_relaxed)) {
        tushare::Client ts;
        sync_all<fundamental::Tu>(*job, *cancel, ts, ts_tasks, "tushare");
      }
    }
    // 同步之后再扫盘, 数字反映本轮落盘结果 (构建失败也清点 — 正是要看清哪张表缺)
    const Catalog cat;
    job->ok = build_asset_info(*job, cat);
    job->st.tables = collect_table_stats(cat);
    job->done = true;
  }).detach();

  boost::asio::steady_timer timer(io_);
  while (!job->done) {
    // 构建中实时把工作线程的阶段说明透给 UI
    std::string msg = job->get_message();
    // 配额确认: 工作线程挂起 ⇒ Confirm (弹窗); 回答后回到 Updating
    if (job->confirm_needed.load(std::memory_order_acquire)) {
      if (decision->load(std::memory_order_acquire) == 0) {
        state_.status = FundamentalStatus::Confirm;
        state_.quota_weekly = job->quota.weekly;
        state_.quota_used = job->quota.used;
      } else if (state_.status == FundamentalStatus::Confirm) {
        state_.status = FundamentalStatus::Updating;
      }
    }
    if (with_network && state_.status == FundamentalStatus::Updating &&
        msg.starts_with("构建"))
      state_.status = FundamentalStatus::Building;
    // 计步阶段 (同步任务 / 并行扫描分片): 进度由 worker 的原子计数器给出 (逐步不再抢 message 锁)
    const std::size_t total = job->steps_total.load(std::memory_order_relaxed);
    if (total != 0) {
      const std::size_t n =
          std::min(job->steps_done.load(std::memory_order_relaxed), total);
      msg += " " + std::to_string(n) + "/" + std::to_string(total);
    }
    state_.message = std::move(msg);
    timer.expires_after(std::chrono::milliseconds(100));
    co_await timer.async_wait(boost::asio::use_awaitable);
  }

  // 本轮未联网 (quota_time 空) ⇒ 保留上次配额读数
  if (job->st.quota_time.empty()) {
    job->st.quota_weekly = state_.quota_weekly;
    job->st.quota_used = state_.quota_used;
    job->st.quota_time = state_.quota_time;
  }
  if (job->ok) {
    // AssetInfo 替换只发生在 io 线程 (GUI 消费与此同线程, 无竞争)
    data_.assetinfo = std::move(job->assetinfo);
    state_ = job->st;
    state_.status = FundamentalStatus::Ready;
    state_.message.clear();
    state_.last_update = now_str();
  } else {
    state_.tables = std::move(job->st.tables);
    state_.quota_weekly = job->st.quota_weekly;
    state_.quota_used = job->st.quota_used;
    state_.quota_time = job->st.quota_time;
    state_.status = FundamentalStatus::Error;
    state_.message = "本地 parquet 缺失 — 点击 Update 联网同步";
  }
  decision_.reset();
  cancel_.reset();
  busy_ = false;
}

void FundamentalService::cancel() {
  if (cancel_)
    cancel_->store(true, std::memory_order_relaxed);
}

void FundamentalService::confirm(bool proceed) {
  assert(state_.status == FundamentalStatus::Confirm && "confirm: 不在 Confirm 态");
  assert(decision_ && decision_->load() == 0 && "confirm: 本轮已回答");
  decision_->store(proceed ? 1 : 2, std::memory_order_release);
  // 同帧退出 Confirm (io 线程 = GUI 线程), 不等协程下一次 100ms 轮询 — 否则弹窗会重开
  state_.status = FundamentalStatus::Updating;
}

} // namespace GUI::Database
