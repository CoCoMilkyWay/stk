// Sync Service - 数据源同步 + AssetInfo 构建
//
// 同步: 四道 (kLaneDef) 同形 = 计划 (一列 Item, 纯本地判定) → drive 逐项执行 (Drive.hpp, 与 AuditService 同一骨架); 道序即执行序
//   bigquant           Bq 的 Month / Meta 表逐表 sync::plan → sync::run<Bq> (月度 parquet, 水位增量, 槽位状态见 fundamental/Store.hpp)
//   tushare            Tu 的表逐表 sync::plan              → sync::run<Tu>
//   tushare.concept    当日缺才有一项                        → save_concepts(fetch_concepts) (当日快照, shared/Concept.hpp)
//   bigquant.bar1m     Bq 的 Daily 表逐表 sync::plan        → sync::run<Bq> (分钟线日文件)
//   四道同一套展示字段 state.lanes: 排队 / 在跑 / 完成 / 取消 + 计步 / 行数 / 收尾时刻
//
// 构建: 同步之后 AssetInfo::build (shared/AssetInfo.cpp) 从本地 parquet 整体重建, 随交接换进
#include "gui/task_database/services/SyncService.hpp"

#include "api/bigquant/client.hpp"
#include "api/tushare/client.hpp"
#include "disk/Disk.hpp"
#include "fundamental/Store.hpp"
#include "fundamental/Sync.hpp"
#include "gui/task_database/services/Drive.hpp"
#include "misc/cross_platform.hpp"
#include "misc/date.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Concept.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

using fundamental::Bq;
using fundamental::Catalog;
using fundamental::Round;
using fundamental::Shard;
using fundamental::Tu;
using misc::Ymd;

std::string now_str() {
  std::time_t t = std::time(nullptr);
  const std::tm tm_buf = safe_localtime(&t);
  char buf[20];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
  return std::string(buf);
}

std::string mtime_str(std::filesystem::file_time_type t) {
  return short_time(std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(t)));
}

// 一张表的分片 → 落盘状态. _meta 单文件片: 不计 months / 最新分片
TableFileStat stat_of(std::string name, const std::vector<Shard> &shards) {
  TableFileStat st;
  st.name = std::move(name);
  if (shards.empty())
    return st;
  auto newest = shards.front().mtime;
  for (const Shard &sh : shards) {
    st.bytes += sh.bytes;
    if (sh.ym != 0) {
      ++st.months;
      st.last_month = std::max(st.last_month, sh.ym);
      st.last_day = std::max(st.last_day, sh.day);
    }
    newest = std::max(newest, sh.mtime);
  }
  st.mtime = mtime_str(newest);
  return st;
}

// 概念快照 YYYYMMDD.tsv 记成日分片
std::vector<Shard> concept_shards() {
  std::vector<Shard> out;
  for (const disk::Key &k : disk::list(disk::ConceptDay)) {
    const std::filesystem::path p = disk::path(disk::ConceptDay, k);
    out.push_back({misc::ym_of(k.date), p, std::filesystem::file_size(p), std::filesystem::last_write_time(p), k.date});
  }
  return out;
}

// worker 一轮的写口 + 产物: 计步走进度面 (svc 即 Published), 展示字段只经 edit 持锁改
struct Job {
  SyncService &svc;
  const std::atomic<bool> &cancel; // = Service::cancel_ (UI 取消 / 新请求 / Stop)
  AssetInfo asset_info;            // 构建产物

  template <class Fn>
  void edit(Fn &&fn) {
    std::lock_guard lk(svc.mutex);
    fn(svc.state);
    svc.publish();
  }
  void say(std::string m) {
    edit([&](SyncState &s) { s.message = std::move(m); });
  }
  void quota(const bigquant::Quota &q) {
    edit([&](SyncState &s) { s.quota_weekly = q.weekly, s.quota_used = q.used, s.quota_time = now_str(); });
  }
  template <class Fn>
  void lane(Lane l, Fn &&fn) {
    edit([&](SyncState &s) { fn(s.lanes[l]); });
  }
};

// ===========================================================================
// 同步: 一道 = 一列 Item; 各道只差计划从哪来、一项怎么跑
// ===========================================================================

using Say = std::function<void(const std::string &)>;

// 一项: 在拉的表 (Sync 页整行底色) + 标签 + 执行 (返回落盘行数)
struct Item {
  std::string table, label;
  std::function<std::int64_t()> run;
};

const std::string &label(const Item &x) { return x.label; }

// 表道: Src 的日频表 (daily = false) / 分钟线 (daily = true) 逐表 sync::plan → sync::run<Src> = put(fetch)
template <class Src>
std::vector<Item> spec_items(typename Src::Client &client, const Round &r, bool daily) {
  std::vector<Item> out;
  for (const fundamental::Spec &s : fundamental::SPECS)
    if (fundamental::from<Src>(s) && (s.shape == fundamental::Shape::Daily) == daily)
      for (const fundamental::Task &t : fundamental::sync::plan(s, r))
        out.push_back({s.name, fundamental::label(t), [&client, t] { return fundamental::sync::run<Src>(client, t); }});
  return out;
}

// 概念道: 当日缺才有一项 → save(fetch) (say = 拉成分的进度); 行数 = 收进快照的概念数
std::vector<Item> concept_items(tushare::Client &tu, const Round &r, Say say) {
  const Ymd day = r.now.today;
  if (concept_last() >= day)
    return {};
  return {{kConceptTable, std::string(kConceptTable) + " " + misc::dash(day), [&tu, day, say] {
             const std::vector<Concept> list = fetch_concepts(tu, day, say);
             save_concepts(day, list);
             return static_cast<std::int64_t>(list.size());
           }}};
}

// 开道的先全部计划 (纯本地, 零网络) 并排队, 再依次 drive (道序即执行序); 配额首尾各取一次
void sync(Job &job, const SyncRequest &req) {
  const Round r; // 本轮开头的本地视图 (plan 只算这一次, sync 照单执行)
  bigquant::Client bq;
  tushare::Client tu;
  job.say("preflight: BigQuant 周配额");
  job.quota(bigquant::preflight(bq));

  job.say("plan 判定 (水位/dedup 窗口)");
  const auto say = [&job](Lane l) -> Say {
    return [&job, tag = std::string(kLaneDef[l].tag)](const std::string &s) { job.say("同步 " + tag + ": " + s); };
  };
  const auto plan = [&](Lane l) -> std::vector<Item> {
    switch (l) {
    case kLaneBq:
      return spec_items<Bq>(bq, r, false);
    case kLaneTu:
      return spec_items<Tu>(tu, r, false);
    case kLaneConcept:
      return concept_items(tu, r, say(l));
    case kLaneBar1m:
      return spec_items<Bq>(bq, r, true);
    case kLanes:
      break;
    }
    assert(false && "sync: 未知道");
    return {};
  };
  struct Plan {
    Lane l;
    std::vector<Item> items;
  };
  std::vector<Plan> plans; // 开的道, 道序
  for (std::size_t i = 0; i < kLanes; ++i)
    if (const Lane l = Lane(i); req.*kLaneDef[l].on) {
      plans.push_back({l, plan(l)});
      job.lane(l, [n = plans.back().items.size()](LaneStat &s) { s = {LaneRun::Queued, n}; });
    }

  std::cout << "[sync] from " << fundamental::kSyncStart << " (lookback=" << fundamental::kLookbackDays
            << "d, dedup=" << fundamental::kDedupSeconds << "s)" << std::endl;
  for (const Plan &p : plans) {
    job.lane(p.l, [](LaneStat &s) { s.run = LaneRun::Syncing; });
    job.svc.begin(Phase::Loading, p.items.size());
    const Outcome o = drive(job.svc, job.cancel, std::string(kLaneDef[p.l].tag) + ".sync", p.items, say(p.l), [&](const Item &x) {
      job.edit([&](SyncState &s) { s.syncing = x.table; });
      const std::int64_t rows = x.run();
      job.lane(p.l, [rows](LaneStat &s) { ++s.done, s.rows += rows; });
      return std::to_string(rows) + " rows";
    });
    job.edit([](SyncState &s) { s.syncing.clear(); });
    job.lane(p.l, [o](LaneStat &s) {
      s.run = o == Outcome::Done ? LaneRun::Done : LaneRun::Cancelled;
      s.time = short_time(std::time(nullptr));
    });
  }
  job.quota(bq.quota());
}

} // namespace

std::string short_time(std::time_t t) {
  const std::tm tm_buf = safe_localtime(&t);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%02d-%02d %02d:%02d", tm_buf.tm_mon + 1,
                tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min);
  return buf;
}

const TableFileStat *SyncState::find_table(const std::string &name) const {
  for (const TableFileStat &t : tables)
    if (t.name == name)
      return &t;
  return nullptr;
}

SyncService::SyncService() { Start(); }

// 异常不捕获 (fail fast). 取消只跳过同步, 构建照常 ⇒ 每轮必 post 一件交接
bool SyncService::run(SyncRequest &req) {
  Job job{*this, cancel_};
  if (req.fundamental || req.bar1m)
    sync(job, req);

  // 同步之后再扫盘, 数字反映本轮落盘结果 (构建失败也清点 — 正是要看清哪张表缺)
  const Catalog cat;
  SyncResult out;
  out.seq = req.seq;
  out.ok = job.asset_info.build(cat, *this, [&job](std::string m) { job.say(std::move(m)); });
  std::vector<TableFileStat> tables;
  tables.reserve(cat.all().size() + 1);
  for (const auto &[name, shards] : cat.all())
    tables.push_back(stat_of(name, shards));
  if (const std::vector<Shard> snaps = concept_shards(); !snaps.empty())
    tables.push_back(stat_of(kConceptTable, snaps));
  job.edit([&](SyncState &s) {
    s.tables = std::move(tables);
    if (!out.ok) {
      s.message = "本地 parquet 缺失 — 点击 Update 联网同步";
      return;
    }
    const AssetInfo &a = job.asset_info;
    s.stock_count = static_cast<std::size_t>(std::ranges::count_if(a.stocks, &StockInfo::known));
    s.factor_stock_count = 0;
    for (std::size_t id = 0; id < a.factors.size(); ++id)
      s.factor_stock_count += !a.factors[id].empty();
    s.trading_days_count = static_cast<std::size_t>(std::ranges::count_if(a.days, &CalendarDay::trading));
    s.date_range_start = a.days.front().date;
    s.date_range_end = a.days.back().date;
    s.last_update = now_str();
    s.message.clear();
  });
  if (out.ok) {
    job.asset_info.events.seq = req.seq;
    out.asset_info = std::move(job.asset_info);
  }
  handoff.post(std::move(out));
  return true;
}

} // namespace GUI::Database
