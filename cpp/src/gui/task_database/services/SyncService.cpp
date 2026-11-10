// Sync Service - 数据源同步 + AssetInfo 构建
//
// 同步: 四道 (kLaneTag) 同形 = 计划 (逐项, 纯本地判定) → drive 逐项执行 (Drive.hpp, 与 AuditService 同一骨架); 道序即执行序
//   bigquant           sync::plan<Bq> 的 Month / Meta 表 → sync::run<Bq> (月度 parquet, 水位增量, 槽位状态见 fundamental/Store.hpp)
//   tushare            sync::plan<Tu>                    → sync::run<Tu>
//   eastmoney.concept  当日缺才有一项                     → sync_concepts (当日快照)
//   bigquant.bar1m     sync::plan<Bq> 的 Daily 表         → sync::run<Bq> (分钟线日文件)
//   四道同一套展示字段 state.lanes: 排队 / 在跑 / 完成 / 取消 + 计步 / 行数 / 收尾时刻
//
// 构建: 同步之后 AssetInfo::build (shared/AssetInfo.cpp) 从本地 parquet 整体重建, 随交接换进
#include "gui/task_database/services/SyncService.hpp"

#include "api/bigquant/client.hpp"
#include "api/eastmoney/client.hpp"
#include "api/tushare/client.hpp"
#include "disk/Disk.hpp"
#include "fundamental/Store.hpp"
#include "fundamental/Sync.hpp"
#include "gui/task_database/services/Drive.hpp"
#include "misc/cross_platform.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

using fundamental::Catalog;
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
TableFileStat stat_of(std::string name, const std::vector<fundamental::Shard> &shards) {
  TableFileStat st;
  st.name = std::move(name);
  if (shards.empty())
    return st;
  auto newest = shards.front().mtime;
  for (const fundamental::Shard &sh : shards) {
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
TableFileStat concept_stat() {
  std::vector<fundamental::Shard> snaps;
  for (const disk::Key &k : disk::list(disk::ConceptDay)) {
    const std::filesystem::path p = disk::path(disk::ConceptDay, k);
    snaps.push_back({misc::ym_of(k.date), p, std::filesystem::file_size(p), std::filesystem::last_write_time(p), k.date});
  }
  return stat_of(kConceptTable, snaps);
}

// worker 一轮的写口 + 产物: 计步走进度面 (svc 即 Published), 阶段说明持 svc.mutex 写进 svc.state
struct Job {
  SyncService &svc;
  const std::atomic<bool> &cancel; // = Service::cancel_ (UI 取消 / 新请求 / Stop)
  bigquant::Quota quota;
  AssetInfo asset_info; // 构建产物

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

// ===========================================================================
// 同步: 概念道的一项 (另三道的项 = fundamental::Task)
// ===========================================================================

// 东财无分片 / 水位: 一项 = 当日一份快照, 当日缺才有
struct ConceptTask {
  Ymd day;
};

std::string label(const ConceptTask &t) { return std::string(kConceptTable) + " " + misc::dash(t.day); }

// 在拉的表 (Sync 页整行底色)
std::string_view table_of(const fundamental::Task &t) { return t.spec->name; }
std::string_view table_of(const ConceptTask &) { return kConceptTable; }

// 板块名单 (名称 → 代码) + 全 A 个股所属概念 (名称) 反排成各板块成分, 题材且够数的收; 返回收进快照的概念数
std::int64_t sync_concepts(Job &job, const ConceptTask &t) {
  eastmoney::Client em;
  job.set_message("同步 eastmoney.concept: 板块名单");
  std::vector<eastmoney::Board> boards = em.concepts();
  std::unordered_map<std::string_view, std::size_t> by_name;
  for (std::size_t k = 0; k < boards.size(); ++k) {
    const bool fresh = by_name.emplace(boards[k].name, k).second;
    assert(fresh && "eastmoney: 板块名称重复");
  }
  job.set_message("同步 eastmoney.concept: 个股所属概念");
  std::vector<std::vector<std::string>> syms(boards.size());
  for (const eastmoney::Stock &s : em.stocks())
    for (const std::string &c : s.concepts) {
      const auto it = by_name.find(c);
      assert(it != by_name.end() && "eastmoney: 个股所属概念不在板块名单");
      syms[it->second].push_back(s.sym);
    }
  std::vector<Concept> list;
  for (std::size_t k = 0; k < boards.size(); ++k)
    if (concept_theme(boards[k].name) && syms[k].size() >= kConceptMin)
      list.push_back({std::move(boards[k].code), std::move(boards[k].name), std::move(syms[k])});
  std::sort(list.begin(), list.end(), [](const Concept &x, const Concept &y) { return x.code < y.code; });
  save_concepts(t.day, list);
  return static_cast<std::int64_t>(list.size());
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
  renative_all(job);
  if (req.fundamental || req.bar1m) {
    using Tasks = std::vector<fundamental::Task>;
    const fundamental::Round round; // 本轮开头的本地视图 (plan 只算这一次, sync 照单执行)
    bigquant::Client bq;
    // 配额读数 → state (探针不耗配额; Sync 页常显本周余额, 未联网的轮次沿用上次读数)
    auto show_quota = [this, &job]() {
      std::lock_guard lk(mutex);
      state.quota_weekly = job.quota.weekly;
      state.quota_used = job.quota.used;
      state.quota_time = now_str();
      publish();
    };
    job.set_message("preflight: BigQuant 周配额");
    job.quota = bigquant::preflight(bq);
    show_quota();

    // 计划: 纯本地判定 (零网络); 全部表在 dedup 窗口内 / 已到水位 / 当日概念已有 ⇒ 空计划. Daily (分钟线) 单拆一道
    job.set_message("plan 判定 (水位/dedup 窗口)");
    auto daily = [](bool d) {
      return [d](const fundamental::Spec &s, const fundamental::Slot &) { return (s.shape == fundamental::Shape::Daily) == d; };
    };
    const Ymd today = misc::today_ymd();
    const Tasks bq_tasks = req.fundamental ? fundamental::sync::plan<fundamental::Bq>(round, daily(false)) : Tasks{};
    const Tasks tu_tasks = req.fundamental ? fundamental::sync::plan<fundamental::Tu>(round) : Tasks{};
    const std::vector<ConceptTask> concept_tasks =
        req.fundamental && concept_last() < today ? std::vector<ConceptTask>{{today}} : std::vector<ConceptTask>{};
    const Tasks bar1m_tasks = req.bar1m ? fundamental::sync::plan<fundamental::Bq>(round, daily(true)) : Tasks{};
    const bool on[kLanes] = {req.fundamental, req.fundamental, req.fundamental, req.bar1m};
    const std::size_t total[kLanes] = {bq_tasks.size(), tu_tasks.size(), concept_tasks.size(), bar1m_tasks.size()};

    // 道的展示字段持锁写: 本轮开道 → 排队; 轮到 → 在跑 (逐项计步 / 行数) → 完成 / 取消 (轮到时已取消 = 整道取消)
    auto mark = [this](Lane l, auto &&fn) {
      std::lock_guard lk(mutex);
      fn(state.lanes[l]);
      publish();
    };
    for (std::size_t l = 0; l < kLanes; ++l)
      if (on[l])
        mark(Lane(l), [n = total[l]](LaneStat &s) { s.run = LaneRun::Queued, s.total = n, s.done = 0, s.rows = 0; });
    auto set_syncing = [this](std::string_view table) {
      std::lock_guard lk(mutex);
      state.syncing = table;
      publish();
    };
    // 一道 = 逐项 fetch (返回落盘行数); 计划为空 = 已到水位, 直接完成
    auto lane = [&](Lane l, const auto &items, auto &&fetch) {
      if (!on[l])
        return;
      Outcome o = Outcome::Cancelled;
      if (items.empty())
        o = Outcome::Done;
      else if (!cancel_.load(std::memory_order_relaxed)) {
        mark(l, [](LaneStat &s) { s.run = LaneRun::Syncing; });
        begin(Phase::Loading, items.size());
        const std::string tag = kLaneTag[l];
        o = drive(*this, cancel_, tag + ".sync", items, [&job, &tag](const std::string &s) { job.set_message("同步 " + tag + ": " + s); }, [&](const auto &t) {
                       set_syncing(table_of(t));
                       const std::int64_t rows = fetch(t);
                       mark(l, [rows](LaneStat &s) { ++s.done, s.rows += rows; });
                       return std::to_string(rows) + " rows"; });
        set_syncing({});
      }
      mark(l, [o](LaneStat &s) {
        s.run = o == Outcome::Done ? LaneRun::Done : LaneRun::Cancelled;
        s.time = short_time(std::time(nullptr));
      });
    };

    // BigQuant 每任务后重取配额 (不耗配额)
    auto run_bq = [&](const fundamental::Task &t) {
      const std::int64_t rows = fundamental::sync::run<fundamental::Bq>(bq, t);
      job.quota = bq.quota();
      show_quota();
      return rows;
    };
    std::optional<tushare::Client> tu;
    auto run_tu = [&](const fundamental::Task &t) {
      if (!tu)
        tu.emplace();
      return fundamental::sync::run<fundamental::Tu>(*tu, t);
    };
    auto run_concept = [&](const ConceptTask &t) { return sync_concepts(job, t); };

    std::cout << "[sync] from " << fundamental::kSyncStart << " (lookback=" << fundamental::kLookbackDays
              << "d, dedup=" << fundamental::kDedupSeconds << "s)" << std::endl;
    lane(kLaneBq, bq_tasks, run_bq);
    lane(kLaneTu, tu_tasks, run_tu);
    lane(kLaneConcept, concept_tasks, run_concept);
    lane(kLaneBar1m, bar1m_tasks, run_bq);
  }

  // 同步之后再扫盘, 数字反映本轮落盘结果 (构建失败也清点 — 正是要看清哪张表缺)
  const Catalog cat;
  SyncResult out;
  out.seq = req.seq;
  out.ok = job.asset_info.build(cat, *this, [&job](std::string m) { job.set_message(std::move(m)); });
  std::vector<TableFileStat> tables;
  tables.reserve(cat.all().size());
  for (const auto &[name, shards] : cat.all())
    tables.push_back(stat_of(name, shards));
  TableFileStat concept_files = concept_stat();
  {
    std::lock_guard lk(mutex);
    state.tables = std::move(tables);
    state.concept_files = std::move(concept_files);
    if (out.ok) {
      const AssetInfo &a = job.asset_info;
      state.stock_count = static_cast<std::size_t>(std::ranges::count_if(a.stocks, &StockInfo::known));
      state.factor_stock_count = 0;
      for (std::size_t id = 0; id < a.factors.size(); ++id)
        state.factor_stock_count += !a.factors[id].empty();
      state.trading_days_count = static_cast<std::size_t>(std::ranges::count_if(a.days, &CalendarDay::trading));
      state.date_range_start = a.days.front().date;
      state.date_range_end = a.days.back().date;
      state.last_update = now_str();
      state.message.clear();
    } else {
      state.message = "本地 parquet 缺失 — 点击 Update 联网同步";
    }
    publish();
  }
  if (out.ok) {
    job.asset_info.events.seq = req.seq;
    out.asset_info = std::move(job.asset_info);
  }
  handoff.post(std::move(out));
  return true;
}

} // namespace GUI::Database
