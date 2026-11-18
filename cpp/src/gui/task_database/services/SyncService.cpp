// SyncService — 数据集统一后台流:
//   Sync  : [按需先齐交易日历] → 纯本地规划三道 → BigQuant 轨 ∥ Tushare 轨并行 (轨内 fetch/put 双阶段流水) → 构建 AssetInfo
//   Audit : 纯本地规划冻结月单元 (表间乱序轮转) → 逐单元指纹扫描 + 日级定位 (同轮按表缓存远端 schema) → 写核对账本
//   Repair: 差异组键 → 分片槽 Replace 流水重拉 → 复核涉及月份 → 构建 AssetInfo
#include "gui/task_database/services/SyncService.hpp"

#include "api/bigquant/client.hpp"
#include "fundamental/Audit.hpp"
#include "fundamental/Remote.hpp"
#include "fundamental/Store.hpp"
#include "fundamental/Sync.hpp"
#include "misc/cross_platform.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

using fundamental::Bq;
using fundamental::Clients;
using fundamental::Grain;
using fundamental::Round;
using fundamental::Shard;
using fundamental::Slot;
using fundamental::Spec;
using fundamental::Task;
using fundamental::Tu;
using fundamental::audit::Book;
using fundamental::audit::Unit;

enum class Outcome { Done,
                     Cancelled };

std::string now_str() {
  std::time_t t = std::time(nullptr);
  const std::tm tm_buf = safe_localtime(&t);
  char buf[20];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
  return buf;
}

std::string stamp_hm(std::time_t t) {
  const std::tm tm_buf = safe_localtime(&t);
  char buf[20];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", &tm_buf);
  return buf;
}

std::string mtime_str(std::filesystem::file_time_type t) {
  return short_time(std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(t)));
}

void log_line(std::string_view s) {
  std::string line(s);
  line.push_back('\n');
  std::cout << line << std::flush;
}

// 单表核对统计: 分片槽与修复流量同源于 frozen(s, r)
AuditStat audit_stat_of(const Spec &s, const Book &book, const Round &r) {
  const std::vector<Slot> fr = fundamental::frozen(s, r);
  std::set<misc::Ym> frozen_m;
  for (const Slot &sl : fr)
    frozen_m.insert(sl.ym());

  const Book::Sheet t = book.sheet(s, r);
  AuditStat st;
  st.frozen_months = frozen_m.size();
  for (const auto &[ym, m] : t.months) {
    st.local_days += m.inv.days;
    st.local_rows += m.inv.rows;
    ++st.audited_months;
    st.updated = std::max(st.updated, m.at);
  }

  std::map<std::string, std::pair<std::int64_t, std::int64_t>> group_n;
  for (const Book::Row &row : t.rows)
    group_n[row.group] = {row.local_n, row.remote_n};

  std::set<std::string> bad_days, bad_months;
  std::map<std::string, std::int64_t> remote_only;
  for (const auto &[g, n] : group_n) {
    const auto [local_n, remote_n] = n;
    const std::string ym = g.substr(0, 7);
    bad_days.insert(g.substr(0, 10));
    bad_months.insert(ym);
    if (fundamental::grain_of(g) == Grain::Day && local_n == 0) {
      ++st.remote_only_days;
      ++remote_only[ym];
    }
    st.added += std::max<std::int64_t>(0, remote_n - local_n);
    st.removed += std::max<std::int64_t>(0, local_n - remote_n);
    assert(t.months.contains(ym) && "audit: 差异月无清点");
  }
  st.bad_days = bad_days.size();
  st.bad_months = bad_months.size();

  if (!group_n.empty()) {
    const Grain sg = fundamental::slot_grain(s);
    for (const Slot &sl : fr) {
      const std::string k = fundamental::group_key(sg, sl.w.lo);
      for (const auto &[g, _] : group_n)
        if (g.starts_with(k) || k.starts_with(g)) {
          ++st.repair_cells;
          break;
        }
    }
  }

  const bool daily = s.shape == fundamental::Shape::Daily;
  for (const auto &[ym, m] : t.months) {
    const auto ro = remote_only.find(ym);
    st.full_cells += !daily ? 1 : m.inv.days + (ro == remote_only.end() ? 0 : ro->second);
  }
  return st;
}

// 全表统一快照 (与 SPECS 等长同序): 本地分片统计 + 核对统计同源产出
std::vector<TableStat> snapshot_tables(const Book &book, const Round &r) {
  std::vector<TableStat> out;
  out.reserve(fundamental::SPECS.size());
  for (const Spec &s : fundamental::SPECS) {
    TableStat st;
    const std::vector<Shard> &shards = r.cat.shards(s.name);
    if (!shards.empty()) {
      st.present = true;
      st.shards = shards.size();
      st.last_at = shards.back().at;
      auto newest = shards.front().mtime;
      for (const Shard &sh : shards) {
        st.bytes += sh.bytes;
        newest = std::max(newest, sh.mtime);
      }
      st.mtime = mtime_str(newest);
    }
    if (fundamental::audit::audited(s))
      st.audit = audit_stat_of(s, book, r);
    out.push_back(std::move(st));
  }
  return out;
}

template <class Src>
std::vector<Task> plan_specs(const Round &r, bool daily) {
  std::vector<Task> out;
  for (const Spec &s : fundamental::SPECS)
    if (fundamental::from<Src>(s) && (s.shape == fundamental::Shape::Daily) == daily)
      for (const Task &t : fundamental::sync::plan(s, r))
        out.push_back(t);
  return out;
}

// 表间乱序轮转: 相邻单元尽量来自不同表, 尽早暴露问题表
std::vector<Unit> interleave(std::vector<std::vector<Unit>> tables, const Round &r) {
  std::mt19937 rng(static_cast<unsigned>(r.now.at.time_since_epoch().count()));
  std::shuffle(tables.begin(), tables.end(), rng);
  std::size_t depth = 0;
  for (std::vector<Unit> &t : tables) {
    std::shuffle(t.begin(), t.end(), rng);
    depth = std::max(depth, t.size());
  }
  std::vector<Unit> out;
  for (std::size_t k = 0; k < depth; ++k)
    for (const std::vector<Unit> &t : tables)
      if (k < t.size())
        out.push_back(t[k]);
  return out;
}

// 单轨双阶段流水: 网络线程逐项 fetch(i+1), 写盘线程并发 put(i); 单槽背压限制驻留内存, 远端严格单并发
template <class OnStart, class OnDone>
Outcome pipe_tasks(Clients &clients, const std::atomic<bool> &cancel, std::string_view tag,
                   const std::vector<Task> &tasks, OnStart &&on_start, OnDone &&on_done) {
  log_line("[" + std::string(tag) + "] " + std::to_string(tasks.size()) + " item(s)");
  if (tasks.empty()) {
    log_line("[" + std::string(tag) + "] done");
    return Outcome::Done;
  }
  struct Pending {
    Task task;
    std::shared_ptr<arrow::Table> tbl;
    std::string lbl;
  };
  std::mutex mu;
  std::condition_variable cv;
  std::optional<Pending> slot;
  bool eof = false;

  std::thread writer([&] {
    while (true) {
      std::optional<Pending> cur;
      {
        std::unique_lock lk(mu);
        cv.wait(lk, [&] { return slot.has_value() || eof; });
        if (!slot)
          return;
        cur.swap(slot);
      }
      cv.notify_one();
      if (!cur->tbl) { // 远端 5xx: 不落盘, 下轮 plan 照旧排到
        log_line("[" + std::string(tag) + "] " + cur->lbl + " ... 远端 5xx, 跳过");
        on_done(0);
        continue;
      }
      const std::int64_t rows = cur->tbl->num_rows();
      fundamental::put(cur->task, cur->tbl);
      cur->tbl.reset();
      log_line("[" + std::string(tag) + "] " + cur->lbl + " ... " + std::to_string(rows) + " rows");
      on_done(rows);
    }
  });

  Outcome out = Outcome::Done;
  for (const Task &t : tasks) {
    if (cancel.load(std::memory_order_relaxed)) {
      out = Outcome::Cancelled;
      break;
    }
    const std::string lbl = fundamental::label(t);
    on_start(t, lbl);
    std::shared_ptr<arrow::Table> tbl = fundamental::fetch(clients, t);
    {
      std::unique_lock lk(mu);
      cv.wait(lk, [&] { return !slot.has_value(); });
      slot = Pending{t, std::move(tbl), lbl};
    }
    cv.notify_one();
  }
  {
    std::lock_guard lk(mu);
    eof = true;
  }
  cv.notify_one();
  writer.join();

  log_line("[" + std::string(tag) + "] " + (out == Outcome::Done ? "done" : "cancelled"));
  return out;
}

} // namespace

std::string short_time(std::time_t t) {
  const std::tm tm_buf = safe_localtime(&t);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%02d-%02d %02d:%02d", tm_buf.tm_mon + 1,
                tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min);
  return buf;
}

SyncService::SyncService() { Start(); }

SyncService::~SyncService() { Stop(); }

bool SyncService::run(SyncRequest &req) {
  if (!book_)
    book_ = std::make_unique<Book>();
  {
    std::lock_guard lk(mutex);
    state.op = req.op;
    state.table = req.table;
    state.incremental = req.incremental;
  }
  switch (req.op) {
  case SyncOp::Sync:
    return run_sync(req);
  case SyncOp::Audit:
    return run_audit(req);
  case SyncOp::Repair:
    return run_repair(req);
  }
  assert(false && "SyncService: 未知操作");
  return false;
}

SyncResult SyncService::build_and_publish(std::uint64_t seq, const Round &r) {
  const auto say = [this](std::string m) {
    std::lock_guard lk(mutex);
    state.message = std::move(m);
    publish();
  };
  SyncResult out;
  out.seq = seq;
  out.rebuilt = true;
  out.ok = out.asset_info.build(r, *this, say);
  std::vector<TableStat> tables = snapshot_tables(*book_, r);
  {
    std::lock_guard lk(mutex);
    state.tables = std::move(tables);
    if (!out.ok) {
      state.message = "本地 parquet 缺失 — 点击 Update 联网同步";
    } else {
      const AssetInfo &a = out.asset_info;
      state.stock_count = static_cast<std::size_t>(std::ranges::count_if(a.stocks, &StockInfo::known));
      state.factor_stock_count = 0;
      for (std::size_t id = 0; id < a.factors.size(); ++id)
        state.factor_stock_count += !a.factors[id].empty();
      state.trading_days_count = static_cast<std::size_t>(std::ranges::count_if(a.days, &CalendarDay::trading));
      state.date_range_start = a.days.front().date;
      state.date_range_end = a.days.back().date;
      state.last_update = now_str();
      state.message.clear();
    }
    publish();
  }
  if (out.ok)
    out.asset_info.events.seq = seq;
  return out;
}

bool SyncService::run_sync(const SyncRequest &req) {
  const auto edit = [this](auto &&fn) {
    std::lock_guard lk(mutex);
    fn(state);
    publish();
  };
  const auto say = [&](std::string m) { edit([&](SyncState &s) { s.message = std::move(m); }); };
  const auto set_quota = [&](const bigquant::Quota &q, bool start) {
    edit([&](SyncState &s) {
      if (start)
        s.quota_start = q.used;
      s.quota_weekly = q.weekly;
      s.quota_used = q.used;
      s.quota_time = now_str();
    });
  };
  const auto run_lane = [&](Lane l, Clients &clients, const std::vector<Task> &tasks) {
    const std::string tag = kLaneDef[l].tag;
    edit([&](SyncState &s) { s.lanes[l].run = LaneRun::Syncing; });
    const Outcome o = pipe_tasks(
        clients, cancel_, tag + ".sync", tasks,
        [&](const Task &t, const std::string &lbl) {
          edit([&](SyncState &s) {
            s.lanes[l].current = t.spec->name;
            s.message = "同步 " + tag + ": " + lbl;
          });
        },
        [&](std::int64_t rows) {
          std::lock_guard lk(mutex);
          ++state.lanes[l].done;
          state.lanes[l].rows += rows;
          publish(1);
        });
    edit([&](SyncState &s) {
      s.lanes[l].current.clear();
      s.lanes[l].run = o == Outcome::Done ? LaneRun::Done : LaneRun::Cancelled;
      s.lanes[l].time = short_time(std::time(nullptr));
    });
  };

  auto r = std::make_unique<Round>();
  if (req.fundamental || req.bar1m) {
    say("plan 判定 (水位/dedup 窗口)");
    Clients bq_clients;
    bool preflighted = false;
    bool wrote = false;

    // 交易日历先齐: 首轮或跨年首日确保 Round::days 完整后再排 bar1m 与快照表
    const std::vector<Task> cal_tasks =
        req.fundamental ? fundamental::sync::plan(fundamental::spec("all_trading_days"), *r) : std::vector<Task>{};
    if (!cal_tasks.empty()) {
      say("preflight: BigQuant 周配额");
      set_quota(bigquant::preflight(bq_clients.bq), true);
      preflighted = true;
      edit([n = cal_tasks.size()](SyncState &s) { s.lanes[kLaneBq] = {LaneRun::Syncing, n}; });
      begin(Phase::Loading, cal_tasks.size());
      run_lane(kLaneBq, bq_clients, cal_tasks);
      wrote = true;
      r = std::make_unique<Round>();
    }

    if (!cancel_.load(std::memory_order_relaxed)) {
      const std::vector<Task> bq_tasks = req.fundamental ? plan_specs<Bq>(*r, false) : std::vector<Task>{};
      const std::vector<Task> tu_tasks = req.fundamental ? plan_specs<Tu>(*r, false) : std::vector<Task>{};
      const std::vector<Task> bar1m_tasks = req.bar1m ? plan_specs<Bq>(*r, true) : std::vector<Task>{};
      const std::size_t cal_done = cal_tasks.size();
      const std::size_t rem = bq_tasks.size() + tu_tasks.size() + bar1m_tasks.size();

      edit([&](SyncState &s) {
        if (req.fundamental) {
          s.lanes[kLaneBq].run = LaneRun::Queued;
          s.lanes[kLaneBq].total = cal_done + bq_tasks.size();
          if (cal_done == 0)
            s.lanes[kLaneBq].done = 0, s.lanes[kLaneBq].rows = 0;
          s.lanes[kLaneTu] = {LaneRun::Queued, tu_tasks.size()};
        }
        if (req.bar1m)
          s.lanes[kLaneBar1m] = {LaneRun::Queued, bar1m_tasks.size()};
      });

      if (rem == 0 && cal_done == 0) {
        const std::string t = short_time(std::time(nullptr));
        edit([&](SyncState &s) {
          for (std::size_t i = 0; i < kLanes; ++i)
            if (const Lane l = Lane(i); req.*kLaneDef[l].on)
              s.lanes[l].run = LaneRun::Done, s.lanes[l].time = t;
        });
      } else {
        if ((!bq_tasks.empty() || !bar1m_tasks.empty()) && !preflighted) {
          say("preflight: BigQuant 周配额");
          set_quota(bigquant::preflight(bq_clients.bq), true);
          preflighted = true;
        }
        total.store(cal_done + rem, std::memory_order_relaxed);
        if (cal_done == 0)
          begin(Phase::Loading, rem);
        else
          publish();

        log_line("[sync] from " + std::to_string(fundamental::kSyncStart) +
                 " (lookback=" + std::to_string(fundamental::kLookbackDays) +
                 "d, dedup=" + std::to_string(fundamental::kDedupSeconds) + "s)");

        std::thread tu_thread;
        if (req.fundamental) {
          tu_thread = std::thread([&] {
            Clients tu_clients;
            run_lane(kLaneTu, tu_clients, tu_tasks);
          });
        }
        if (req.fundamental)
          run_lane(kLaneBq, bq_clients, bq_tasks);
        if (req.bar1m)
          run_lane(kLaneBar1m, bq_clients, bar1m_tasks);
        if (tu_thread.joinable())
          tu_thread.join();

        if (preflighted)
          set_quota(bq_clients.bq.quota(), false);
        wrote = true;
      }
    }
    if (wrote)
      r = std::make_unique<Round>();
  }

  handoff.post(build_and_publish(req.seq, *r));
  return true;
}

bool SyncService::run_audit(const SyncRequest &req) {
  const auto publish_snap = [this](const Round &r) {
    std::vector<TableStat> snap = snapshot_tables(*book_, r);
    std::lock_guard lk(mutex);
    state.tables = std::move(snap);
    publish();
  };
  const auto set_quota = [this](const bigquant::Quota &q, bool start) {
    std::lock_guard lk(mutex);
    if (start)
      state.quota_start = q.used;
    state.quota_weekly = q.weekly;
    state.quota_used = q.used;
    state.quota_time = now_str();
  };

  const Round r;
  Clients clients;
  set_quota(bigquant::preflight(clients.bq), true);

  std::vector<std::vector<Unit>> tables;
  for (const Spec &s : fundamental::SPECS) {
    if (!fundamental::audit::audited(s) || (!req.table.empty() && s.name != req.table))
      continue;
    std::set<std::string> done_m;
    if (req.incremental)
      for (const auto &[ym, _] : book_->sheet(s, r).months)
        done_m.insert(ym);
    tables.push_back(
        fundamental::audit::plan(s, r, [&](const Slot &sl) { return !done_m.contains(misc::dash_ym(sl.ym())); }));
  }
  const std::vector<Unit> units = interleave(std::move(tables), r);
  begin(Phase::Running, units.size());
  publish_snap(r);

  log_line("[bigquant.audit] " + (req.table.empty() ? std::string("全部表") : req.table) +
           (req.incremental ? " (增量) " : " ") + std::to_string(units.size()) + " item(s)");

  Outcome stop = Outcome::Done;
  for (const Unit &u : units) {
    if (cancel_.load(std::memory_order_relaxed)) {
      stop = Outcome::Cancelled;
      break;
    }
    const std::string lbl = fundamental::audit::label(u);
    {
      std::lock_guard lk(mutex);
      state.message = lbl;
      publish();
    }
    const fundamental::audit::Result res = fundamental::audit::run(clients, u);
    book_->put(u, res, stamp_hm(std::time(nullptr)));
    set_quota(clients.bq.quota(), false);
    std::vector<TableStat> snap = snapshot_tables(*book_, r);
    {
      std::lock_guard lk(mutex);
      state.tables = std::move(snap);
    }
    publish(1);
    log_line("[bigquant.audit] " + lbl + " ... " + std::to_string(res.diffs.size()) + " 处不一致");
  }
  log_line(std::string("[bigquant.audit] ") + (stop == Outcome::Done ? "done" : "cancelled"));

  {
    std::lock_guard lk(mutex);
    state.message.clear();
  }
  handoff.post({req.seq, false, false, {}});
  return stop == Outcome::Done;
}

bool SyncService::run_repair(const SyncRequest &req) {
  const Spec &s = fundamental::spec(req.table);
  auto r = std::make_unique<Round>();
  std::vector<std::string> groups;
  for (const Book::Row &row : book_->sheet(s, *r).rows)
    groups.push_back(row.group);
  assert(!groups.empty() && "repair: 该表无差异");

  const std::vector<Task> tasks = fundamental::audit::repair(s, *r, groups);
  std::set<misc::Ym> months;
  std::set<int> years;
  for (const Task &t : tasks) {
    const misc::Ym ym = misc::ym_of(t.at);
    months.insert(ym);
    years.insert(ym / 100);
  }

  const auto set_quota = [this](const bigquant::Quota &q, bool start) {
    std::lock_guard lk(mutex);
    if (start)
      state.quota_start = q.used;
    state.quota_weekly = q.weekly;
    state.quota_used = q.used;
    state.quota_time = now_str();
  };

  Clients clients;
  set_quota(bigquant::preflight(clients.bq), true);
  begin(Phase::Running, tasks.size() + years.size());

  const Outcome pulled = pipe_tasks(
      clients, cancel_, "bigquant.repair", tasks,
      [this](const Task &, const std::string &lbl) {
        std::lock_guard lk(mutex);
        state.message = "重拉 " + lbl;
        publish();
      },
      [this](std::int64_t) { publish(1); });
  set_quota(clients.bq.quota(), false);

  r = std::make_unique<Round>();
  if (pulled == Outcome::Done) {
    const std::vector<Unit> units =
        fundamental::audit::plan(s, *r, [&](const Slot &sl) { return months.contains(sl.ym()); });
    assert(units.size() == years.size() && "repair: 复核单元数不符");
    for (const Unit &u : units) {
      if (cancel_.load(std::memory_order_relaxed))
        break;
      const std::string lbl = fundamental::audit::label(u);
      {
        std::lock_guard lk(mutex);
        state.message = "复核 " + lbl;
        publish();
      }
      const fundamental::audit::Result res = fundamental::audit::run(clients, u);
      book_->put(u, res, stamp_hm(std::time(nullptr)));
      set_quota(clients.bq.quota(), false);
      publish(1);
      log_line("[bigquant.repair] " + lbl + " ... " + std::to_string(res.diffs.size()) + " 处不一致");
    }
  }

  handoff.post(build_and_publish(req.seq, *r));
  return true;
}

} // namespace GUI::Database
