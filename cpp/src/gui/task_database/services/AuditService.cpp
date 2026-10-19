#include "gui/task_database/services/AuditService.hpp"

#include "api/bigquant/client.hpp"
#include "fundamental/Audit.hpp"
#include "fundamental/Store.hpp"
#include "fundamental/Sync.hpp"
#include "gui/task_database/services/Drive.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <ctime>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>

namespace GUI::Database {

namespace {

using fundamental::Grain;
using fundamental::Round;
using fundamental::Slot;
using fundamental::Spec;
using fundamental::Task;
using fundamental::audit::Book;
using fundamental::audit::Unit;

// "YYYY-MM-DD HH:MM" (本地时区)
std::string stamp(std::time_t t) {
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[20];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", &tm_buf);
  return buf;
}

// 组 → (本地 n, 远端 n): 同组多格的行只计一次
std::map<std::string, std::pair<std::int64_t, std::int64_t>> group_n(const std::vector<Book::Row> &rows) {
  std::map<std::string, std::pair<std::int64_t, std::int64_t>> out;
  for (const Book::Row &r : rows)
    out[r.group] = {r.local_n, r.remote_n};
  return out;
}

// 表当前冻结的月数 (覆盖率分母; Daily: 含冻结日的月; 与 audit::plan 同一来源)
std::size_t frozen_months(const Spec &s, const Round &r) {
  std::set<misc::Ym> months;
  for (const Slot &sl : fundamental::frozen(s, r))
    months.insert(sl.ym);
  return months.size();
}

// 一表的有效结论 → 统计
AuditTableStat stat_of(const std::string &name, const Book::Sheet &t, std::size_t frozen) {
  struct Acc {
    AuditTableStat st;
    std::set<std::string> bad, row_bad, content_bad, months;
    std::map<std::string, std::set<std::string>> col_days; // 改值列 → 涉及的日
    std::map<std::string, std::int64_t> delta;             // ym → 远端 − 本地行数
  };
  Acc a;
  a.st.frozen_months = frozen;
  for (const auto &[ym, m] : t.months) {
    a.st.local_days += m.inv.days;
    a.st.local_rows += m.inv.rows;
    ++a.st.audited_months;
    a.st.updated = std::max(a.st.updated, m.at);
  }
  for (const Book::Row &r : t.rows)
    if (r.column != "-")
      a.col_days[r.column].insert(r.group.substr(0, 10));
  // 修复粒度 = 定位组 (与 audit::repair 同一来源): Month 键整月 Replace, Day / Asset 键只拉该组; 流量 = 组远端行 × 列
  std::set<std::string> whole;
  for (const auto &[g, n] : group_n(t.rows)) {
    const auto [local_n, remote_n] = n;
    const std::string ym = g.substr(0, 7), day = g.substr(0, 10);
    const Grain grain = fundamental::grain_of(g);
    a.bad.insert(day);
    a.months.insert(ym);
    (local_n != remote_n ? a.row_bad : a.content_bad).insert(day);
    if (grain == Grain::Month)
      whole.insert(ym);
    if (grain == Grain::Day && local_n == 0)
      ++a.st.remote_only_days;
    a.st.added += std::max<std::int64_t>(0, remote_n - local_n);
    a.st.removed += std::max<std::int64_t>(0, local_n - remote_n);
    a.delta[ym] += remote_n - local_n;
    assert(t.months.contains(ym) && "audit: 差异月无清点");
    a.st.repair_cells += remote_n * t.months.at(ym).inv.cols;
  }

  AuditTableStat &st = a.st;
  st.table = name;
  st.bad_days = a.bad.size();
  st.row_days = a.row_bad.size();
  st.content_days = a.content_bad.size();
  st.bad_months = a.months.size();
  st.whole_months = whole.size();

  // 差异行的月必有清点 (Book::put 同步写入, Book::sheet 同月同滤)
  for (const auto &[ym, m] : t.months) {
    const auto d = a.delta.find(ym);
    st.full_cells += (m.inv.rows + (d == a.delta.end() ? 0 : d->second)) * m.inv.cols;
  }

  constexpr std::size_t kTop = 3;
  std::vector<std::pair<std::size_t, std::string>> cols;
  for (const auto &[c, days] : a.col_days)
    cols.emplace_back(days.size(), c);
  std::sort(cols.begin(), cols.end(), [](const auto &x, const auto &y) {
    return x.first != y.first ? x.first > y.first : x.second < y.second;
  });
  for (std::size_t i = 0; i < cols.size() && i < kTop; ++i) {
    if (i > 0)
      st.top_columns += ", ";
    st.top_columns += cols[i].second + " " + std::to_string(cols[i].first);
  }
  if (cols.size() > kTop)
    st.top_columns += " +" + std::to_string(cols.size() - kTop);
  return std::move(st);
}

// 逐表统计 (表名升序): 参与核对的表恒有一条; 只计 r 视角下仍有效的结论
std::vector<AuditTableStat> aggregate(const Book &b, const Round &r) {
  std::vector<AuditTableStat> out;
  for (const Spec &s : fundamental::SPECS)
    if (fundamental::audit::audited(s))
      out.push_back(stat_of(s.name, b.sheet(s, r), frozen_months(s, r)));
  std::sort(out.begin(), out.end(), [](const AuditTableStat &x, const AuditTableStat &y) { return x.table < y.table; });
  return out;
}

} // namespace

const AuditTableStat *AuditState::find_table(const std::string &name) const {
  for (const AuditTableStat &t : tables)
    if (t.table == name)
      return &t;
  return nullptr;
}

// worker 独占: 核对账本 + 本轮视图 + 写口 (计步走进度面 svc, 展示字段持 svc.mutex 写进 svc.state)
struct AuditService::Worker {
  AuditService &svc;
  std::optional<Book> book;     // 核对账本 (Load 读入, 跨轮承接)
  std::unique_ptr<Round> round; // 本轮的本地视图 (每轮开头 / 改盘后换新)

  void refresh() { round = std::make_unique<Round>(); }

  // 统计快照 → state.tables (每单元后整份重算: 表数十张, 月数百, 远低于一次 DoGet)
  void publish() {
    std::vector<AuditTableStat> snap = aggregate(*book, *round);
    std::lock_guard lk(svc.mutex);
    svc.state.tables = std::move(snap);
    svc.publish();
  }

  void say(const std::string &s) {
    std::lock_guard lk(svc.mutex);
    svc.state.current = s;
  }

  // 配额读数: start = 开跑时 (quota_start 起点), 否则一步跑完后
  void quota(bigquant::Client &client, bool start = false) {
    const bigquant::Quota q = client.quota();
    std::lock_guard lk(svc.mutex);
    if (start) {
      svc.state.quota_start = q.used;
      svc.state.quota_weekly = q.weekly;
    }
    svc.state.quota_used = q.used;
  }

  // 开跑: 计划步数
  void begin(std::size_t steps) {
    svc.begin(Phase::Running, steps);
    publish();
  }

  // 核一单元 → 结论入账本 (Book::put 整写该表文件) → 配额 → 发布 (drive 的 step)
  std::string check(bigquant::Client &client, const Unit &u) {
    const fundamental::audit::Result res = fundamental::audit::run<fundamental::Bq>(client, u);
    book->put(u, res, stamp(std::time(nullptr)));
    quota(client);
    publish();
    return std::to_string(res.diffs.size()) + " 处不一致";
  }
};

AuditService::AuditService()
    : worker_(new Worker{*this, {}, {}}) {
  state.log_dir = Book::dir().string();
  Start();
}

AuditService::~AuditService() { Stop(); }

// 异常不捕获 (fail fast). 每轮必 post 一件交接 (取消也发: 状态文件可能已改)
bool AuditService::run(AuditRequest &req) {
  {
    std::lock_guard lk(mutex);
    state.op = req.op;
    state.table = req.table;
    state.incremental = req.incremental;
  }
  bool completed = false;
  switch (req.op) {
  case AuditOp::Load:
    completed = load();
    break;
  case AuditOp::Audit:
    completed = audit(req.table, req.incremental);
    break;
  case AuditOp::Repair:
    completed = repair(req.table);
    break;
  }
  worker_->say({});
  handoff.post(req.seq);
  return completed;
}

bool AuditService::load() {
  Worker &w = *worker_;
  w.book.emplace();
  w.refresh();
  w.publish();
  return true;
}

// audit::plan<Bq> (table 空 = 全部表, 否则只该表; incremental = 跳过仍有效的已核月)
//   → drive 逐单元 audit::run (月扫描 + 逐级定位) → Book::put 覆盖该 (表, 月)
bool AuditService::audit(const std::string &table, bool incremental) {
  Worker &w = *worker_;
  w.refresh();
  bigquant::Client client;
  w.quota(client, true);
  // 已核 = 有效结论里有该月 (plan 在开跑前算完, 之后账本的改动不影响)
  std::map<std::string, std::set<std::string>> done;
  if (incremental)
    for (const Spec &s : fundamental::SPECS)
      if (fundamental::audit::audited(s))
        for (const auto &[ym, e] : w.book->sheet(s, *w.round).months)
          done[s.name].insert(ym);
  const std::vector<Unit> units = fundamental::audit::plan<fundamental::Bq>(*w.round, [&](const Spec &sp, const Slot &sl) {
    if (!table.empty() && sp.name != table)
      return false;
    const auto it = done.find(sp.name);
    return it == done.end() || !it->second.contains(misc::dash_ym(sl.ym));
  });
  w.begin(units.size());
  std::cout << "[bigquant.audit] " << (table.empty() ? "全部表" : table) << (incremental ? " (增量)" : "") << std::endl;
  return drive(*this, cancel_, "bigquant.audit", units, [&](const std::string &l) { w.say(l); }, [&](const Unit &u) { return w.check(client, u); }) == Outcome::Done;
}

// table 的差异: audit::repair → drive 逐任务 sync::run (整槽 Replace / Splice 日·资产)
//   → 新视图 (重写的月版本变, 旧结论自动作废) → plan(keep = 这些月) → drive 复核
bool AuditService::repair(const std::string &table) {
  Worker &w = *worker_;
  const Spec &s = fundamental::spec(table);
  w.refresh();
  std::vector<std::string> groups;
  for (const Book::Row &r : w.book->sheet(s, *w.round).rows)
    groups.push_back(r.group);
  assert(!groups.empty() && "repair: 该表无差异");

  bigquant::Client client;
  w.quota(client, true);
  const std::vector<Task> tasks = fundamental::audit::repair(s, *w.round, groups);
  std::set<misc::Ym> months; // 重拉的月
  std::set<int> years;       // 复核单元 (一表 × 一年)
  for (const Task &t : tasks) {
    months.insert(t.ym);
    years.insert(t.ym / 100);
  }
  w.begin(tasks.size() + years.size());
  const Outcome pulled = drive(*this, cancel_, "bigquant.repair", tasks, [&](const std::string &l) { w.say("重拉 " + l); }, [&](const Task &t) {
                              const std::int64_t rows = fundamental::sync::run<fundamental::Bq>(client, t);
                              w.quota(client);
                              return std::to_string(rows) + " rows"; });
  if (pulled != Outcome::Done)
    return false;

  w.refresh();
  w.publish();
  const std::vector<Unit> units = fundamental::audit::plan<fundamental::Bq>(
      *w.round, [&](const Spec &sp, const Slot &sl) { return &sp == &s && months.contains(sl.ym); });
  assert(units.size() == years.size() && "repair: 复核单元数不符");
  return drive(*this, cancel_, "bigquant.repair", units, [&](const std::string &l) { w.say(l); }, [&](const Unit &u) { return w.check(client, u); }) == Outcome::Done;
}

} // namespace GUI::Database
