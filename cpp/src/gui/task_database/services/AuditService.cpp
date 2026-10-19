#include "gui/task_database/services/AuditService.hpp"

#include "api/bigquant/client.hpp"
#include "fundamental/Audit.hpp"
#include "fundamental/Store.hpp"
#include "fundamental/Sync.hpp"
#include "misc/date.hpp"
#include "misc/fs.hpp"
#include "misc/parquet.hpp"

#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>

namespace GUI::Database {

namespace {

namespace fs = std::filesystem;
using fundamental::Grain;

// output/fundamental/_audit/ (逐表状态 <表>.tsv); 不放 output/log: 特征计算开跑会整删 log_dir
fs::path log_dir() {
  const auto dir = misc::git_root() / "output" / "fundamental" / "_audit";
  fs::create_directories(dir);
  return dir;
}

// "YYYY-MM-DD HH:MM" (本地时区)
std::string stamp(std::time_t t) {
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[20];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", &tm_buf);
  return buf;
}

// 一个不一致格 (只差 n: column / cell / local / remote 为 "-"; 统计只用到前几列, 原值留给人看)
struct LogRow {
  std::string group; // 定位组键 (Month / Day / Asset 级); 前 7 位 = 所属月, 前 10 位 = 所属日
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::string column = "-";
  std::string cell = "-"; // "nn" / "s"
  std::string local = "-";
  std::string remote = "-";
};

// Diff → 缓存行: 只差 n 一行; 否则每个不一致格一行
void append_rows(std::vector<LogRow> &out, const fundamental::audit::Diff &d) {
  if (d.cells.empty()) {
    out.push_back({d.group, d.local_n, d.remote_n});
    return;
  }
  for (const fundamental::audit::Diff::Cell &c : d.cells)
    out.push_back({d.group, d.local_n, d.remote_n, c.column, c.sum ? "s" : "nn", c.local, c.remote});
}

// 组 → (本地 n, 远端 n): 同组多格的行只计一次
std::map<std::string, std::pair<std::int64_t, std::int64_t>> group_n(const std::vector<LogRow> &rows) {
  std::map<std::string, std::pair<std::int64_t, std::int64_t>> out;
  for (const LogRow &r : rows)
    out[r.group] = {r.local_n, r.remote_n};
  return out;
}

// 已核月的本地清点 (分母 + 流量), 取自核对当时的分片 (Daily: 该月各日分片累加)
struct MonthInv {
  std::int64_t rows = 0;
  std::int64_t days = 0;
  std::int64_t cols = 0;
};

// 一个分片的清点
MonthInv count_shard(const fundamental::Spec &s, const fundamental::Shard &sh) {
  MonthInv m;
  m.cols = parquet::ParquetFileReader::OpenFile(sh.path.string())->metadata()->num_columns();
  auto t = fundamental::read(sh, {s.vd});
  m.rows = t->num_rows();
  std::set<std::int32_t> days;
  std::vector<std::int32_t> d;
  for (const auto &chunk : t->column(0)->chunks()) {
    misc::pq::Col(chunk).yyyymmdd_all(d);
    days.insert(d.begin(), d.end());
  }
  m.days = static_cast<std::int64_t>(days.size());
  return m;
}

struct MonthRec {
  MonthInv inv;
  std::string audited_at; // 核对时刻 "YYYY-MM-DD HH:MM"; 空 = 未核
};

// 一表的核对状态: 每月只留最近一次核对的结论
struct TableBook {
  std::map<std::string, MonthRec> months; // "YYYY-MM" (组键前 7 位同形)
  std::vector<LogRow> rows;
};
using Books = std::map<std::string, TableBook>; // 表名 → 状态

// 一单元跑完: 覆盖该表单元各月的清点 / 核对时刻 / 差异格
void apply(TableBook &t, const fundamental::audit::Unit &u, const std::vector<fundamental::audit::Diff> &diffs,
           const std::string &at) {
  std::set<std::string> keys;
  for (const fundamental::Slot &sl : u.slots) {
    const std::string key = misc::dash_ym(sl.ym);
    MonthRec &m = t.months[key];
    if (keys.insert(key).second)
      m.inv = {};
    const MonthInv one = count_shard(*u.spec, *sl.sh);
    m.inv.rows += one.rows;
    m.inv.days += one.days;
    m.inv.cols = std::max(m.inv.cols, one.cols);
    m.audited_at = at;
  }
  std::erase_if(t.rows, [&](const LogRow &r) { return keys.contains(r.group.substr(0, 7)); });
  for (const auto &d : diffs)
    append_rows(t.rows, d);
}

// 分片重拉过: 这些月的旧结论作废 (待复核)
void forget(TableBook &t, const std::set<std::string> &keys) {
  for (const std::string &k : keys)
    t.months.erase(k);
  std::erase_if(t.rows, [&](const LogRow &r) { return keys.contains(r.group.substr(0, 7)); });
}

std::vector<std::string> split_tab(const std::string &line) {
  std::vector<std::string> f;
  std::size_t s = 0;
  for (std::size_t i = 0; i <= line.size(); ++i)
    if (i == line.size() || line[i] == '\t') {
      f.push_back(line.substr(s, i - s));
      s = i + 1;
    }
  return f;
}

// 状态文件 <表>.tsv (# 起头为注释):
//   month \t YYYY-MM \t 核对时刻 (- = 未核) \t 本地行 \t 本地天数 \t 列数
//         \t 问题天数 \t 行差+ \t 行差−   (末三列由 diff 导出, 只供人看, load 不读)
//   diff  \t 组 \t local_n \t remote_n \t 列 \t nn|s \t 本地 \t 远端
void save(const std::string &name, const TableBook &t) {
  struct MonthSum {
    std::set<std::string> bad;
    std::int64_t added = 0, removed = 0;
  };
  std::map<std::string, MonthSum> sum;
  for (const auto &[g, n] : group_n(t.rows)) {
    MonthSum &s = sum[g.substr(0, 7)];
    s.bad.insert(g.substr(0, 10));
    s.added += std::max<std::int64_t>(0, n.second - n.first);
    s.removed += std::max<std::int64_t>(0, n.first - n.second);
  }
  auto at = [](const std::string &s) { return s.empty() ? std::string("-") : s; };

  const fs::path p = log_dir() / (name + ".tsv");
  const fs::path tmp = p.string() + ".tmp";
  {
    std::ofstream out(tmp);
    assert(out && "audit: 状态文件无法写入");
    out << "# " << name << "\n"
        << "# month\tym\taudited_at\tlocal_rows\tlocal_days\tcols\tbad_days\tadded\tremoved\n"
        << "# diff\tgroup\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
    for (const auto &[ym, m] : t.months) {
      const auto s = sum.find(ym);
      const bool bad = s != sum.end();
      out << "month\t" << ym << '\t' << at(m.audited_at) << '\t' << m.inv.rows << '\t'
          << m.inv.days << '\t' << m.inv.cols << '\t' << (bad ? s->second.bad.size() : 0) << '\t'
          << (bad ? s->second.added : 0) << '\t' << (bad ? s->second.removed : 0) << '\n';
    }
    std::vector<const LogRow *> rows;
    for (const LogRow &r : t.rows)
      rows.push_back(&r);
    std::stable_sort(rows.begin(), rows.end(), [](const LogRow *x, const LogRow *y) { return x->group < y->group; });
    for (const LogRow *r : rows)
      out << "diff\t" << r->group << '\t' << r->local_n << '\t' << r->remote_n << '\t' << r->column << '\t'
          << r->cell << '\t' << r->local << '\t' << r->remote << '\n';
    out.flush();
    assert(out && "audit: 状态文件写入失败");
  }
  fs::rename(tmp, p);
}

std::optional<std::int64_t> to_int(const std::string &s) {
  std::int64_t v = 0;
  const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc{} || p != s.data() + s.size())
    return std::nullopt;
  return v;
}

// 与 grain_of / key_day / key_ym 同形: YYYY-MM[-DD[|资产]]
bool valid_group(const std::string &g) {
  const std::string_view v = g;
  if (misc::parse_ym(v.substr(0, 7)) == 0)
    return false;
  if (g.size() == 7)
    return true;
  return misc::parse_ymd(v.substr(0, 10)) > 0 && (g.size() == 10 || (g.size() > 11 && g[10] == '|'));
}

// 一个状态文件; 表不参与核对 (audit::audited) / 任一行格式不符 / 差异月无清点 → nullopt
std::optional<TableBook> parse_book(const fs::path &p) {
  const std::string name = p.stem().string();
  const auto s = std::ranges::find_if(fundamental::SPECS, [&](const fundamental::Spec &x) { return x.name == name; });
  if (s == fundamental::SPECS.end() || !fundamental::audit::audited(*s))
    return std::nullopt;
  TableBook t;
  std::ifstream in(p);
  assert(in && "audit: 状态文件无法打开");
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line.starts_with("#"))
      continue;
    const std::vector<std::string> f = split_tab(line);
    if (f[0] == "month") {
      if (f.size() != 9 || misc::parse_ym(f[1]) == 0)
        return std::nullopt;
      const auto rows = to_int(f[3]), days = to_int(f[4]), cols = to_int(f[5]);
      if (!rows || !days || !cols)
        return std::nullopt;
      MonthRec &m = t.months[f[1]];
      m.audited_at = f[2] == "-" ? "" : f[2];
      m.inv = {*rows, *days, *cols};
      continue;
    }
    if (f[0] != "diff" || f.size() != 8 || !valid_group(f[1]))
      return std::nullopt;
    const auto local_n = to_int(f[2]), remote_n = to_int(f[3]);
    if (!local_n || !remote_n)
      return std::nullopt;
    t.rows.push_back({f[1], *local_n, *remote_n, f[4], f[5], f[6], f[7]});
  }
  for (const LogRow &r : t.rows)
    if (!t.months.contains(r.group.substr(0, 7)))
      return std::nullopt;
  return t;
}

// log_dir 下全部 <表>.tsv (.tmp 残留不读); 解析不过的直接删 (旧格式等), 该表视为未核, 重新 audit 即可
Books load_book() {
  Books b;
  for (const auto &e : fs::directory_iterator(log_dir())) {
    if (e.path().extension() != ".tsv")
      continue;
    std::optional<TableBook> t = parse_book(e.path());
    if (!t) {
      std::cout << "[bigquant.audit] 状态文件格式不符, 已删除: " << e.path().string() << std::endl;
      fs::remove(e.path());
      continue;
    }
    b[e.path().stem().string()] = std::move(*t);
  }
  return b;
}

// 每张参与核对的表当前冻结的月数 (Daily: 含冻结日的月; 与 audit::plan 同一来源; 只看目录与 mtime)
std::map<std::string, std::size_t> count_frozen() {
  const fundamental::Catalog cat;
  const fundamental::Now now = fundamental::now();
  std::map<std::string, std::size_t> out;
  for (const fundamental::Spec &s : fundamental::SPECS) {
    if (!fundamental::audit::audited(s))
      continue;
    std::set<misc::Ym> months;
    for (const fundamental::Slot &sl : fundamental::frozen(s, cat, now))
      months.insert(sl.ym);
    out[s.name] = months.size();
  }
  return out;
}

// 一表状态 → 统计
AuditTableStat stat_of(const std::string &name, const TableBook &t, std::size_t frozen) {
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
    a.st.audited_months += !m.audited_at.empty();
    a.st.updated = std::max(a.st.updated, m.audited_at);
  }
  for (const LogRow &r : t.rows)
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

  // 差异行的月必有清点 (apply 同步写入, forget 同步删除)
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

// 逐表统计 (表名升序): 冻结清单里的表恒有一条, 另含有状态文件的表
std::vector<AuditTableStat> aggregate(const Books &b, const std::map<std::string, std::size_t> &frozen) {
  std::set<std::string> names;
  for (const auto &[name, n] : frozen)
    names.insert(name);
  for (const auto &[name, t] : b)
    names.insert(name);
  const TableBook empty;
  std::vector<AuditTableStat> out;
  for (const std::string &name : names) {
    const auto t = b.find(name);
    const auto f = frozen.find(name);
    out.push_back(stat_of(name, t == b.end() ? empty : t->second, f == frozen.end() ? 0 : f->second));
  }
  return out;
}

} // namespace

const AuditTableStat *AuditState::find_table(const std::string &name) const {
  for (const AuditTableStat &t : tables)
    if (t.table == name)
      return &t;
  return nullptr;
}

// worker 独占的核对状态 + 写口: 计步走进度面 (svc 即 Published), 展示字段持 svc.mutex 写进 svc.state
struct AuditService::Book {
  AuditService &svc;
  Books books;                               // 表名 → 状态 (跨轮承接)
  std::map<std::string, std::size_t> frozen; // 每表冻结月数 (覆盖率分母)

  // 统计快照 → state.tables (每单元后整份重算: 表数十张, 月数百, 远低于一次 DoGet)
  void publish() {
    std::vector<AuditTableStat> snap = aggregate(books, frozen);
    std::lock_guard lk(svc.mutex);
    svc.state.tables = std::move(snap);
    svc.publish();
  }

  // 整写该表状态文件 + 发布 (写盘不持锁: books 只 worker 碰)
  void commit(const std::string &name) {
    save(name, books.at(name));
    publish();
  }

  void set_current(std::string s) {
    std::lock_guard lk(svc.mutex);
    svc.state.current = std::move(s);
  }

  void start_quota(bigquant::Client &client) {
    const bigquant::Quota q = client.quota();
    std::lock_guard lk(svc.mutex);
    svc.state.quota_start = q.used;
    svc.state.quota_used = q.used;
    svc.state.quota_weekly = q.weekly;
  }

  // 一步跑完: 配额 + 计步
  void step_done(bigquant::Client &client) {
    const bigquant::Quota q = client.quota();
    {
      std::lock_guard lk(svc.mutex);
      svc.state.quota_used = q.used;
    }
    svc.publish(1);
  }

  // 核一单元 → 覆盖该表状态
  void audit_unit(bigquant::Client &client, const fundamental::audit::Unit &u) {
    const std::string label = fundamental::audit::label(u);
    set_current(label);
    const auto diffs = fundamental::audit::run(client, u);
    if (!diffs.empty())
      std::cout << "[bigquant.audit] " << label << ": " << diffs.size() << " 处不一致" << std::endl;
    apply(books[u.spec->name], u, diffs, stamp(std::time(nullptr)));
    step_done(client);
    commit(u.spec->name);
  }

  // 开跑: 计划步数 + 覆盖率分母
  void start(std::size_t steps) {
    frozen = count_frozen();
    svc.begin(Phase::Running, steps);
    publish();
  }
};

AuditService::AuditService()
    : book_(new Book{*this, {}, {}}) {
  state.log_dir = log_dir().string();
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
  book_->set_current({});
  handoff.post(req.seq);
  return completed;
}

bool AuditService::load() {
  book_->books = load_book();
  book_->frozen = count_frozen();
  book_->publish();
  return true;
}

// audit::plan (table 空 = 全部表, 否则只该表; incremental = 跳过状态里已核的月)
//   → 逐单元 audit::run (月扫描 + 逐级定位), 每单元覆盖该 (表, 月)
bool AuditService::audit(const std::string &table, bool incremental) {
  Book &bk = *book_;
  const fundamental::Now now = fundamental::now();
  const fundamental::Catalog cat;
  bigquant::Client client;
  bk.start_quota(client);
  // 已核 = 状态里该月有核对时刻 (plan 在开跑前算完, 之后 books 的改动不影响)
  auto done = [&](const std::string &name, misc::Ym ym) {
    const auto t = bk.books.find(name);
    if (t == bk.books.end())
      return false;
    const auto m = t->second.months.find(misc::dash_ym(ym));
    return m != t->second.months.end() && !m->second.audited_at.empty();
  };
  const auto units = fundamental::audit::plan(
      cat, now, static_cast<unsigned>(std::time(nullptr)), [&](const fundamental::Spec &sp, misc::Ym ym) {
        return (table.empty() || sp.name == table) && !(incremental && done(sp.name, ym));
      });
  bk.start(units.size());
  const std::string scope = (table.empty() ? "全部表" : table) + (incremental ? " (增量)" : "");
  std::cout << "[bigquant.audit] " << scope << ": " << units.size() << " units" << std::endl;
  for (const fundamental::audit::Unit &u : units) {
    if (cancel_.load(std::memory_order_relaxed))
      break;
    bk.audit_unit(client, u);
  }
  const bool cancelled = cancel_.load(std::memory_order_relaxed);
  std::cout << "[bigquant.audit] " << scope << ": " << (cancelled ? "cancelled" : "done") << std::endl;
  return !cancelled;
}

// table 的差异: audit::repair → 逐任务 sync::run (Splice 日·资产 / Replace 整月 / Daily 逐日 Replace) → 这些月旧结论作废 → plan(keep = 这些月) 复核
bool AuditService::repair(const std::string &table) {
  Book &bk = *book_;
  const fundamental::Spec &s = fundamental::spec(table);
  std::vector<std::string> groups;
  for (const LogRow &r : bk.books.at(table).rows)
    groups.push_back(r.group);
  assert(!groups.empty() && "repair: 该表无差异月");

  bigquant::Client client;
  bk.start_quota(client);

  std::set<std::string> keys; // 有差异的月 "YYYY-MM"
  std::set<int> years;
  {
    // 定位键 → 修复任务 (整月 Replace / 日·资产 Splice / Daily 逐日 Replace, 见 audit::repair)
    const fundamental::Catalog cat;
    const std::vector<fundamental::Task> tasks =
        fundamental::audit::repair(s, fundamental::frozen(s, cat, fundamental::now()), groups);
    for (const fundamental::Task &t : tasks) {
      keys.insert(misc::dash_ym(t.ym));
      years.insert(t.ym / 100);
    }
    // 重拉月 + 复核单元 (一表 × 一年)
    bk.start(tasks.size() + years.size());
    for (const fundamental::Task &t : tasks) {
      if (cancel_.load(std::memory_order_relaxed))
        return false;
      const std::string label = fundamental::sync::label(t);
      bk.set_current("重拉 " + label);
      std::cout << "[bigquant.repair] " << label << " ... " << std::flush;
      const std::int64_t rows = fundamental::sync::run<fundamental::Bq>(client, t);
      std::cout << rows << " rows" << std::endl;
      bk.step_done(client);
    }
  }
  forget(bk.books.at(table), keys);
  bk.commit(table);

  // 重拉换了分片: 新清单
  const fundamental::Catalog cat;
  const auto units = fundamental::audit::plan(
      cat, fundamental::now(), static_cast<unsigned>(std::time(nullptr)),
      [&](const fundamental::Spec &sp, misc::Ym ym) { return &sp == &s && keys.contains(misc::dash_ym(ym)); });
  assert(units.size() == years.size() && "repair: 复核单元数不符");
  for (const fundamental::audit::Unit &u : units) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    bk.audit_unit(client, u);
  }
  std::cout << "[bigquant.repair] " << table << " done" << std::endl;
  return true;
}

} // namespace GUI::Database
