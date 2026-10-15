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
#include <atomic>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>

namespace GUI::Database {

namespace {

namespace fs = std::filesystem;
using fundamental::audit::Pass;

// output/log/audit/ (逐表状态 <表>.tsv)
fs::path log_dir() {
  const auto dir = misc::git_root() / "output" / "log" / "audit";
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

// 一个不一致格 (Rows 轮: column / cell / local / remote 为 "-"; 统计只用到前几列, 原值留给人看)
struct LogRow {
  std::string group; // "YYYY-MM-DD" / 回落 "YYYY-MM"; 前 7 位 = 所属月
  bool rows = false; // pass == rows
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::string column = "-";
  std::string cell = "-"; // "nn" / "s"
  std::string local = "-";
  std::string remote = "-";
};

// Diff → 缓存行: Rows 一行; Content 每个不一致格一行
void append_rows(std::vector<LogRow> &out, const fundamental::audit::Diff &d) {
  const bool rows = d.pass == Pass::Rows;
  if (d.cells.empty()) {
    out.push_back({d.group, rows, d.local_n, d.remote_n});
    return;
  }
  for (const fundamental::audit::Diff::Cell &c : d.cells)
    out.push_back({d.group, rows, d.local_n, d.remote_n, c.column, c.sum ? "s" : "nn", c.local, c.remote});
}

// 已核月的本地清点 (分母 + 流量), 取自核对当时的分片
struct MonthInv {
  std::int64_t rows = 0;
  std::int64_t days = 0;
  std::int64_t cols = 0;
};

MonthInv count_month(const fundamental::Spec &s, const fundamental::Shard &sh) {
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
  std::string rows_at;    // Rows 轮核对时刻 "YYYY-MM-DD HH:MM"; 空 = 未跑
  std::string content_at; // Content 轮同上
};

// 一表的核对状态: 每个 (月, 轮) 只留最近一次核对的结论
struct TableBook {
  std::map<std::string, MonthRec> months; // "YYYY-MM" (组键前 7 位同形)
  std::vector<LogRow> rows;
};
using Book = std::map<std::string, TableBook>; // 表名 → 状态

// 一单元跑完: 覆盖该表 (单元各月, 轮) 的清点 / 核对时刻 / 差异格
void apply(TableBook &t, const fundamental::audit::Unit &u, const std::vector<fundamental::audit::Diff> &diffs,
           const std::string &at) {
  const bool rows = u.pass == Pass::Rows;
  std::set<std::string> keys;
  for (const fundamental::Slot &sl : u.months) {
    const std::string key = misc::dash_ym(sl.ym);
    MonthRec &m = t.months[key];
    m.inv = count_month(*u.spec, *sl.sh);
    (rows ? m.rows_at : m.content_at) = at;
    keys.insert(key);
  }
  std::erase_if(t.rows, [&](const LogRow &r) { return r.rows == rows && keys.contains(r.group.substr(0, 7)); });
  for (const auto &d : diffs)
    append_rows(t.rows, d);
}

// 分片重拉过: 这些月两轮的旧结论一并作废 (待复核)
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
//   month \t YYYY-MM \t rows 核对时刻 \t content 核对时刻 (- = 未跑) \t 本地行 \t 本地天数 \t 列数
//         \t 问题天数 \t 行差+ \t 行差−   (末三列由 diff 导出, 只供人看, load 不读)
//   diff  \t 组 \t rows|content \t local_n \t remote_n \t 列 \t nn|s \t 本地 \t 远端
void save(const std::string &name, const TableBook &t) {
  struct MonthSum {
    std::set<std::string> bad;
    std::int64_t added = 0, removed = 0;
  };
  std::map<std::string, MonthSum> sum;
  for (const LogRow &r : t.rows) {
    MonthSum &s = sum[r.group.substr(0, 7)];
    s.bad.insert(r.group);
    if (r.rows) {
      s.added += std::max<std::int64_t>(0, r.remote_n - r.local_n);
      s.removed += std::max<std::int64_t>(0, r.local_n - r.remote_n);
    }
  }
  auto at = [](const std::string &s) { return s.empty() ? std::string("-") : s; };

  const fs::path p = log_dir() / (name + ".tsv");
  const fs::path tmp = p.string() + ".tmp";
  {
    std::ofstream out(tmp);
    assert(out && "audit: 状态文件无法写入");
    out << "# " << name << "\n"
        << "# month\tym\trows_at\tcontent_at\tlocal_rows\tlocal_days\tcols\tbad_days\tadded\tremoved\n"
        << "# diff\tgroup\tpass\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
    for (const auto &[ym, m] : t.months) {
      const auto s = sum.find(ym);
      const bool bad = s != sum.end();
      out << "month\t" << ym << '\t' << at(m.rows_at) << '\t' << at(m.content_at) << '\t' << m.inv.rows << '\t'
          << m.inv.days << '\t' << m.inv.cols << '\t' << (bad ? s->second.bad.size() : 0) << '\t'
          << (bad ? s->second.added : 0) << '\t' << (bad ? s->second.removed : 0) << '\n';
    }
    std::vector<const LogRow *> rows;
    for (const LogRow &r : t.rows)
      rows.push_back(&r);
    std::stable_sort(rows.begin(), rows.end(), [](const LogRow *x, const LogRow *y) { return x->group < y->group; });
    for (const LogRow *r : rows)
      out << "diff\t" << r->group << '\t' << (r->rows ? "rows" : "content") << '\t' << r->local_n << '\t'
          << r->remote_n << '\t' << r->column << '\t' << r->cell << '\t' << r->local << '\t' << r->remote << '\n';
    out.flush();
    assert(out && "audit: 状态文件写入失败");
  }
  fs::rename(tmp, p);
}

// log_dir 下全部 <表>.tsv (.tmp 残留不读)
Book load_book() {
  Book b;
  for (const auto &e : fs::directory_iterator(log_dir())) {
    if (e.path().extension() != ".tsv")
      continue;
    const std::string name = e.path().stem().string();
    [[maybe_unused]] const fundamental::Spec &spec = fundamental::spec(name); // 不在 SPECS → assert
    assert(fundamental::from<fundamental::Bq>(spec) && spec.shape == fundamental::Shape::Month &&
           "audit: 状态文件不是 DAI·Month 表");
    TableBook &t = b[name];
    std::ifstream in(e.path());
    assert(in && "audit: 状态文件无法打开");
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty() || line.starts_with("#"))
        continue;
      const std::vector<std::string> f = split_tab(line);
      if (f[0] == "month") {
        assert(f.size() == 10 && "audit: 状态 month 行列数不符");
        assert(misc::parse_ym(f[1]) > 0 && "audit: 状态 month 行月份不符");
        MonthRec &m = t.months[f[1]];
        m.rows_at = f[2] == "-" ? "" : f[2];
        m.content_at = f[3] == "-" ? "" : f[3];
        m.inv = {std::stoll(f[4]), std::stoll(f[5]), std::stoll(f[6])};
        continue;
      }
      assert(f[0] == "diff" && f.size() == 9 && "audit: 状态行格式不符");
      assert((f[2] == "rows" || f[2] == "content") && "audit: 状态 pass 列不符");
      t.rows.push_back({f[1], f[2] == "rows", std::stoll(f[3]), std::stoll(f[4]), f[5], f[6], f[7], f[8]});
    }
  }
  return b;
}

// 每张 DAI·Month 表当前冻结的月数 (与 audit::plan 同一来源; 只看目录与 mtime)
std::map<std::string, std::size_t> count_frozen() {
  const fundamental::Catalog cat;
  const fundamental::Now now = fundamental::now();
  std::map<std::string, std::size_t> out;
  for (const fundamental::Spec &s : fundamental::SPECS)
    if (fundamental::from<fundamental::Bq>(s) && s.shape == fundamental::Shape::Month)
      out[s.name] = fundamental::frozen(s, cat, now).size();
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
    a.st.rows_months += !m.rows_at.empty();
    a.st.content_months += !m.content_at.empty();
    a.st.updated = std::max({a.st.updated, m.rows_at, m.content_at});
  }
  // 修复粒度与 repair 同口径: 能定位到日 → 只重拉那几天; 组键只到月 / Snap::Day 表 → 整月
  const bool snap_day = fundamental::spec(name).snap == fundamental::Snap::Day;
  std::set<std::string> whole;
  std::map<std::string, std::int64_t> day_n; // 日 → 远端行数
  for (const LogRow &r : t.rows) {
    a.bad.insert(r.group);
    a.months.insert(r.group.substr(0, 7));
    if (snap_day || misc::parse_ymd(r.group) == 0)
      whole.insert(r.group.substr(0, 7));
    else
      day_n[r.group] = r.remote_n;
    if (r.rows) {
      a.row_bad.insert(r.group);
      if (r.local_n == 0)
        ++a.st.remote_only_days;
      a.st.added += std::max<std::int64_t>(0, r.remote_n - r.local_n);
      a.st.removed += std::max<std::int64_t>(0, r.local_n - r.remote_n);
      a.delta[r.group.substr(0, 7)] += r.remote_n - r.local_n;
    } else {
      a.content_bad.insert(r.group);
      a.col_days[r.column].insert(r.group);
    }
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
  for ([[maybe_unused]] const std::string &ym : a.months)
    assert(t.months.contains(ym) && "audit: 差异月无清点");
  for (const std::string &ym : whole) {
    const MonthInv &m = t.months.at(ym).inv;
    const auto d = a.delta.find(ym);
    st.repair_cells += (m.rows + (d == a.delta.end() ? 0 : d->second)) * m.cols;
  }
  for (const auto &[day, n] : day_n) {
    const std::string ym = day.substr(0, 7);
    if (!whole.contains(ym))
      st.repair_cells += n * t.months.at(ym).inv.cols;
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
std::vector<AuditTableStat> aggregate(const Book &b, const std::map<std::string, std::size_t> &frozen) {
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

struct AuditService::Job {
  std::atomic<bool> done = false;
  std::atomic<bool> cancel = false;
  Book book;                  // worker 独占 (开跑前由 next_job 承接上一轮; done 后只读)
  std::mutex mu;              // 以下字段 worker 写, io 线程轮询读
  std::size_t units_done = 0; // 已跑步数
  std::size_t total = 0;
  std::string current;
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  std::map<std::string, std::size_t> frozen; // 每表冻结月数 (覆盖率分母)
  std::vector<AuditTableStat> snapshot;      // aggregate(book, frozen)
  std::size_t version = 0;                   // snapshot 每更新一次 +1

  // 持 mu 调
  void publish() {
    snapshot = aggregate(book, frozen);
    ++version;
  }

  // 整写该表状态文件 + 发布 (写盘不持锁: book 只 worker 碰)
  void commit(const std::string &name) {
    save(name, book.at(name));
    std::lock_guard lk(mu);
    publish();
  }

  void start_quota(bigquant::Client &client) {
    const bigquant::Quota q = client.quota();
    std::lock_guard lk(mu);
    quota_start = q.used;
    quota_used = q.used;
    quota_weekly = q.weekly;
  }

  // 一步跑完: 计步 + 配额
  void step_done(bigquant::Client &client) {
    const bigquant::Quota q = client.quota();
    std::lock_guard lk(mu);
    ++units_done;
    quota_used = q.used;
  }

  // 核一单元 → 覆盖该表状态
  void audit_unit(bigquant::Client &client, const fundamental::audit::Unit &u) {
    const std::string label = fundamental::audit::label(u);
    {
      std::lock_guard lk(mu);
      current = label;
    }
    const auto diffs = fundamental::audit::run(client, u);
    if (!diffs.empty())
      std::cout << "[bigquant.audit] " << label << ": " << diffs.size() << " 处不一致" << std::endl;
    apply(book[u.spec->name], u, diffs, stamp(std::time(nullptr)));
    step_done(client);
    commit(u.spec->name);
  }

  // 开跑: 计划步数 + 覆盖率分母
  void start(std::size_t steps) {
    auto fz = count_frozen();
    std::lock_guard lk(mu);
    total = steps;
    frozen = std::move(fz);
    publish();
  }

  void finish() {
    {
      std::lock_guard lk(mu);
      current.clear();
    }
    done = true;
  }
};

boost::asio::awaitable<void> AuditService::follow(std::shared_ptr<Job> job) {
  std::size_t seen_version = 0;
  auto sync = [this, &job, &seen_version]() {
    std::lock_guard lk(job->mu);
    state_.done = job->units_done;
    state_.total = job->total;
    state_.current = job->current;
    state_.quota_start = job->quota_start;
    state_.quota_used = job->quota_used;
    state_.quota_weekly = job->quota_weekly;
    if (job->version != seen_version) {
      state_.tables = job->snapshot;
      seen_version = job->version;
    }
  };

  boost::asio::steady_timer timer(io_);
  while (!job->done) {
    sync();
    timer.expires_after(std::chrono::milliseconds(200));
    co_await timer.async_wait(boost::asio::use_awaitable);
  }
  sync();
}

std::shared_ptr<AuditService::Job> AuditService::next_job() {
  assert(job_ && job_->done && "audit: 上一轮未结束");
  auto job = std::make_shared<Job>();
  job->book = job_->book;
  job->frozen = job_->frozen;
  job_ = job;
  return job;
}

boost::asio::awaitable<void> AuditService::load() {
  assert(state_.loading && !job_ && "audit: load 只在启动调一次");
  auto job = std::make_shared<Job>();
  job_ = job;
  state_.log_dir = log_dir().string();

  std::thread([job]() {
    Book b = load_book();
    auto fz = count_frozen();
    std::lock_guard lk(job->mu);
    job->book = std::move(b);
    job->frozen = std::move(fz);
    job->publish();
    job->done = true;
  }).detach();

  co_await follow(job);
  state_.loading = false;
}

boost::asio::awaitable<void> AuditService::run(std::string table, bool incremental) {
  if (is_busy())
    co_return;
  auto job = next_job();
  state_.op = AuditOp::Audit;
  state_.table = table;
  state_.incremental = incremental;

  // 异常不捕获 (fail fast)
  std::thread([job, table, incremental]() {
    const fundamental::Now now = fundamental::now();
    const fundamental::Catalog cat;
    bigquant::Client client;
    job->start_quota(client);
    // 已核 = 状态里该 (月, 轮) 有核对时刻 (plan 在开跑前算完, 之后 book 的改动不影响)
    auto done = [&](const std::string &name, misc::Ym ym, Pass pass) {
      const auto t = job->book.find(name);
      if (t == job->book.end())
        return false;
      const auto m = t->second.months.find(misc::dash_ym(ym));
      return m != t->second.months.end() && !(pass == Pass::Rows ? m->second.rows_at : m->second.content_at).empty();
    };
    const auto units = fundamental::audit::plan(
        cat, now, static_cast<unsigned>(std::time(nullptr)),
        [&](const fundamental::Spec &sp, misc::Ym ym, Pass pass) {
          return (table.empty() || sp.name == table) && !(incremental && done(sp.name, ym, pass));
        });
    job->start(units.size());
    const std::string scope = (table.empty() ? "全部表" : table) + (incremental ? " (增量)" : "");
    std::cout << "[bigquant.audit] " << scope << ": " << units.size() << " units" << std::endl;
    for (const fundamental::audit::Unit &u : units) {
      if (job->cancel.load(std::memory_order_relaxed))
        break;
      job->audit_unit(client, u);
    }
    std::cout << "[bigquant.audit] " << scope << ": "
              << (job->cancel.load(std::memory_order_relaxed) ? "cancelled" : "done") << std::endl;
    job->finish();
  }).detach();

  co_await follow(job);
  state_.op = AuditOp::None;
  state_.table.clear();
  state_.incremental = false;
}

boost::asio::awaitable<void> AuditService::repair(std::string table) {
  if (is_busy())
    co_return;
  auto job = next_job();
  state_.op = AuditOp::Repair;
  state_.table = table;

  std::thread([job, table]() {
    const fundamental::Spec &s = fundamental::spec(table);
    // 差异定位: 月 → 有差异的日; 组键只到月 (下钻对不上日) 或 Snap::Day 表 → 该月整月重拉
    std::set<std::string> keys; // 有差异的月 "YYYY-MM"
    std::set<std::string> whole;
    std::map<std::string, std::set<misc::Ymd>> days;
    std::set<int> years;
    for (const LogRow &r : job->book.at(table).rows) {
      const std::string ym = r.group.substr(0, 7);
      keys.insert(ym);
      years.insert(misc::parse_ym(ym) / 100);
      const misc::Ymd d = misc::parse_ymd(r.group);
      if (d == 0 || s.snap == fundamental::Snap::Day)
        whole.insert(ym);
      else
        days[ym].insert(d);
    }
    assert(!keys.empty() && "repair: 该表无差异月");

    bigquant::Client client;
    job->start_quota(client);
    // 重拉月 + 复核单元 (一表 × 一年 × 一轮)
    job->start(keys.size() + 2 * years.size());

    {
      const fundamental::Catalog cat;
      std::size_t fetched = 0;
      for (const fundamental::Slot &sl : fundamental::frozen(s, cat, fundamental::now())) {
        const std::string key = misc::dash_ym(sl.ym);
        if (!keys.contains(key))
          continue;
        const bool month = whole.contains(key);
        const std::vector<misc::Ymd> ds = month ? std::vector<misc::Ymd>{}
                                                : std::vector<misc::Ymd>(days[key].begin(), days[key].end());
        const fundamental::sync::Task t{&s, sl.ym, sl.w, fundamental::Write::Replace};
        const std::string label = month ? "整月 " + fundamental::sync::label(t)
                                        : "按日 " + table + " " + key + " (" + std::to_string(ds.size()) + " 天)";
        {
          std::lock_guard lk(job->mu);
          job->current = "重拉 " + label;
        }
        std::cout << "[bigquant.repair] " << label << " ... " << std::flush;
        const std::int64_t rows = month ? fundamental::sync::run<fundamental::Bq>(client, t)
                                        : fundamental::sync::patch(client, s, *sl.sh, ds);
        std::cout << rows << " rows" << std::endl;
        job->step_done(client);
        ++fetched;
      }
      assert(fetched == keys.size() && "repair: 差异月不全在冻结月里");
    }
    forget(job->book.at(table), keys);
    job->commit(table);

    // 重拉换了分片: 新清单
    const fundamental::Catalog cat;
    const auto units = fundamental::audit::plan(
        cat, fundamental::now(), static_cast<unsigned>(std::time(nullptr)),
        [&](const fundamental::Spec &sp, misc::Ym ym, Pass) { return &sp == &s && keys.contains(misc::dash_ym(ym)); });
    assert(units.size() == 2 * years.size() && "repair: 复核单元数不符");
    for (const fundamental::audit::Unit &u : units)
      job->audit_unit(client, u);
    std::cout << "[bigquant.repair] " << table << " done" << std::endl;
    job->finish();
  }).detach();

  co_await follow(job);
  state_.op = AuditOp::None;
  state_.table.clear();
}

void AuditService::cancel() {
  assert(is_running() && job_ && "audit: 只有核对可取消");
  job_->cancel.store(true, std::memory_order_relaxed);
}

} // namespace GUI::Database
