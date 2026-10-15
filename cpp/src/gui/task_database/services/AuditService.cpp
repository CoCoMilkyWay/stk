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

// output/log/audit.tsv
fs::path cache_path() {
  const auto dir = misc::git_root() / "output" / "log";
  fs::create_directories(dir);
  return dir / "audit.tsv";
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
  std::string table;
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
    out.push_back({d.table, d.group, rows, d.local_n, d.remote_n});
    return;
  }
  for (const fundamental::audit::Diff::Cell &c : d.cells)
    out.push_back({d.table, d.group, rows, d.local_n, d.remote_n, c.column, c.sum ? "s" : "nn", c.local, c.remote});
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
  bool rows = false;    // 已跑 Rows 轮
  bool content = false; // 已跑 Content 轮
};

// 核对缓存 = 当前状态: 每个 (表, 月, 轮) 只留最近一次核对的结论
struct Book {
  std::map<std::string, std::map<std::string, MonthRec>> months; // 表 → "YYYY-MM" (组键前 7 位同形)
  std::vector<LogRow> rows;
};

// 一单元跑完: 覆盖该 (表, 单元各月, 轮) 的清点 / 轮标记 / 差异格
void apply(Book &b, const fundamental::audit::Unit &u, const std::vector<fundamental::audit::Diff> &diffs) {
  const std::string &name = u.spec->name;
  const bool rows = u.pass == Pass::Rows;
  std::set<std::string> keys;
  for (const fundamental::Slot &sl : u.months) {
    const std::string key = misc::dash_ym(sl.ym);
    MonthRec &m = b.months[name][key];
    m.inv = count_month(*u.spec, *sl.sh);
    (rows ? m.rows : m.content) = true;
    keys.insert(key);
  }
  std::erase_if(b.rows, [&](const LogRow &r) {
    return r.table == name && r.rows == rows && keys.contains(r.group.substr(0, 7));
  });
  for (const auto &d : diffs)
    append_rows(b.rows, d);
}

// 分片重拉过: 这些月两轮的旧结论一并作废 (待复核)
void forget(Book &b, const std::string &name, const std::set<std::string> &keys) {
  auto &months = b.months[name];
  for (const std::string &k : keys)
    months.erase(k);
  std::erase_if(b.rows, [&](const LogRow &r) { return r.table == name && keys.contains(r.group.substr(0, 7)); });
}

// 缓存格式 (tsv, # 起头为注释):
//   month \t 表 \t YYYY-MM \t rows(0/1) \t content(0/1) \t 本地行 \t 本地天数 \t 列数
//   diff  \t 表 \t 组 \t rows|content \t local_n \t remote_n \t 列 \t nn|s \t 本地 \t 远端
void save(const Book &b, const fs::path &p) {
  const fs::path tmp = p.string() + ".tmp";
  {
    std::ofstream out(tmp);
    assert(out && "audit: 缓存无法写入");
    out << "# month\ttable\tym\trows\tcontent\tlocal_rows\tlocal_days\tcols\n"
        << "# diff\ttable\tgroup\tpass\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
    for (const auto &[name, months] : b.months)
      for (const auto &[ym, m] : months)
        out << "month\t" << name << '\t' << ym << '\t' << m.rows << '\t' << m.content << '\t'
            << m.inv.rows << '\t' << m.inv.days << '\t' << m.inv.cols << '\n';
    std::vector<const LogRow *> rows;
    for (const LogRow &r : b.rows)
      rows.push_back(&r);
    std::stable_sort(rows.begin(), rows.end(), [](const LogRow *x, const LogRow *y) {
      return x->table != y->table ? x->table < y->table : x->group < y->group;
    });
    for (const LogRow *r : rows)
      out << "diff\t" << r->table << '\t' << r->group << '\t' << (r->rows ? "rows" : "content") << '\t'
          << r->local_n << '\t' << r->remote_n << '\t' << r->column << '\t' << r->cell << '\t'
          << r->local << '\t' << r->remote << '\n';
    out.flush();
    assert(out && "audit: 缓存写入失败");
  }
  fs::rename(tmp, p);
}

Book load_book(const fs::path &p) {
  std::ifstream in(p);
  assert(in && "audit: 缓存无法打开");
  Book b;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line.starts_with("#"))
      continue;
    std::vector<std::string> f;
    std::size_t s = 0;
    for (std::size_t i = 0; i <= line.size(); ++i)
      if (i == line.size() || line[i] == '\t') {
        f.push_back(line.substr(s, i - s));
        s = i + 1;
      }
    if (f[0] == "month") {
      assert(f.size() == 8 && "audit: 缓存 month 行列数不符");
      assert(misc::parse_ym(f[2]) > 0 && "audit: 缓存 month 行月份不符");
      MonthRec &m = b.months[f[1]][f[2]];
      m.rows = f[3] == "1";
      m.content = f[4] == "1";
      m.inv = {std::stoll(f[5]), std::stoll(f[6]), std::stoll(f[7])};
      continue;
    }
    assert(f[0] == "diff" && f.size() == 10 && "audit: 缓存行格式不符");
    assert((f[3] == "rows" || f[3] == "content") && "audit: 缓存 pass 列不符");
    b.rows.push_back({f[1], f[2], f[3] == "rows", std::stoll(f[4]), std::stoll(f[5]), f[6], f[7], f[8], f[9]});
  }
  return b;
}

// 当前冻结的 (表, 月) 数 (与 audit::plan 同一来源; 只看目录与 mtime)
std::size_t count_frozen() {
  const fundamental::Catalog cat;
  const fundamental::Now now = fundamental::now();
  std::size_t n = 0;
  for (const fundamental::Spec &s : fundamental::SPECS)
    if (fundamental::from<fundamental::Bq>(s) && s.shape == fundamental::Shape::Month)
      n += fundamental::frozen(s, cat, now).size();
  return n;
}

// 缓存 → 每表统计 (表名升序)
std::vector<AuditTableStat> aggregate(const Book &b) {
  struct Acc {
    AuditTableStat st;
    std::set<std::string> bad, row_bad, content_bad, months;
    std::map<std::string, std::set<std::string>> col_days; // 改值列 → 涉及的日
    std::map<std::string, std::int64_t> delta;             // ym → 远端 − 本地行数
  };
  std::map<std::string, Acc> acc;
  for (const auto &[name, months] : b.months) {
    Acc &a = acc[name];
    for (const auto &[ym, m] : months) {
      a.st.local_days += m.inv.days;
      a.st.local_rows += m.inv.rows;
    }
  }
  for (const LogRow &r : b.rows) {
    Acc &a = acc[r.table];
    a.bad.insert(r.group);
    a.months.insert(r.group.substr(0, 7));
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

  std::vector<AuditTableStat> out;
  for (auto &[name, a] : acc) {
    AuditTableStat &st = a.st;
    st.table = name;
    st.bad_days = a.bad.size();
    st.row_days = a.row_bad.size();
    st.content_days = a.content_bad.size();
    st.bad_months = a.months.size();

    // 差异行的月必有清点 (apply 同步写入, forget 同步删除)
    const auto &months = b.months.at(name);
    for (const auto &[ym, m] : months) {
      const auto d = a.delta.find(ym);
      st.full_cells += (m.inv.rows + (d == a.delta.end() ? 0 : d->second)) * m.inv.cols;
    }
    for (const std::string &ym : a.months) {
      const auto m = months.find(ym);
      assert(m != months.end() && "audit: 差异月无清点");
      const auto d = a.delta.find(ym);
      st.repair_cells += (m->second.inv.rows + (d == a.delta.end() ? 0 : d->second)) * m->second.inv.cols;
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
    out.push_back(std::move(st));
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
  std::vector<AuditTableStat> snapshot; // aggregate(book)
  std::size_t rows_months = 0;
  std::size_t content_months = 0;
  std::size_t frozen_months = 0;
  std::size_t version = 0; // snapshot 每更新一次 +1
  std::string log_path;
  std::string updated;

  // 持 mu 调
  void publish() {
    snapshot = aggregate(book);
    rows_months = content_months = 0;
    for (const auto &[name, months] : book.months)
      for (const auto &[ym, m] : months) {
        rows_months += m.rows;
        content_months += m.content;
      }
    ++version;
  }

  // 整写缓存 + 发布 (写盘不持锁: book 只 worker 碰)
  void commit() {
    save(book, cache_path());
    std::lock_guard lk(mu);
    updated = stamp(std::time(nullptr));
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

  // 核一单元 → 覆盖缓存
  void audit_unit(bigquant::Client &client, const fundamental::audit::Unit &u) {
    const std::string label = fundamental::audit::label(u);
    {
      std::lock_guard lk(mu);
      current = label;
    }
    const auto diffs = fundamental::audit::run(client, u);
    if (!diffs.empty())
      std::cout << "[bigquant.audit] " << label << ": " << diffs.size() << " 处不一致" << std::endl;
    apply(book, u, diffs);
    step_done(client);
    commit();
  }

  // 开跑: 计划步数 + 覆盖率分母
  void start(std::size_t steps) {
    const std::size_t fz = count_frozen();
    std::lock_guard lk(mu);
    total = steps;
    frozen_months = fz;
    log_path = cache_path().string();
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
    state_.log_path = job->log_path;
    state_.updated = job->updated;
    state_.rows_months = job->rows_months;
    state_.content_months = job->content_months;
    state_.frozen_months = job->frozen_months;
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
  job->rows_months = job_->rows_months;
  job->content_months = job_->content_months;
  job->frozen_months = job_->frozen_months;
  job->log_path = job_->log_path;
  job->updated = job_->updated;
  job_ = job;
  return job;
}

boost::asio::awaitable<void> AuditService::load() {
  assert(state_.loading && !job_ && "audit: load 只在启动调一次");
  auto job = std::make_shared<Job>();
  job_ = job;

  std::thread([job]() {
    const fs::path p = cache_path();
    if (fs::exists(p)) {
      Book b = load_book(p);
      const std::size_t fz = count_frozen();
      const std::time_t t = std::chrono::system_clock::to_time_t(
          std::chrono::clock_cast<std::chrono::system_clock>(fs::last_write_time(p)));
      std::lock_guard lk(job->mu);
      job->book = std::move(b);
      job->frozen_months = fz;
      job->log_path = p.string();
      job->updated = stamp(t);
      job->publish();
    }
    job->done = true;
  }).detach();

  co_await follow(job);
  state_.loading = false;
}

boost::asio::awaitable<void> AuditService::run() {
  if (is_busy())
    co_return;
  auto job = next_job();
  state_.running = true;

  // 异常不捕获 (fail fast)
  std::thread([job]() {
    const fundamental::Now now = fundamental::now();
    const fundamental::Catalog cat;
    bigquant::Client client;
    job->start_quota(client);
    const auto units = fundamental::audit::plan(cat, now, static_cast<unsigned>(std::time(nullptr)));
    job->start(units.size());
    std::cout << "[bigquant.audit] " << units.size() << " units -> " << cache_path().string() << std::endl;
    for (const fundamental::audit::Unit &u : units) {
      if (job->cancel.load(std::memory_order_relaxed))
        break;
      job->audit_unit(client, u);
    }
    std::cout << "[bigquant.audit] "
              << (job->cancel.load(std::memory_order_relaxed) ? "cancelled" : "done") << std::endl;
    job->finish();
  }).detach();

  co_await follow(job);
  state_.running = false;
}

boost::asio::awaitable<void> AuditService::repair(std::string table) {
  if (is_busy())
    co_return;
  auto job = next_job();
  state_.repairing = table;

  std::thread([job, table]() {
    const fundamental::Spec &s = fundamental::spec(table);
    std::set<std::string> keys; // 有差异的月 "YYYY-MM"
    std::set<int> years;
    for (const LogRow &r : job->book.rows)
      if (r.table == table) {
        keys.insert(r.group.substr(0, 7));
        years.insert(misc::parse_ym(r.group.substr(0, 7)) / 100);
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
        if (!keys.contains(misc::dash_ym(sl.ym)))
          continue;
        const fundamental::sync::Task t{&s, sl.ym, sl.w, fundamental::Write::Replace};
        const std::string label = fundamental::sync::label(t);
        {
          std::lock_guard lk(job->mu);
          job->current = "重拉 " + label;
        }
        std::cout << "[bigquant.repair] " << label << " ... " << std::flush;
        std::cout << fundamental::sync::run<fundamental::Bq>(client, t) << " rows" << std::endl;
        job->step_done(client);
        ++fetched;
      }
      assert(fetched == keys.size() && "repair: 差异月不全在冻结月里");
    }
    forget(job->book, table, keys);
    job->commit();

    // 重拉换了分片: 新清单
    const fundamental::Catalog cat;
    const auto units = fundamental::audit::plan(
        cat, fundamental::now(), static_cast<unsigned>(std::time(nullptr)),
        [&](const fundamental::Spec &sp, misc::Ym ym) { return &sp == &s && keys.contains(misc::dash_ym(ym)); });
    assert(units.size() == 2 * years.size() && "repair: 复核单元数不符");
    for (const fundamental::audit::Unit &u : units)
      job->audit_unit(client, u);
    std::cout << "[bigquant.repair] " << table << " done" << std::endl;
    job->finish();
  }).detach();

  co_await follow(job);
  state_.repairing.clear();
}

void AuditService::cancel() {
  assert(state_.running && job_ && "audit: 只有全量核对可取消");
  job_->cancel.store(true, std::memory_order_relaxed);
}

} // namespace GUI::Database
