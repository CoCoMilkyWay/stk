#include "gui/task_database/services/AuditService.hpp"

#include "api/bigquant/client.hpp"
#include "fundamental/Audit.hpp"
#include "fundamental/Store.hpp"
#include "misc/fs.hpp"
#include "misc/parquet.hpp"

#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string_view>
#include <thread>
#include <utility>

namespace GUI::Database {

namespace {

// output/log/audit_YYYYMMDD_HHMMSS.tsv
std::filesystem::path new_log_path() {
  std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "audit_%Y%m%d_%H%M%S.tsv", &tm_buf);
  const auto dir = misc::git_root() / "output" / "log";
  std::filesystem::create_directories(dir);
  return dir / buf;
}

// Rows 差异一行 (column/cell 为 "-"); Content 每个不一致格一行
void write_diff(std::ofstream &out, const fundamental::audit::Diff &d) {
  const char *pass = d.pass == fundamental::audit::Pass::Rows ? "rows" : "content";
  auto head = [&]() -> std::ofstream & {
    out << d.table << '\t' << d.group << '\t' << pass << '\t' << d.local_n
        << '\t' << d.remote_n << '\t';
    return out;
  };
  if (d.cells.empty()) {
    head() << "-\t-\t-\t-\n";
    return;
  }
  for (const fundamental::audit::Diff::Cell &c : d.cells)
    head() << c.column << '\t' << (c.sum ? "s" : "nn") << '\t' << c.local
           << '\t' << c.remote << '\n';
}

// 日志一行 (统计只用到这几列; 本地 / 远端原值留给人看)
struct LogRow {
  std::string table;
  std::string group; // "YYYY-MM-DD" / 回落 "YYYY-MM"
  bool rows = false; // pass == rows
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::string column; // rows 轮为 "-"
};

// 与 write_diff 一行对一行
void append_rows(std::vector<LogRow> &out, const fundamental::audit::Diff &d) {
  const bool rows = d.pass == fundamental::audit::Pass::Rows;
  if (d.cells.empty()) {
    out.push_back({d.table, d.group, rows, d.local_n, d.remote_n, "-"});
    return;
  }
  for (const fundamental::audit::Diff::Cell &c : d.cells)
    out.push_back({d.table, d.group, rows, d.local_n, d.remote_n, c.column});
}

// output/log 下最新的 audit_*.tsv (文件名含时间戳, 字典序 = 时间序); 无则空
std::filesystem::path latest_log() {
  const auto dir = misc::git_root() / "output" / "log";
  std::filesystem::path best;
  if (!std::filesystem::exists(dir))
    return best;
  for (const auto &e : std::filesystem::directory_iterator(dir)) {
    const std::string f = e.path().filename().string();
    if (f.starts_with("audit_") && f.ends_with(".tsv") &&
        (best.empty() || f > best.filename().string()))
      best = e.path();
  }
  return best;
}

// "audit_20261015_114548.tsv" → 20261015
misc::Ymd log_date(const std::filesystem::path &p) {
  const std::string f = p.filename().string();
  assert(f.size() >= 21 && f.starts_with("audit_") && "audit: 日志文件名不符");
  const misc::Ymd d = misc::parse_ymd(std::string_view(f).substr(6, 8));
  assert(d > 0 && "audit: 日志文件名日期不符");
  return d;
}

// "audit_20261015_114548.tsv" → "2026-10-15 11:45"
std::string log_time(const std::filesystem::path &p) {
  const std::string f = p.filename().string();
  return f.substr(6, 4) + "-" + f.substr(10, 2) + "-" + f.substr(12, 2) + " " +
         f.substr(15, 2) + ":" + f.substr(17, 2);
}

std::string footer_status(std::string_view kind, std::size_t done, std::size_t total) {
  return std::string(kind == "done" ? "完成" : "已取消") + " " + std::to_string(done) +
         "/" + std::to_string(total);
}

// 解析日志 → rows; 返回状态 ("完成 k/n" / "已取消 k/n" / 无末行标记)
std::string parse_log(const std::filesystem::path &p, std::vector<LogRow> &rows) {
  std::ifstream in(p);
  assert(in && "audit: 日志无法打开");
  std::string line, status = "无完成标记 (旧格式或中途退出)";
  while (std::getline(in, line)) {
    if (line.empty() || line.starts_with("table\t"))
      continue;
    if (line.starts_with("# ")) {
      char kind[16] = {};
      std::size_t done = 0, total = 0;
      const int n = std::sscanf(line.c_str(), "# %15s %zu/%zu", kind, &done, &total);
      assert(n == 3 && "audit: 日志末行格式不符");
      status = footer_status(kind, done, total);
      continue;
    }
    std::vector<std::string> f;
    std::size_t s = 0;
    for (std::size_t i = 0; i <= line.size(); ++i)
      if (i == line.size() || line[i] == '\t') {
        f.push_back(line.substr(s, i - s));
        s = i + 1;
      }
    assert(f.size() == 9 && "audit: 日志行列数不符");
    assert((f[2] == "rows" || f[2] == "content") && "audit: 日志 pass 列不符");
    rows.push_back({f[0], f[1], f[2] == "rows", std::stoll(f[3]), std::stoll(f[4]), f[5]});
  }
  return status;
}

// 本地冻结月清点 (分母 + 流量): as_of 当天视角下 fundamental::frozen 的月 (与 audit::plan 同一来源)
struct MonthInv {
  std::int64_t rows = 0;
  std::int64_t days = 0;
  std::int64_t cols = 0;
};
using Inventory = std::map<std::string, std::map<std::string, MonthInv>>; // 表 → "YYYY-MM" (日志组键同形) → 清点

Inventory build_inventory(const fundamental::Catalog &cat, const fundamental::Now &as_of) {
  Inventory inv;
  for (const fundamental::Spec &s : fundamental::SPECS) {
    if (!fundamental::from<fundamental::Bq>(s) || s.shape != fundamental::Shape::Month)
      continue;
    auto &months = inv[s.name];
    for (const fundamental::Slot &sl : fundamental::frozen(s, cat, as_of)) {
      MonthInv &m = months[misc::dash_ym(sl.ym)];
      m.cols = parquet::ParquetFileReader::OpenFile(sl.sh->path.string())->metadata()->num_columns();
      auto t = fundamental::read(*sl.sh, {s.vd});
      m.rows = t->num_rows();
      std::set<std::int32_t> days;
      std::vector<std::int32_t> d;
      for (const auto &chunk : t->column(0)->chunks()) {
        misc::pq::Col(chunk).yyyymmdd_all(d);
        days.insert(d.begin(), d.end());
      }
      m.days = static_cast<std::int64_t>(days.size());
    }
  }
  return inv;
}

// 日志行 + 清点 → 每表统计 (表名升序)
std::vector<AuditTableStat> aggregate(const std::vector<LogRow> &rows, const Inventory &inv) {
  struct Acc {
    AuditTableStat st;
    std::set<std::string> bad, row_bad, content_bad, months;
    std::map<std::string, std::set<std::string>> col_days; // 改值列 → 涉及的日
    std::map<std::string, std::int64_t> delta;             // ym → 远端 − 本地行数
  };
  std::map<std::string, Acc> acc;
  for (const auto &[name, months] : inv) {
    Acc &a = acc[name];
    for (const auto &[ym, m] : months) {
      a.st.local_days += m.days;
      a.st.local_rows += m.rows;
    }
  }
  for (const LogRow &r : rows) {
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

    const auto it = inv.find(name);
    const std::map<std::string, MonthInv> empty;
    const auto &months = it == inv.end() ? empty : it->second;
    const std::int64_t any_cols = months.empty() ? 0 : months.begin()->second.cols;
    for (const auto &[ym, m] : months) {
      const auto d = a.delta.find(ym);
      st.full_cells += (m.rows + (d == a.delta.end() ? 0 : d->second)) * m.cols;
    }
    for (const std::string &ym : a.months) {
      const auto m = months.find(ym);
      const auto d = a.delta.find(ym);
      const std::int64_t rows_m = (m == months.end() ? 0 : m->second.rows) +
                                  (d == a.delta.end() ? 0 : d->second);
      st.repair_cells += rows_m * (m == months.end() ? any_cols : m->second.cols);
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
  std::mutex mu; // 以下字段 worker 写, io 线程轮询读
  std::size_t units_done = 0;
  std::size_t total = 0;
  std::string current;
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  Inventory inv;
  std::vector<LogRow> rows;
  std::vector<AuditTableStat> snapshot; // aggregate(rows, inv)
  std::size_t version = 0;              // snapshot 每更新一次 +1
  std::string log_path;
  std::string log_status;

  void publish() {
    snapshot = aggregate(rows, inv);
    ++version;
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
    state_.log_status = job->log_status;
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

boost::asio::awaitable<void> AuditService::load_last() {
  if (is_busy())
    co_return;
  auto job = std::make_shared<Job>();
  job_ = job;
  state_.loading = true;

  std::thread([job]() {
    const auto p = latest_log();
    if (!p.empty()) {
      std::vector<LogRow> rows;
      std::string status = parse_log(p, rows);
      Inventory inv = build_inventory(fundamental::Catalog{}, {.today = log_date(p)});
      std::lock_guard lk(job->mu);
      job->rows = std::move(rows);
      job->inv = std::move(inv);
      job->log_path = p.string();
      job->log_status = log_time(p) + "  " + status;
      job->publish();
    }
    job->done = true;
  }).detach();

  co_await follow(job);
  state_.loading = false;
  job_.reset();
}

boost::asio::awaitable<void> AuditService::run() {
  if (is_busy())
    co_return;
  auto job = std::make_shared<Job>();
  job_ = job;
  state_ = AuditState{};
  state_.running = true;

  // 异常不捕获 (fail fast)
  std::thread([job]() {
    const auto log_path = new_log_path();
    std::ofstream log(log_path);
    assert(log && "audit: 日志文件无法打开");
    log << "table\tgroup\tpass\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
    log.flush();
    std::cout << "[bigquant.audit] log -> " << log_path.string() << std::endl;
    const fundamental::Now now = fundamental::now();
    const fundamental::Catalog cat;
    Inventory inv = build_inventory(cat, now);
    {
      std::lock_guard lk(job->mu);
      job->log_path = log_path.string();
      job->log_status = log_time(log_path) + "  进行中";
      job->inv = std::move(inv);
      job->publish();
    }
    bigquant::Client client;
    const bigquant::Quota q0 = client.quota();
    {
      std::lock_guard lk(job->mu);
      job->quota_start = q0.used;
      job->quota_used = q0.used;
      job->quota_weekly = q0.weekly;
    }
    const auto units = fundamental::audit::plan(cat, now, static_cast<unsigned>(std::time(nullptr)));
    {
      std::lock_guard lk(job->mu);
      job->total = units.size();
    }
    std::cout << "[bigquant.audit] " << units.size() << " units" << std::endl;
    for (const fundamental::audit::Unit &u : units) {
      if (job->cancel.load(std::memory_order_relaxed))
        break;
      {
        std::lock_guard lk(job->mu);
        job->current = fundamental::audit::label(u);
      }
      const auto diffs = fundamental::audit::run(client, u);
      if (!diffs.empty())
        std::cout << "[bigquant.audit] " << fundamental::audit::label(u) << ": "
                  << diffs.size() << " 处不一致" << std::endl;
      for (const auto &d : diffs)
        write_diff(log, d);
      log.flush();
      const bigquant::Quota q = client.quota();
      std::lock_guard lk(job->mu);
      for (const auto &d : diffs)
        append_rows(job->rows, d);
      if (!diffs.empty())
        job->publish();
      ++job->units_done;
      job->quota_used = q.used;
    }
    const bool cancelled = job->cancel.load(std::memory_order_relaxed);
    {
      std::lock_guard lk(job->mu);
      const std::string_view kind = cancelled ? "cancelled" : "done";
      log << "# " << kind << " " << job->units_done << "/" << job->total << "\n";
      log.flush();
      job->current.clear();
      job->log_status = log_time(log_path) + "  " + footer_status(kind, job->units_done, job->total);
    }
    std::cout << "[bigquant.audit] done" << std::endl;
    job->done = true;
  }).detach();

  co_await follow(job);
  state_.cancelled = job->cancel.load(std::memory_order_relaxed);
  state_.running = false;
  job_.reset();
}

void AuditService::cancel() {
  if (job_)
    job_->cancel.store(true, std::memory_order_relaxed);
}

} // namespace GUI::Database
