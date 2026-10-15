#include "fundamental/Store.hpp"

#include "misc/fs.hpp"
#include "misc/parquet.hpp"
#include "shared/Config.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <ctime>

namespace fundamental {

namespace fs = std::filesystem;
namespace pq = misc::pq;

namespace {

const std::vector<Shard> kNoShards;

// file_clock 时刻 → 本地时区 (Now 与写盘日同一换算)
std::tm local_tm(fs::file_time_type t) {
  const std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(t));
  std::tm tm{};
  localtime_r(&tt, &tm);
  return tm;
}

Ymd ymd_of(const std::tm &tm) { return (tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday; }

// avail_hour 0 → 恒 today (排程提前); 24 → 恒昨天 (全天涓流, 次日才完整)
Ymd horizon(int avail_hour, const Now &now) {
  return now.hour >= avail_hour ? now.today : misc::add_days(now.today, -1);
}

Ymd write_date(const Shard &sh) { return ymd_of(local_tm(sh.mtime)); }

fs::path root() { return misc::git_root() / "output" / "fundamental"; }

// name 的分片路径: ym > 0 → YYYY-MM/<name>.parquet; ym = 0 → _meta/<name>.parquet
fs::path path(std::string_view name, Ym ym) {
  assert(ym >= 0);
  return root() / (ym == 0 ? std::string("_meta") : misc::dash_ym(ym)) / (std::string(name) + ".parquet");
}

bool closed(Ym m, Ymd today) {
  return misc::last_day(m) < misc::add_days(today, -::config::PIPELINE_LOOKBACK_DAYS);
}

// s 的分片槽: Meta = {0}; Month = [ym(start), ym(today)]
std::vector<Ym> slots(const Spec &s, const Now &now) {
  if (s.shape == Shape::Meta)
    return {0};
  std::vector<Ym> out;
  for (Ym m = misc::ym_of(::config::PIPELINE_START_DATE);; m = misc::next_ym(m)) {
    out.push_back(m);
    if (m == misc::ym_of(now.today))
      break;
  }
  return out;
}

// 分片槽 m 的整段窗口 (见 Slot::w)
Window window(const Spec &s, Ym m, const Now &now) {
  const Ymd start = ::config::PIPELINE_START_DATE;
  const Ymd hz = horizon(s.avail_hour, now);
  if (s.shape == Shape::Meta)
    return s.vd.empty() ? Window{} : Window{start, hz};
  return {std::max(misc::first_day(m), start), closed(m, now.today) ? misc::last_day(m) : std::min(misc::last_day(m), hz)};
}

// sh = 该分片 (nullptr = 盘上没有); Month: m = 分片月, Meta: m = 0
State state(const Spec &s, const Shard *sh, Ym m, const Now &now) {
  if (!sh)
    return State::Missing;
  if (s.shape == Shape::Meta) {
    const bool fresh = s.vd.empty() ? write_date(*sh) == now.today
                                    : mark(s, *sh) >= horizon(s.avail_hour, now);
    return fresh ? State::Frozen : State::Stale;
  }
  if (!closed(m, now.today))
    return State::Open;
  // 冻结 = 写盘日 ≥ 月末 + lookback: 写盘时该月已出 lookback 窗口, "整月行 + 全部回填" 才都已入盘
  return write_date(*sh) >= misc::add_days(misc::last_day(m), ::config::PIPELINE_LOOKBACK_DAYS) ? State::Frozen
                                                                                                : State::Stale;
}

// p 里 vd ∈ days 的行删掉, tbl 追加在末尾, 其余天原样; 原子重写 (见 put)
void splice(const Spec &s, const fs::path &p, const std::vector<Ymd> &days, const std::shared_ptr<arrow::Table> &tbl) {
  assert(s.shape == Shape::Month && "put: Splice 只用于月分片");
  assert(fs::exists(p) && "put: Splice 的分片须已存在");
  assert(std::is_sorted(days.begin(), days.end()) && "put: Splice 的 days 须升序");
  auto in_days = [&](Ymd d) { return std::binary_search(days.begin(), days.end(), d); };
  auto vds = [&](const arrow::Table &t) {
    const auto c = t.GetColumnByName(s.vd);
    assert(c && "put: Splice 缺 vd 列");
    std::vector<Ymd> out, d;
    for (const auto &chunk : c->chunks()) {
      pq::Col(chunk).yyyymmdd_all(d);
      out.insert(out.end(), d.begin(), d.end());
    }
    return out;
  };
  for ([[maybe_unused]] Ymd d : vds(*tbl))
    assert(in_days(d) && "put: Splice 新行的 vd 不在替换日内");

  // 保留段 = 旧分片里 vd ∉ days 的连续行 (零拷贝 Slice)
  const auto old = pq::read_table(p);
  const std::vector<Ymd> od = vds(*old);
  std::vector<std::shared_ptr<arrow::Table>> parts;
  const std::int64_t n = static_cast<std::int64_t>(od.size());
  for (std::int64_t i = 0; i < n;) {
    if (in_days(od[i])) {
      ++i;
      continue;
    }
    std::int64_t j = i;
    while (j < n && !in_days(od[j]))
      ++j;
    parts.push_back(old->Slice(i, j - i));
    i = j;
  }
  parts.push_back(tbl);
  arrow::ConcatenateTablesOptions opt;
  opt.unify_schemas = true;
  auto joined = arrow::ConcatenateTables(parts, opt);
  assert(joined.ok() && "put: Splice 新旧 schema 无法合并");
  pq::write_table_atomic(p, std::move(joined).ValueUnsafe());
}

} // namespace

Now now() {
  const fs::file_time_type at = fs::file_time_type::clock::now();
  const std::tm tm = local_tm(at);
  return {at, ymd_of(tm), tm.tm_hour};
}

Catalog::Catalog() {
  const fs::path r = root();
  assert(fs::exists(r) && "output/fundamental 缺失");
  for (const fs::directory_entry &d : fs::directory_iterator(r)) {
    if (!d.is_directory())
      continue;
    const std::string dir = d.path().filename().string();
    // 数据集只有 "_meta" 和 "YYYY-MM" 两种目录; 其余 (pool 等) 不是本流水线产物
    const Ym m = dir == "_meta" ? 0 : misc::parse_ym(dir);
    if (dir != "_meta" && m == 0)
      continue;
    for (const fs::directory_entry &f : fs::directory_iterator(d.path())) {
      if (f.path().extension() != ".parquet")
        continue;
      by_name_[f.path().stem().string()].push_back({m, f.path(), f.file_size(), f.last_write_time()});
    }
  }
  for (auto &[name, v] : by_name_)
    std::sort(v.begin(), v.end(), [](const Shard &a, const Shard &b) { return a.ym < b.ym; });
}

const std::vector<Shard> &Catalog::shards(std::string_view name) const {
  const auto it = by_name_.find(name);
  return it == by_name_.end() ? kNoShards : it->second;
}

const Shard *Catalog::find(std::string_view name, Ym m) const {
  const std::vector<Shard> &v = shards(name);
  const auto it = std::lower_bound(v.begin(), v.end(), m, [](const Shard &s, Ym x) { return s.ym < x; });
  return (it != v.end() && it->ym == m) ? &*it : nullptr;
}

std::vector<Slot> ledger(const Spec &s, const Catalog &cat, const Now &now) {
  assert(::config::PIPELINE_START_DATE > 0 && ::config::PIPELINE_START_DATE <= now.today);
  std::vector<Slot> out;
  for (const Ym m : slots(s, now)) {
    const Shard *sh = cat.find(s.name, m);
    out.push_back({m, sh, state(s, sh, m, now), window(s, m, now)});
  }
  return out;
}

std::vector<Slot> frozen(const Spec &s, const Catalog &cat, const Now &now) {
  std::vector<Slot> out;
  if (s.shape != Shape::Month)
    return out;
  for (const Slot &sl : ledger(s, cat, now))
    if (sl.st == State::Frozen)
      out.push_back(sl);
  return out;
}

std::optional<Task> todo(const Spec &s, const Slot &sl) {
  if (sl.st == State::Frozen)
    return std::nullopt;
  Task t{&s, sl.ym, sl.w};
  const Ymd w = sl.st == State::Open ? mark(s, *sl.sh) : 0; // 水位; 0 = 整段覆盖 (Missing / Stale / 0 行文件)
  if (w != 0) {
    if (s.snap == Snap::Day)
      return std::nullopt; // 快照已入盘
    t.w.lo = misc::add_days(w, 1);
    t.how = Write::Append;
  }
  if (t.w.lo > t.w.hi)
    return std::nullopt; // 已到水位; {0, 0} (vd 空) 照拉
  return t;
}

Ymd mark(const Spec &s, const Shard &sh) {
  assert(!s.vd.empty() && "mark: vd 空的表无水位");
  return pq::max_yyyymmdd(sh.path, s.vd);
}

std::shared_ptr<arrow::Table> read(const Shard &sh, const std::vector<std::string> &cols,
                                   const std::vector<std::string> &dict) {
  return pq::read_table(sh.path, cols, dict);
}

void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl) {
  const Spec &s = *t.spec;
  assert((t.ym == 0) == (s.shape == Shape::Meta) && "put: 分片与表形态不符");
  const fs::path p = path(s.name, t.ym);
  switch (t.how) {
  case Write::Replace:
    pq::write_table_atomic(p, tbl);
    return;
  case Write::Append:
    pq::append_table_atomic(p, tbl);
    return;
  case Write::Splice:
    assert(!t.days.empty() && misc::ym_of(t.days.front()) == t.ym && misc::ym_of(t.days.back()) == t.ym &&
           "put: Splice 的 days 须非空且同属 ym");
    splice(s, p, t.days, tbl);
    return;
  }
  assert(false && "put: 未知写法");
}

bool renative(const Shard &sh) { return pq::renative(sh.path); }

} // namespace fundamental
