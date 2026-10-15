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

// 本地时区当前小时 [0, 23]
int now_hour_local() {
  const std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  return tm.tm_hour;
}

// avail_hour 0 → 恒 today (排程提前); 24 → 恒昨天 (全天涓流, 次日才完整)
Ymd horizon(int avail_hour, Ymd today) {
  assert(avail_hour >= 0 && avail_hour <= 24);
  return now_hour_local() >= avail_hour ? today : misc::add_days(today, -1);
}

long age_seconds(const Shard &sh) {
  return std::chrono::duration_cast<std::chrono::seconds>(fs::file_time_type::clock::now() - sh.mtime).count();
}

Ymd write_date(const Shard &sh) {
  const std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(sh.mtime));
  std::tm tm{};
  localtime_r(&t, &tm);
  return (tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday;
}

// 文件内 max(vd); 0 行 / 全 null → 0. 只解 vd 一列 (宽表全列解是纯浪费).
Ymd max_vd(const Shard &sh, const std::string &vd) {
  pq::TableView tv(pq::read_table(sh.path, {vd}));
  if (tv.rows() == 0)
    return 0;
  std::vector<Ymd> d;
  tv.col(vd).yyyymmdd_all(d);
  return *std::max_element(d.begin(), d.end());
}

bool closed(Ym m, Ymd today) {
  return misc::last_day(m) < misc::add_days(today, -::config::PIPELINE_LOOKBACK_DAYS);
}

} // namespace

fs::path root() { return misc::git_root() / "output" / "fundamental"; }

fs::path month_path(Ym m, std::string_view name) {
  assert(m > 0);
  return root() / misc::dash_ym(m) / (std::string(name) + ".parquet");
}

fs::path meta_path(std::string_view name) {
  return root() / "_meta" / (std::string(name) + ".parquet");
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

State state(const Spec &s, const Shard *sh, Ym m, Ymd today) {
  if (!sh)
    return State::Missing;
  if (s.shape == Shape::Meta) {
    const bool fresh = s.vd.empty() ? write_date(*sh) == today
                                    : max_vd(*sh, s.vd) >= horizon(s.avail_hour, today);
    return fresh ? State::Frozen : State::Stale;
  }
  if (!closed(m, today))
    return State::Open;
  // 冻结 = 写盘日 ≥ 月末 + lookback: 写盘时该月已出 lookback 窗口, "整月行 + 全部回填" 才都已入盘
  return write_date(*sh) >= misc::add_days(misc::last_day(m), ::config::PIPELINE_LOOKBACK_DAYS) ? State::Frozen
                                                                                                : State::Stale;
}

std::vector<Task> plan(const Spec &s, const Catalog &cat, Ymd today) {
  const Ymd start = ::config::PIPELINE_START_DATE;
  assert(start > 0 && start <= today);
  const Ymd hz = horizon(s.avail_hour, today);
  // 已存在的分片刚写过 → 本轮不动
  auto throttled = [](const Shard *sh) {
    return sh && age_seconds(*sh) < ::config::PIPELINE_DEDUP_WINDOW_SECONDS;
  };

  std::vector<Task> out;
  if (s.shape == Shape::Meta) {
    const Shard *sh = cat.find(s.name, 0);
    if (state(s, sh, 0, today) != State::Frozen && !throttled(sh))
      out.push_back({0, s.vd.empty() ? Window{} : Window{start, hz, 0}});
    return out;
  }

  assert(!s.vd.empty() && "Month 表必须配 vd");
  for (Ym m = misc::ym_of(start);; m = misc::next_ym(m)) {
    const Shard *sh = cat.find(s.name, m);
    const State st = state(s, sh, m, today);
    const Ymd lo = std::max(misc::first_day(m), start);
    const Ymd hi = closed(m, today) ? misc::last_day(m) : std::min(misc::last_day(m), hz);
    if (st != State::Frozen && !throttled(sh) && lo <= hi) {
      const Ymd w = st == State::Open ? max_vd(*sh, s.vd) : 0; // 水位; 0 = 整段覆盖 (Missing / Stale / 0 行文件)
      if (w == 0)
        out.push_back({m, {lo, hi, 0}});
      else if (misc::add_days(w, 1) <= hi)
        out.push_back({m, {lo, hi, misc::add_days(w, 1)}});
    }
    if (m == misc::ym_of(today))
      break;
  }
  return out;
}

std::vector<Ym> frozen(const Spec &s, const Catalog &cat, Ymd today) {
  std::vector<Ym> out;
  if (s.shape != Shape::Month)
    return out;
  const Ym start = misc::ym_of(::config::PIPELINE_START_DATE);
  for (const Shard &sh : cat.shards(s.name))
    if (sh.ym >= start && state(s, &sh, sh.ym, today) == State::Frozen)
      out.push_back(sh.ym);
  return out;
}

void put(const Spec &s, const Task &t, const std::shared_ptr<arrow::Table> &tbl) {
  assert((t.ym == 0) == (s.shape == Shape::Meta) && "put: 任务与表形态不符");
  const fs::path p = t.ym == 0 ? meta_path(s.name) : month_path(t.ym, s.name);
  if (t.w.inc != 0)
    pq::append_table_atomic(p, tbl);
  else
    pq::write_table_atomic(p, tbl);
}

} // namespace fundamental
