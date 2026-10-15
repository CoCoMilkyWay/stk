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

long age_seconds(const Shard &sh, const Now &now) {
  return std::chrono::duration_cast<std::chrono::seconds>(now.at - sh.mtime).count();
}

Ymd write_date(const Shard &sh) { return ymd_of(local_tm(sh.mtime)); }

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

// 分片槽 m 的整段窗口: Meta = [start, horizon] (vd 空 → {0, 0});
//                     Month = [max(月初, start), 关月 ? 月末 : min(月末, horizon)]
Window window(const Spec &s, Ym m, const Now &now) {
  const Ymd start = ::config::PIPELINE_START_DATE;
  const Ymd hz = horizon(s.avail_hour, now);
  if (s.shape == Shape::Meta)
    return s.vd.empty() ? Window{} : Window{start, hz};
  return {std::max(misc::first_day(m), start), closed(m, now.today) ? misc::last_day(m) : std::min(misc::last_day(m), hz)};
}

} // namespace

Now now() {
  const fs::file_time_type at = fs::file_time_type::clock::now();
  const std::tm tm = local_tm(at);
  return {at, ymd_of(tm), tm.tm_hour};
}

fs::path root() { return misc::git_root() / "output" / "fundamental"; }

fs::path path(std::string_view name, Ym ym) {
  assert(ym >= 0);
  return root() / (ym == 0 ? std::string("_meta") : misc::dash_ym(ym)) / (std::string(name) + ".parquet");
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

State state(const Spec &s, const Shard *sh, Ym m, const Now &now) {
  if (!sh)
    return State::Missing;
  if (s.shape == Shape::Meta) {
    const bool fresh = s.vd.empty() ? write_date(*sh) == now.today
                                    : max_vd(*sh, s.vd) >= horizon(s.avail_hour, now);
    return fresh ? State::Frozen : State::Stale;
  }
  if (!closed(m, now.today))
    return State::Open;
  // 冻结 = 写盘日 ≥ 月末 + lookback: 写盘时该月已出 lookback 窗口, "整月行 + 全部回填" 才都已入盘
  return write_date(*sh) >= misc::add_days(misc::last_day(m), ::config::PIPELINE_LOOKBACK_DAYS) ? State::Frozen
                                                                                                : State::Stale;
}

std::vector<Task> plan(const Spec &s, const Catalog &cat, const Now &now) {
  assert(::config::PIPELINE_START_DATE > 0 && ::config::PIPELINE_START_DATE <= now.today);
  std::vector<Task> out;
  for (const Ym m : slots(s, now)) {
    const Shard *sh = cat.find(s.name, m);
    const State st = state(s, sh, m, now);
    // 已冻结 / 已存在的分片刚写过 → 本轮不动
    if (st == State::Frozen || (sh && age_seconds(*sh, now) < ::config::PIPELINE_DEDUP_WINDOW_SECONDS))
      continue;
    Task t{&s, m, window(s, m, now), false};
    const Ymd w = st == State::Open ? max_vd(*sh, s.vd) : 0; // 水位; 0 = 整段覆盖 (Missing / Stale / 0 行文件)
    if (w != 0) {
      if (s.snap)
        continue; // 快照已入盘
      t.w.lo = misc::add_days(w, 1);
      t.append = true;
    }
    if (t.w.lo <= t.w.hi) // {0, 0} (vd 空) 亦过
      out.push_back(t);
  }
  return out;
}

std::vector<Ym> frozen(const Spec &s, const Catalog &cat, const Now &now) {
  std::vector<Ym> out;
  if (s.shape != Shape::Month)
    return out;
  const Ym start = misc::ym_of(::config::PIPELINE_START_DATE);
  for (const Shard &sh : cat.shards(s.name))
    if (sh.ym >= start && state(s, &sh, sh.ym, now) == State::Frozen)
      out.push_back(sh.ym);
  return out;
}

void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl) {
  assert(t.spec && (t.ym == 0) == (t.spec->shape == Shape::Meta) && "put: 任务与表形态不符");
  const fs::path p = path(t.spec->name, t.ym);
  if (t.append)
    pq::append_table_atomic(p, tbl);
  else
    pq::write_table_atomic(p, tbl);
}

} // namespace fundamental
