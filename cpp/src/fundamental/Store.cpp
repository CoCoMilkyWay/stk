#include "fundamental/Store.hpp"

#include "disk/Disk.hpp"
#include "misc/cross_platform.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <ctime>

namespace fundamental {

namespace fs = std::filesystem;
namespace pq = misc::pq;

namespace {

const std::vector<Shard> kNoShards;

std::tm local_tm(fs::file_time_type t) {
  const std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(t));
  return safe_localtime(&tt);
}

Ymd ymd_of(const std::tm &tm) { return (tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday; }

// avail_hour 0 → 恒 today (排程提前); 24 → 恒昨天 (全天涓流, 次日才完整)
Ymd horizon(int avail_hour, const Now &now) {
  return now.hour >= avail_hour ? now.today : misc::add_days(now.today, -1);
}

// 不晚于 hz 的最近交易日 (日历未入盘时退回 hz)
Ymd trade_hz(Ymd hz, const std::vector<Ymd> &days) {
  const auto it = std::upper_bound(days.begin(), days.end(), hz);
  return it == days.begin() ? hz : *std::prev(it);
}

Ymd write_date(const Shard &sh) { return ymd_of(local_tm(sh.mtime)); }

// 分片末次写盘时的 horizon = 该次同步已拉到的日
Ymd synced(const Spec &s, const Shard &sh) {
  const std::tm tm = local_tm(sh.mtime);
  return horizon(s.avail_hour, {sh.mtime, ymd_of(tm), tm.tm_hour});
}

bool closed(Ym m, Ymd today) {
  return misc::last_day(m) < misc::add_days(today, -kLookbackDays);
}

fs::path shard_path(const Spec &s, Ymd at) {
  switch (s.shape) {
  case Shape::Meta:
    assert(at == 0);
    return disk::path(disk::FundMetaShard, {.name = s.name});
  case Shape::Month:
    assert(at >= 19000100 && at % 100 == 0);
    return disk::path(disk::FundShard, {.date = at, .name = s.name});
  case Shape::Daily:
    assert(at >= 19000101 && at % 100 != 0);
    return disk::path(disk::FundDay, {.date = at, .name = s.name});
  }
  assert(false && "shard_path: 未知形态");
  return {};
}

} // namespace

Now now() {
  const fs::file_time_type at = fs::file_time_type::clock::now();
  const std::tm tm = local_tm(at);
  return {at, ymd_of(tm), tm.tm_hour};
}

Catalog::Catalog() {
  assert(disk::exists(disk::Fundamental) && "output/data/fundamental 缺失");
  const auto add = [&](disk::Node n, const disk::Key &k, Ymd at) {
    const fs::path p = disk::path(n, k);
    by_name_[k.name].push_back({at, p, fs::file_size(p), fs::last_write_time(p)});
  };
  for (const disk::Key &k : disk::list(disk::FundMetaShard))
    add(disk::FundMetaShard, k, 0);
  for (const disk::Key &k : disk::list(disk::FundShard))
    add(disk::FundShard, k, k.date);
  for (const disk::Key &k : disk::list(disk::FundDay))
    add(disk::FundDay, k, k.date);
  for (auto &[name, v] : by_name_)
    std::sort(v.begin(), v.end(), [](const Shard &a, const Shard &b) { return a.at < b.at; });
}

const std::vector<Shard> &Catalog::shards(std::string_view name) const {
  const auto it = by_name_.find(name);
  return it == by_name_.end() ? kNoShards : it->second;
}

const Shard *Catalog::find(std::string_view name, Ymd at) const {
  assert((at == 0 || at >= 19000100) && "Catalog::find: at 须为 0 / YYYYMM00 / YYYYMMDD");
  const std::vector<Shard> &v = shards(name);
  const auto it = std::lower_bound(v.begin(), v.end(), at, [](const Shard &s, Ymd x) { return s.at < x; });
  return (it != v.end() && it->at == at) ? &*it : nullptr;
}

Round::Round() : now(fundamental::now()) {
  if (!cat.shards("all_trading_days").empty())
    days = calendar(cat, now.today);
}

std::vector<Slot> slots(const Spec &s, const Round &r) {
  assert(kSyncStart > 0 && kSyncStart <= r.now.today);
  const Ymd hz = horizon(s.avail_hour, r.now);
  std::vector<Slot> out;
  switch (s.shape) {
  case Shape::Meta: {
    const Shard *sh = r.cat.find(s.name, 0);
    if (s.vd.empty()) {
      const State st = !sh ? State::Missing : synced(s, *sh) >= hz ? State::Frozen
                                                                   : State::Stale;
      out.push_back({0, {}, sh, st});
    } else {
      out.push_back({0, {kSyncStart, trade_hz(hz, r.days)}, sh, !sh ? State::Missing : State::Open});
    }
    return out;
  }
  case Shape::Month:
    for (const Ym m : misc::months_between(kSyncStart, r.now.today)) {
      const Ymd at = month_at(m);
      const bool cl = closed(m, r.now.today);
      const Window w{std::max(misc::first_day(m), kSyncStart), cl ? misc::last_day(m) : std::min(misc::last_day(m), hz)};
      const Shard *sh = r.cat.find(s.name, at);
      const State st = !sh                                                                   ? State::Missing
                       : !cl                                                                 ? State::Open
                       : write_date(*sh) >= misc::add_days(misc::last_day(m), kLookbackDays) ? State::Frozen
                                                                                             : State::Stale;
      out.push_back({at, w, sh, st});
    }
    return out;
  case Shape::Daily:
    for (const Ymd d : r.days) {
      if (d > hz)
        break;
      if (d < kSyncStart)
        continue;
      const Shard *sh = r.cat.find(s.name, d);
      const State st = !sh ? State::Missing : closed(misc::ym_of(d), r.now.today) ? State::Frozen
                                                                                  : State::Open;
      out.push_back({d, {d, d}, sh, st});
    }
    return out;
  }
  assert(false && "slots: 未知形态");
  return out;
}

std::vector<Slot> frozen(const Spec &s, const Round &r) {
  assert(s.shape != Shape::Meta && "frozen: Meta 无冻结槽");
  std::vector<Slot> out;
  for (const Slot &sl : slots(s, r))
    if (sl.st == State::Frozen)
      out.push_back(sl);
  return out;
}

Ymd mark(const Spec &s, const Shard &sh) {
  assert(!s.vd.empty() && "mark: vd 空的表无水位");
  return pq::max_yyyymmdd(sh.path, s.vd);
}

Ymd covered(const Spec &s, const Catalog &cat, bool dense) {
  assert(s.shape != Shape::Daily && "covered: Daily 表按日文件判");
  if (s.shape == Shape::Meta) {
    assert(!dense && "covered: Meta 无逐日行");
    const Shard *sh = cat.find(s.name, 0);
    return !sh ? 0 : s.vd.empty() ? synced(s, *sh)
                                  : mark(s, *sh);
  }
  Ymd out = 0;
  for (Ym m = misc::ym_of(kSyncStart);; m = misc::next_ym(m)) {
    const Shard *sh = cat.find(s.name, month_at(m));
    if (!sh)
      return out;
    const Ymd end = misc::last_day(m);
    if (synced(s, *sh) >= end || (s.snap == Snap::Day && mark(s, *sh) > 0)) {
      out = end;
      continue;
    }
    const Ymd c = dense ? mark(s, *sh) : std::max(mark(s, *sh), synced(s, *sh));
    return std::max(out, std::min(c, end));
  }
}

std::shared_ptr<arrow::Table> read(const Shard &sh, const std::vector<std::string> &cols,
                                   const std::vector<std::string> &dict) {
  return pq::read_table(sh.path, cols, dict);
}

std::vector<Ymd> calendar(const Catalog &cat, Ymd today) {
  std::vector<Ymd> out, d;
  for (const Shard &sh : cat.shards("all_trading_days")) {
    pq::TableView v(read(sh, {"date", "market_code"}));
    if (v.rows() == 0)
      continue;
    v.col("date").yyyymmdd_all(d);
    const pq::Col mc = v.col("market_code");
    for (std::int64_t i = 0; i < v.rows(); ++i)
      if (mc.str(i) == "CN" && d[static_cast<std::size_t>(i)] > 0 && d[static_cast<std::size_t>(i)] <= today)
        out.push_back(d[static_cast<std::size_t>(i)]); // 排程提前含未来日, 截到 today
  }
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  assert(!out.empty() && "all_trading_days 无 CN 行 (基本面未同步?)");
  return out;
}

void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl) {
  const Spec &s = *t.spec;
  const fs::path p = shard_path(s, t.at);
  if (s.shape == Shape::Daily) {
    assert(t.how == Write::Replace && t.w.lo == t.at && t.w.hi == t.at && "put: Daily 一日一任务, 整日 Replace");
    const auto vd = tbl->GetColumnByName(s.vd);
    assert(vd && tbl->num_rows() > 0 && "put: Daily 该日 0 行 (交易日远端未入库?)");
    std::vector<Ymd> d;
    for (const auto &chunk : vd->chunks()) {
      pq::Col(chunk).yyyymmdd_all(d);
      assert(std::all_of(d.begin(), d.end(), [day = t.at](Ymd x) { return x == day; }) && "put: Daily 行的日与任务日不符");
    }
    pq::write_table_atomic(p, tbl);
    return;
  }
  if (t.how == Write::Append) {
    assert(s.shape == Shape::Month && s.snap == Snap::All && "put: Append 只用于 Month·All");
    pq::append_table_atomic(p, tbl);
    return;
  }
  if (s.shape == Shape::Meta && s.snap == Snap::Day && tbl->num_rows() == 0) {
    assert(fs::exists(p) && "put: Meta·Day 首拉 0 行");
    fs::last_write_time(p, fs::file_time_type::clock::now()); // 新交易日远端未出: 保留现存快照, 探测计入 dedup
    return;
  }
  pq::write_table_atomic(p, tbl);
}

} // namespace fundamental
