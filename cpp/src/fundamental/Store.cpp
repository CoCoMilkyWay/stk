#include "fundamental/Store.hpp"

#include "misc/cross_platform.hpp"
#include "misc/fs.hpp"
#include "misc/parquet.hpp"
#include "shared/Config.hpp"

#include <arrow/array.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <ctime>
#include <iterator>
#include <map>
#include <utility>

namespace fundamental {

namespace fs = std::filesystem;
namespace pq = misc::pq;

namespace {

const std::vector<Shard> kNoShards;

// file_clock 时刻 → 本地时区 (Now 与写盘日同一换算)
std::tm local_tm(fs::file_time_type t) {
  const std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::clock_cast<std::chrono::system_clock>(t));
  return safe_localtime(&tt);
}

Ymd ymd_of(const std::tm &tm) { return (tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday; }

// avail_hour 0 → 恒 today (排程提前); 24 → 恒昨天 (全天涓流, 次日才完整)
Ymd horizon(int avail_hour, const Now &now) {
  return now.hour >= avail_hour ? now.today : misc::add_days(now.today, -1);
}

Ymd write_date(const Shard &sh) { return ymd_of(local_tm(sh.mtime)); }

// 本地 d 日 hour 点 → file_clock (与 Shard::mtime 同钟); hour = 24 → 次日 0 点 (mktime 归一)
fs::file_time_type local_at(Ymd d, int hour) {
  std::tm tm{};
  tm.tm_year = d / 10000 - 1900;
  tm.tm_mon = d / 100 % 100 - 1;
  tm.tm_mday = d % 100;
  tm.tm_hour = hour;
  tm.tm_isdst = -1;
  const std::time_t tt = std::mktime(&tm);
  assert(tt != -1 && "local_at: mktime 失败");
  return std::chrono::clock_cast<fs::file_time_type::clock>(std::chrono::system_clock::from_time_t(tt));
}

fs::path root() { return misc::git_root() / "output" / "fundamental"; }

// name 的分片路径: ym > 0 → YYYY-MM/<name>.parquet; ym = 0 → _meta/<name>.parquet
fs::path path(std::string_view name, Ym ym) {
  assert(ym >= 0);
  return root() / (ym == 0 ? std::string("_meta") : misc::dash_ym(ym)) / (std::string(name) + ".parquet");
}

// Daily 的日文件路径: YYYY-MM/<name>/YYYY-MM-DD.parquet
fs::path day_path(std::string_view name, Ymd d) {
  assert(d > 0);
  return root() / misc::dash_ym(misc::ym_of(d)) / std::string(name) / (misc::dash(d) + ".parquet");
}

bool is_daily(std::string_view name) {
  for (const Spec &s : SPECS)
    if (s.shape == Shape::Daily && s.name == name)
      return true;
  return false;
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
  if (s.shape == Shape::Daily)
    return State::Frozen; // 只拉到 horizon (昨天) ⇒ 落盘即整日
  if (s.shape == Shape::Meta) {
    const Ymd hz = horizon(s.avail_hour, now);
    const bool fresh = s.vd.empty() ? sh->mtime >= local_at(hz, s.avail_hour) : mark(s, *sh) >= hz;
    return fresh ? State::Frozen : State::Stale;
  }
  if (!closed(m, now.today))
    return State::Open;
  // 冻结 = 写盘日 ≥ 月末 + lookback: 写盘时该月已出 lookback 窗口, "整月行 + 全部回填" 才都已入盘
  return write_date(*sh) >= misc::add_days(misc::last_day(m), ::config::PIPELINE_LOOKBACK_DAYS) ? State::Frozen
                                                                                                : State::Stale;
}

// t 的逐行 vd (YYYYMMDD) 与逐行是否命中 keys (Day 键整日命中, Asset 键按 日|资产 命中)
void hits(const Spec &s, const arrow::Table &t, const std::vector<std::string> &keys, std::vector<Ymd> &d,
          std::vector<char> &hit) {
  const auto vd = t.GetColumnByName(s.vd);
  assert(vd && "put: Splice 缺 vd 列");
  d.clear();
  std::vector<Ymd> part;
  for (const auto &chunk : vd->chunks()) {
    pq::Col(chunk).yyyymmdd_all(part);
    d.insert(d.end(), part.begin(), part.end());
  }
  const bool by_asset =
      std::any_of(keys.begin(), keys.end(), [](const std::string &k) { return grain_of(k) == Grain::Asset; });
  std::vector<std::string_view> asset;
  if (by_asset) {
    const auto key = t.GetColumnByName(std::string(key_col(s)));
    assert(key && "put: Splice 缺资产键列");
    for (const auto &chunk : key->chunks()) {
      const pq::Col k(chunk);
      for (std::int64_t i = 0; i < chunk->length(); ++i)
        asset.push_back(k.str(i));
    }
  }
  auto in = [&](const std::string &k) { return std::binary_search(keys.begin(), keys.end(), k); };
  hit.assign(d.size(), 0);
  for (std::size_t i = 0; i < d.size(); ++i) {
    assert(d[i] > 0 && "put: Splice 行的 vd 为 null");
    hit[i] = in(group_key(Grain::Day, d[i])) || (by_asset && in(group_key(Grain::Asset, d[i], asset[i])));
  }
}

// p 里组键 ∈ keys 的行删掉, tbl 的行按日归位后原子重写 (见 put):
//   新行插在旧分片该日行块之后; 旧分片无该日 → 前一日行块之后, 无前一日 → 最前; 同位多日按日升序
//   ⇒ 布局与整拉同为 "按日成块". 列序 / 列型同旧分片 (全 null 的 NA 列按新行提升), 新行不得带新列
void splice(const Spec &s, const fs::path &p, const std::vector<std::string> &keys,
            const std::shared_ptr<arrow::Table> &tbl) {
  assert(s.shape == Shape::Month && "put: Splice 只用于月分片");
  assert(fs::exists(p) && "put: Splice 的分片须已存在");
  assert(std::is_sorted(keys.begin(), keys.end()) && std::adjacent_find(keys.begin(), keys.end()) == keys.end() &&
         "put: Splice 的 keys 须升序去重");
  std::vector<Ymd> nd, od;
  std::vector<char> nhit, ohit;
  hits(s, *tbl, keys, nd, nhit);
  for ([[maybe_unused]] char h : nhit)
    assert(h && "put: Splice 新行不在替换键内");
  const auto old = pq::read_table(p);
  hits(s, *old, keys, od, ohit);
  const std::int64_t n = static_cast<std::int64_t>(od.size());

  // 新行按日成段, 每段的插入位 at = 插在旧行 at 之前
  std::map<Ymd, std::int64_t> last; // 旧分片各日末行
  for (std::int64_t i = 0; i < n; ++i)
    last[od[i]] = i;
  auto anchor = [&](Ymd d) -> std::int64_t {
    auto it = last.lower_bound(d);
    if (it != last.end() && it->first == d)
      return it->second + 1;
    return it == last.begin() ? 0 : std::prev(it)->second + 1;
  };
  struct Run {
    std::int64_t at;
    Ymd day;
    std::int64_t start, len;
  };
  std::vector<Run> runs;
  for (std::int64_t j = 0; j < static_cast<std::int64_t>(nd.size());) {
    std::int64_t k = j;
    while (k < static_cast<std::int64_t>(nd.size()) && nd[k] == nd[j])
      ++k;
    runs.push_back({anchor(nd[j]), nd[j], j, k - j});
    j = k;
  }
  std::stable_sort(runs.begin(), runs.end(),
                   [](const Run &a, const Run &b) { return std::pair(a.at, a.day) < std::pair(b.at, b.day); });

  // 首段放旧分片 0 行切片 ⇒ 合并后列序 = 旧分片
  std::vector<std::shared_ptr<arrow::Table>> parts{old->Slice(0, 0)};
  std::size_t r = 0;
  auto place = [&](std::int64_t at) {
    for (; r < runs.size() && runs[r].at == at; ++r)
      parts.push_back(tbl->Slice(runs[r].start, runs[r].len));
  };
  for (std::int64_t i = 0; i < n;) {
    place(i);
    if (ohit[i]) {
      ++i;
      continue;
    }
    std::int64_t j = i + 1;
    while (j < n && !ohit[j] && !(r < runs.size() && runs[r].at == j))
      ++j;
    parts.push_back(old->Slice(i, j - i));
    i = j;
  }
  place(n);
  assert(r == runs.size());
  arrow::ConcatenateTablesOptions opt;
  opt.unify_schemas = true;
  auto joined = arrow::ConcatenateTables(parts, opt);
  assert(joined.ok() && "put: Splice 新旧 schema 无法合并");
  auto out = std::move(joined).ValueUnsafe();
  assert(out->num_columns() == old->num_columns() && "put: Splice 新行带本地分片没有的列 (远端 schema 变了, 整月重拉)");
  pq::write_table_atomic(p, out);
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
    assert(!is_daily(dir) && "Catalog: Daily 表须在 YYYY-MM/<name>/ 下, 不能是顶层目录");
    // 只有 "_meta" 和 "YYYY-MM" 两种目录; 再其余 (pool 等) 不是本流水线产物
    const Ym m = dir == "_meta" ? 0 : misc::parse_ym(dir);
    if (dir != "_meta" && m == 0)
      continue;
    for (const fs::directory_entry &f : fs::directory_iterator(d.path())) {
      // Daily 表的子目录 YYYY-MM/<name>/YYYY-MM-DD.parquet
      if (f.is_directory()) {
        const std::string name = f.path().filename().string();
        assert(m > 0 && is_daily(name) && "Catalog: 月目录下的子目录只应是 Daily 表");
        for (const fs::directory_entry &g : fs::directory_iterator(f.path())) {
          if (g.path().extension() != ".parquet")
            continue;
          const Ymd day = misc::parse_ymd(g.path().stem().string());
          assert(day > 0 && misc::ym_of(day) == m && "Catalog: Daily 文件名须为所属月内的 YYYY-MM-DD");
          by_name_[name].push_back({m, g.path(), g.file_size(), g.last_write_time(), day});
        }
        continue;
      }
      if (f.path().extension() != ".parquet")
        continue;
      by_name_[f.path().stem().string()].push_back({m, f.path(), f.file_size(), f.last_write_time()});
    }
  }
  for (auto &[name, v] : by_name_)
    std::sort(v.begin(), v.end(), [](const Shard &a, const Shard &b) { return std::pair(a.ym, a.day) < std::pair(b.ym, b.day); });
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

const Shard *Catalog::find_day(std::string_view name, Ymd d) const {
  const std::vector<Shard> &v = shards(name);
  const auto it = std::lower_bound(v.begin(), v.end(), d, [](const Shard &s, Ymd x) { return s.day < x; });
  return (it != v.end() && it->day == d) ? &*it : nullptr;
}

Round::Round() : now(fundamental::now()) {
  if (!cat.shards("all_trading_days").empty())
    days = calendar(cat, now.today);
}

std::vector<Slot> ledger(const Spec &s, const Round &r) {
  assert(::config::PIPELINE_START_DATE > 0 && ::config::PIPELINE_START_DATE <= r.now.today);
  std::vector<Slot> out;
  if (s.shape == Shape::Daily) {
    // 日历未入盘 (首轮同步前) → days 空: 本轮无槽, 下轮再排
    const Ymd hz = horizon(s.avail_hour, r.now);
    for (const Ymd d : r.days) {
      if (d > hz)
        break;
      if (d < ::config::PIPELINE_START_DATE)
        continue;
      const Shard *sh = r.cat.find_day(s.name, d);
      out.push_back({misc::ym_of(d), sh, state(s, sh, misc::ym_of(d), r.now), {d, d}});
    }
    return out;
  }
  for (const Ym m : slots(s, r.now)) {
    const Shard *sh = r.cat.find(s.name, m);
    out.push_back({m, sh, state(s, sh, m, r.now), window(s, m, r.now)});
  }
  return out;
}

std::vector<Slot> frozen(const Spec &s, const Round &r) {
  std::vector<Slot> out;
  if (s.shape == Shape::Meta)
    return out;
  for (const Slot &sl : ledger(s, r))
    if (sl.st == State::Frozen)
      out.push_back(sl);
  return out;
}

std::string slot_key(const Spec &s, const Slot &sl) { return group_key(slot_grain(s), sl.w.lo); }

std::optional<Task> todo(const Spec &s, const Slot &sl) {
  if (sl.st == State::Frozen)
    return std::nullopt;
  Task t = whole(s, sl);
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

Task whole(const Spec &s, const Slot &sl) {
  Task t{&s, sl.ym, sl.w};
  if (s.shape == Shape::Daily)
    t.keys = {slot_key(s, sl)};
  return t;
}

void add(std::vector<Task> &plan, Task t) {
  if (t.spec->shape == Shape::Daily && !plan.empty()) {
    Task &b = plan.back();
    if (b.spec == t.spec && b.ym == t.ym && static_cast<int>(b.keys.size()) < ::config::BIGQUANT_PACK_DAYS) {
      assert(t.keys.size() == 1 && t.w.lo > b.w.hi && "add: Daily 逐日升序并入");
      b.w.hi = t.w.hi;
      b.keys.push_back(std::move(t.keys.front()));
      return;
    }
  }
  plan.push_back(std::move(t));
}

Ymd mark(const Spec &s, const Shard &sh) {
  assert(!s.vd.empty() && "mark: vd 空的表无水位");
  return pq::max_yyyymmdd(sh.path, s.vd);
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
  assert((t.ym == 0) == (s.shape == Shape::Meta) && "put: 分片与表形态不符");
  if (s.shape == Shape::Daily) {
    assert(t.how == Write::Replace && !t.keys.empty() && "put: Daily 只整日 Replace, keys = 待拉日");
    const auto vd = tbl->GetColumnByName(s.vd);
    assert(vd && vd->num_chunks() == static_cast<int>(t.keys.size()) && "put: Daily 须一日一块, 与待拉日一一对应 (交易日远端未入库?)");
    std::vector<Ymd> d;
    std::int64_t at = 0;
    for (int c = 0; c < vd->num_chunks(); ++c) {
      const Ymd day = key_day(t.keys[static_cast<std::size_t>(c)]);
      pq::Col(vd->chunk(c)).yyyymmdd_all(d);
      assert(grain_of(t.keys[static_cast<std::size_t>(c)]) == Grain::Day && misc::ym_of(day) == t.ym && "put: Daily 待拉日须为 ym 内的 Day 键");
      assert(!d.empty() && std::all_of(d.begin(), d.end(), [day](Ymd x) { return x == day; }) && "put: Daily 块内日与待拉日不符");
      pq::write_table_atomic(day_path(s.name, day), tbl->Slice(at, static_cast<std::int64_t>(d.size())));
      at += static_cast<std::int64_t>(d.size());
    }
    return;
  }
  const fs::path p = path(s.name, t.ym);
  switch (t.how) {
  case Write::Replace:
    pq::write_table_atomic(p, tbl);
    return;
  case Write::Append:
    pq::append_table_atomic(p, tbl);
    return;
  case Write::Splice:
    assert(!t.keys.empty() && "put: Splice 的 keys 为空");
    for ([[maybe_unused]] const std::string &k : t.keys)
      assert(grain_of(k) != Grain::Month && key_ym(k) == t.ym && "put: Splice 键须为 Day / Asset 级且同属 ym");
    splice(s, p, t.keys, tbl);
    return;
  }
  assert(false && "put: 未知写法");
}

bool renative(const Shard &sh) { return pq::renative(sh.path); }

} // namespace fundamental
