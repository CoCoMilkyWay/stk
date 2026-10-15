#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"
#include "shared/Config.hpp"

#include <cassert>
#include <chrono>

namespace fundamental::sync {

namespace {

long age_seconds(const Shard &sh, const Now &now) {
  return std::chrono::duration_cast<std::chrono::seconds>(now.at - sh.mtime).count();
}

} // namespace

template <class Src>
std::vector<Task> plan(const Catalog &cat, const Now &now) {
  std::vector<Task> out;
  for (const Spec &s : SPECS) {
    if (!from<Src>(s))
      continue;
    for (const Slot &sl : ledger(s, cat, now)) {
      // 已冻结 / 已存在的分片刚写过 → 本轮不动
      if (sl.st == State::Frozen || (sl.sh && age_seconds(*sl.sh, now) < ::config::PIPELINE_DEDUP_WINDOW_SECONDS))
        continue;
      Task t{&s, sl.ym, sl.w, Write::Replace};
      const Ymd w = sl.st == State::Open ? mark(s, *sl.sh) : 0; // 水位; 0 = 整段覆盖 (Missing / Stale / 0 行文件)
      if (w != 0) {
        if (s.snap == Snap::Day)
          continue; // 快照已入盘
        t.w.lo = misc::add_days(w, 1);
        t.how = Write::Append;
      }
      if (t.w.lo <= t.w.hi) // {0, 0} (vd 空) 亦过
        out.push_back(t);
    }
  }
  return out;
}

template <class Src>
std::int64_t run(typename Src::Client &client, const Task &t) {
  const Spec &s = *t.spec;
  assert(!(t.how == Write::Append && s.snap == Snap::Day) && "sync::run: Snap::Day 表只整段覆盖");
  const auto tbl = fetch<Src>(client, s, t.w);
  put(s, t.ym, t.how, tbl);
  return tbl->num_rows();
}

std::int64_t patch(bigquant::Client &client, const Spec &s, const Shard &sh, const std::vector<Ymd> &days) {
  const auto tbl = fetch_days(client, s, days);
  splice(s, sh, days, tbl);
  return tbl->num_rows();
}

std::string label(const Task &t) {
  return t.spec->name + " " + (t.ym == 0 ? "_meta" : misc::dash_ym(t.ym)) +
         (t.how == Write::Append ? " +[" + misc::dense(t.w.lo) + "..]" : "");
}

template std::vector<Task> plan<Bq>(const Catalog &, const Now &);
template std::vector<Task> plan<Tu>(const Catalog &, const Now &);
template std::int64_t run<Bq>(bigquant::Client &, const Task &);
template std::int64_t run<Tu>(tushare::Client &, const Task &);

} // namespace fundamental::sync
