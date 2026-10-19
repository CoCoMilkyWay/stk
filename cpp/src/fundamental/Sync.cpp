#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"
#include "shared/Config.hpp"

#include <cassert>
#include <chrono>
#include <string>
#include <utility>

namespace fundamental::sync {

namespace {

long age_seconds(const Shard &sh, const Now &now) {
  return std::chrono::duration_cast<std::chrono::seconds>(now.at - sh.mtime).count();
}

} // namespace

template <class Src>
std::vector<Task> plan(const Round &r, const Keep &keep) {
  std::vector<Task> out;
  for (const Spec &s : SPECS) {
    if (!from<Src>(s))
      continue;
    for (const Slot &sl : ledger(s, r)) {
      // 已存在的分片刚写过 → 本轮不动
      if (sl.sh && age_seconds(*sl.sh, r.now) < ::config::PIPELINE_DEDUP_WINDOW_SECONDS)
        continue;
      if (keep && !keep(s, sl))
        continue;
      if (auto t = todo(s, sl))
        out.push_back(std::move(*t));
    }
  }
  return out;
}

template <class Src>
std::int64_t run(typename Src::Client &client, const Task &t) {
  const auto tbl = fetch<Src>(client, t);
  put(t, tbl);
  return tbl->num_rows();
}

std::string label(const Task &t) {
  const std::string at = t.spec->name + " " +
                         (t.spec->shape == Shape::Daily ? misc::dash(t.w.lo) : t.ym == 0 ? "_meta"
                                                                                         : misc::dash_ym(t.ym));
  switch (t.how) {
  case Write::Replace:
    return at;
  case Write::Append:
    return at + " +[" + misc::dense(t.w.lo) + "..]";
  case Write::Splice: {
    std::size_t day = 0, asset = 0;
    for (const std::string &k : t.keys)
      ++(grain_of(k) == Grain::Day ? day : asset);
    return at + " ~[" + std::to_string(day) + " 日 + " + std::to_string(asset) + " 资产]";
  }
  }
  assert(false && "sync::label: 未知写法");
  return at;
}

template std::vector<Task> plan<Bq>(const Round &, const Keep &);
template std::vector<Task> plan<Tu>(const Round &, const Keep &);
template std::int64_t run<Bq>(bigquant::Client &, const Task &);
template std::int64_t run<Tu>(tushare::Client &, const Task &);

} // namespace fundamental::sync
