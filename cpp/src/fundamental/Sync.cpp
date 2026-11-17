#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"

#include <cassert>
#include <chrono>
#include <string>
#include <utility>

namespace fundamental {

namespace sync {

std::vector<Task> plan(const Spec &s, const Round &r) {
  std::vector<Task> out;
  for (const Slot &sl : ledger(s, r)) {
    // 已存在的分片刚写过 → 本轮不动
    if (sl.sh && r.now.at - sl.sh->mtime < std::chrono::seconds(kDedupSeconds))
      continue;
    if (auto t = todo(s, sl))
      out.push_back(std::move(*t));
  }
  return out;
}

template <class Src>
std::int64_t run(typename Src::Client &client, const Task &t) {
  const auto tbl = fetch<Src>(client, t);
  put(t, tbl);
  return tbl->num_rows();
}

template std::int64_t run<Bq>(bigquant::Client &, const Task &);
template std::int64_t run<Tu>(tushare::Client &, const Task &);

} // namespace sync

std::string label(const Task &t) {
  if (t.spec->shape == Shape::Daily)
    return t.spec->name + " " + misc::dash(t.w.lo);
  const std::string at = t.spec->name + " " + (t.ym == 0 ? "_meta" : misc::dash_ym(t.ym));
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
  assert(false && "label: 未知写法");
  return at;
}

} // namespace fundamental
