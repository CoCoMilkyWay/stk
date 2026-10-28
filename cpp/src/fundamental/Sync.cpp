#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"
#include "misc/parquet.hpp"
#include "shared/Config.hpp"

#include <cassert>
#include <chrono>
#include <map>
#include <string>
#include <string_view>
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

Budget budget(bigquant::Client &client, const std::vector<Task> &tasks) {
  assert(!tasks.empty() && "budget: 计划为空");
  const Spec &s = *tasks.front().spec;
  Budget b{tasks.size(), tasks.front().w.lo, tasks.back().w.hi};
  std::vector<std::string> keys;
  for (const Task &t : tasks) {
    assert(t.spec == &s && s.shape == Shape::Daily && t.how == Write::Replace && "budget: 只估单张 Daily 表的整日计划");
    assert((keys.empty() || group_key(Grain::Day, t.w.lo) > keys.back()) && "budget: 计划日须升序");
    keys.push_back(group_key(Grain::Day, t.w.lo));
  }
  const Window w{b.lo, b.hi};
  b.cols = client.query(select(s, "*", w, " LIMIT 0"))->num_columns();
  const misc::pq::TableView v(
      client.query(select(s, group_expr(s, Grain::Day) + " AS g, COUNT(*) AS n", w, key_filter(s, keys) + " GROUP BY 1")));
  const misc::pq::Col g = v.col("g"), n = v.col("n");
  std::map<std::string_view, std::int64_t> rows;
  for (std::int64_t i = 0; i < v.rows(); ++i)
    rows[g.str(i)] = n.i32(i, 0);
  for (const std::string &k : keys) {
    const auto it = rows.find(k);
    assert(it != rows.end() && it->second > 0 && "budget: 计划日远端 0 行 (未入库)");
    b.rows += it->second;
  }
  b.cells = b.rows * b.cols;
  return b;
}

template std::vector<Task> plan<Bq>(const Round &, const Keep &);
template std::vector<Task> plan<Tu>(const Round &, const Keep &);
template std::int64_t run<Bq>(bigquant::Client &, const Task &);
template std::int64_t run<Tu>(tushare::Client &, const Task &);

} // namespace fundamental::sync
