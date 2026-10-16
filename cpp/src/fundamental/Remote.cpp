#include "fundamental/Remote.hpp"

#include "api/tushare/client.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace fundamental {

namespace {

std::string literal(std::string_view v) { return "'" + std::string(v) + "'"; }
std::string literal(Ymd d) { return literal(misc::dash(d)); }

std::vector<bigquant::Query> queries(const Spec &s, const Bq &, const Task &t) {
  return {select(s, "*", t.w, t.how == Write::Splice ? key_filter(s, t.keys) : "")};
}

std::vector<tushare::Query> queries(const Spec &, const Tu &src, const Task &t) {
  assert(t.how != Write::Splice && "fetch: Tushare 无精确修补");
  std::vector<tushare::Query> out;
  if (src.day_params.empty()) {
    out.push_back({src.api, {{"start_date", misc::dense(t.w.lo)}, {"end_date", misc::dense(t.w.hi)}}, src.num});
    return out;
  }
  for (Ymd d = t.w.lo; d <= t.w.hi; d = misc::add_days(d, 1))
    for (const std::string &p : src.day_params)
      out.push_back({src.api, {{p, misc::dense(d)}}, src.num});
  return out;
}

// 剥 Spec::drop
std::shared_ptr<arrow::Table> strip(const Spec &s, std::shared_ptr<arrow::Table> tbl) {
  for (const std::string &c : s.drop) {
    const int i = tbl->schema()->GetFieldIndex(c);
    assert(i >= 0 && "fetch: drop 声明的列不在响应里");
    auto r = tbl->RemoveColumn(i);
    assert(r.ok());
    tbl = std::move(r).ValueUnsafe();
  }
  return tbl;
}

} // namespace

template <class Src>
std::shared_ptr<arrow::Table> fetch(typename Src::Client &client, const Task &t) {
  const Spec &s = *t.spec;
  assert(from<Src>(s) && "fetch: 表与 client 的数据源不符");
  assert((t.how != Write::Append || s.snap == Snap::All) && "fetch: Snap::Day 表无水位增量 (MIN / MAX 须看整窗)");
  assert((t.how != Write::Splice || !t.keys.empty()) && "fetch: Splice 的 keys 为空");
  for ([[maybe_unused]] const std::string &k : t.keys)
    assert(grain_of(k) != Grain::Month && key_day(k) >= t.w.lo && key_day(k) <= t.w.hi &&
           "fetch: Splice 键须为 Day / Asset 级且在窗口内");
  std::vector<std::shared_ptr<arrow::Table>> parts;
  for (const auto &q : queries(s, std::get<Src>(s.src), t))
    parts.push_back(client.query(q));
  assert(!parts.empty());
  auto joined = arrow::ConcatenateTables(parts);
  assert(joined.ok() && "fetch: 响应 schema 跨查询不一致");
  return strip(s, std::move(joined).ValueUnsafe());
}

template std::shared_ptr<arrow::Table> fetch<Bq>(bigquant::Client &, const Task &);
template std::shared_ptr<arrow::Table> fetch<Tu>(tushare::Client &, const Task &);

std::string ident(std::string_view c) { return "\"" + std::string(c) + "\""; }

std::string group_expr(const Spec &s, Grain g) {
  assert(!s.vd.empty() && "group_expr: vd 空的表无组键");
  const std::string day = "strftime(" + ident(s.vd) + ", '%Y-%m-%d')";
  switch (g) {
  case Grain::Month:
    return "strftime(" + ident(s.vd) + ", '%Y-%m')";
  case Grain::Day:
    return day;
  case Grain::Asset:
    return day + " || '|' || " + ident(key_col(s));
  }
  assert(false && "group_expr: 未知粒度");
  return {};
}

std::string key_filter(const Spec &s, const std::vector<std::string> &keys) {
  assert(!keys.empty() && "key_filter: keys 为空");
  std::string in[3];
  for (const std::string &k : keys) {
    std::string &v = in[static_cast<int>(grain_of(k))];
    v += (v.empty() ? "" : ", ") + literal(k);
  }
  std::string any;
  for (const Grain g : {Grain::Month, Grain::Day, Grain::Asset}) {
    const std::string &v = in[static_cast<int>(g)];
    if (!v.empty())
      any += (any.empty() ? "" : " OR ") + group_expr(s, g) + " IN (" + v + ")";
  }
  return " AND (" + any + ")";
}

bigquant::Query select(const Spec &s, std::string_view cols, Window w, std::string_view tail) {
  assert(from<Bq>(s) && "select: 表不来自 BigQuant");
  const std::string &table = std::get<Bq>(s.src).table.empty() ? s.name : std::get<Bq>(s.src).table;
  std::string sql = "SELECT " + std::string(cols) + " FROM " + table;
  if (s.vd.empty()) {
    assert(w.lo == 0 && w.hi == 0 && "select: vd 空的表无窗口");
    return {sql + std::string(tail), {}};
  }
  assert(w.lo > 0 && w.lo <= w.hi && "select: 窗口须非空");
  const std::string vd = ident(s.vd);
  // Daily 的 vd 是分钟时间戳, "<= 'hi'" 只到 hi 当日 00:00 ⇒ 半开 [lo, hi + 1) (分区 filters 同放宽到次日)
  const bool minute = s.shape == Shape::Daily;
  const Ymd end = minute ? misc::add_days(w.hi, 1) : w.hi;
  const std::string rng = vd + " >= " + literal(w.lo) + " AND " + vd + (minute ? " < " : " <= ") + literal(end);
  sql += " WHERE " + rng;
  if (s.snap == Snap::Day) {
    const bool month = s.shape == Shape::Month;
    const std::string group = month ? " GROUP BY strftime(" + vd + ", '%Y-%m')" : "";
    sql += " AND " + vd + " IN (SELECT " + (month ? "MIN" : "MAX") + "(" + vd + ") FROM " + table + " WHERE " + rng + group + ")";
  }
  bigquant::Filters f;
  if (std::get<Bq>(s.src).prune == Prune::Partition)
    f = {{"date", {misc::dash(w.lo), misc::dash(end)}}};
  return {sql + std::string(tail), std::move(f)};
}

} // namespace fundamental
