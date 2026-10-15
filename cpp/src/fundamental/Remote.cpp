#include "fundamental/Remote.hpp"

#include "api/tushare/client.hpp"

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

namespace fundamental {

namespace {

std::string quoted(Ymd d) { return "'" + misc::dash(d) + "'"; }

std::vector<bigquant::Query> queries(const Spec &s, const Bq &, Window w) {
  return {select(s, "*", w)};
}

std::vector<tushare::Query> queries(const Spec &, const Tu &src, Window w) {
  std::vector<tushare::Query> out;
  if (src.day_params.empty()) {
    out.push_back({src.api, {{"start_date", misc::dense(w.lo)}, {"end_date", misc::dense(w.hi)}}, src.num});
    return out;
  }
  for (Ymd d = w.lo; d <= w.hi; d = misc::add_days(d, 1))
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
std::shared_ptr<arrow::Table> fetch(typename Src::Client &client, const Spec &s, Window w) {
  assert(from<Src>(s) && "fetch: 表与 client 的数据源不符");
  std::vector<std::shared_ptr<arrow::Table>> parts;
  for (const auto &q : queries(s, std::get<Src>(s.src), w))
    parts.push_back(client.query(q));
  assert(!parts.empty());
  auto joined = arrow::ConcatenateTables(parts);
  assert(joined.ok() && "fetch: 响应 schema 跨查询不一致");
  return strip(s, std::move(joined).ValueUnsafe());
}

std::shared_ptr<arrow::Table> fetch_days(bigquant::Client &client, const Spec &s, const std::vector<Ymd> &days) {
  assert(s.snap == Snap::All && "fetch_days: Snap::Day 表只能整段 fetch");
  assert(!days.empty() && std::is_sorted(days.begin(), days.end()) && "fetch_days: days 须升序非空");
  assert(misc::ym_of(days.front()) == misc::ym_of(days.back()) && "fetch_days: days 须同一月");
  std::string in;
  for (Ymd d : days)
    in += (in.empty() ? "" : ", ") + quoted(d);
  const std::string tail = " AND strftime(" + ident(s.vd) + ", '%Y-%m-%d') IN (" + in + ")";
  return strip(s, client.query(select(s, "*", {days.front(), days.back()}, tail)));
}

template std::shared_ptr<arrow::Table> fetch<Bq>(bigquant::Client &, const Spec &, Window);
template std::shared_ptr<arrow::Table> fetch<Tu>(tushare::Client &, const Spec &, Window);

std::string ident(std::string_view c) { return "\"" + std::string(c) + "\""; }

bigquant::Query select(const Spec &s, std::string_view cols, Window w, std::string_view tail) {
  assert(from<Bq>(s) && "select: 表不来自 BigQuant");
  std::string sql = "SELECT " + std::string(cols) + " FROM " + s.name;
  if (s.vd.empty()) {
    assert(w.lo == 0 && w.hi == 0 && "select: vd 空的表无窗口");
    return {sql + std::string(tail), {}};
  }
  assert(w.lo > 0 && w.lo <= w.hi && "select: 窗口须非空");
  const std::string vd = ident(s.vd);
  const std::string rng = vd + " >= " + quoted(w.lo) + " AND " + vd + " <= " + quoted(w.hi);
  sql += " WHERE " + rng;
  if (s.snap == Snap::Day) {
    const bool month = s.shape == Shape::Month;
    const std::string group = month ? " GROUP BY strftime(" + vd + ", '%Y-%m')" : "";
    sql += " AND " + vd + " IN (SELECT " + (month ? "MIN" : "MAX") + "(" + vd + ") FROM " + s.name + " WHERE " + rng + group + ")";
  }
  bigquant::Filters f;
  if (std::get<Bq>(s.src).prune == Prune::Partition)
    f = {{"date", {misc::dash(w.lo), misc::dash(w.hi)}}};
  return {sql + std::string(tail), std::move(f)};
}

} // namespace fundamental
