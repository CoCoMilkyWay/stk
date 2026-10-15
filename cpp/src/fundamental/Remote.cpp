#include "fundamental/Remote.hpp"

#include "api/tushare/client.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace fundamental {

namespace {

std::string quoted(Ymd d) { return "'" + misc::dash(d) + "'"; }

// Splice 的按日谓词 (日键与 audit 指纹组键同形)
std::string in_days(const Spec &s, const std::vector<Ymd> &days) {
  std::string in;
  for (Ymd d : days)
    in += (in.empty() ? "" : ", ") + quoted(d);
  return " AND strftime(" + ident(s.vd) + ", '%Y-%m-%d') IN (" + in + ")";
}

std::vector<bigquant::Query> queries(const Spec &s, const Bq &, const Task &t) {
  return {select(s, "*", t.w, t.how == Write::Splice ? in_days(s, t.days) : "")};
}

std::vector<tushare::Query> queries(const Spec &, const Tu &src, const Task &t) {
  assert(t.how != Write::Splice && "fetch: Tushare 无按日修补");
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
  assert((t.how == Write::Replace || s.snap == Snap::All) && "fetch: Snap::Day 表只整段 Replace (MIN / MAX 须看整窗)");
  assert((t.how != Write::Splice ||
          (!t.days.empty() && t.w.lo == t.days.front() && t.w.hi == t.days.back())) &&
         "fetch: Splice 的窗口须 = [首日, 末日]");
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
