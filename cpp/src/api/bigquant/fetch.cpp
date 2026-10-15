#include "api/bigquant/fetch.hpp"

#include <cassert>
#include <string>

namespace bigquant {

namespace {

using fundamental::Pick;
using fundamental::Shape;

std::string quoted(fundamental::Ymd d) { return "'" + misc::dash(d) + "'"; }

std::string ident(const std::string &c) { return "\"" + c + "\""; }

} // namespace

std::string where(const fundamental::Spec &s, fundamental::Ymd lo, fundamental::Ymd hi) {
  if (s.vd.empty()) {
    assert(s.shape == Shape::Meta && s.pick == Pick::All && "vd 空只允许 Meta·All");
    return {};
  }
  assert(lo > 0 && lo <= hi && "where: 窗口须非空");
  assert((s.pick != Pick::First || s.shape == Shape::Month) && "Pick::First 只配 Month");
  assert((s.pick != Pick::Last || s.shape == Shape::Meta) && "Pick::Last 只配 Meta");
  const std::string vd = ident(s.vd);
  const std::string rng = vd + " >= " + quoted(lo) + " AND " + vd + " <= " + quoted(hi);
  std::string w = " WHERE " + rng;
  if (s.pick == Pick::All)
    return w;
  const char *agg = s.pick == Pick::First ? "MIN" : "MAX";
  const std::string group = s.shape == Shape::Month ? " GROUP BY strftime(" + vd + ", '%Y-%m')" : "";
  w += " AND " + vd + " IN (SELECT " + agg + "(" + vd + ") FROM " + s.name + " WHERE " + rng + group + ")";
  return w;
}

DaiFilters prune(const fundamental::Spec &s, fundamental::Ymd lo, fundamental::Ymd hi) {
  if (!std::get<fundamental::Dai>(s.src).partition)
    return {};
  return {{"date", {misc::dash(lo), misc::dash(hi)}}};
}

std::shared_ptr<arrow::Table> fetch(DaiClient &client, const fundamental::Spec &s,
                                    const fundamental::Dai &, const fundamental::Window &w) {
  if (s.vd.empty()) {
    assert(w.inc == 0 && "vd 空的表无水位, 不支持增量");
    return client.query("SELECT * FROM " + s.name);
  }
  assert((w.inc == 0 || (w.inc >= w.lo && w.inc <= w.hi)) && "增量下界须落在窗口内");
  const fundamental::Ymd lo = (s.pick == Pick::All && w.inc != 0) ? w.inc : w.lo;
  std::string sql = "SELECT * FROM " + s.name + where(s, lo, w.hi);
  if (s.pick != Pick::All && w.inc != 0)
    sql += " AND " + ident(s.vd) + " >= " + quoted(w.inc);
  return client.query(sql, prune(s, lo, w.hi));
}

} // namespace bigquant
