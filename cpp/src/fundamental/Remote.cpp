#include "fundamental/Remote.hpp"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace fundamental {

namespace {

constexpr const char *kDcConcept = "概念板块";
constexpr std::string_view kDcSuffix = ".DC";
constexpr int kDcLookback = 15;
constexpr std::int64_t kDcRows = 8000;

std::string literal(std::string_view v) { return "'" + std::string(v) + "'"; }
std::string literal(Ymd d) { return literal(misc::dash(d)); }

std::string order(const Spec &s) {
  if (s.vd.empty())
    return {};
  return s.shape == Shape::Daily ? ident(key_col(s)) + ", " + ident(s.vd) : ident(s.vd);
}

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

std::vector<std::string> strings(const arrow::Table &t, const char *col) {
  const auto c = t.GetColumnByName(col);
  assert(c && c->num_chunks() == 1 && c->null_count() == 0 && "concept: 列缺失或有空值");
  const auto &a = static_cast<const arrow::StringArray &>(*c->chunk(0));
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(a.length()));
  for (std::int64_t i = 0; i < a.length(); ++i)
    out.emplace_back(a.GetView(i));
  return out;
}

std::shared_ptr<arrow::Table> fetch_concept(tushare::Client &tu, Window w) {
  assert(w.lo > 0 && w.lo <= w.hi);
  const auto sch = arrow::schema({arrow::field("trade_date", arrow::utf8()),
                                  arrow::field("ts_code", arrow::utf8()),
                                  arrow::field("code", arrow::utf8()),
                                  arrow::field("name", arrow::utf8())});
  std::shared_ptr<arrow::Table> boards;
  std::string at;
  const Ymd floor = std::max(w.lo, misc::add_days(w.hi, -kDcLookback));
  for (Ymd d = w.hi; d >= floor; d = misc::add_days(d, -1)) {
    at = misc::dense(d);
    boards = tu.query({"dc_index", {{"trade_date", at}, {"idx_type", kDcConcept}}, {"pct_change", "leading_pct", "total_mv", "turnover_rate", "up_num", "down_num"}});
    if (boards->num_rows() > 0)
      break;
  }
  if (!boards || boards->num_rows() == 0) {
    assert(w.lo > kSyncStart && "concept: 近期无板块数据");
    auto empty = arrow::Table::MakeEmpty(sch);
    assert(empty.ok());
    return std::move(empty).ValueUnsafe();
  }
  assert(boards->num_rows() < kDcRows && "concept: dc_index 被截断");
  const std::vector<std::string> codes = strings(*boards, "ts_code"), names = strings(*boards, "name");
  for ([[maybe_unused]] const std::string &x : strings(*boards, "idx_type"))
    assert(x == kDcConcept && "concept: dc_index 混入非概念板块");
  for ([[maybe_unused]] const std::string &x : codes)
    assert(x.ends_with(kDcSuffix) && "concept: ts_code 不是 BKxxxx.DC");

  std::vector<std::size_t> idx(codes.size());
  std::iota(idx.begin(), idx.end(), 0);
  std::sort(idx.begin(), idx.end(), [&](std::size_t a, std::size_t b) { return codes[a] < codes[b]; });

  std::vector<tushare::Query> qs;
  qs.reserve(idx.size());
  for (const std::size_t k : idx)
    qs.push_back({"dc_member", {{"trade_date", at}, {"ts_code", codes[k]}}, {}});
  const std::vector<std::shared_ptr<arrow::Table>> members = tu.batch(qs);

  arrow::StringBuilder b_td, b_sym, b_code, b_name;
  for (std::size_t i = 0; i < idx.size(); ++i) {
    const std::size_t k = idx[i];
    assert(members[i]->num_rows() < kDcRows && "concept: dc_member 被截断");
    std::vector<std::string> syms = strings(*members[i], "con_code");
    std::sort(syms.begin(), syms.end());
    assert(std::adjacent_find(syms.begin(), syms.end()) == syms.end() && "concept: 板块成分重复");
    const std::string_view code(codes[k].data(), codes[k].size() - kDcSuffix.size());
    for (const std::string &sym : syms) {
      const arrow::Status s0 = b_td.Append(at);
      const arrow::Status s1 = b_sym.Append(sym);
      const arrow::Status s2 = b_code.Append(code.data(), static_cast<int32_t>(code.size()));
      const arrow::Status s3 = b_name.Append(names[k]);
      assert(s0.ok() && s1.ok() && s2.ok() && s3.ok());
    }
  }
  std::shared_ptr<arrow::Array> a_td, a_sym, a_code, a_name;
  const arrow::Status f0 = b_td.Finish(&a_td), f1 = b_sym.Finish(&a_sym), f2 = b_code.Finish(&a_code), f3 = b_name.Finish(&a_name);
  assert(f0.ok() && f1.ok() && f2.ok() && f3.ok());
  return arrow::Table::Make(sch, {a_td, a_sym, a_code, a_name});
}

std::shared_ptr<arrow::Table> fetch_src(Clients &c, const Spec &s, const Bq &, const Task &t) {
  bigquant::Query q = select(s, "*", t.w);
  q.order = order(s);
  return c.bq.query(q);
}

std::shared_ptr<arrow::Table> fetch_src(Clients &c, const Spec &, const Tu &src, const Task &t) {
  if (src.mode == TuMode::Concept)
    return fetch_concept(c.tu, t.w);
  std::vector<tushare::Query> qs;
  if (src.mode == TuMode::Range) {
    qs.push_back({src.api, {{"start_date", misc::dense(t.w.lo)}, {"end_date", misc::dense(t.w.hi)}}, src.num});
  } else {
    for (Ymd d = t.w.lo; d <= t.w.hi; d = misc::add_days(d, 1))
      qs.push_back({src.api, {{src.param, misc::dense(d)}}, src.num});
  }
  const std::vector<std::shared_ptr<arrow::Table>> parts = c.tu.batch(qs);
  assert(!parts.empty());
  auto joined = arrow::ConcatenateTables(parts);
  assert(joined.ok() && "fetch: 响应 schema 跨查询不一致");
  return std::move(joined).ValueUnsafe();
}

} // namespace

std::shared_ptr<arrow::Table> fetch(Clients &c, const Task &t) {
  const Spec &s = *t.spec;
  assert((t.how != Write::Append || (s.shape == Shape::Month && s.snap == Snap::All)) &&
         "fetch: Append 只用于 Month·All");
  assert((s.shape != Shape::Daily || (t.how == Write::Replace && t.w.lo == t.at && t.w.hi == t.at)) &&
         "fetch: Daily 一日一任务, 整日 Replace");
  std::shared_ptr<arrow::Table> tbl = std::visit([&](const auto &src) { return fetch_src(c, s, src, t); }, s.src);
  return strip(s, std::move(tbl));
}

std::shared_ptr<arrow::Schema> schema(Clients &c, const Spec &s, Window w) {
  auto &cached = c.schemas[s.name];
  if (!cached)
    cached = c.bq.query(select(s, "*", w, " LIMIT 0"))->schema();
  return cached;
}

std::string ident(std::string_view c) { return "\"" + std::string(c) + "\""; }

std::string group_expr(const Spec &s, Grain g) {
  assert(!s.vd.empty() && "group_expr: vd 空的表无组键");
  return "strftime(" + ident(s.vd) + (g == Grain::Month ? ", '%Y-%m')" : ", '%Y-%m-%d')");
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
