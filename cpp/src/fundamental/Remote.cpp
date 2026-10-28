#include "fundamental/Remote.hpp"

#include "api/tushare/client.hpp"
#include "misc/parquet.hpp"

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>

#include <algorithm>
#include <cassert>
#include <string>
#include <type_traits>
#include <vector>

namespace fundamental {

namespace {

std::string literal(std::string_view v) { return "'" + std::string(v) + "'"; }
std::string literal(Ymd d) { return literal(misc::dash(d)); }

std::vector<bigquant::Query> queries(const Spec &s, const Bq &, const Task &t) {
  return {select(s, "*", t.w, t.how == Write::Splice ? key_filter(s, t.keys) : "")};
}

std::vector<tushare::Query> queries(const Spec &, const Tu &src, const Task &t) {
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

const std::string kPacked = "b"; // 打包列名

// 键, LIST({'c': "c", ..} ORDER BY vd) AS b —— flat 里除键以外的列按原序进 STRUCT
std::string packed_cols(const Spec &s, const arrow::Schema &flat) {
  std::string fields;
  for (const auto &f : flat.fields())
    if (f->name() != key_col(s))
      fields += (fields.empty() ? "" : ", ") + literal(f->name()) + ": " + ident(f->name());
  return ident(key_col(s)) + ", LIST({" + fields + "} ORDER BY " + ident(s.vd) + ") AS " + ident(kPacked);
}

// 打包响应 → flat 同形: 逐资产按日切段 (零拷贝), 同日各段按资产序拼成一块 ⇒ 一日一 chunk, 日升序
std::shared_ptr<arrow::Table> unpack(const Spec &s, const std::shared_ptr<arrow::Schema> &flat, const arrow::Table &packed) {
  const std::string key(key_col(s));
  assert(packed.num_columns() == kPackCols && packed.schema()->field(0)->name() == key &&
         packed.schema()->field(1)->name() == kPacked && packed.schema()->field(1)->type()->id() == arrow::Type::LIST &&
         "unpack: 打包响应须为 (键, LIST)");
  const auto keys = packed.column(0), lists = packed.column(1);
  assert(keys->num_chunks() == lists->num_chunks());
  // 一段 = 一资产一日的连续元素
  struct Seg {
    Ymd day;
    std::shared_ptr<arrow::Scalar> asset;
    const arrow::StructArray *rows;
    std::int64_t start, len;
  };
  std::vector<Seg> segs;
  std::vector<std::shared_ptr<arrow::StructArray>> hold;
  std::vector<Ymd> days;
  for (int c = 0; c < lists->num_chunks(); ++c) {
    const auto &list = static_cast<const arrow::ListArray &>(*lists->chunk(c));
    assert(list.null_count() == 0 && keys->chunk(c)->null_count() == 0 && "unpack: 资产行的键 / LIST 为 null");
    hold.push_back(std::static_pointer_cast<arrow::StructArray>(list.values()));
    const arrow::StructArray &rows = *hold.back();
    assert(rows.null_count() == 0 && "unpack: LIST 元素为 null");
    misc::pq::Col(rows.GetFieldByName(s.vd)).yyyymmdd_all(days);
    for (std::int64_t i = 0; i < list.length(); ++i) {
      const auto asset = keys->chunk(c)->GetScalar(i).ValueOrDie();
      const std::int64_t end = list.value_offset(i + 1);
      for (std::int64_t j = list.value_offset(i); j < end;) {
        std::int64_t k = j + 1;
        while (k < end && days[static_cast<std::size_t>(k)] == days[static_cast<std::size_t>(j)])
          ++k;
        assert(days[static_cast<std::size_t>(j)] > 0 && (k == end || days[static_cast<std::size_t>(k)] > days[static_cast<std::size_t>(j)]) &&
               "unpack: 资产内 vd 须非 null 且升序");
        segs.push_back({days[static_cast<std::size_t>(j)], asset, &rows, j, k - j});
        j = k;
      }
    }
  }
  std::stable_sort(segs.begin(), segs.end(), [](const Seg &a, const Seg &b) { return a.day < b.day; });

  std::vector<arrow::ArrayVector> cols(static_cast<std::size_t>(flat->num_fields()));
  for (auto b = segs.begin(); b != segs.end();) {
    const auto e = std::find_if(b, segs.end(), [&](const Seg &x) { return x.day != b->day; });
    for (int f = 0; f < flat->num_fields(); ++f) {
      const auto &field = flat->field(f);
      arrow::ArrayVector parts;
      for (auto it = b; it != e; ++it) {
        if (field->name() == key) {
          parts.push_back(arrow::MakeArrayFromScalar(*it->asset, it->len).ValueOrDie());
          continue;
        }
        const auto col = it->rows->GetFieldByName(field->name());
        assert(col && "unpack: STRUCT 缺 schema 里的列");
        parts.push_back(col->Slice(it->start, it->len));
      }
      auto day = arrow::Concatenate(parts).ValueOrDie();
      assert(day->type()->Equals(*field->type()) && "unpack: 打包列型与 schema 不符");
      cols[static_cast<std::size_t>(f)].push_back(std::move(day));
    }
    b = e;
  }
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked;
  for (int f = 0; f < flat->num_fields(); ++f)
    chunked.push_back(std::make_shared<arrow::ChunkedArray>(std::move(cols[static_cast<std::size_t>(f)]), flat->field(f)->type()));
  return arrow::Table::Make(flat, std::move(chunked));
}

// Daily: LIMIT 0 schema 定列 (0 cell) → 一条打包查询 → 拆回
std::shared_ptr<arrow::Table> packed(bigquant::Client &client, const Task &t) {
  const Spec &s = *t.spec;
  const auto flat = client.query(select(s, "*", t.w, " LIMIT 0"))->schema();
  const std::string key = ident(key_col(s));
  const auto tbl = client.query(select(s, packed_cols(s, *flat), t.w, key_filter(s, t.keys) + " GROUP BY " + key + " ORDER BY " + key));
  return unpack(s, flat, *tbl);
}

} // namespace

template <class Src>
std::shared_ptr<arrow::Table> fetch(typename Src::Client &client, const Task &t) {
  const Spec &s = *t.spec;
  assert(from<Src>(s) && "fetch: 表与 client 的数据源不符");
  assert((t.how != Write::Append || s.snap == Snap::All) && "fetch: Snap::Day 表无水位增量 (MIN / MAX 须看整窗)");
  assert((t.how != Write::Splice || Src::exact) && "fetch: 非精确源无 Splice");
  assert((t.how != Write::Splice || !t.keys.empty()) && "fetch: Splice 的 keys 为空");
  assert((s.shape != Shape::Daily || (t.how == Write::Replace && !t.keys.empty())) && "fetch: Daily 只整日 Replace, keys = 待拉日");
  for ([[maybe_unused]] const std::string &k : t.keys)
    assert(grain_of(k) != Grain::Month && key_day(k) >= t.w.lo && key_day(k) <= t.w.hi &&
           "fetch: keys 须为 Day / Asset 级且在窗口内");
  if constexpr (std::is_same_v<Src, Bq>)
    if (s.shape == Shape::Daily)
      return strip(s, packed(client, t));
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

std::vector<std::int64_t> pack_rows(bigquant::Client &client, const std::vector<Task> &tasks) {
  assert(!tasks.empty() && "pack_rows: 计划为空");
  const Spec &s = *tasks.front().spec;
  std::string bucket = "CASE";
  std::vector<std::string> keys;
  for (std::size_t i = 0; i < tasks.size(); ++i) {
    const Task &t = tasks[i];
    assert(t.spec == &s && s.shape == Shape::Daily && !t.keys.empty() && "pack_rows: 只估单张 Daily 表的打包任务");
    assert((i == 0 || t.w.lo > tasks[i - 1].w.hi) && "pack_rows: 任务窗口须升序不相交");
    bucket += " WHEN " + ident(s.vd) + " < " + literal(misc::add_days(t.w.hi, 1)) + " THEN " + std::to_string(i);
    keys.insert(keys.end(), t.keys.begin(), t.keys.end());
  }
  const Window w{tasks.front().w.lo, tasks.back().w.hi};
  const misc::pq::TableView v(client.query(select(s, bucket + " END AS i, COUNT(DISTINCT " + ident(key_col(s)) + ") AS n", w,
                                                  key_filter(s, keys) + " GROUP BY 1")));
  const misc::pq::Col i = v.col("i"), n = v.col("n");
  std::vector<std::int64_t> out(tasks.size(), 0);
  for (std::int64_t r = 0; r < v.rows(); ++r) {
    const int k = i.i32(r, -1);
    assert(k >= 0 && k < static_cast<int>(tasks.size()) && "pack_rows: 桶号越界");
    out[static_cast<std::size_t>(k)] = n.i32(r, 0);
  }
  return out;
}

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
