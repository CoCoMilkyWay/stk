#include "api/bigquant/audit.hpp"

#include "misc/date.hpp"
#include "misc/parquet.hpp"
#include "misc/schedule.hpp"
#include "shared/Config.hpp"

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace bigquant {

namespace {

namespace pq = misc::pq;
using i128 = __int128;

enum class Kind { Real,
                  Int,
                  Ts,
                  Str,
                  Null };

enum class Grain { Month,
                   Day };

Kind kind_of(const arrow::DataType &t) {
  switch (t.id()) {
  case arrow::Type::DOUBLE:
  case arrow::Type::FLOAT:
    return Kind::Real;
  case arrow::Type::BOOL:
  case arrow::Type::INT8:
  case arrow::Type::INT16:
  case arrow::Type::INT32:
  case arrow::Type::INT64:
  case arrow::Type::UINT8:
  case arrow::Type::UINT16:
  case arrow::Type::UINT32:
  case arrow::Type::UINT64:
    return Kind::Int;
  case arrow::Type::TIMESTAMP:
    return Kind::Ts;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return Kind::Str;
  case arrow::Type::NA:
    return Kind::Null;
  default:
    break;
  }
  std::cerr << "[bigquant.audit] 不支持的列类型: " << t.ToString() << std::endl;
  assert(false && "audit: 不支持的列类型");
  return Kind::Null;
}

// 指纹格: col < 0 = n; sum=false → 该列 nn, true → 该列 s
struct Cell {
  int col;
  bool sum;
};

struct Layout {
  std::vector<std::string> names; // Content 才有
  std::vector<Kind> kinds;
  std::vector<Cell> cells; // cells[0] = n
  std::vector<int> nn_at;  // 列 → cells 下标
  std::vector<int> s_at;   // 列 → cells 下标; -1 = Null 列无 s
};

// 组键 → 指纹格 (十进制字符串, 与 Layout::cells 同序)
using Fps = std::map<std::string, std::vector<std::string>>;

// "YYYYMMDD" → "YYYY-MM-DD"
std::string dashed(std::string_view d) {
  assert(d.size() == 8);
  return std::string(d.substr(0, 4)) + "-" + std::string(d.substr(4, 2)) + "-" +
         std::string(d.substr(6, 2));
}

std::string ident(std::string_view c) { return "\"" + std::string(c) + "\""; }

std::string i128_str(i128 v) {
  if (v == 0)
    return "0";
  const bool neg = v < 0;
  unsigned __int128 u = neg ? -static_cast<unsigned __int128>(v)
                            : static_cast<unsigned __int128>(v);
  std::string s;
  while (u != 0) {
    s.push_back(static_cast<char>('0' + static_cast<int>(u % 10)));
    u /= 10;
  }
  if (neg)
    s.push_back('-');
  std::reverse(s.begin(), s.end());
  return s;
}

// 窗口谓词; MonthFirst 表只留窗口内每月最早一天 (与 fetch 同语义)
std::string where_of(const TableSpec &s, std::string_view from,
                     std::string_view to) {
  const std::string vd = ident(s.visible_date);
  const std::string rng = vd + " >= '" + dashed(from) + "' AND " + vd +
                          " <= '" + dashed(to) + "'";
  std::string w = " WHERE " + rng;
  if (s.freq == FetchFreq::MonthFirst)
    w += " AND " + vd + " IN (SELECT MIN(" + vd + ") FROM " + s.name +
         " WHERE " + rng + " GROUP BY strftime(" + vd + ", '%Y-%m'))";
  return w;
}

// Partition 表必须带分区裁剪 filters, DAI 才放行 (与 fetch 一致)
std::shared_ptr<arrow::Table> run(DaiClient &client, const TableSpec &s,
                                  const std::string &sql, std::string_view from,
                                  std::string_view to) {
  DaiFilters f;
  if (s.kind == FetchKind::Partition)
    f = {{"date", {dashed(from), dashed(to)}}};
  return client.query(sql, f);
}

Layout layout_of(DaiClient &client, const AuditUnit &u) {
  Layout L;
  L.cells.push_back({-1, false});
  if (u.pass == AuditPass::Rows)
    return L;
  const TableSpec &s = *u.spec;
  // LIMIT 0: 只要 schema, 0 cell
  auto t = run(client, s, "SELECT * FROM " + s.name + where_of(s, u.from, u.to) + " LIMIT 0",
               u.from, u.to);
  for (const auto &f : t->schema()->fields()) {
    const int c = static_cast<int>(L.names.size());
    L.names.push_back(f->name());
    L.kinds.push_back(kind_of(*f->type()));
    L.nn_at.push_back(static_cast<int>(L.cells.size()));
    L.cells.push_back({c, false});
    if (L.kinds.back() == Kind::Null) {
      L.s_at.push_back(-1);
      continue;
    }
    L.s_at.push_back(static_cast<int>(L.cells.size()));
    L.cells.push_back({c, true});
  }
  return L;
}

std::string sum_expr(Kind k, const std::string &col) {
  switch (k) {
  case Kind::Real:
    // 1e6 是 DOUBLE 字面量 (写成 1000000.0 会变 DECIMAL, 乘法口径就和本地不一样了)
    return "SUM(CASE WHEN isfinite(" + col + ") THEN CAST(ROUND(" + col +
           " * 1e6) AS HUGEINT) END)";
  case Kind::Int:
    return "SUM(CAST(" + col + " AS HUGEINT))";
  case Kind::Ts:
    return "SUM(epoch_ms(" + col + "))";
  case Kind::Str:
    return "SUM(strlen(" + col + "))";
  case Kind::Null:
    break;
  }
  assert(false && "audit: Null 列无 s");
  return {};
}

Fps remote_fps(DaiClient &client, const TableSpec &s, const Layout &L, Grain g,
               std::string_view from, std::string_view to) {
  const std::string vd = ident(s.visible_date);
  std::string sql = "SELECT strftime(" + vd + ", '" +
                    (g == Grain::Month ? "%Y-%m" : "%Y-%m-%d") + "') AS g";
  for (std::size_t k = 0; k < L.cells.size(); ++k) {
    const Cell &c = L.cells[k];
    std::string e;
    if (c.col < 0)
      e = "COUNT(*)";
    else if (!c.sum)
      e = "COUNT(" + ident(L.names[c.col]) + ")";
    else
      e = sum_expr(L.kinds[c.col], ident(L.names[c.col]));
    sql += ", CAST(COALESCE(" + e + ", 0) AS VARCHAR) AS f" + std::to_string(k);
  }
  sql += " FROM " + s.name + where_of(s, from, to) + " GROUP BY 1";

  pq::TableView tv(run(client, s, sql, from, to));
  const pq::Col gc = tv.col("g");
  std::vector<pq::Col> fc;
  for (std::size_t k = 0; k < L.cells.size(); ++k)
    fc.push_back(tv.col("f" + std::to_string(k)));
  Fps out;
  for (std::int64_t i = 0; i < tv.rows(); ++i) {
    const std::string key(gc.str(i));
    assert(!key.empty() && "audit: 远端组键为 null");
    auto &v = out[key];
    assert(v.empty() && "audit: 远端组键重复");
    for (const pq::Col &c : fc)
      v.emplace_back(c.str(i));
  }
  return out;
}

template <class A, class F>
void each_valid(const arrow::Array &a, F &&f) {
  const auto &x = static_cast<const A &>(a);
  for (std::int64_t i = 0; i < x.length(); ++i)
    if (x.IsValid(i))
      f(i, x.Value(i));
}

i128 real_cell(double x) {
  if (!std::isfinite(x))
    return 0;
  const double r = std::round(x * 1e6);
  assert(std::fabs(r) < 1e36 && "audit: 数值超 HUGEINT 量程");
  return static_cast<i128>(r);
}

// epoch_ms 与 DuckDB 同为截断除法
i128 ts_ms(std::int64_t v, arrow::TimeUnit::type u) {
  switch (u) {
  case arrow::TimeUnit::SECOND:
    return static_cast<i128>(v) * 1000;
  case arrow::TimeUnit::MILLI:
    return v;
  case arrow::TimeUnit::MICRO:
    return v / 1000;
  case arrow::TimeUnit::NANO:
    return v / 1000000;
  }
  assert(false);
  return 0;
}

// 一个 chunk 的一列累加进各行所属组: nn += 非 null, s += 按类型取整
void accumulate(const arrow::Array &a, Kind k, int nn_at, int s_at,
                const std::vector<std::vector<i128> *> &slot, std::int64_t base) {
  if (a.type_id() == arrow::Type::NA)
    return;
  assert(kind_of(*a.type()) == k && "audit: 本地列类型与远端不符");
  auto add = [&](std::int64_t i, i128 s) {
    std::vector<i128> &v = *slot[base + i];
    ++v[nn_at];
    v[s_at] += s;
  };
  switch (a.type_id()) {
  case arrow::Type::DOUBLE:
    each_valid<arrow::DoubleArray>(a, [&](std::int64_t i, double x) { add(i, real_cell(x)); });
    return;
  case arrow::Type::FLOAT:
    each_valid<arrow::FloatArray>(a, [&](std::int64_t i, float x) { add(i, real_cell(static_cast<double>(x))); });
    return;
  case arrow::Type::BOOL:
    each_valid<arrow::BooleanArray>(a, [&](std::int64_t i, bool x) { add(i, x ? 1 : 0); });
    return;
  case arrow::Type::INT8:
    each_valid<arrow::Int8Array>(a, [&](std::int64_t i, std::int8_t x) { add(i, x); });
    return;
  case arrow::Type::INT16:
    each_valid<arrow::Int16Array>(a, [&](std::int64_t i, std::int16_t x) { add(i, x); });
    return;
  case arrow::Type::INT32:
    each_valid<arrow::Int32Array>(a, [&](std::int64_t i, std::int32_t x) { add(i, x); });
    return;
  case arrow::Type::INT64:
    each_valid<arrow::Int64Array>(a, [&](std::int64_t i, std::int64_t x) { add(i, x); });
    return;
  case arrow::Type::UINT8:
    each_valid<arrow::UInt8Array>(a, [&](std::int64_t i, std::uint8_t x) { add(i, x); });
    return;
  case arrow::Type::UINT16:
    each_valid<arrow::UInt16Array>(a, [&](std::int64_t i, std::uint16_t x) { add(i, x); });
    return;
  case arrow::Type::UINT32:
    each_valid<arrow::UInt32Array>(a, [&](std::int64_t i, std::uint32_t x) { add(i, x); });
    return;
  case arrow::Type::UINT64:
    each_valid<arrow::UInt64Array>(a, [&](std::int64_t i, std::uint64_t x) { add(i, x); });
    return;
  case arrow::Type::TIMESTAMP: {
    const auto unit = static_cast<const arrow::TimestampType &>(*a.type()).unit();
    each_valid<arrow::TimestampArray>(a, [&](std::int64_t i, std::int64_t x) { add(i, ts_ms(x, unit)); });
    return;
  }
  case arrow::Type::STRING:
    each_valid<arrow::StringArray>(a, [&](std::int64_t i, std::string_view x) {
      add(i, static_cast<i128>(x.size()));
    });
    return;
  case arrow::Type::LARGE_STRING:
    each_valid<arrow::LargeStringArray>(a, [&](std::int64_t i, std::string_view x) {
      add(i, static_cast<i128>(x.size()));
    });
    return;
  default:
    break;
  }
  assert(false && "audit: 不支持的列类型");
}

// 本地: months 各分片逐行累加, 与 remote_fps 同组键同口径
Fps local_fps(const TableSpec &s, const Layout &L, Grain g,
              const std::vector<std::string> &months) {
  const bool content = L.cells.size() > 1;
  std::map<std::string, std::vector<i128>> acc;
  for (const std::string &ym : months) {
    auto t = pq::read_table(pq::month_path(ym, s.name),
                            content ? std::vector<std::string>{}
                                    : std::vector<std::string>{s.visible_date});
    // 行 → 所属组的累加槽 (同日短路: 分片内 vd 按日成块)
    std::vector<std::vector<i128> *> slot(static_cast<std::size_t>(t->num_rows()));
    {
      auto vd = t->GetColumnByName(s.visible_date);
      assert(vd && "audit: 本地分片缺 visible_date 列");
      std::int64_t base = 0;
      std::vector<std::int32_t> d;
      std::int32_t prev = 0;
      std::vector<i128> *prev_slot = nullptr;
      for (const auto &chunk : vd->chunks()) {
        pq::Col(chunk).yyyymmdd_all(d);
        for (std::size_t i = 0; i < d.size(); ++i) {
          if (d[i] != prev) {
            assert(d[i] > 0 && "audit: 本地 visible_date 为 null");
            const int y = d[i] / 10000, m = d[i] / 100 % 100, dd = d[i] % 100;
            char key[16];
            std::snprintf(key, sizeof(key), "%04d-%02d", y, m);
            assert(ym == key && "audit: 行的 visible_date 不在所属月分片");
            if (g == Grain::Day)
              std::snprintf(key, sizeof(key), "%04d-%02d-%02d", y, m, dd);
            auto &v = acc[key];
            if (v.empty())
              v.assign(L.cells.size(), 0);
            prev = d[i];
            prev_slot = &v;
          }
          slot[static_cast<std::size_t>(base) + i] = prev_slot;
        }
        base += static_cast<std::int64_t>(d.size());
      }
    }
    for (std::vector<i128> *v : slot)
      ++(*v)[0];
    for (std::size_t c = 0; c < L.names.size(); ++c) {
      auto col = t->GetColumnByName(L.names[c]);
      if (!col)
        continue; // 本地缺列 = 全 null
      std::int64_t base = 0;
      for (const auto &chunk : col->chunks()) {
        accumulate(*chunk, L.kinds[c], L.nn_at[c], L.s_at[c], slot, base);
        base += chunk->length();
      }
    }
  }
  Fps out;
  for (const auto &[key, v] : acc) {
    auto &o = out[key];
    for (i128 x : v)
      o.push_back(i128_str(x));
  }
  return out;
}

const std::vector<std::string> &find_or_zero(const Fps &fps, const std::string &key,
                                             const std::vector<std::string> &zeros) {
  auto it = fps.find(key);
  return it == fps.end() ? zeros : it->second;
}

// 不一致的 "列(nn|s)" (n 另比)
std::string diff_detail(const Layout &L, const std::vector<std::string> &a,
                        const std::vector<std::string> &b) {
  constexpr int kShow = 8;
  std::string out;
  int shown = 0, more = 0;
  for (std::size_t k = 1; k < L.cells.size(); ++k) {
    if (a[k] == b[k])
      continue;
    if (shown == kShow) {
      ++more;
      continue;
    }
    if (!out.empty())
      out += ", ";
    out += L.names[L.cells[k].col] + (L.cells[k].sum ? "(s)" : "(nn)");
    ++shown;
  }
  if (more > 0)
    out += " +" + std::to_string(more);
  return out;
}

AuditDiff make_diff(const AuditUnit &u, const Layout &L, const std::string &group,
                    const std::vector<std::string> &l,
                    const std::vector<std::string> &r) {
  AuditDiff d;
  d.table = u.spec->name;
  d.group = group;
  d.pass = u.pass;
  d.local_n = std::stoll(l[0]);
  d.remote_n = std::stoll(r[0]);
  if (u.pass == AuditPass::Content) {
    d.detail = diff_detail(L, l, r);
    for (std::size_t k = 1; k < L.cells.size(); ++k)
      if (l[k] != r[k])
        d.cells.push_back({L.names[L.cells[k].col], L.cells[k].sum, l[k], r[k]});
  }
  return d;
}

} // namespace

std::vector<AuditUnit> audit_plan(const std::vector<TableSpec> &specs,
                                  std::string_view start, std::string_view today,
                                  int lookback_days, unsigned seed) {
  assert(start.size() == 8 && today.size() == 8 && start <= today);
  const std::string frozen_before = misc::add_days(today, -lookback_days);
  const std::string start_ym =
      std::string(start.substr(0, 4)) + "-" + std::string(start.substr(4, 2));

  std::vector<std::vector<AuditUnit>> per_table;
  for (const TableSpec &s : specs) {
    if (s.kind == FetchKind::Static || s.kind == FetchKind::Snapshot)
      continue;
    // plan_months 仍要拉的关月 = 未冻结 (缺失 / 写盘早于月末 + lookback), 不核
    std::set<std::string> pending;
    for (const auto &m : misc::plan_months(s.name, s.visible_date, s.avail_hour,
                                           start, today, lookback_days,
                                           ::config::PIPELINE_DEDUP_WINDOW_SECONDS))
      pending.insert(m.ym);
    std::map<int, AuditUnit> by_year;
    for (const auto &[ym, path] : pq::list_month_files(s.name)) {
      const std::string yyyymm = ym.substr(0, 4) + ym.substr(5, 2);
      const std::string m_last = yyyymm + misc::month_last_dd(yyyymm);
      if (ym < start_ym || m_last >= frozen_before || pending.contains(ym))
        continue;
      AuditUnit &u = by_year[std::stoi(ym.substr(0, 4))];
      if (u.months.empty())
        u.from = std::max(yyyymm + "01", std::string(start));
      u.months.push_back(ym);
      u.to = m_last;
    }
    std::vector<AuditUnit> units;
    for (auto &[y, u] : by_year) {
      u.spec = &s;
      u.year = y;
      units.push_back(std::move(u));
    }
    if (!units.empty())
      per_table.push_back(std::move(units));
  }

  std::mt19937 rng(seed);
  std::vector<AuditUnit> out;
  for (AuditPass pass : {AuditPass::Rows, AuditPass::Content}) {
    auto tables = per_table;
    std::shuffle(tables.begin(), tables.end(), rng);
    std::size_t depth = 0;
    for (auto &t : tables) {
      std::shuffle(t.begin(), t.end(), rng);
      depth = std::max(depth, t.size());
    }
    for (std::size_t k = 0; k < depth; ++k)
      for (const auto &t : tables)
        if (k < t.size()) {
          AuditUnit u = t[k];
          u.pass = pass;
          out.push_back(std::move(u));
        }
  }
  return out;
}

std::vector<AuditDiff> audit_unit(DaiClient &client, const AuditUnit &u) {
  assert(u.spec && !u.months.empty());
  const TableSpec &s = *u.spec;
  const Layout L = layout_of(client, u);
  const std::vector<std::string> zeros(L.cells.size(), "0");

  // 月级: Rows 只看 n; Content 只看 n 相同而内容不同 (n 不同 Rows 轮已报)
  auto flagged = [&](const std::vector<std::string> &l,
                     const std::vector<std::string> &r) {
    if (l[0] != r[0])
      return u.pass == AuditPass::Rows;
    return u.pass == AuditPass::Content && l != r;
  };

  const Fps rm = remote_fps(client, s, L, Grain::Month, u.from, u.to);
  // Rows 轮只有 n 一格: 本地直接按日累加, 月级由日级求和 —— 下钻时不必重读分片
  Fps ld_all, lm;
  if (u.pass == AuditPass::Rows) {
    ld_all = local_fps(s, L, Grain::Day, u.months);
    std::map<std::string, std::int64_t> month_n;
    for (const auto &[day, v] : ld_all)
      month_n[day.substr(0, 7)] += std::stoll(v[0]);
    for (const auto &[ym, n] : month_n)
      lm[ym] = {std::to_string(n)};
  } else {
    lm = local_fps(s, L, Grain::Month, u.months);
  }

  std::vector<AuditDiff> out;
  for (const std::string &ym : u.months) {
    const auto &l = find_or_zero(lm, ym, zeros);
    const auto &r = find_or_zero(rm, ym, zeros);
    if (!flagged(l, r))
      continue;

    // 日级下钻: 同月内任何不一致都报 (Content 轮里 n 跨日挪动也算)
    const std::string yyyymm = ym.substr(0, 4) + ym.substr(5, 2);
    const std::string from = std::max(yyyymm + "01", u.from);
    const std::string to = yyyymm + misc::month_last_dd(yyyymm);
    const Fps rd = remote_fps(client, s, L, Grain::Day, from, to);
    Fps ld;
    if (u.pass == AuditPass::Rows) {
      for (auto it = ld_all.lower_bound(ym); it != ld_all.end() && it->first.starts_with(ym); ++it)
        ld.insert(*it);
    } else {
      ld = local_fps(s, L, Grain::Day, {ym});
    }
    std::set<std::string> days;
    for (const auto &[k, v] : rd)
      days.insert(k);
    for (const auto &[k, v] : ld)
      days.insert(k);
    const std::size_t before = out.size();
    for (const std::string &day : days) {
      const auto &dl = find_or_zero(ld, day, zeros);
      const auto &dr = find_or_zero(rd, day, zeros);
      const bool diff = u.pass == AuditPass::Rows ? dl[0] != dr[0] : dl != dr;
      if (diff)
        out.push_back(make_diff(u, L, day, dl, dr));
    }
    if (out.size() == before)
      out.push_back(make_diff(u, L, ym, l, r));
  }
  return out;
}

std::string audit_label(const AuditUnit &u) {
  return u.spec->name + " " + std::to_string(u.year) +
         (u.pass == AuditPass::Rows ? " rows" : " content");
}

} // namespace bigquant
