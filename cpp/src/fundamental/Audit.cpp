#include "fundamental/Audit.hpp"

#include "fundamental/Remote.hpp"
#include "misc/parquet.hpp"

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace fundamental::audit {

namespace {

namespace pq = misc::pq;
using i128 = __int128;

enum class Kind { Real,
                  Int,
                  Ts,
                  Str,
                  Null };

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

// 指纹格: cols 空 = n; 否则 cols 上的 nn (sum=false) / s (sum=true), 多列按 weight 加权折叠
struct Cell {
  bool sum = false;
  std::vector<int> cols;
};
using Cells = std::vector<Cell>; // [0] = n

// 折叠格里列 c 的权 (单列格权 1, 原值可读)
std::int64_t weight(const Cell &c, int col) { return c.cols.size() > 1 ? col + 1 : 1; }

struct Layout {
  std::vector<std::string> names; // 远端 schema 列序
  std::vector<Kind> kinds;
  bool asset = false; // 远端有资产键列 ⇒ 可定位到 Asset 级
  Cells all;          // n + 逐列 nn / s (Null 列无 s)
};

// 组键 (Spec.hpp 键形, 与服务端 group_expr 同形) → 指纹格 (十进制字符串, 与所查 Cells 同序)
using Fps = std::map<std::string, std::vector<std::string>>;

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

Layout layout_of(bigquant::Client &client, const Unit &u) {
  Layout L;
  L.all.push_back({});
  // LIMIT 0: 只要 schema, 0 cell
  auto t = client.query(select(*u.spec, "*", u.w, " LIMIT 0"));
  const std::vector<std::string> &drop = u.spec->drop;
  for (const auto &f : t->schema()->fields()) {
    if (std::find(drop.begin(), drop.end(), f->name()) != drop.end())
      continue; // 不落盘的列 (同 fetch 的 strip)
    const int c = static_cast<int>(L.names.size());
    L.names.push_back(f->name());
    L.kinds.push_back(kind_of(*f->type()));
    L.asset = L.asset || f->name() == key_col(*u.spec);
    L.all.push_back({false, {c}});
    if (L.kinds.back() != Kind::Null)
      L.all.push_back({true, {c}});
  }
  return L;
}

// 父组下钻用的子级格: n + 父组不一致格涉及的列 D
//   逐列 = D 每列 nn / s;  折叠 = D 的 nn 一格 + s 一格 (Null 列只进 nn)
Cells per_column(const Layout &L, const std::set<int> &D) {
  Cells out{{}};
  for (int c : D) {
    out.push_back({false, {c}});
    if (L.kinds[c] != Kind::Null)
      out.push_back({true, {c}});
  }
  return out;
}

Cells folded(const Layout &L, const std::set<int> &D) {
  Cells out{{}};
  if (D.empty())
    return out;
  Cell nn{false, {D.begin(), D.end()}}, s{true, {}};
  for (int c : D)
    if (L.kinds[c] != Kind::Null)
      s.cols.push_back(c);
  out.push_back(std::move(nn));
  if (!s.cols.empty())
    out.push_back(std::move(s));
  return out;
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

// 窗口 w 内 (parents 非空 → 只这些父组) 按 g 级分组, 只算 cells
Fps remote_fps(bigquant::Client &client, const Spec &s, const Layout &L, const Cells &cells, Grain g, Window w,
               const std::vector<std::string> &parents) {
  std::string cols = group_expr(s, g) + " AS g";
  for (std::size_t k = 0; k < cells.size(); ++k) {
    const Cell &c = cells[k];
    std::string e;
    if (c.cols.empty())
      e = "COUNT(*)";
    for (int col : c.cols) {
      const std::string one = c.sum ? sum_expr(L.kinds[col], ident(L.names[col])) : "COUNT(" + ident(L.names[col]) + ")";
      const std::int64_t wt = weight(c, col);
      e += (e.empty() ? "" : " + ") + ("COALESCE(" + one + ", 0)") + (wt == 1 ? "" : " * " + std::to_string(wt));
    }
    cols += ", CAST(COALESCE(" + e + ", 0) AS VARCHAR) AS f" + std::to_string(k);
  }

  const std::string tail = (parents.empty() ? std::string() : key_filter(s, parents)) + " GROUP BY 1";
  pq::TableView tv(client.query(select(s, cols, w, tail)));
  const pq::Col gc = tv.col("g");
  std::vector<pq::Col> fc;
  for (std::size_t k = 0; k < cells.size(); ++k)
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

// 列 c 进入的指纹格: 第 pos 格 += weight × (sum ? s : 1)
struct Use {
  std::size_t pos;
  bool sum;
  std::int64_t w;
};

// 一个 chunk 的一列累加进各行所属组 (slot 空 = 该行不在所查父组内): nn += 非 null, s += 按类型取整
void accumulate(const arrow::Array &a, Kind k, const std::vector<Use> &uses,
                const std::vector<std::vector<i128> *> &slot, std::int64_t base) {
  if (a.type_id() == arrow::Type::NA)
    return;
  assert(kind_of(*a.type()) == k && "audit: 本地列类型与远端不符");
  auto add = [&](std::int64_t i, i128 s) {
    std::vector<i128> *v = slot[base + i];
    if (!v)
      return;
    for (const Use &u : uses)
      (*v)[u.pos] += u.w * (u.sum ? s : 1);
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

// 本地: slots 各分片逐行累加, 与 remote_fps 同组键同口径 (parents 非空 → 只这些父组, 父键 = 组键前缀)
Fps local_fps(const Spec &s, const Layout &L, const Cells &cells, Grain g, const std::vector<Slot> &slots,
              const std::vector<std::string> &parents) {
  std::vector<std::vector<Use>> uses(L.names.size());
  for (std::size_t k = 1; k < cells.size(); ++k)
    for (int c : cells[k].cols)
      uses[c].push_back({k, cells[k].sum, weight(cells[k], c)});
  std::vector<int> read_cols;
  for (std::size_t c = 0; c < L.names.size(); ++c)
    if (!uses[c].empty())
      read_cols.push_back(static_cast<int>(c));
  // 只数 n → 只解 vd (+ 资产键); 否则全列 (本地缺列 = 全 null, 不能按名点读)
  const std::string kcol(key_col(s));
  std::vector<std::string> need;
  if (read_cols.empty()) {
    need.push_back(s.vd);
    if (g == Grain::Asset)
      need.push_back(kcol);
  }
  assert(std::is_sorted(parents.begin(), parents.end()) && "audit: parents 须升序");
  const Grain up = g == Grain::Asset ? Grain::Day : Grain::Month; // 父级 (Month 级无父组, parents 空)
  auto in_parents = [&](Ymd d) {
    return parents.empty() || std::binary_search(parents.begin(), parents.end(), group_key(up, d));
  };

  std::map<std::string, std::vector<i128>> acc;
  auto slot_of = [&](const std::string &k) {
    auto &v = acc[k];
    if (v.empty())
      v.assign(cells.size(), 0);
    return &v;
  };
  for (const Slot &sl : slots) {
    const Ym ym = sl.ym;
    auto t = read(*sl.sh, need);
    // 行 → 所属组的累加槽 (Month / Day 级同日短路: 分片内 vd 按日成块)
    std::vector<std::vector<i128> *> slot(static_cast<std::size_t>(t->num_rows()));
    {
      auto vd = t->GetColumnByName(s.vd);
      assert(vd && "audit: 本地分片缺 vd 列");
      std::vector<std::string_view> asset;
      if (g == Grain::Asset) {
        const auto kc = t->GetColumnByName(kcol);
        assert(kc && "audit: 本地分片缺资产键列");
        for (const auto &chunk : kc->chunks()) {
          const pq::Col k(chunk);
          for (std::int64_t i = 0; i < chunk->length(); ++i)
            asset.push_back(k.str(i));
        }
      }
      std::int64_t base = 0;
      std::vector<Ymd> d;
      Ymd prev = 0;
      bool prev_in = false;
      std::vector<i128> *prev_slot = nullptr;
      for (const auto &chunk : vd->chunks()) {
        pq::Col(chunk).yyyymmdd_all(d);
        for (std::size_t i = 0; i < d.size(); ++i) {
          const std::size_t row = static_cast<std::size_t>(base) + i;
          if (d[i] != prev) {
            assert(d[i] > 0 && "audit: 本地 vd 为 null");
            assert(misc::ym_of(d[i]) == ym && "audit: 行的 vd 不在所属月分片");
            prev = d[i];
            prev_in = in_parents(d[i]);
            prev_slot = prev_in && g != Grain::Asset ? slot_of(group_key(g, d[i])) : nullptr;
          }
          slot[row] = !prev_in ? nullptr : g == Grain::Asset ? slot_of(group_key(g, d[i], asset[row]))
                                                             : prev_slot;
        }
        base += static_cast<std::int64_t>(d.size());
      }
    }
    for (std::vector<i128> *v : slot)
      if (v)
        ++(*v)[0];
    for (int c : read_cols) {
      auto col = t->GetColumnByName(L.names[c]);
      if (!col)
        continue; // 本地缺列 = 全 null
      std::int64_t base = 0;
      for (const auto &chunk : col->chunks()) {
        accumulate(*chunk, L.kinds[c], uses[c], slot, base);
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

std::string cell_name(const Layout &L, const Cell &c) {
  return c.cols.size() == 1 ? L.names[c.cols[0]] : "Σ" + std::to_string(c.cols.size()) + "列";
}

// 一个不一致的组: 组键 + 所查格 + 两侧指纹 (与 cells 同序)
struct Probe {
  std::string key;
  std::shared_ptr<const Cells> cells;
  std::vector<std::string> l, r;
};
using Probes = std::vector<Probe>;

// 不一致的 "列(nn|s)" (n 另比)
std::string diff_detail(const Layout &L, const Probe &p) {
  constexpr int kShow = 8;
  std::string out;
  int shown = 0, more = 0;
  for (std::size_t k = 1; k < p.cells->size(); ++k) {
    if (p.l[k] == p.r[k])
      continue;
    if (shown == kShow) {
      ++more;
      continue;
    }
    if (!out.empty())
      out += ", ";
    out += cell_name(L, (*p.cells)[k]) + ((*p.cells)[k].sum ? "(s)" : "(nn)");
    ++shown;
  }
  if (more > 0)
    out += " +" + std::to_string(more);
  return out;
}

Diff make_diff(const Spec &s, const Layout &L, const Probe &p) {
  Diff d;
  d.table = s.name;
  d.group = p.key;
  d.local_n = std::stoll(p.l[0]);
  d.remote_n = std::stoll(p.r[0]);
  d.detail = diff_detail(L, p);
  for (std::size_t k = 1; k < p.cells->size(); ++k)
    if (p.l[k] != p.r[k])
      d.cells.push_back({cell_name(L, (*p.cells)[k]), (*p.cells)[k].sum, p.l[k], p.r[k]});
  return d;
}

// 一个月 (槽 month, 窗口 w) 内一个不一致的组逐级往下定位 (Day → Asset), 每级一次远端查询; 返回最终组 (组键彼此不相交)
Probes locate(bigquant::Client &client, const Spec &s, const Layout &L, const std::vector<Slot> &month, Window w,
              Probe top) {
  const std::int64_t C = static_cast<std::int64_t>(L.names.size()); // 直接修复一行的 cell (SELECT *)
  std::int64_t span = 0;                                            // 月窗口天数 = Day 级子组数上界
  for (Ymd d = w.lo; d <= w.hi; d = misc::add_days(d, 1))
    ++span;
  Probes done, frontier{std::move(top)};
  for (const Grain g : {Grain::Day, Grain::Asset}) {
    // 可下钻的父组: 子级存在 (Daily 无 Asset 级修复) ∧ 两侧皆非空 (一侧空 ⇒ 子组全不一致); D = 不一致格涉及的列
    Probes drill;
    std::set<int> D;
    std::int64_t bound = 0, fix = 0; // 子组数上界 / 直接按父组修复的 cell
    for (Probe &p : frontier) {
      const std::int64_t ln = std::stoll(p.l[0]), rn = std::stoll(p.r[0]);
      if ((g == Grain::Asset && (!L.asset || s.shape == Shape::Daily)) || ln == 0 || rn == 0) {
        done.push_back(std::move(p));
        continue;
      }
      for (std::size_t k = 1; k < p.cells->size(); ++k)
        if (p.l[k] != p.r[k])
          D.insert((*p.cells)[k].cols.begin(), (*p.cells)[k].cols.end());
      bound += g == Grain::Day ? std::min(ln + rn, span) : ln + rn;
      fix += rn * C;
      drill.push_back(std::move(p));
    }
    frontier.clear();
    if (drill.empty())
      break;
    // 子级格取逐列 (可诊断) / 折叠中更省的; 下钻上界不低于直接修复 → 停在父组
    Cells pc = per_column(L, D), fc = folded(L, D);
    const std::int64_t cost_pc = bound * static_cast<std::int64_t>(1 + pc.size());
    const std::int64_t cost_fc = bound * static_cast<std::int64_t>(1 + fc.size());
    if (std::min(cost_pc, cost_fc) >= fix) {
      done.insert(done.end(), std::make_move_iterator(drill.begin()), std::make_move_iterator(drill.end()));
      break;
    }
    const auto cells = std::make_shared<const Cells>(cost_pc <= cost_fc ? std::move(pc) : std::move(fc));
    std::vector<std::string> parents;
    for (const Probe &p : drill)
      parents.push_back(p.key);
    std::sort(parents.begin(), parents.end());
    const Fps rc = remote_fps(client, s, L, *cells, g, w, parents);
    const Fps lc = local_fps(s, L, *cells, g, month, parents);
    const std::vector<std::string> zeros(cells->size(), "0");
    for (Probe &p : drill) {
      std::set<std::string> kids; // 两侧以 p.key 为前缀的组
      for (const Fps *f : {&lc, &rc})
        for (auto it = f->lower_bound(p.key); it != f->end() && it->first.starts_with(p.key); ++it)
          kids.insert(it->first);
      Probes bad;
      for (const std::string &k : kids) {
        const auto &l = find_or_zero(lc, k, zeros);
        const auto &r = find_or_zero(rc, k, zeros);
        if (l != r)
          bad.push_back({k, cells, l, r});
      }
      // 一个都对不上 (差异在本级格之外) / 全部不一致 → 父组即最精确
      if (bad.empty() || bad.size() == kids.size())
        done.push_back(std::move(p));
      else
        frontier.insert(frontier.end(), std::make_move_iterator(bad.begin()), std::make_move_iterator(bad.end()));
    }
  }
  done.insert(done.end(), std::make_move_iterator(frontier.begin()), std::make_move_iterator(frontier.end()));
  return done;
}

} // namespace

std::vector<Unit> plan(const Catalog &cat, const Now &now, unsigned seed, const Keep &keep) {
  std::mt19937 rng(seed);
  std::vector<std::vector<Unit>> tables;
  for (const Spec &s : SPECS) {
    if (!audited(s))
      continue;
    std::map<int, Unit> by_year;
    for (const Slot &sl : frozen(s, cat, now)) {
      if (keep && !keep(s, sl.ym))
        continue;
      Unit &u = by_year[sl.ym / 100];
      if (u.slots.empty())
        u.w.lo = sl.w.lo;
      u.slots.push_back(sl);
      u.w.hi = sl.w.hi;
    }
    std::vector<Unit> units;
    for (auto &[y, u] : by_year) {
      u.spec = &s;
      u.year = y;
      units.push_back(std::move(u));
    }
    if (!units.empty())
      tables.push_back(std::move(units));
  }
  std::shuffle(tables.begin(), tables.end(), rng);
  std::size_t depth = 0;
  for (auto &t : tables) {
    std::shuffle(t.begin(), t.end(), rng);
    depth = std::max(depth, t.size());
  }
  std::vector<Unit> out;
  for (std::size_t k = 0; k < depth; ++k)
    for (const auto &t : tables)
      if (k < t.size())
        out.push_back(t[k]);
  return out;
}

std::vector<Diff> run(bigquant::Client &client, const Unit &u) {
  assert(u.spec && !u.slots.empty());
  const Spec &s = *u.spec;
  const Layout L = layout_of(client, u);
  const auto all = std::make_shared<const Cells>(L.all);
  const std::vector<std::string> zeros(all->size(), "0");

  // 月级 (第一遍扫描): 全部格
  const Fps rm = remote_fps(client, s, L, *all, Grain::Month, u.w, {});
  const Fps lm = local_fps(s, L, *all, Grain::Month, u.slots, {});

  // 同月的槽成组 (Month 一月一槽, Daily 一月多日槽); 组窗口 = 首槽 w.lo .. 末槽 w.hi
  std::vector<Diff> out;
  for (auto b = u.slots.begin(); b != u.slots.end();) {
    const auto e = std::find_if(b, u.slots.end(), [&](const Slot &x) { return x.ym != b->ym; });
    const std::vector<Slot> month(b, e);
    b = e;
    const std::string key = misc::dash_ym(month.front().ym);
    const auto &l = find_or_zero(lm, key, zeros);
    const auto &r = find_or_zero(rm, key, zeros);
    if (l == r)
      continue;
    for (const Probe &p : locate(client, s, L, month, {month.front().w.lo, month.back().w.hi}, {key, all, l, r}))
      out.push_back(make_diff(s, L, p));
  }
  return out;
}

std::string label(const Unit &u) { return u.spec->name + " " + std::to_string(u.year); }

std::vector<Task> repair(const Spec &s, const std::vector<Slot> &frozen, const std::vector<std::string> &groups) {
  if (s.shape == Shape::Daily) {
    std::set<Ymd> days; // 差异日
    for (const std::string &g : groups) {
      const Grain grain = grain_of(g);
      assert(grain != Grain::Asset && "repair: Daily 定位最细到 Day");
      if (grain == Grain::Day) {
        days.insert(key_day(g));
        continue;
      }
      [[maybe_unused]] bool any = false;
      for (const Slot &sl : frozen)
        if (sl.ym == key_ym(g)) {
          days.insert(sl.w.lo);
          any = true;
        }
      assert(any && "repair: Daily 差异月无冻结日");
    }
    std::vector<Task> out;
    for (const Ymd d : days)
      out.push_back({&s, misc::ym_of(d), {d, d}, Write::Replace});
    return out;
  }
  std::map<Ym, std::set<std::string>> by_month; // 差异月 → 定位键
  for (const std::string &g : groups)
    by_month[key_ym(g)].insert(g);
  std::vector<Task> out;
  for (const Slot &sl : frozen) {
    const auto it = by_month.find(sl.ym);
    if (it == by_month.end())
      continue;
    const std::set<std::string> &keys = it->second;
    const bool whole =
        std::any_of(keys.begin(), keys.end(), [](const std::string &k) { return grain_of(k) == Grain::Month; });
    if (whole)
      out.push_back({&s, sl.ym, sl.w, Write::Replace});
    else
      out.push_back({&s, sl.ym, sl.w, Write::Splice, {keys.begin(), keys.end()}});
  }
  assert(out.size() == by_month.size() && "repair: 差异月不全在冻结月里");
  return out;
}

} // namespace fundamental::audit
