#include "fundamental/Audit.hpp"

#include "fundamental/Remote.hpp"
#include "misc/fs.hpp"
#include "misc/parquet.hpp"

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace fundamental::audit {

namespace {

namespace fs = std::filesystem;
namespace pq = misc::pq;
using i128 = __int128;

enum class Kind { Real,
                  Int,
                  Ts,
                  Str,
                  Null };

// a 的每个有效行 i → f(i, 值)
template <class A, class F>
void valid(const A &a, F &&f) {
  for (std::int64_t i = 0; i < a.length(); ++i)
    if (a.IsValid(i))
      f(i, a.Value(i));
}

// ---------------------------------------------------------------------------
// 指纹列型: 一型一行, 远端 SQL 求和式 sql(列) 与本地逐值整数 each(数组, f(i, 整数)) 相邻成对, 逐位同口径
//   Null (全 null 的 NA 列) 只有 nn 格, 无 sql / each
// ---------------------------------------------------------------------------
struct Real {
  static constexpr Kind kind = Kind::Real;
  // Σ round(c·1e6) (仅有限值); 1e6 是 DOUBLE 字面量 (写成 1000000.0 会变 DECIMAL, 乘法口径就和本地不一样了)
  static std::string sql(const std::string &c) {
    return "SUM(CASE WHEN isfinite(" + c + ") THEN CAST(ROUND(" + c + " * 1e6) AS HUGEINT) END)";
  }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    valid(a, [&](std::int64_t i, auto x) { f(i, cell(static_cast<double>(x))); });
  }
  static i128 cell(double x) {
    if (!std::isfinite(x))
      return 0;
    const double r = std::round(x * 1e6);
    assert(std::fabs(r) < 1e36 && "audit: 数值超 HUGEINT 量程");
    return static_cast<i128>(r);
  }
};

struct Int {
  static constexpr Kind kind = Kind::Int;
  // Σ c (整数 / bool)
  static std::string sql(const std::string &c) { return "SUM(CAST(" + c + " AS HUGEINT))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    valid(a, [&](std::int64_t i, auto x) { f(i, static_cast<i128>(x)); });
  }
};

struct Ts {
  static constexpr Kind kind = Kind::Ts;
  // Σ epoch_ms(c)
  static std::string sql(const std::string &c) { return "SUM(epoch_ms(" + c + "))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    const auto u = static_cast<const arrow::TimestampType &>(*a.type()).unit();
    valid(a, [&](std::int64_t i, std::int64_t x) { f(i, ms(x, u)); });
  }
  // epoch_ms 与 DuckDB 同为截断除法
  static i128 ms(std::int64_t v, arrow::TimeUnit::type u) {
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
};

struct Str {
  static constexpr Kind kind = Kind::Str;
  // Σ strlen(c) (等长改写测不出)
  static std::string sql(const std::string &c) { return "SUM(strlen(" + c + "))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    valid(a, [&](std::int64_t i, std::string_view x) { f(i, static_cast<i128>(x.size())); });
  }
};

struct Null {
  static constexpr Kind kind = Kind::Null;
};

// arrow 类型 → (数组类型 A, 列型 K): f.template operator()<A, K>() —— 全文件唯一的类型分派; 表外类型 → assert
template <class F>
decltype(auto) dispatch(const arrow::DataType &t, F &&f) {
  switch (t.id()) {
  case arrow::Type::DOUBLE:
    return f.template operator()<arrow::DoubleArray, Real>();
  case arrow::Type::FLOAT:
    return f.template operator()<arrow::FloatArray, Real>();
  case arrow::Type::BOOL:
    return f.template operator()<arrow::BooleanArray, Int>();
  case arrow::Type::INT8:
    return f.template operator()<arrow::Int8Array, Int>();
  case arrow::Type::INT16:
    return f.template operator()<arrow::Int16Array, Int>();
  case arrow::Type::INT32:
    return f.template operator()<arrow::Int32Array, Int>();
  case arrow::Type::INT64:
    return f.template operator()<arrow::Int64Array, Int>();
  case arrow::Type::UINT8:
    return f.template operator()<arrow::UInt8Array, Int>();
  case arrow::Type::UINT16:
    return f.template operator()<arrow::UInt16Array, Int>();
  case arrow::Type::UINT32:
    return f.template operator()<arrow::UInt32Array, Int>();
  case arrow::Type::UINT64:
    return f.template operator()<arrow::UInt64Array, Int>();
  case arrow::Type::TIMESTAMP:
    return f.template operator()<arrow::TimestampArray, Ts>();
  case arrow::Type::STRING:
    return f.template operator()<arrow::StringArray, Str>();
  case arrow::Type::LARGE_STRING:
    return f.template operator()<arrow::LargeStringArray, Str>();
  case arrow::Type::NA:
    return f.template operator()<arrow::NullArray, Null>();
  default:
    break;
  }
  std::cerr << "[bigquant.audit] 不支持的列类型: " << t.ToString() << std::endl;
  assert(false && "audit: 不支持的列类型");
  return f.template operator()<arrow::NullArray, Null>();
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
  std::vector<std::string> sums; // 列的 s 格 SQL 求和式 (Null 列空)
  std::vector<int> nn, s;        // 列在 all 里的 nn / s 格号 (Null 列 s = -1)
  bool asset = false;            // 远端有资产键列 ⇒ 可定位到 Asset 级
  Cells all;                     // n + 逐列 nn / s (Null 列无 s) —— 本地逐日基格同序
};

// 组键 (Spec.hpp 键形, 与服务端 group_expr 同形) → 指纹格 (与所查 Cells 同序)
using Fps = std::map<std::string, std::vector<i128>>;

// 远端指纹格 (CAST .. AS VARCHAR 的十进制整数) → i128
i128 parse_i128(std::string_view v) {
  assert(!v.empty() && "audit: 远端指纹格为空");
  const bool neg = v.front() == '-';
  i128 x = 0;
  for (std::size_t i = neg ? 1 : 0; i < v.size(); ++i) {
    assert(v[i] >= '0' && v[i] <= '9' && "audit: 远端指纹格不是十进制整数");
    x = x * 10 + (v[i] - '0');
  }
  return neg ? -x : x;
}

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
    dispatch(*f->type(), [&]<class A, class K>() {
      L.kinds.push_back(K::kind);
      if constexpr (K::kind == Kind::Null)
        L.sums.emplace_back();
      else
        L.sums.push_back(K::sql(ident(f->name())));
    });
    L.asset = L.asset || f->name() == key_col(*u.spec);
    L.nn.push_back(static_cast<int>(L.all.size()));
    L.all.push_back({false, {c}});
    L.s.push_back(L.kinds.back() == Kind::Null ? -1 : static_cast<int>(L.all.size()));
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

// ---------------------------------------------------------------------------
// 两侧同一接口 fps(cells, g, parents): 按 g 级分组的指纹, parents 非空 → 只这些父组 (父键 = 组键前缀, 升序)
// ---------------------------------------------------------------------------

// 远端: 窗口 w 内每次一条 SQL GROUP BY
struct Remote {
  bigquant::Client &client;
  const Spec &s;
  const Layout &L;
  Window w;

  Fps fps(const Cells &cells, Grain g, const std::vector<std::string> &parents) const {
    std::string cols = group_expr(s, g) + " AS g";
    for (std::size_t k = 0; k < cells.size(); ++k) {
      const Cell &c = cells[k];
      std::string e;
      if (c.cols.empty())
        e = "COUNT(*)";
      for (int col : c.cols) {
        const std::string one = c.sum ? L.sums[col] : "COUNT(" + ident(L.names[col]) + ")";
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
        v.push_back(parse_i128(c.str(i)));
    }
    return out;
  }
};

// 列 c 进入的指纹格: 第 pos 格 += w × (sum ? s : 1)
struct Use {
  std::size_t pos;
  bool sum;
  std::int64_t w;
};

// 一列累加进各行所属组 (acc[行] 空 = 该行不计): nn += 非 null, s += 列型整数
void accumulate(const arrow::ChunkedArray &col, [[maybe_unused]] Kind k, const std::vector<Use> &uses,
                const std::vector<std::vector<i128> *> &acc) {
  std::int64_t base = 0;
  for (const auto &chunk : col.chunks()) {
    dispatch(*chunk->type(), [&]<class A, class K>() {
      if constexpr (K::kind != Kind::Null) {
        assert(K::kind == k && "audit: 本地列类型与远端不符");
        K::each(static_cast<const A &>(*chunk), [&](std::int64_t i, i128 x) {
          std::vector<i128> *v = acc[static_cast<std::size_t>(base + i)];
          if (!v)
            return;
          for (const Use &u : uses)
            (*v)[u.pos] += u.w * (u.sum ? x : 1);
        });
      }
    });
    base += chunk->length();
  }
}

// t 的逐行 vd 日 (分片内按日成块); null / 不在 ym 月 → assert
std::vector<Ymd> days_of(const arrow::Table &t, const Spec &s, Ym ym) {
  const auto vd = t.GetColumnByName(s.vd);
  assert(vd && "audit: 本地分片缺 vd 列");
  std::vector<Ymd> out, part;
  out.reserve(static_cast<std::size_t>(t.num_rows()));
  for (const auto &chunk : vd->chunks()) {
    pq::Col(chunk).yyyymmdd_all(part);
    out.insert(out.end(), part.begin(), part.end());
  }
  Ymd prev = 0;
  for (const Ymd d : out)
    if (d != prev) {
      assert(d > 0 && "audit: 本地 vd 为 null");
      assert(misc::ym_of(d) == ym && "audit: 行的 vd 不在所属月分片");
      prev = d;
    }
  return out;
}

// 本地: 一个月 (同月的槽). 构造即读盘一次 → 逐日基格 (与 L.all 同序) + 清点;
//   Month / Day 级指纹 = 逐日基格按所查格线性导出 (零再读); Asset 级再读一次, 只解所查列 + vd + 资产键
class Local {
public:
  Local(const Spec &s, const Layout &L, std::vector<Slot> month)
      : s_(s), L_(L), month_(std::move(month)), has_(L.names.size(), 0) {
    // 逐日基格: 列 c 进 nn / s 两格, 权 1
    std::vector<std::vector<Use>> uses(L.names.size());
    for (std::size_t c = 0; c < L.names.size(); ++c) {
      uses[c].push_back({static_cast<std::size_t>(L.nn[c]), false, 1});
      if (L.s[c] >= 0)
        uses[c].push_back({static_cast<std::size_t>(L.s[c]), true, 1});
    }
    for (const Slot &sl : month_) {
      const auto t = read(*sl.sh);
      inv_.rows += t->num_rows();
      inv_.cols = std::max<std::int64_t>(inv_.cols, t->num_columns());
      const std::vector<Ymd> d = days_of(*t, s, sl.ym);
      std::vector<std::vector<i128> *> acc(d.size());
      std::vector<i128> *cur = nullptr;
      for (std::size_t i = 0; i < d.size(); ++i) {
        if (i == 0 || d[i] != d[i - 1]) {
          cur = &days_[d[i]];
          if (cur->empty())
            cur->assign(L.all.size(), 0);
        }
        acc[i] = cur;
        ++(*cur)[0];
      }
      for (std::size_t c = 0; c < L.names.size(); ++c) {
        const auto col = t->GetColumnByName(L.names[c]);
        if (!col)
          continue; // 本地缺列 = 全 null
        has_[c] = 1;
        accumulate(*col, L.kinds[c], uses[c], acc);
      }
    }
    inv_.days = static_cast<std::int64_t>(days_.size());
  }

  const Inv &inv() const { return inv_; }

  Fps fps(const Cells &cells, Grain g, const std::vector<std::string> &parents) const {
    assert(std::is_sorted(parents.begin(), parents.end()) && "audit: parents 须升序");
    if (g == Grain::Asset)
      return assets(cells, parents);
    assert((g == Grain::Day || parents.empty()) && "audit: Month 级无父组");
    Fps out;
    for (const auto &[d, b] : days_) {
      if (!parents.empty() && !std::binary_search(parents.begin(), parents.end(), group_key(Grain::Month, d)))
        continue;
      std::vector<i128> &o = out[group_key(g, d)];
      if (o.empty())
        o.assign(cells.size(), 0);
      o[0] += b[0];
      for (std::size_t k = 1; k < cells.size(); ++k)
        for (int c : cells[k].cols) {
          const int pos = cells[k].sum ? L_.s[c] : L_.nn[c];
          assert(pos > 0);
          o[k] += weight(cells[k], c) * b[static_cast<std::size_t>(pos)];
        }
    }
    return out;
  }

private:
  Fps assets(const Cells &cells, const std::vector<std::string> &parents) const {
    assert(month_.size() == 1 && "audit: Asset 级只下钻单分片的月 (Daily 最细到 Day)");
    std::vector<std::vector<Use>> uses(L_.names.size());
    for (std::size_t k = 1; k < cells.size(); ++k)
      for (int c : cells[k].cols)
        uses[c].push_back({k, cells[k].sum, weight(cells[k], c)});
    const std::string kcol(key_col(s_));
    std::vector<std::string> need{s_.vd, kcol};
    for (std::size_t c = 0; c < L_.names.size(); ++c)
      if (!uses[c].empty() && has_[c] && std::find(need.begin(), need.end(), L_.names[c]) == need.end())
        need.push_back(L_.names[c]);

    const Slot &sl = month_.front();
    const auto t = read(*sl.sh, need);
    const std::vector<Ymd> d = days_of(*t, s_, sl.ym);
    const auto kc = t->GetColumnByName(kcol);
    assert(kc && "audit: 本地分片缺资产键列");
    Fps out;
    std::vector<std::vector<i128> *> acc(d.size(), nullptr);
    std::size_t i = 0;
    Ymd prev = 0;
    bool in = false;
    for (const auto &chunk : kc->chunks()) {
      const pq::Col k(chunk);
      for (std::int64_t j = 0; j < chunk->length(); ++j, ++i) {
        if (d[i] != prev) {
          prev = d[i];
          in = std::binary_search(parents.begin(), parents.end(), group_key(Grain::Day, d[i]));
        }
        if (!in)
          continue;
        std::vector<i128> &v = out[group_key(Grain::Asset, d[i], k.str(j))];
        if (v.empty())
          v.assign(cells.size(), 0);
        acc[i] = &v;
        ++v[0];
      }
    }
    for (std::size_t c = 0; c < L_.names.size(); ++c)
      if (!uses[c].empty() && has_[c])
        accumulate(*t->GetColumnByName(L_.names[c]), L_.kinds[c], uses[c], acc);
    return out;
  }

  const Spec &s_;
  const Layout &L_;
  std::vector<Slot> month_;
  std::map<Ymd, std::vector<i128>> days_; // 日 → 基格 (与 L.all 同序)
  std::vector<char> has_;                 // 本地有列 c (缺列 = 全 null)
  Inv inv_;
};

const std::vector<i128> &find_or_zero(const Fps &fps, const std::string &key, const std::vector<i128> &zeros) {
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
  std::vector<i128> l, r;
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
  d.local_n = static_cast<std::int64_t>(p.l[0]);
  d.remote_n = static_cast<std::int64_t>(p.r[0]);
  d.detail = diff_detail(L, p);
  for (std::size_t k = 1; k < p.cells->size(); ++k)
    if (p.l[k] != p.r[k])
      d.cells.push_back({cell_name(L, (*p.cells)[k]), (*p.cells)[k].sum, i128_str(p.l[k]), i128_str(p.r[k])});
  return d;
}

// 一个月 (远端 = 该月窗口, 本地 = 该月的槽) 内一个不一致的组逐级往下定位 (Day → Asset), 每级两侧各一次;
// 返回最终组 (组键彼此不相交)
Probes locate(const Remote &remote, const Local &local, Probe top) {
  const Spec &s = remote.s;
  const Layout &L = remote.L;
  const std::int64_t C = static_cast<std::int64_t>(L.names.size()); // 直接修复一行的 cell (SELECT *)
  std::int64_t span = 0;                                            // 月窗口天数 = Day 级子组数上界
  for (Ymd d = remote.w.lo; d <= remote.w.hi; d = misc::add_days(d, 1))
    ++span;
  Probes done, frontier{std::move(top)};
  for (const Grain g : {Grain::Day, Grain::Asset}) {
    // 可下钻的父组: 子级可单独重写 (g ≤ finest) 且可分组 (Asset 级远端须有资产键列)
    //   ∧ 两侧皆非空 (一侧空 ⇒ 子组全不一致); D = 不一致格涉及的列
    const bool deeper = g <= finest(s) && (g != Grain::Asset || L.asset);
    Probes drill;
    std::set<int> D;
    std::int64_t bound = 0, fix = 0; // 子组数上界 / 直接按父组修复的 cell
    for (Probe &p : frontier) {
      const std::int64_t ln = static_cast<std::int64_t>(p.l[0]), rn = static_cast<std::int64_t>(p.r[0]);
      if (!deeper || ln == 0 || rn == 0) {
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
    const Fps rc = remote.fps(*cells, g, parents);
    const Fps lc = local.fps(*cells, g, parents);
    const std::vector<i128> zeros(cells->size(), 0);
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

// 月 → 版本 = 该月槽的 mtime 之和 (回绕无妨, 只比相等)
std::map<Ym, std::uint64_t> versions(const std::vector<Slot> &slots) {
  std::map<Ym, std::uint64_t> out;
  for (const Slot &sl : slots)
    out[sl.ym] += static_cast<std::uint64_t>(sl.sh->mtime.time_since_epoch().count());
  return out;
}

// Diff → 差异格行: 只差 n 一行; 否则每个不一致格一行
void append_rows(std::vector<Book::Row> &out, const Diff &d) {
  if (d.cells.empty()) {
    out.push_back({d.group, d.local_n, d.remote_n});
    return;
  }
  for (const Diff::Cell &c : d.cells)
    out.push_back({d.group, d.local_n, d.remote_n, c.column, c.sum ? "s" : "nn", c.local, c.remote});
}

std::vector<std::string> split_tab(const std::string &line) {
  std::vector<std::string> f;
  std::size_t s = 0;
  for (std::size_t i = 0; i <= line.size(); ++i)
    if (i == line.size() || line[i] == '\t') {
      f.push_back(line.substr(s, i - s));
      s = i + 1;
    }
  return f;
}

template <class T>
std::optional<T> to_num(const std::string &s) {
  T v = 0;
  const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc{} || p != s.data() + s.size())
    return std::nullopt;
  return v;
}

// 与 grain_of / key_day / key_ym 同形: YYYY-MM[-DD[|资产]]
bool valid_group(const std::string &g) {
  const std::string_view v = g;
  if (misc::parse_ym(v.substr(0, 7)) == 0)
    return false;
  if (g.size() == 7)
    return true;
  return misc::parse_ymd(v.substr(0, 10)) > 0 && (g.size() == 10 || (g.size() > 11 && g[10] == '|'));
}

// 状态文件 <表>.tsv (# 起头为注释):
//   month \t YYYY-MM \t 核对时刻 \t 版本 \t 本地行 \t 本地天数 \t 列数
//         \t 问题天数 \t 行差+ \t 行差−   (末三列由 diff 导出, 只供人看, 读时不用)
//   diff  \t 组 \t local_n \t remote_n \t 列 \t nn|s \t 本地 \t 远端
void save(const std::string &name, const Book::Sheet &t) {
  struct MonthSum {
    std::set<std::string> bad;
    std::int64_t added = 0, removed = 0;
  };
  std::map<std::string, std::pair<std::int64_t, std::int64_t>> group_n; // 同组多格只计一次
  for (const Book::Row &r : t.rows)
    group_n[r.group] = {r.local_n, r.remote_n};
  std::map<std::string, MonthSum> sum;
  for (const auto &[g, n] : group_n) {
    MonthSum &s = sum[g.substr(0, 7)];
    s.bad.insert(g.substr(0, 10));
    s.added += std::max<std::int64_t>(0, n.second - n.first);
    s.removed += std::max<std::int64_t>(0, n.first - n.second);
  }

  const fs::path p = Book::dir() / (name + ".tsv");
  const fs::path tmp = p.string() + ".tmp";
  {
    std::ofstream out(tmp);
    assert(out && "audit: 状态文件无法写入");
    out << "# " << name << "\n"
        << "# month\tym\taudited_at\tver\tlocal_rows\tlocal_days\tcols\tbad_days\tadded\tremoved\n"
        << "# diff\tgroup\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
    for (const auto &[ym, m] : t.months) {
      const auto s = sum.find(ym);
      const bool bad = s != sum.end();
      out << "month\t" << ym << '\t' << m.at << '\t' << m.ver << '\t' << m.inv.rows << '\t' << m.inv.days << '\t'
          << m.inv.cols << '\t' << (bad ? s->second.bad.size() : 0) << '\t' << (bad ? s->second.added : 0) << '\t'
          << (bad ? s->second.removed : 0) << '\n';
    }
    std::vector<const Book::Row *> rows;
    for (const Book::Row &r : t.rows)
      rows.push_back(&r);
    std::stable_sort(rows.begin(), rows.end(),
                     [](const Book::Row *x, const Book::Row *y) { return x->group < y->group; });
    for (const Book::Row *r : rows)
      out << "diff\t" << r->group << '\t' << r->local_n << '\t' << r->remote_n << '\t' << r->column << '\t'
          << r->cell << '\t' << r->local << '\t' << r->remote << '\n';
    out.flush();
    assert(out && "audit: 状态文件写入失败");
  }
  fs::rename(tmp, p);
}

// 一个状态文件; 表不参与核对 (audited) / 任一行格式不符 / 差异月无结论 → nullopt
std::optional<Book::Sheet> parse(const fs::path &p) {
  const std::string name = p.stem().string();
  const auto s = std::ranges::find_if(SPECS, [&](const Spec &x) { return x.name == name; });
  if (s == SPECS.end() || !audited(*s))
    return std::nullopt;
  Book::Sheet t;
  std::ifstream in(p);
  assert(in && "audit: 状态文件无法打开");
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line.starts_with("#"))
      continue;
    const std::vector<std::string> f = split_tab(line);
    if (f[0] == "month") {
      if (f.size() != 10 || misc::parse_ym(f[1]) == 0 || f[2].empty())
        return std::nullopt;
      const auto ver = to_num<std::uint64_t>(f[3]);
      const auto rows = to_num<std::int64_t>(f[4]), days = to_num<std::int64_t>(f[5]), cols = to_num<std::int64_t>(f[6]);
      if (!ver || !rows || !days || !cols)
        return std::nullopt;
      t.months[f[1]] = {{*rows, *days, *cols}, f[2], *ver};
      continue;
    }
    if (f[0] != "diff" || f.size() != 8 || !valid_group(f[1]))
      return std::nullopt;
    const auto local_n = to_num<std::int64_t>(f[2]), remote_n = to_num<std::int64_t>(f[3]);
    if (!local_n || !remote_n)
      return std::nullopt;
    t.rows.push_back({f[1], *local_n, *remote_n, f[4], f[5], f[6], f[7]});
  }
  for (const Book::Row &r : t.rows)
    if (!t.months.contains(r.group.substr(0, 7)))
      return std::nullopt;
  return t;
}

} // namespace

template <class Src>
std::vector<Unit> plan(const Round &r, const Keep &keep) {
  static_assert(Src::exact, "audit: 只核精确源");
  std::mt19937 rng(static_cast<unsigned>(r.now.at.time_since_epoch().count()));
  std::vector<std::vector<Unit>> tables;
  for (const Spec &s : SPECS) {
    if (!from<Src>(s) || !audited(s))
      continue;
    std::map<int, Unit> by_year;
    for (const Slot &sl : frozen(s, r)) {
      if (keep && !keep(s, sl))
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

template <class Src>
Result run(typename Src::Client &client, const Unit &u) {
  static_assert(Src::exact, "audit: 只核精确源");
  assert(u.spec && !u.slots.empty());
  const Spec &s = *u.spec;
  const Layout L = layout_of(client, u);
  const auto all = std::make_shared<const Cells>(L.all);
  const std::vector<i128> zeros(all->size(), 0);

  // 月级 (第一遍扫描), 全部格: 远端整单元一次查询; 本地逐月读盘一次 (Local, 清点随之得出)
  const Fps rm = Remote{client, s, L, u.w}.fps(*all, Grain::Month, {});

  // 同月的槽成组 (Month 一月一槽, Daily 一月多日槽); 组窗口 = 首槽 w.lo .. 末槽 w.hi
  Result out;
  for (auto b = u.slots.begin(); b != u.slots.end();) {
    const auto e = std::find_if(b, u.slots.end(), [&](const Slot &x) { return x.ym != b->ym; });
    const Ym ym = b->ym;
    const Window w{b->w.lo, std::prev(e)->w.hi};
    const Local local(s, L, std::vector<Slot>(b, e));
    b = e;
    out.inv[ym] = local.inv();
    const std::string key = group_key(Grain::Month, w.lo);
    const Fps lm = local.fps(*all, Grain::Month, {});
    const auto &l = find_or_zero(lm, key, zeros);
    const auto &r = find_or_zero(rm, key, zeros);
    if (l == r)
      continue;
    for (const Probe &p : locate(Remote{client, s, L, w}, local, {key, all, l, r}))
      out.diffs.push_back(make_diff(s, L, p));
  }
  return out;
}

template std::vector<Unit> plan<Bq>(const Round &, const Keep &);
template Result run<Bq>(bigquant::Client &, const Unit &);

std::string label(const Unit &u) { return u.spec->name + " " + std::to_string(u.year); }

std::vector<Task> repair(const Spec &s, const Round &r, const std::vector<std::string> &groups) {
  const std::set<std::string> keys(groups.begin(), groups.end()); // 升序去重
  const Grain sg = slot_grain(s);
  std::set<std::string> hit;
  std::vector<Task> out;
  for (const Slot &sl : frozen(s, r)) {
    const std::string k = slot_key(s, sl);
    std::vector<std::string> in; // 与槽相交的差异键 (升序)
    bool whole = false;
    for (const std::string &g : keys)
      if (g.starts_with(k) || k.starts_with(g)) {
        in.push_back(g);
        whole = whole || grain_of(g) <= sg;
      }
    if (in.empty())
      continue;
    hit.insert(in.begin(), in.end());
    if (whole) {
      out.push_back({&s, sl.ym, sl.w, Write::Replace});
      continue;
    }
    assert(finest(s) > sg && "repair: 槽级以下不可 Splice");
    out.push_back({&s, sl.ym, sl.w, Write::Splice, std::move(in)});
  }
  assert(hit.size() == keys.size() && "repair: 差异键不全落在冻结槽里");
  return out;
}

fs::path Book::dir() {
  const fs::path d = misc::git_root() / "output" / "fundamental" / "_audit";
  fs::create_directories(d);
  return d;
}

Book::Book() {
  for (const auto &e : fs::directory_iterator(dir())) {
    if (e.path().extension() != ".tsv")
      continue;
    std::optional<Sheet> t = parse(e.path());
    if (!t) {
      std::cout << "[bigquant.audit] 状态文件格式不符, 已删除: " << e.path().string() << std::endl;
      fs::remove(e.path());
      continue;
    }
    sheets_[e.path().stem().string()] = std::move(*t);
  }
}

void Book::put(const Unit &u, const Result &res, const std::string &at) {
  Sheet &t = sheets_[u.spec->name];
  const std::map<Ym, std::uint64_t> ver = versions(u.slots);
  std::set<std::string> keys;
  for (const auto &[ym, inv] : res.inv) {
    const std::string key = misc::dash_ym(ym);
    t.months[key] = {inv, at, ver.at(ym)};
    keys.insert(key);
  }
  std::erase_if(t.rows, [&](const Row &r) { return keys.contains(r.group.substr(0, 7)); });
  for (const Diff &d : res.diffs)
    append_rows(t.rows, d);
  save(u.spec->name, t);
}

Book::Sheet Book::sheet(const Spec &s, const Round &r) const {
  Sheet out;
  const auto it = sheets_.find(s.name);
  if (it == sheets_.end())
    return out;
  const std::map<Ym, std::uint64_t> ver = versions(frozen(s, r));
  for (const auto &[key, e] : it->second.months) {
    const auto v = ver.find(key_ym(key));
    if (v != ver.end() && v->second == e.ver)
      out.months.insert({key, e});
  }
  for (const Row &row : it->second.rows)
    if (out.months.contains(row.group.substr(0, 7)))
      out.rows.push_back(row);
  return out;
}

} // namespace fundamental::audit
