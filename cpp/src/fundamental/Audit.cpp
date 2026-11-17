#include "fundamental/Audit.hpp"

#include "disk/Disk.hpp"
#include "fundamental/Remote.hpp"
#include "misc/exec.hpp"
#include "misc/parquet.hpp"

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
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

template <class A, class F>
void valid(const A &a, F &&f) {
  for (std::int64_t i = 0; i < a.length(); ++i)
    if (a.IsValid(i))
      f(i, a.Value(i));
}

// ---------------------------------------------------------------------------
// 指纹列型: 远端 SQL 求和式 sql(列) ↔ 本地逐值整数 each(数组, f(i, 整数)), 逐位同口径
// ---------------------------------------------------------------------------
struct Real {
  static constexpr Kind kind = Kind::Real;
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
  static std::string sql(const std::string &c) { return "SUM(CAST(" + c + " AS HUGEINT))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    valid(a, [&](std::int64_t i, auto x) { f(i, static_cast<i128>(x)); });
  }
};

struct Ts {
  static constexpr Kind kind = Kind::Ts;
  static std::string sql(const std::string &c) { return "SUM(epoch_ms(" + c + "))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    const auto u = static_cast<const arrow::TimestampType &>(*a.type()).unit();
    valid(a, [&](std::int64_t i, std::int64_t x) { f(i, ms(x, u)); });
  }
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
  static std::string sql(const std::string &c) { return "SUM(strlen(" + c + "))"; }
  template <class A, class F>
  static void each(const A &a, F &&f) {
    valid(a, [&](std::int64_t i, std::string_view x) { f(i, static_cast<i128>(x.size())); });
  }
};

struct Null {
  static constexpr Kind kind = Kind::Null;
};

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

struct Cell {
  int col = -1;
  bool sum = false;
};
using Cells = std::vector<Cell>; // [0] = n

struct Layout {
  std::vector<std::string> names;
  std::vector<Kind> kinds;
  std::vector<std::string> sums;
  std::vector<int> nn, s;
  Cells all;
};

using Fps = std::map<std::string, std::vector<i128>>;

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

Layout layout_of(Clients &clients, const Unit &u) {
  Layout L;
  L.all.push_back({});
  const std::shared_ptr<arrow::Schema> sch = schema(clients, *u.spec, u.w);
  const std::vector<std::string> &drop = u.spec->drop;
  for (const auto &f : sch->fields()) {
    if (std::find(drop.begin(), drop.end(), f->name()) != drop.end())
      continue;
    const int c = static_cast<int>(L.names.size());
    L.names.push_back(f->name());
    dispatch(*f->type(), [&]<class A, class K>() {
      L.kinds.push_back(K::kind);
      if constexpr (K::kind == Kind::Null)
        L.sums.emplace_back();
      else
        L.sums.push_back(K::sql(ident(f->name())));
    });
    L.nn.push_back(static_cast<int>(L.all.size()));
    L.all.push_back({c, false});
    L.s.push_back(L.kinds.back() == Kind::Null ? -1 : static_cast<int>(L.all.size()));
    if (L.kinds.back() != Kind::Null)
      L.all.push_back({c, true});
  }
  return L;
}

Cells per_column(const Layout &L, const std::set<int> &D) {
  Cells out{{}};
  for (int c : D) {
    out.push_back({c, false});
    if (L.kinds[c] != Kind::Null)
      out.push_back({c, true});
  }
  return out;
}

// ---------------------------------------------------------------------------
// Remote ↔ Local 同签名对偶: fps(cells, g)
// ---------------------------------------------------------------------------
struct Remote {
  bigquant::Client &client;
  const Spec &s;
  const Layout &L;
  Window w;

  Fps fps(const Cells &cells, Grain g) const {
    std::string cols = group_expr(s, g) + " AS g";
    for (std::size_t k = 0; k < cells.size(); ++k) {
      const Cell &c = cells[k];
      const std::string e = c.col < 0 ? "COUNT(*)" : c.sum ? L.sums[c.col]
                                                           : "COUNT(" + ident(L.names[c.col]) + ")";
      cols += ", CAST(COALESCE(" + e + ", 0) AS VARCHAR) AS f" + std::to_string(k);
    }

    pq::TableView tv(client.query(select(s, cols, w, " GROUP BY 1")));
    const pq::Col gc = tv.col("g");
    std::vector<pq::Col> fc;
    fc.reserve(cells.size());
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

struct Use {
  std::size_t pos;
  bool sum;
};

void accumulate(const arrow::ChunkedArray &col, [[maybe_unused]] Kind k, const std::vector<Use> &uses,
                const std::vector<std::vector<i128> *> &acc) {
  std::int64_t base = 0;
  for (const auto &chunk : col.chunks()) {
    dispatch(*chunk->type(), [&]<class A, class K>() {
      if constexpr (K::kind != Kind::Null) {
        assert(K::kind == k && "audit: 本地列类型与远端不符");
        K::each(static_cast<const A &>(*chunk), [&](std::int64_t i, i128 x) {
          std::vector<i128> *v = acc[static_cast<std::size_t>(base + i)];
          for (const Use &u : uses)
            (*v)[u.pos] += u.sum ? x : 1;
        });
      }
    });
    base += chunk->length();
  }
}

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

std::int64_t distinct_keys(const arrow::Table &t, const Spec &s) {
  const auto key = t.GetColumnByName(std::string(key_col(s)));
  assert(key && "audit: 本地分片缺资产键列");
  std::unordered_set<std::string_view> seen;
  for (const auto &chunk : key->chunks()) {
    const pq::Col k(chunk);
    for (std::int64_t i = 0; i < chunk->length(); ++i)
      seen.insert(k.str(i));
  }
  return static_cast<std::int64_t>(seen.size());
}

// 本地: 一个月 (同月的槽). 构造即并行读盘一次 → 逐日基格 + 清点; Month / Day 级均由逐日基格内存线性导出
class Local {
public:
  Local(const Spec &s, const Layout &L, std::vector<Slot> month) : L_(L) {
    std::vector<std::vector<Use>> uses(L.names.size());
    for (std::size_t c = 0; c < L.names.size(); ++c) {
      uses[c].push_back({static_cast<std::size_t>(L.nn[c]), false});
      if (L.s[c] >= 0)
        uses[c].push_back({static_cast<std::size_t>(L.s[c]), true});
    }
    struct Part {
      Inv inv;
      std::map<Ymd, std::vector<i128>> days;
    };
    std::vector<Part> parts(month.size());
    misc::exec::ForkJoin ex(misc::exec::fit(month.size()));
    ex.tasks(static_cast<int>(month.size()), [&](int k, int) {
      const Slot &sl = month[static_cast<std::size_t>(k)];
      Part &p = parts[static_cast<std::size_t>(k)];
      const auto t = read(*sl.sh);
      p.inv.rows = t->num_rows();
      p.inv.cols = t->num_columns();
      if (s.shape == Shape::Daily)
        p.inv.assets = distinct_keys(*t, s);
      const std::vector<Ymd> d = days_of(*t, s, sl.ym());
      std::vector<std::vector<i128> *> acc(d.size());
      std::vector<i128> *cur = nullptr;
      for (std::size_t i = 0; i < d.size(); ++i) {
        if (i == 0 || d[i] != d[i - 1]) {
          cur = &p.days[d[i]];
          if (cur->empty())
            cur->assign(L.all.size(), 0);
        }
        acc[i] = cur;
        ++(*cur)[0];
      }
      for (std::size_t c = 0; c < L.names.size(); ++c)
        if (const auto col = t->GetColumnByName(L.names[c]))
          accumulate(*col, L.kinds[c], uses[c], acc);
    });
    for (Part &p : parts) {
      inv_.rows += p.inv.rows;
      inv_.cols = std::max(inv_.cols, p.inv.cols);
      inv_.assets = std::max(inv_.assets, p.inv.assets);
      for (auto &[d, b] : p.days) {
        std::vector<i128> &o = days_[d];
        if (o.empty()) {
          o = std::move(b);
          continue;
        }
        for (std::size_t k = 0; k < o.size(); ++k)
          o[k] += b[k];
      }
    }
    inv_.days = static_cast<std::int64_t>(days_.size());
  }

  const Inv &inv() const { return inv_; }

  Fps fps(const Cells &cells, Grain g) const {
    Fps out;
    for (const auto &[d, b] : days_) {
      std::vector<i128> &o = out[group_key(g, d)];
      if (o.empty())
        o.assign(cells.size(), 0);
      o[0] += b[0];
      for (std::size_t k = 1; k < cells.size(); ++k) {
        const int pos = cells[k].sum ? L_.s[cells[k].col] : L_.nn[cells[k].col];
        assert(pos > 0);
        o[k] += b[static_cast<std::size_t>(pos)];
      }
    }
    return out;
  }

private:
  const Layout &L_;
  std::map<Ymd, std::vector<i128>> days_;
  Inv inv_;
};

const std::vector<i128> &find_or_zero(const Fps &fps, const std::string &key, const std::vector<i128> &zeros) {
  const auto it = fps.find(key);
  return it == fps.end() ? zeros : it->second;
}

struct Probe {
  std::string key;
  std::shared_ptr<const Cells> cells;
  std::vector<i128> l, r;
};
using Probes = std::vector<Probe>;

Diff make_diff(const Layout &L, const Probe &p) {
  Diff d{p.key, static_cast<std::int64_t>(p.l[0]), static_cast<std::int64_t>(p.r[0]), {}};
  for (std::size_t k = 1; k < p.cells->size(); ++k)
    if (p.l[k] != p.r[k])
      d.cells.push_back({L.names[(*p.cells)[k].col], (*p.cells)[k].sum, i128_str(p.l[k]), i128_str(p.r[k])});
  return d;
}

// 不一致的月下钻到日级 (一侧 0 行或全月各日皆差则保留月级)
Probes locate(const Remote &remote, const Local &local, Probe top) {
  if (top.l[0] == 0 || top.r[0] == 0)
    return {std::move(top)};
  std::set<int> D;
  for (std::size_t k = 1; k < top.cells->size(); ++k)
    if (top.l[k] != top.r[k])
      D.insert((*top.cells)[k].col);
  const auto cells = std::make_shared<const Cells>(per_column(remote.L, D));
  const Fps rc = remote.fps(*cells, Grain::Day);
  const Fps lc = local.fps(*cells, Grain::Day);
  const std::vector<i128> zeros(cells->size(), 0);
  std::set<std::string> kids;
  for (const Fps *f : {&lc, &rc})
    for (const auto &[k, _] : *f)
      kids.insert(k);
  Probes bad;
  for (const std::string &k : kids) {
    const auto &l = find_or_zero(lc, k, zeros);
    const auto &r = find_or_zero(rc, k, zeros);
    if (l != r)
      bad.push_back({k, cells, l, r});
  }
  if (bad.empty() || bad.size() == kids.size())
    return {std::move(top)};
  return bad;
}

std::map<Ym, std::uint64_t> versions(const std::vector<Slot> &slots) {
  std::map<Ym, std::uint64_t> out;
  for (const Slot &sl : slots)
    out[sl.ym()] += static_cast<std::uint64_t>(sl.sh->mtime.time_since_epoch().count());
  return out;
}

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

bool valid_group(const std::string &g) {
  const std::string_view v = g;
  if (misc::parse_ym(v.substr(0, 7)) == 0)
    return false;
  return g.size() == 7 || (g.size() == 10 && misc::parse_ymd(v) > 0);
}

void save(const std::string &name, const Book::Sheet &t) {
  struct MonthSum {
    std::set<std::string> bad;
    std::int64_t added = 0, removed = 0;
  };
  std::map<std::string, std::pair<std::int64_t, std::int64_t>> group_n;
  for (const Book::Row &r : t.rows)
    group_n[r.group] = {r.local_n, r.remote_n};
  std::map<std::string, MonthSum> sum;
  for (const auto &[g, n] : group_n) {
    MonthSum &s = sum[g.substr(0, 7)];
    s.bad.insert(g.substr(0, 10));
    s.added += std::max<std::int64_t>(0, n.second - n.first);
    s.removed += std::max<std::int64_t>(0, n.first - n.second);
  }

  std::ostringstream out;
  out << "# " << name << "\n"
      << "# month\tym\taudited_at\tver\tlocal_rows\tlocal_days\tcols\tassets\tbad_days\tadded\tremoved\n"
      << "# diff\tgroup\tlocal_n\tremote_n\tcolumn\tcell\tlocal\tremote\n";
  for (const auto &[ym, m] : t.months) {
    const auto s = sum.find(ym);
    const bool bad = s != sum.end();
    out << "month\t" << ym << '\t' << m.at << '\t' << m.ver << '\t' << m.inv.rows << '\t' << m.inv.days << '\t'
        << m.inv.cols << '\t' << m.inv.assets << '\t' << (bad ? s->second.bad.size() : 0) << '\t'
        << (bad ? s->second.added : 0) << '\t' << (bad ? s->second.removed : 0) << '\n';
  }
  std::vector<const Book::Row *> rows;
  rows.reserve(t.rows.size());
  for (const Book::Row &r : t.rows)
    rows.push_back(&r);
  std::stable_sort(rows.begin(), rows.end(),
                   [](const Book::Row *x, const Book::Row *y) { return x->group < y->group; });
  for (const Book::Row *r : rows)
    out << "diff\t" << r->group << '\t' << r->local_n << '\t' << r->remote_n << '\t' << r->column << '\t'
        << r->cell << '\t' << r->local << '\t' << r->remote << '\n';
  disk::write(disk::FundAuditSheet, {.name = name}, out.str());
}

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
      if (f.size() != 11 || misc::parse_ym(f[1]) == 0 || f[2].empty())
        return std::nullopt;
      const auto ver = to_num<std::uint64_t>(f[3]);
      const auto rows = to_num<std::int64_t>(f[4]), days = to_num<std::int64_t>(f[5]), cols = to_num<std::int64_t>(f[6]),
                 assets = to_num<std::int64_t>(f[7]);
      if (!ver || !rows || !days || !cols || !assets)
        return std::nullopt;
      t.months[f[1]] = {{*rows, *days, *cols, *assets}, f[2], *ver};
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

std::vector<Unit> plan(const Spec &s, const Round &r, const Keep &keep) {
  assert(audited(s) && "audit::plan: 表不参与核对");
  std::vector<Unit> out;
  for (const Slot &sl : frozen(s, r)) {
    if (!keep(sl))
      continue;
    const int year = sl.ym() / 100;
    if (out.empty() || out.back().year != year)
      out.push_back({&s, year, {}, {sl.w.lo, 0}});
    out.back().slots.push_back(sl);
    out.back().w.hi = sl.w.hi;
  }
  return out;
}

Result run(Clients &c, const Unit &u) {
  assert(u.spec && audited(*u.spec) && !u.slots.empty());
  const Spec &s = *u.spec;
  const Layout L = layout_of(c, u);
  const auto all = std::make_shared<const Cells>(L.all);
  const std::vector<i128> zeros(all->size(), 0);

  // 月级扫描: 远端整单元一次查询与本地各月并行读盘重叠
  std::future<Fps> rm_f = std::async(std::launch::async, [&] { return Remote{c.bq, s, L, u.w}.fps(*all, Grain::Month); });

  struct MonthSlots {
    Ym ym;
    Window w;
    std::vector<Slot> slots;
  };
  std::vector<MonthSlots> months;
  for (auto b = u.slots.begin(); b != u.slots.end();) {
    const Ym ym = b->ym();
    const auto e = std::find_if(b, u.slots.end(), [&](const Slot &x) { return x.ym() != ym; });
    const Window mw{std::max(misc::first_day(ym), u.w.lo), std::min(misc::last_day(ym), u.w.hi)};
    months.push_back({ym, mw, std::vector<Slot>(b, e)});
    b = e;
  }
  std::vector<std::optional<Local>> locals(months.size());
  misc::exec::ForkJoin ex(misc::exec::fit(months.size()));
  ex.tasks(static_cast<int>(months.size()), [&](int k, int) {
    MonthSlots &m = months[static_cast<std::size_t>(k)];
    locals[static_cast<std::size_t>(k)].emplace(s, L, std::move(m.slots));
  });
  const Fps rm = rm_f.get();

  Result out;
  for (std::size_t k = 0; k < months.size(); ++k) {
    const Ym ym = months[k].ym;
    const Window w = months[k].w;
    const Local &local = *locals[k];
    out.inv[ym] = local.inv();
    const std::string key = group_key(Grain::Month, w.lo);
    const Fps lm = local.fps(*all, Grain::Month);
    const auto &l = find_or_zero(lm, key, zeros);
    const auto &r = find_or_zero(rm, key, zeros);
    if (l == r)
      continue;
    for (const Probe &p : locate(Remote{c.bq, s, L, w}, local, {key, all, l, r}))
      out.diffs.push_back(make_diff(L, p));
  }
  return out;
}

std::string label(const Unit &u) { return u.spec->name + " " + std::to_string(u.year); }

std::vector<Task> repair(const Spec &s, const Round &r, const std::vector<std::string> &groups) {
  const std::set<std::string> keys(groups.begin(), groups.end());
  const Grain sg = slot_grain(s);
  std::set<std::string> hit;
  std::vector<Task> out;
  for (const Slot &sl : frozen(s, r)) {
    const std::string k = group_key(sg, sl.w.lo);
    bool match = false;
    for (const std::string &g : keys)
      if (g.starts_with(k) || k.starts_with(g)) {
        hit.insert(g);
        match = true;
      }
    if (match)
      out.push_back({&s, sl.at, sl.w, Write::Replace});
  }
  assert(hit.size() == keys.size() && "repair: 差异键不全落在冻结槽里");
  return out;
}

fs::path Book::dir() { return disk::ensure(disk::FundAudit); }

Book::Book() {
  for (const disk::Key &k : disk::list(disk::FundAuditSheet)) {
    const fs::path p = disk::path(disk::FundAuditSheet, k);
    std::optional<Sheet> t = parse(p);
    if (!t) {
      std::cout << "[bigquant.audit] 状态文件格式不符, 已删除: " << p.string() << std::endl;
      disk::remove(disk::FundAuditSheet, k);
      continue;
    }
    sheets_[k.name] = std::move(*t);
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
