#include "features/Panel/PoolSelect.hpp"

#include "features/Panel/Fund.hpp" // fund::Board

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <limits>
#include <mutex>
#include <string_view>

namespace pool {

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// all.bar L_1day 里 code 的列下标 (选池原料一律原值落盘)
size_t col_of(std::string_view code) {
  const LevelInfo &L = level_info(TierId::Bar, L_1day);
  for (size_t i = 0; i < L.field_count; ++i)
    if (code == L.fields[i].code) {
      assert(L.fields[i].ts_tf == ts::TfId::None && "all.bar: 选池原料列须原值落盘");
      return i;
    }
  assert(false && "all.bar: 字段表里没有这一列");
  return 0;
}

// 原料带的列 (L_1day 行 D: 资产态 = D+1 盘前, 收盘 / 股本 = D 收盘)
enum TapeCol : size_t { kTs,
                        kClose,
                        kPre,
                        kShares,
                        kBoardCol,
                        kListAge,
                        kDelistAge,
                        kStLevel,
                        kMarginCol,
                        kFlt0, // 过滤表 kFilterDefs 序
                        kTapeCols = kFlt0 + kFilters };
constexpr std::array<const char *, kTapeCols> kTapeCodes = [] {
  std::array<const char *, kTapeCols> c = {"ts_valid", "close_day", "pre_close", "total_shares", "board", "list_age", "delist_age", "st_level", "is_margin"};
  for (int k = 0; k < kFilters; ++k)
    c[kFlt0 + k] = kFilterDefs[k].code;
  return c;
}();
static_assert(int{kBoardShMain} == fund::kBoardShMain && int{kBoardSzMain} == fund::kBoardSzMain && int{kBoardStar} == fund::kBoardStar &&
              int{kBoardChiNext} == fund::kBoardChiNext && int{kBoardBj} == fund::kBoardBj && kBoards == fund::kBoardBj + 1);

// 风格键 = f(列 0, 列 1, 收盘总市值); 列空 = 不读库 (只用原料带)
struct KeyDef {
  const char *col[2];
  float (*f)(float x, float y, float mc);
};
constexpr float raw(float x, float, float) { return x; }
constexpr float per_cap(float x, float, float mc) { return mc > 0.0f ? x / mc : kNaN; }
constexpr KeyDef kKeyDefs[kStyles] = {
    {{}, [](float, float, float mc) { return mc > 0.0f ? mc : kNaN; }},
    {{"pl_npp_ttm"}, per_cap},
    {{"bs_eq"}, per_cap},
    {{"dy_ttm"}, raw},
    {{"pl_npp_q", "bs_eq"}, [](float x, float y, float) { return y > 0.0f ? x / y : kNaN; }},
    {{"ta_yoy"}, raw},
    {{"ret_12_1"}, raw},
    {{"ret_1m"}, raw},
    {{"turn"}, raw},
    {{"ivol"}, raw},
    {{"resvol"}, raw},
    {{"beta"}, raw},
    {{"trend"}, raw},
    {{"close_day"}, raw},
};

// 类号 (st_level 0..3, 板块 1..5, 非两融 0 / 1, 过滤位)
uint16_t class_of(int lvl, int bd, int off, unsigned flt) {
  return static_cast<uint16_t>((((lvl * (kBoards - 1) + bd - 1) * (kMargins - 1) + off) << kFilters) | flt);
}

// 周序号 (周一起; 1970-01-01 = 周四)
int64_t week_of(misc::Ymd d) { return (misc::to_days(d).time_since_epoch().count() + 3) / 7; }

// 一日的读 (列序 kTapeCodes 起头) 里资产 a 的收盘总市值 (缺 → 0) 与类号 (不在市 = kOut)
struct AssetCell {
  float mc;
  uint16_t cls;
};
AssetCell asset_cell(const FeatureRead::DayColumns &c, size_t a) {
  const auto v = [&](TapeCol k) { return static_cast<float>(c.get(0, k, a)); };
  const float px = v(kClose), sh = v(kShares), mc = px > 0.0f && sh > 0.0f ? px * sh : 0.0f, bd = v(kBoardCol), lvl = v(kStLevel);
  if (!(mc > 0.0f && v(kListAge) >= 0.0f && !(v(kDelistAge) >= 0.0f) && bd >= 1.0f && bd < static_cast<float>(kBoards)))
    return {mc, kOut};
  assert(lvl >= 0.0f && lvl < static_cast<float>(kSts - 1) && "all.bar: 在市资产的 st_level 须是 0..3");
  unsigned flt = 0;
  for (int k = 0; k < kFilters; ++k)
    flt |= (v(static_cast<TapeCol>(kFlt0 + k)) > 0.5f ? 1u : 0u) << k;
  return {mc, class_of(static_cast<int>(lvl), static_cast<int>(bd), v(kMarginCol) > 0.5f ? 0 : 1, flt)};
}

// 一行的键 k [A] → 有限键资产按 (键, 下标) 升序写进 o (返回个数) + 各类的有限键数 fin [kClasses]
size_t rank_row(const float *k, const uint16_t *cl, size_t A, uint32_t *o, uint16_t *fin) {
  std::fill(fin, fin + kClasses, uint16_t{0});
  size_t n = 0;
  for (size_t a = 0; a < A; ++a)
    if (std::isfinite(k[a])) {
      o[n++] = static_cast<uint32_t>(a);
      if (cl[a] != kOut)
        ++fin[cl[a]];
    }
  std::sort(o, o + n, [&](uint32_t a, uint32_t b) { return k[a] < k[b] || (k[a] == k[b] && a < b); });
  return n;
}

} // namespace

bool covers(const Spec &s, size_t c) {
  const int flt = static_cast<int>(c & ((1u << kFilters) - 1)), g = static_cast<int>(c >> kFilters);
  const int off = g % (kMargins - 1), bd = g / (kMargins - 1) % (kBoards - 1) + 1, lvl = g / (kMargins - 1) / (kBoards - 1);
  return (s.st == kStAll || s.st == lvl + 1) && (s.board == kBoardAll || s.board == bd) && (s.margin == kMarginAll || s.margin == off + 1) &&
         !(flt & s.filter);
}

bool load_tape(const FeatureLib &lib, const FeatureRead &rd, std::span<const misc::Ymd> dates, misc::exec::ForkJoin &ex, const Cancel &cancel,
               const Tick &tick, Tape &t) {
  const size_t D = dates.size(), A = lib.axis.size();
  assert(D >= 2 && A > 0 && A <= UINT16_MAX);
  t = Tape{};
  t.dates.assign(dates.begin(), dates.end());
  t.A = A, t.axis = lib.axis.hash;
  t.cls.assign(D * A, kOut), t.cnt.assign(D * kClasses, 0), t.ret.assign(D * A, 0.0f), t.mcap.assign(D * A, 0.0f);
  std::vector<float> pre(D * A), close(D * A); // 行 d 的前收 (= d+1 的) / 收盘 (d 无成交 → 0)
  std::vector<size_t> cols;
  for (const char *c : kTapeCodes)
    cols.push_back(col_of(c));
  std::vector<FeatureRead::DayColumns> dc(static_cast<size_t>(ex.threads()));
  for (FeatureRead::DayColumns &c : dc)
    c.preallocate(A, L_1day, kTapeCols);
  ex.tasks(static_cast<int>(D), [&](int i, int tid) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    FeatureRead::DayColumns &c = dc[static_cast<size_t>(tid)];
    rd.load_day_columns(dates[static_cast<size_t>(i)], cols, c);
    for (size_t a = 0; a < A; ++a) {
      const auto v = [&](TapeCol k) { return static_cast<float>(c.get(0, k, a)); };
      const size_t row = static_cast<size_t>(i) * A + a;
      const AssetCell x = asset_cell(c, a);
      t.mcap[row] = x.mc;
      pre[row] = v(kPre) > 0.0f ? v(kPre) : 0.0f;
      close[row] = v(kTs) > 0.5f && v(kClose) > 0.0f ? v(kClose) : 0.0f;
      t.cls[row] = x.cls;
      if (x.cls != kOut)
        ++t.cnt[static_cast<size_t>(i) * kClasses + x.cls];
    }
    tick();
  });
  if (cancel.load(std::memory_order_relaxed) || rd.stale())
    return false;
  for (size_t d = 1; d < D; ++d)
    for (size_t a = 0; a < A; ++a) {
      const float p = pre[(d - 1) * A + a], c = close[d * A + a];
      t.ret[d * A + a] = p > 0.0f && c > 0.0f ? c / p - 1.0f : 0.0f;
    }
  t.rebal[kRebalDay].assign(D, 1);
  t.rebal[kRebalWeek].resize(D);
  for (size_t r = 0; r < D; ++r)
    t.rebal[kRebalWeek][r] = r + 1 < D ? week_of(dates[r + 1]) != week_of(dates[r]) : misc::weekday_of(dates[r]) == 4;
  return true;
}

bool key_reads(Style st) { return kKeyDefs[st].col[0] != nullptr; }

bool load_key(const FeatureRead &rd, const Tape &t, Style st, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, std::vector<float> &key) {
  const KeyDef &k = kKeyDefs[st];
  const size_t D = t.days(), A = t.A;
  key.assign(D * A, kNaN);
  if (!k.col[0]) {
    for (size_t i = 0; i < D * A; ++i)
      key[i] = k.f(0.0f, 0.0f, t.mcap[i]);
    return true;
  }
  std::vector<size_t> cols = {col_of(k.col[0])};
  if (k.col[1])
    cols.push_back(col_of(k.col[1]));
  std::vector<FeatureRead::DayColumns> dc(static_cast<size_t>(ex.threads()));
  for (FeatureRead::DayColumns &c : dc)
    c.preallocate(A, L_1day, cols.size());
  ex.tasks(static_cast<int>(D), [&](int i, int tid) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    FeatureRead::DayColumns &c = dc[static_cast<size_t>(tid)];
    rd.load_day_columns(t.dates[static_cast<size_t>(i)], cols, c);
    for (size_t a = 0; a < A; ++a) {
      const size_t row = static_cast<size_t>(i) * A + a;
      key[row] = k.f(static_cast<float>(c.get(0, 0, a)), cols.size() > 1 ? static_cast<float>(c.get(0, 1, a)) : 0.0f, t.mcap[row]);
    }
    tick();
  });
  return !cancel.load(std::memory_order_relaxed) && !rd.stale();
}

bool rank_key(const Tape &t, const std::vector<float> &key, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, Ranks &rk) {
  const size_t D = t.days(), A = t.A;
  assert(key.size() == D * A);
  rk.off.assign(D + 1, 0);
  for (size_t r = 0; r < D; ++r) {
    const float *k = key.data() + r * A;
    size_t n = 0;
    for (size_t a = 0; a < A; ++a)
      n += std::isfinite(k[a]);
    rk.off[r + 1] = rk.off[r] + n;
  }
  rk.ord.resize(rk.off[D]);
  rk.fin.assign(D * kClasses, 0);
  ex.tasks(static_cast<int>(D), [&](int i, int) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    const size_t r = static_cast<size_t>(i);
    [[maybe_unused]] const size_t n = rank_row(key.data() + r * A, t.cls.data() + r * A, A, rk.ord.data() + rk.off[r], rk.fin.data() + r * kClasses);
    assert(n == rk.off[r + 1] - rk.off[r]);
    tick();
  });
  return !cancel.load(std::memory_order_relaxed);
}

RowView row_view(const Tape &t, const Ranks *rk, size_t r) {
  RowView v{t.A, t.cls.data() + r * t.A, t.cnt.data() + r * kClasses};
  if (rk)
    v.o = rk->ord.data() + rk->off[r], v.e = rk->ord.data() + rk->off[r + 1], v.fin = rk->fin.data() + r * kClasses;
  return v;
}

Selector::Selector(const Spec &s) : s_(s), in_(kClasses + 1, 0) {
  for (size_t c = 0; c < kClasses; ++c)
    if (covers(s, c))
      in_[c] = 1, cls_.push_back(static_cast<uint16_t>(c));
}

void Selector::at(const RowView &r, std::vector<uint32_t> &out) const {
  out.clear();
  const auto sum = [&](const uint16_t *p) {
    size_t n = 0;
    for (const uint16_t c : cls_)
      n += p[c];
    return n;
  };
  const size_t A = r.A, n_in = sum(r.cnt);
  if (n_in == 0)
    return;
  const auto in = [&](uint32_t a) { return in_[r.cls[a]] != 0; };
  assert((s_.n == 0 || r.fin) && "N > 0 须有键次序");
  const size_t n_fin = s_.n == 0 ? 0 : sum(r.fin);
  if (s_.n == 0 || static_cast<float>(n_fin) < kCover * static_cast<float>(n_in)) {
    for (size_t a = 0; a < A; ++a)
      if (in(static_cast<uint32_t>(a)))
        out.push_back(static_cast<uint32_t>(a));
    return;
  }
  const size_t want = std::min<size_t>(s_.n, n_fin);
  const uint32_t *o = r.o, *e = r.e;
  if (s_.end == kEndLow) {
    for (; o != e && out.size() < want; ++o)
      if (in(*o))
        out.push_back(*o);
  } else {
    while (e != o && out.size() < want)
      if (in(*--e))
        out.push_back(*e);
  }
  std::ranges::sort(out);
}

void run_book(const Tape &t, const Ranks *rk, const Spec &s, Book &b) {
  const size_t D = t.days(), A = t.A;
  assert((s.n == 0 || rk) && "N > 0 须有键次序");
  for (int w = 0; w < kWeights; ++w)
    b.ret[w].assign(D, 0.0f), b.buy[w] = 0.0;
  b.first = D;
  const Selector sel(s);
  const std::vector<uint8_t> &reb = t.rebal[s.rebal];
  std::vector<uint32_t> mem, nx; // 当前 / 新选的成员 (资产下标升序)
  // [Weight][成员] 持仓额 (调仓行 = 目标权, 其后各乘 1 + r; 漂移后的权 = 额 / 总额) / 新目标权; [Weight] 总额
  std::vector<double> v[kWeights], tw[kWeights];
  double sum[kWeights] = {};
  for (size_t d = 1; d < D; ++d) {
    if (reb[d - 1]) {
      sel.at(row_view(t, rk, d - 1), nx);
      const size_t n = nx.size();
      const float *mc = t.mcap.data() + (d - 1) * A;
      double cap = 0.0;
      for (const uint32_t a : nx)
        cap += mc[a];
      tw[kWeightEq].assign(n, n ? 1.0 / static_cast<double>(n) : 0.0);
      tw[kWeightCap].resize(n);
      for (size_t i = 0; i < n; ++i)
        tw[kWeightCap][i] = mc[nx[i]] / cap;
      for (size_t i = 0, j = 0; i < n; ++i) {
        while (j < mem.size() && mem[j] < nx[i])
          ++j;
        const bool held = j < mem.size() && mem[j] == nx[i];
        for (int w = 0; w < kWeights; ++w)
          b.buy[w] += std::max(tw[w][i] - (held ? v[w][j] / sum[w] : 0.0), 0.0);
      }
      for (int w = 0; w < kWeights; ++w)
        v[w].swap(tw[w]), sum[w] = 1.0;
      mem.swap(nx);
      if (b.first == D && !mem.empty())
        b.first = d;
    }
    if (mem.empty())
      continue;
    const float *rd = t.ret.data() + d * A;
    double *ve = v[kWeightEq].data(), *vc = v[kWeightCap].data(), se = 0.0, sc = 0.0;
    for (size_t i = 0; i < mem.size(); ++i) {
      const double g = 1.0 + rd[mem[i]];
      se += ve[i] *= g, sc += vc[i] *= g;
    }
    b.ret[kWeightEq][d] = static_cast<float>(se / sum[kWeightEq] - 1.0);
    b.ret[kWeightCap][d] = static_cast<float>(sc / sum[kWeightCap] - 1.0);
    sum[kWeightEq] = se, sum[kWeightCap] = sc;
  }
}

std::span<const uint16_t> Picks::of(PoolId p, misc::Ymd d) const {
  assert(p != kAllPool && p < pool_count() && off.size() + 1 == pool_count() && "选股表: 不是子池");
  const auto it = pool_def(p).spec.rebal == kRebalWeek
                      ? std::partition_point(rows.begin(), rows.end(), [w = week_of(d)](misc::Ymd r) { return week_of(r) < w; })
                      : std::lower_bound(rows.begin(), rows.end(), d);
  if (it == rows.begin())
    return {};
  const size_t r = static_cast<size_t>(it - rows.begin()) - 1;
  const std::vector<uint32_t> &o = off[p - 1];
  return {ids[p - 1].data() + o[r], o[r + 1] - o[r]};
}

std::shared_ptr<const Picks> picks(const FeatureLib &lib) {
  assert(lib.id == kAllBar && lib.ready() && "选股表的源 = 已建的 all.bar 库");
  static std::mutex mu;
  static std::shared_ptr<const Picks> cur;
  const std::lock_guard<std::mutex> lock(mu);
  const FeatureRead rd(lib);
  std::vector<misc::Ymd> rows;
  for (const misc::Ymd d : lib.dates)
    if (rd.has_date(d))
      rows.push_back(d);
  const bool keep = cur && cur->axis == lib.axis.hash && cur->rows.size() <= rows.size() && std::equal(cur->rows.begin(), cur->rows.end(), rows.begin());
  if (keep && cur->rows.size() == rows.size())
    return cur;
  const size_t lo = keep ? cur->rows.size() : 0, R = rows.size(), A = lib.axis.size(), NP = pool_count();
  assert(A <= UINT16_MAX);
  auto pk = keep ? std::make_shared<Picks>(*cur) : std::make_shared<Picks>(Picks{lib.axis.hash, {}, std::vector<std::vector<uint32_t>>(NP - 1, {0}), std::vector<std::vector<uint16_t>>(NP - 1)});
  pk->rows = std::move(rows);

  // 读的列 = 原料带 + 子池表用到的风格的键列; 子池按风格分组 (同风格共用一次排序), 全选池不排
  std::vector<size_t> cols;
  for (const char *c : kTapeCodes)
    cols.push_back(col_of(c));
  const auto slot = [&](const char *code) {
    const size_t c = col_of(code), i = static_cast<size_t>(std::ranges::find(cols, c) - cols.begin());
    if (i == cols.size())
      cols.push_back(c);
    return i;
  };
  std::vector<PoolId> whole, by_style[kStyles];
  size_t key_at[kStyles][2] = {};
  std::vector<Selector> sel;
  for (PoolId p = 1; p < NP; ++p) {
    const Spec &s = pool_def(p).spec;
    sel.emplace_back(s);
    (s.n ? by_style[s.style] : whole).push_back(p);
  }
  for (int st = 0; st < kStyles; ++st)
    for (int j = 0; j < 2; ++j)
      if (!by_style[st].empty() && kKeyDefs[st].col[j])
        key_at[st][j] = slot(kKeyDefs[st].col[j]);

  struct Lane {
    FeatureRead::DayColumns dc;
    std::vector<uint16_t> cls, cnt, fin;
    std::vector<float> mc, key;
    std::vector<uint32_t> ord, out;
  };
  misc::exec::ForkJoin ex(misc::exec::fit(R - lo));
  std::vector<Lane> lanes(static_cast<size_t>(ex.threads()));
  for (Lane &l : lanes) {
    l.dc.preallocate(A, L_1day, cols.size());
    l.cls.resize(A), l.cnt.resize(kClasses), l.fin.resize(kClasses), l.mc.resize(A), l.key.resize(A), l.ord.resize(A);
  }
  std::vector<std::vector<std::vector<uint16_t>>> got(R - lo, std::vector<std::vector<uint16_t>>(NP - 1)); // [新行][p − 1]
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    Lane &l = lanes[static_cast<size_t>(tid)];
    rd.load_day_columns(pk->rows[lo + static_cast<size_t>(i)], cols, l.dc);
    std::ranges::fill(l.cnt, uint16_t{0});
    for (size_t a = 0; a < A; ++a) {
      const AssetCell x = asset_cell(l.dc, a);
      l.mc[a] = x.mc, l.cls[a] = x.cls;
      if (x.cls != kOut)
        ++l.cnt[x.cls];
    }
    RowView v{A, l.cls.data(), l.cnt.data()};
    const auto take = [&](PoolId p) {
      sel[p - 1u].at(v, l.out);
      got[static_cast<size_t>(i)][p - 1u].assign(l.out.begin(), l.out.end());
    };
    for (const PoolId p : whole)
      take(p);
    for (int st = 0; st < kStyles; ++st) {
      if (by_style[st].empty())
        continue;
      const KeyDef &k = kKeyDefs[st];
      for (size_t a = 0; a < A; ++a)
        l.key[a] = k.f(k.col[0] ? static_cast<float>(l.dc.get(0, key_at[st][0], a)) : 0.0f, k.col[1] ? static_cast<float>(l.dc.get(0, key_at[st][1], a)) : 0.0f, l.mc[a]);
      v.o = l.ord.data(), v.e = v.o + rank_row(l.key.data(), l.cls.data(), A, l.ord.data(), l.fin.data()), v.fin = l.fin.data();
      for (const PoolId p : by_style[st])
        take(p);
    }
  });
  if (rd.stale())
    return nullptr;
  for (size_t p = 0; p + 1 < NP; ++p)
    for (size_t i = 0; i < R - lo; ++i) {
      pk->ids[p].insert(pk->ids[p].end(), got[i][p].begin(), got[i][p].end());
      pk->off[p].push_back(static_cast<uint32_t>(pk->ids[p].size()));
    }
  cur = std::move(pk);
  return cur;
}

} // namespace pool
