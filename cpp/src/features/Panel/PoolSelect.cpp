#include "features/Panel/PoolSelect.hpp"

#include "features/Panel/Fund.hpp" // fund::Board
#include "features/Storage/FeatureRead.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <mutex>

namespace pool {

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
constexpr size_t kSparse = 16; // 切片全选: 切片内数 × kSparse < A → 按类段收拢再排 (否则沿 A 轴扫)

// 日行各列: 代码 / 落盘变换
struct ColDef {
  const char *code;
  ts::TfId tf;
};
constexpr ColDef kDayColDefs[kDayCols] = {
    {"ts_valid", ts::TfId::None},
    {"open_day", ts::TfId::None},
    {"high_day", ts::TfId::None},
    {"low_day", ts::TfId::None},
    {"close_day", ts::TfId::None},
    {"amount_day", ts::TfId::Log},
    {"pre_close", ts::TfId::None},
    {"cs_valid", ts::TfId::None},
    {"total_shares", ts::TfId::None},
    {"ind_l1", ts::TfId::None},
    {"list_age", ts::TfId::None},
    {"delist_age", ts::TfId::None},
    {"flt_susp", ts::TfId::None},
    {"st_level", ts::TfId::None},
    {"board", ts::TfId::None},
    {"is_margin", ts::TfId::None},
    {"flt_new", ts::TfId::None},
    {"flt_warn", ts::TfId::None},
};
static_assert(kFltNew == 0 && kFltWarn == 1 && kFilters == 2, "过滤项 = kDayColDefs 末两列, 同序");

// 划分维的类: 列值 → 类 (−1 = 不在市)
constexpr int flag(float x) { return x > 0.5f ? 1 : 0; }
constexpr int st_cls(float x) { return x >= 0.0f && x < static_cast<float>(kSts) ? static_cast<int>(x) : -1; }               // st_level 0..3
constexpr int board_cls(float x) { return x >= 1.0f && x < static_cast<float>(kBoards + 1) ? static_cast<int>(x) - 1 : -1; } // board 1..5
static_assert(kBoardShMain + 1 == fund::kBoardShMain && kBoardSzMain + 1 == fund::kBoardSzMain && kBoardStar + 1 == fund::kBoardStar &&
              kBoardChiNext + 1 == fund::kBoardChiNext && kBoardBj + 1 == fund::kBoardBj && kBoards == fund::kBoardBj);
static_assert(kDimSt + 1 == kDimBoard && kDimBoard + 1 == kDimMargin && kDimMargin + 1 == kDimFilter && kDimFilter + 1 == kDims, "资产类的进制按此维序");

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

// 一组风格的键列 (同列只读一次)
struct KeyCols {
  std::vector<size_t> cols;   // 库列下标 (空 = 都只用原料带)
  size_t at[kStyles][2] = {}; // [风格][键列] 在 cols 里的位置
  void add(Style st) {
    for (int j = 0; j < 2; ++j)
      if (const char *c = kKeyDefs[st].col[j]) {
        const size_t k = bar_col(L_1day, c);
        at[st][j] = static_cast<size_t>(std::ranges::find(cols, k) - cols.begin());
        if (at[st][j] == cols.size())
          cols.push_back(k);
      }
  }
  // 风格 st 一行的键 [A] ← dc (cols 那天的读) + 该行收盘总市值 mc
  void row(Style st, const FeatureRead::DayColumns &dc, const float *mc, size_t A, float *key) const {
    const KeyDef &k = kKeyDefs[st];
    for (size_t a = 0; a < A; ++a)
      key[a] = k.f(k.col[0] ? static_cast<float>(dc.get(0, at[st][0], a)) : 0.0f, k.col[1] ? static_cast<float>(dc.get(0, at[st][1], a)) : 0.0f, mc[a]);
  }
};

// 按类分段的表: 段长 n [kClasses] → 各类的段首 at [kClasses]
void seg_starts(const uint16_t *n, size_t *at) {
  for (size_t c = 0, s = 0; c < kClasses; s += n[c++])
    at[c] = s;
}

// 一日的读 (列序 = day_cols) → 原料带行 r; pre / close [A] = 该行前收 / 收盘 (无成交 → 0)
void fill_row(const FeatureRead::DayColumns &c, Tape &t, size_t r, float *pre, float *close) {
  const size_t A = t.A;
  uint16_t *cnt = t.cnt.data() + r * kClasses;
  for (size_t a = 0; a < A; ++a) {
    const auto v = [&](size_t k) { return static_cast<float>(c.get(0, k, a)); };
    const size_t i = r * A + a;
    const float px = v(kColClose), sh = v(kColShares), mc = px > 0.0f && sh > 0.0f ? px * sh : 0.0f, ind = v(kColInd);
    t.mcap[i] = mc;
    t.live[i] = v(kColCs) > 0.5f && px > 0.0f;
    t.ind[i] = ind >= 1.0f && ind < static_cast<float>(kAllCuts) ? static_cast<uint8_t>(ind + 0.5f) : 0;
    pre[a] = v(kColPre) > 0.0f ? v(kColPre) : 0.0f;
    close[a] = v(kColTs) > 0.5f && px > 0.0f ? px : 0.0f;
    t.cls[i] = kOut;
    if (!(mc > 0.0f && v(kColListAge) >= 0.0f && !(v(kColDelistAge) >= 0.0f) && !flag(v(kColSusp)) && t.ind[i]))
      continue;
    const int st = st_cls(v(kColSt)), bd = board_cls(v(kColBoard));
    if (st < 0 || bd < 0)
      continue;
    int flt = 0;
    for (int f = 0; f < kFilters; ++f)
      flt |= flag(v(kColFilter0 + static_cast<size_t>(f))) << f;
    const size_t id = ((static_cast<size_t>(st) * set_radix(1) + static_cast<size_t>(bd)) * set_radix(2) + static_cast<size_t>(flag(v(kColMargin)))) * set_radix(3) + static_cast<size_t>(flt);
    t.cls[i] = static_cast<uint16_t>(id), ++cnt[id];
  }
  size_t at[kClasses];
  seg_starts(cnt, at);
  const uint16_t *cl = t.cls.data() + r * A;
  uint16_t *byc = t.byc.data() + r * A;
  for (size_t a = 0; a < A; ++a)
    if (cl[a] != kOut)
      byc[at[cl[a]]++] = static_cast<uint16_t>(a);
}

// 一行的键 k [A] → 有限键资产按 (键, 下标) 升序写进 o (返回个数) + 各类的有限键数 fin [kClasses] + 在市的在 o 里的位置按类分段 rbc
size_t rank_row(const float *k, const uint16_t *cl, size_t A, uint32_t *o, uint16_t *fin, uint16_t *rbc) {
  std::fill(fin, fin + kClasses, uint16_t{0});
  size_t n = 0;
  for (size_t a = 0; a < A; ++a)
    if (std::isfinite(k[a])) {
      o[n++] = static_cast<uint32_t>(a);
      if (cl[a] != kOut)
        ++fin[cl[a]];
    }
  std::sort(o, o + n, [&](uint32_t a, uint32_t b) { return k[a] < k[b] || (k[a] == k[b] && a < b); });
  size_t at[kClasses];
  seg_starts(fin, at);
  for (size_t i = 0; i < n; ++i)
    if (const uint16_t c = cl[o[i]]; c != kOut)
      rbc[at[c]++] = static_cast<uint16_t>(i);
  return n;
}

bool cancelled(const Cancel *c) { return c && c->load(std::memory_order_relaxed); }

} // namespace

size_t bar_col(size_t level, std::string_view code, ts::TfId tf) {
  const LevelInfo &L = level_info(TierId::Bar, level);
  for (size_t i = 0; i < L.field_count; ++i)
    if (code == L.fields[i].code) {
      assert(L.fields[i].ts_tf == tf && "all.bar: 列的落盘变换与读法不符");
      return i;
    }
  assert(false && "all.bar: 字段表里没有这一列");
  return 0;
}

std::vector<size_t> day_cols(size_t n) {
  assert(n <= kDayCols);
  std::vector<size_t> c;
  for (size_t k = 0; k < n; ++k)
    c.push_back(bar_col(L_1day, kDayColDefs[k].code, kDayColDefs[k].tf));
  return c;
}

int64_t week_of(misc::Ymd d) { return (misc::to_days(d).time_since_epoch().count() + 3) / 7; } // 1970-01-01 = 周四

size_t src_of(Rebal rb, std::span<const misc::Ymd> rows, misc::Ymd d) {
  const auto it = rb == kRebalWeek ? std::partition_point(rows.begin(), rows.end(), [w = week_of(d)](misc::Ymd r) { return week_of(r) < w; })
                                   : std::lower_bound(rows.begin(), rows.end(), d);
  return it == rows.begin() ? SIZE_MAX : static_cast<size_t>(it - rows.begin()) - 1;
}

bool rebal_at(Rebal rb, misc::Ymd prev, misc::Ymd d) { return rb == kRebalDay || week_of(prev) != week_of(d); }

std::vector<misc::Ymd> landed(const FeatureLib &lib) {
  std::vector<misc::Ymd> v;
  for (const misc::Ymd d : lib.dates)
    if (FeatureRead::has_date(lib.id, d))
      v.push_back(d);
  return v;
}

bool extends(std::span<const misc::Ymd> done, std::span<const misc::Ymd> now) {
  return !done.empty() && done.size() <= now.size() && std::equal(done.begin(), done.end(), now.begin());
}

AllBar all_bar(std::string_view what) {
  AllBar b{load_lib(kAllBar), {}, {}};
  if (!b.lib.ready()) {
    b.lack = std::string(what) + ": 先在 Beta → Compute 算好 all 池的 Bar 库.";
    return b;
  }
  b.dates = landed(b.lib);
  if (b.dates.size() < 2) // 首日只给成员 / 基准, 次日起才有收益
    b.lack = "all.bar 特征库落盘不足 2 日.";
  return b;
}

bool covers(const Spec &s, size_t c) {
  for (int k = kSets; k-- > 0;) {
    const size_t r = set_radix(k), x = c % r;
    c /= r;
    const uint8_t m = s.v[kDimSt + k];
    if (kDimDefs[kDimSt + k].kind == kKindFilter ? (m & x) != 0 : m && !(m >> x & 1))
      return false;
  }
  return true;
}

void Tape::omega(size_t r, size_t q, size_t a, float om[kWeights]) const {
  assert(r <= q && q < days());
  double g = 1.0;
  for (size_t k = r + 1; k <= q; ++k)
    g *= 1.0 + ret[k * A + a];
  om[kWeightEq] = static_cast<float>(g), om[kWeightCap] = static_cast<float>(mcap[r * A + a] * g);
}

std::shared_ptr<const Tape> tape(const FeatureLib &lib, const Cancel *cancel, const Tick &tick) {
  assert(lib.id == kAllBar && lib.ready() && "原料带的源 = 已建的 all.bar 库");
  static std::mutex mu;
  static std::shared_ptr<const Tape> cur;
  const std::lock_guard<std::mutex> lock(mu);
  std::vector<misc::Ymd> rows = landed(lib);
  const bool keep = cur && cur->axis == lib.axis.hash && extends(cur->dates, rows);
  if (keep && cur->days() == rows.size())
    return cur;
  const size_t A = lib.axis.size(), lo = keep ? cur->days() : 0, R = rows.size();
  assert(A > 0 && A <= UINT16_MAX && R > lo);
  auto t = keep ? std::make_shared<Tape>(*cur) : std::make_shared<Tape>();
  t->dates = std::move(rows), t->A = A, t->axis = lib.axis.hash;
  t->cls.resize(R * A), t->cnt.resize(R * kClasses), t->byc.resize(R * A), t->live.resize(R * A), t->ind.resize(R * A), t->mcap.resize(R * A), t->ret.resize(R * A);
  t->pre.resize(A);
  std::vector<float> pre((R - lo) * A), close((R - lo) * A); // [新行 · A]
  const FeatureRead rd(lib);
  const std::vector<size_t> cols = day_cols();
  misc::exec::ForkJoin ex(misc::exec::fit(R - lo));
  std::vector<FeatureRead::DayColumns> dc(static_cast<size_t>(ex.threads()));
  for (FeatureRead::DayColumns &c : dc)
    c.preallocate(A, L_1day, cols.size());
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    if (cancelled(cancel))
      return;
    FeatureRead::DayColumns &c = dc[static_cast<size_t>(tid)];
    const size_t k = static_cast<size_t>(i);
    rd.load_day_columns(t->dates[lo + k], cols, c);
    fill_row(c, *t, lo + k, pre.data() + k * A, close.data() + k * A);
    if (tick)
      tick();
  });
  if (cancelled(cancel) || rd.stale())
    return nullptr;
  for (size_t r = std::max<size_t>(lo, 1); r < R; ++r) {
    const float *p = r == lo ? t->pre.data() : pre.data() + (r - 1 - lo) * A, *c = close.data() + (r - lo) * A;
    for (size_t a = 0; a < A; ++a)
      t->ret[r * A + a] = p[a] > 0.0f && c[a] > 0.0f ? c[a] / p[a] - 1.0f : 0.0f;
  }
  t->pre.assign(pre.end() - static_cast<std::ptrdiff_t>(A), pre.end());
  cur = std::move(t);
  return cur;
}

void live_at(const Tape &t, size_t r, std::vector<uint32_t> &out) {
  out.clear();
  const uint8_t *l = t.live.data() + r * t.A;
  for (size_t a = 0; a < t.A; ++a)
    if (l[a])
      out.push_back(static_cast<uint32_t>(a));
}

bool rank_rows(const FeatureLib &lib, const Tape &t, Style st, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, Ranks &rk) {
  const size_t lo = rk.rows(), R = t.days(), A = t.A;
  assert(lo <= R);
  KeyCols kc;
  kc.add(st);
  const FeatureRead rd(lib);
  struct Lane {
    FeatureRead::DayColumns dc;
    std::vector<float> key;
  };
  std::vector<Lane> lanes(static_cast<size_t>(ex.threads()));
  for (Lane &l : lanes) {
    if (!kc.cols.empty())
      l.dc.preallocate(A, L_1day, kc.cols.size());
    l.key.resize(A);
  }
  std::vector<std::vector<uint32_t>> ord(R - lo); // [新行]
  std::vector<std::vector<uint16_t>> rbc(R - lo); // [新行]
  rk.fin.resize(R * kClasses);
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    Lane &l = lanes[static_cast<size_t>(tid)];
    const size_t r = lo + static_cast<size_t>(i);
    if (!kc.cols.empty())
      rd.load_day_columns(t.dates[r], kc.cols, l.dc);
    kc.row(st, l.dc, t.mcap.data() + r * A, A, l.key.data());
    std::vector<uint32_t> &o = ord[static_cast<size_t>(i)];
    std::vector<uint16_t> &b = rbc[static_cast<size_t>(i)];
    o.resize(A), b.resize(A);
    o.resize(rank_row(l.key.data(), t.cls.data() + r * A, A, o.data(), rk.fin.data() + r * kClasses, b.data()));
    b.resize(o.size());
    tick();
  });
  if (cancel.load(std::memory_order_relaxed) || rd.stale()) {
    rk.fin.resize(lo * kClasses);
    return false;
  }
  for (size_t i = 0; i < R - lo; ++i) {
    rk.ord.insert(rk.ord.end(), ord[i].begin(), ord[i].end());
    rk.rbc.insert(rk.rbc.end(), rbc[i].begin(), rbc[i].end());
    rk.off.push_back(rk.ord.size());
  }
  return true;
}

RowView row_view(const Tape &t, const Ranks *rk, size_t r) {
  RowView v{t.A, t.cls.data() + r * t.A, t.cnt.data() + r * kClasses, t.byc.data() + r * t.A};
  if (rk)
    v.o = rk->ord.data() + rk->off[r], v.e = rk->ord.data() + rk->off[r + 1], v.fin = rk->fin.data() + r * kClasses, v.rbc = rk->rbc.data() + rk->off[r];
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
  // 切片内各类在按类分段的表 (段长 n [kClasses]) 里的段 [b, e) 依次收进 out
  const auto gather = [&](const uint16_t *n, const uint16_t *seg) {
    for (size_t c = 0, b = 0; c < kClasses; b += n[c++])
      if (in_[c])
        out.insert(out.end(), seg + b, seg + b + n[c]);
  };
  const size_t A = r.A, n_in = sum(r.cnt);
  if (n_in == 0)
    return;
  const auto in = [&](uint32_t a) { return in_[r.cls[a]] != 0; };
  assert((!s_.keyed() || r.fin) && "风格池须有键次序");
  const size_t n_fin = s_.keyed() ? sum(r.fin) : 0;
  if (s_.keyed() && static_cast<float>(n_fin) < kCover * static_cast<float>(n_in))
    return;
  if (!s_.keyed()) {
    if (n_in * kSparse < A) {
      gather(r.cnt, r.byc);
      std::ranges::sort(out);
    } else
      for (size_t a = 0; a < A; ++a)
        if (in(static_cast<uint32_t>(a)))
          out.push_back(static_cast<uint32_t>(a));
    return;
  }
  const size_t want = std::min<size_t>(static_cast<size_t>(s_.n()), n_fin), len = static_cast<size_t>(r.e - r.o);
  if (want == n_fin || 3 * n_fin * n_fin < want * len) { // 收拢 + 选秩 ≈ 3 n_fin; 沿次序扫 ≈ want · len / n_fin
    gather(r.fin, r.rbc);
    if (want < n_fin) {
      const auto cut = s_.end() == kEndLow ? out.begin() + static_cast<std::ptrdiff_t>(want) : out.end() - static_cast<std::ptrdiff_t>(want);
      std::nth_element(out.begin(), cut, out.end());
      if (s_.end() == kEndLow)
        out.erase(cut, out.end());
      else
        out.erase(out.begin(), cut);
    }
    for (uint32_t &x : out)
      x = r.o[x];
  } else {
    const uint32_t *o = r.o, *e = r.e;
    if (s_.end() == kEndLow) {
      for (; o != e && out.size() < want; ++o)
        if (in(*o))
          out.push_back(*o);
    } else {
      while (e != o && out.size() < want)
        if (in(*--e))
          out.push_back(*e);
    }
  }
  std::ranges::sort(out);
}

void run_book(const Tape &t, Rebal rb, const Pick &pick, Book &b) {
  const size_t D = t.days(), A = t.A;
  for (int w = 0; w < kWeights; ++w)
    b.ret[w].assign(D, 0.0f), b.buy[w].assign(D, 0.0f);
  b.first = D;
  std::vector<uint32_t> mem, nx; // 当前 / 新选的成员 (资产下标升序)
  // [Weight][成员] 持仓额 (源行 = 目标权, 其后各乘 1 + r; 漂移后的权 = 额 / 总额) / 新目标权; [Weight] 总额
  std::vector<double> v[kWeights], tw[kWeights];
  double sum[kWeights] = {};
  size_t src = SIZE_MAX;
  for (size_t d = 1; d < D; ++d) {
    if (const size_t r = src_of(rb, t.dates, t.dates[d]); r != src) {
      src = r;
      pick(r, nx);
      const size_t n = nx.size();
      const float *mc = t.mcap.data() + r * A;
      double cap = 0.0;
      for (const uint32_t a : nx)
        cap += mc[a];
      tw[kWeightEq].assign(n, n ? 1.0 / static_cast<double>(n) : 0.0);
      tw[kWeightCap].resize(n);
      for (size_t i = 0; i < n; ++i)
        tw[kWeightCap][i] = cap > 0.0 ? mc[nx[i]] / cap : 0.0;
      double buy[kWeights] = {};
      for (size_t i = 0, j = 0; i < n; ++i) {
        while (j < mem.size() && mem[j] < nx[i])
          ++j;
        const bool held = j < mem.size() && mem[j] == nx[i];
        for (int w = 0; w < kWeights; ++w)
          buy[w] += std::max(tw[w][i] - (held ? v[w][j] / sum[w] : 0.0), 0.0);
      }
      for (int w = 0; w < kWeights; ++w)
        v[w].swap(tw[w]), sum[w] = 1.0, b.buy[w][d] = static_cast<float>(buy[w]);
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

size_t Picks::src(PoolId p, misc::Ymd d) const {
  assert(p != kAllPool && p < pool_count() && off.size() + 1 == pool_count() && "选股表: 不是子池");
  return src_of(pool_def(p).spec.rebal(), tape->dates, d);
}

std::span<const uint16_t> Picks::row(PoolId p, size_t r) const {
  const std::vector<uint32_t> &o = off[p - 1];
  return {ids[p - 1].data() + o[r], o[r + 1] - o[r]};
}

std::span<const uint16_t> Picks::of(PoolId p, misc::Ymd d) const {
  const size_t r = src(p, d);
  return r == SIZE_MAX ? std::span<const uint16_t>{} : row(p, r);
}

std::shared_ptr<const Picks> picks(const FeatureLib &lib, const Cancel *cancel, const Tick &tick) {
  static std::mutex mu;
  static std::shared_ptr<const Picks> cur;
  const std::lock_guard<std::mutex> lock(mu);
  std::shared_ptr<const Tape> t = tape(lib, cancel, tick);
  if (!t)
    return nullptr;
  if (cur && cur->tape == t)
    return cur;
  const bool keep = cur && cur->tape->axis == t->axis && extends(cur->tape->dates, t->dates);
  const size_t lo = keep ? cur->tape->days() : 0, R = t->days(), A = t->A, NP = pool_count();
  auto pk = keep ? std::make_shared<Picks>(*cur) : std::make_shared<Picks>(Picks{{}, std::vector<std::vector<uint32_t>>(NP - 1, {0}), std::vector<std::vector<uint16_t>>(NP - 1)});
  pk->tape = t;

  // 子池按风格分组 (同风格共用一次排序), 全选池不排; 各风格的键列并成一张
  std::vector<PoolId> whole, by_style[kStyles];
  std::vector<Selector> sel;
  KeyCols kc;
  for (PoolId p = 1; p < NP; ++p) {
    const Spec &s = pool_def(p).spec;
    sel.emplace_back(s);
    (s.keyed() ? by_style[s.style()] : whole).push_back(p);
  }
  for (int st = 0; st < kStyles; ++st)
    if (!by_style[st].empty())
      kc.add(static_cast<Style>(st));

  struct Lane {
    FeatureRead::DayColumns dc;
    std::vector<float> key;
    std::vector<uint16_t> fin, rbc;
    std::vector<uint32_t> ord, out;
  };
  const FeatureRead rd(lib);
  misc::exec::ForkJoin ex(misc::exec::fit(R - lo));
  std::vector<Lane> lanes(static_cast<size_t>(ex.threads()));
  for (Lane &l : lanes) {
    if (!kc.cols.empty())
      l.dc.preallocate(A, L_1day, kc.cols.size());
    l.key.resize(A), l.fin.resize(kClasses), l.rbc.resize(A), l.ord.resize(A);
  }
  std::vector<std::vector<std::vector<uint16_t>>> got(R - lo, std::vector<std::vector<uint16_t>>(NP - 1)); // [新行][p − 1]
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    if (cancelled(cancel))
      return;
    Lane &l = lanes[static_cast<size_t>(tid)];
    const size_t r = lo + static_cast<size_t>(i);
    if (!kc.cols.empty())
      rd.load_day_columns(t->dates[r], kc.cols, l.dc);
    RowView v = row_view(*t, nullptr, r);
    const auto take = [&](PoolId p) {
      sel[p - 1u].at(v, l.out);
      got[static_cast<size_t>(i)][p - 1u].assign(l.out.begin(), l.out.end());
    };
    for (const PoolId p : whole)
      take(p);
    for (int st = 0; st < kStyles; ++st) {
      if (by_style[st].empty())
        continue;
      kc.row(static_cast<Style>(st), l.dc, t->mcap.data() + r * A, A, l.key.data());
      v.o = l.ord.data(), v.e = v.o + rank_row(l.key.data(), v.cls, A, l.ord.data(), l.fin.data(), l.rbc.data()), v.fin = l.fin.data(), v.rbc = l.rbc.data();
      for (const PoolId p : by_style[st])
        take(p);
    }
  });
  if (cancelled(cancel) || rd.stale())
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
