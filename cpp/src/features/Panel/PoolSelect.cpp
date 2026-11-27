#include "features/Panel/PoolSelect.hpp"

#include "define/Ord.hpp"
#include "factor/Radix.hpp"
#include "factor/Stat/Book.hpp" // 篮子账: 账集 book_rows (Stat 同一核)

#include "features/MetaFlag.hpp"   // fmeta::lab_ok (标签有效判据, 与评估装载同一份)
#include "features/Panel/Fund.hpp" // fund::Board / SW2021_L1_COUNT
#include "features/Storage/FeatureRead.hpp"
#include "misc/epoch.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cmath>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>

namespace pool {

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// 进程级成品 (原料带 / 选股表): 每进程代一份, 代内增量补
template <class T>
struct Shelf {
  std::mutex mu;
  std::shared_ptr<const T> cur;
};

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
    {"bs_eq", ts::TfId::None},
    {"pl_npp_ttm", ts::TfId::None},
    {"flt_div", ts::TfId::None},
    {"cs_valid", ts::TfId::None},
    {"lb_t1", ts::TfId::None},
    {"lb_nom_t1", ts::TfId::None},
    {"lb_entry_ok", ts::TfId::None},
};
static_assert(kFltNew == 0 && kFltWarn == 1 && kFltPb == 2 && kFltPe == 3 && kFltDiv == 4 && kFilters == 5, "过滤项 = kDayColDefs 自 kColFilter0 起五列, 同序");

// 划分维的类: 列值 → 类 (−1 = 不在市)
constexpr int flag(float x) { return x > 0.5f ? 1 : 0; }
constexpr int non_pos(float x) { return x > 0.0f ? 0 : 1; } // 缺 (NaN) 亦剔
// 过滤项 f 的剔法: 列值 → 1 = 剔
constexpr int (*kFilterDrop[kFilters])(float) = {flag, flag, non_pos, non_pos, flag};
constexpr int st_cls(float x) { return x >= 0.0f && x < static_cast<float>(kSts) ? static_cast<int>(x) : -1; }               // st_level 0..3
constexpr int board_cls(float x) { return x >= 1.0f && x < static_cast<float>(kBoards + 1) ? static_cast<int>(x) - 1 : -1; } // board 1..5
static_assert(kBoardShMain + 1 == fund::kBoardShMain && kBoardSzMain + 1 == fund::kBoardSzMain && kBoardStar + 1 == fund::kBoardStar &&
              kBoardChiNext + 1 == fund::kBoardChiNext && kBoardBj + 1 == fund::kBoardBj && kBoards == fund::kBoardBj);
static_assert(kDimSt + 1 == kDimBoard && kDimBoard + 1 == kDimMargin && kDimMargin + 1 == kDimFilter && kDimFilter + 1 == kDims, "资产类的进制按此维序");

// 风格键 = f(列 0, 列 1, 收盘总市值); 列空 = 不读库 (只用原料带); 全市场无键 (次序空, Selector 整片取)
struct KeyDef {
  const char *col[2];
  float (*f)(float x, float y, float mc);
};
constexpr float raw(float x, float, float) { return x; }
constexpr float per_cap(float x, float, float mc) { return mc > 0.0f ? x / mc : kNaN; }
constexpr KeyDef kKeyDefs[kStyles] = {
    {{"ab_shares", "close_day"}, [](float x, float y, float) { return x > 0.0f && y > 0.0f ? x * y : kNaN; }},
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
    {{}, [](float, float, float) { return kNaN; }},
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

// seq [len] 的在市者按类分桶: n [kClasses] = 类内数, pos = 在市者在 seq 里的位置按 (类, 位置) 升序
void order_row(const uint16_t *cls, const Aid *seq, size_t len, uint16_t *n, Aid *pos) {
  std::fill(n, n + kClasses, uint16_t{0});
  for (size_t i = 0; i < len; ++i)
    if (const uint16_t c = cls[seq[i]]; c != kOut)
      ++n[c];
  size_t at[kClasses];
  for (size_t c = 0, s = 0; c < kClasses; s += n[c++])
    at[c] = s;
  for (size_t i = 0; i < len; ++i)
    if (const uint16_t c = cls[seq[i]]; c != kOut)
      pos[at[c]++] = static_cast<Aid>(i);
}

// 一日的读 (列序 = day_cols) → 原料带行 r (资产态 + 类号 + 类内在市数); pre / close [A] = 该行前收 / 收盘 (无成交 → 0)
void fill_row(const FeatureRead::DayColumns &c, Strip &t, size_t r, float *pre, float *close) {
  const size_t A = t.A();
  uint16_t *cls = t.cls.data() + r * A, *cnt = t.cnt.data() + r * kClasses;
  std::fill(cnt, cnt + kClasses, uint16_t{0});
  for (size_t a = 0; a < A; ++a) {
    const auto v = [&](size_t k) { return static_cast<float>(c.get(0, k, a)); };
    const size_t i = r * A + a;
    const float px = v(kColClose), sh = v(kColShares), mc = px > 0.0f && sh > 0.0f ? px * sh : 0.0f, ind = v(kColInd);
    t.mcap[i] = mc;
    t.ind[i] = ind >= 1.0f && ind < static_cast<float>(fund::SW2021_L1_COUNT) ? static_cast<uint8_t>(ind + 0.5f) : 0;
    pre[a] = v(kColPre) > 0.0f ? v(kColPre) : 0.0f;
    const bool gate = fmeta::data_valid(v(kColTs)) && fmeta::data_valid(v(kColCs));
    factor::stat::LabelPlanes &l = t.t1;
    const fmeta::LabBits lb = fmeta::lab_bits(gate, c.get(0, kColLab, a), c.get(0, kColNom, a), true);
    l.lv[i] = lb.lv, l.nv[i] = lb.nv, l.m[i] = lb.m;
    t.eo[i] = fmeta::entry_code(gate, c.get(0, kColEntry, a));
    assert((!l.m[i] || t.eo[i]) && "标签有效格须有建仓可成交码");
    close[a] = v(kColTs) > 0.5f && px > 0.0f ? px : 0.0f;
    cls[a] = kOut;
    if (!(mc > 0.0f && v(kColListAge) >= 0.0f && !(v(kColDelistAge) >= 0.0f) && !flag(v(kColSusp)) && t.ind[i]))
      continue;
    const int st = st_cls(v(kColSt)), bd = board_cls(v(kColBoard));
    if (st < 0 || bd < 0)
      continue;
    int flt = 0;
    for (int f = 0; f < kFilters; ++f)
      flt |= kFilterDrop[f](v(kColFilter0 + static_cast<size_t>(f))) << f;
    const size_t id = ((static_cast<size_t>(st) * slice_radix(1) + static_cast<size_t>(bd)) * slice_radix(2) + static_cast<size_t>(flag(v(kColMargin)))) * slice_radix(3) + static_cast<size_t>(flt);
    cls[a] = static_cast<uint16_t>(id);
    ++cnt[id];
  }
}

// 一行的键 k [A] → 有限键资产按 (键, 下标) 升序写进 seq, 返回个数;
//   稳定基数排序 ((键 << 16 | 下标), factor/Radix.hpp): 输入下标升序 ⇒ 并列按下标升序; a / b = 两条 [A] 暂存带
size_t sort_keys(const float *k, size_t A, Aid *seq, uint64_t *a, uint64_t *b) {
  size_t n = 0;
  for (size_t i = 0; i < A; ++i)
    if (std::isfinite(k[i]))
      a[n++] = (static_cast<uint64_t>(ord(k[i])) << 16) | i;
  factor::cpu::radix_sort(a, b, static_cast<int>(n), 16, 32);
  for (size_t i = 0; i < n; ++i)
    seq[i] = static_cast<Aid>(a[i]);
  return n;
}

bool cancelled(const Cancel *c) { return c && c->load(std::memory_order_relaxed); }
void begin(const Tick &tick, size_t n) {
  if (tick.begin)
    tick.begin(n);
}
void step(const Tick &tick) {
  if (tick.step)
    tick.step();
}

// 行扫 (调优 rank / 落地 picks 同一条): 原料带 t 的行 [lo, 末行] 逐行并行, 每行为 sts 各风格现排一行次序 → visit(行, 风格, 次序, 线程);
//   键读库 (各风格的键列同列只读一次); 取消 / 库判废 → false
template <class Visit>
bool sweep(const FeatureLib &lib, const Strip &t, size_t lo, std::span<const Style> sts, misc::exec::ForkJoin &ex, const Cancel *cancel, const Tick &tick, Visit &&visit) {
  const size_t R = t.days(), A = t.A();
  assert(lo <= R);
  KeyCols kc;
  for (const Style st : sts)
    kc.add(st);
  struct Lane {
    FeatureRead::DayColumns dc;
    std::vector<float> key;
    std::vector<uint64_t> a, b; // 基数排序暂存带
    std::vector<Aid> seq, pos;
    std::vector<uint16_t> n;
  };
  std::vector<Lane> lanes(static_cast<size_t>(ex.threads()));
  for (Lane &l : lanes) {
    if (!kc.cols.empty())
      l.dc.preallocate(A, L_1day, kc.cols.size());
    l.key.resize(A), l.a.resize(A), l.b.resize(A), l.seq.resize(A), l.pos.resize(A), l.n.resize(kClasses);
  }
  const FeatureRead rd(lib);
  begin(tick, R - lo);
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    if (cancelled(cancel))
      return;
    Lane &l = lanes[static_cast<size_t>(tid)];
    const size_t r = lo + static_cast<size_t>(i);
    if (!kc.cols.empty())
      rd.load_day_columns(t.dates[r], kc.cols, l.dc);
    for (const Style st : sts) {
      kc.row(st, l.dc, t.mcap.data() + r * A, A, l.key.data());
      const Order o{t.cls.data() + r * A, t.cnt.data() + r * kClasses, l.seq.data(), sort_keys(l.key.data(), A, l.seq.data(), l.a.data(), l.b.data()), l.n.data(), l.pos.data(), A};
      order_row(o.cls, o.seq, o.len, l.n.data(), l.pos.data());
      visit(r, st, o, tid);
    }
    step(tick);
  });
  return !cancelled(cancel) && !rd.stale();
}

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

misc::Ymd next_day(std::span<const misc::Ymd> rows, misc::Ymd d) {
  assert(!rows.empty());
  if (const auto it = std::ranges::upper_bound(rows, d); it != rows.end())
    return *it;
  misc::Ymd o = misc::add_days(d, 1);
  while (misc::weekday_of(o) >= 5)
    o = misc::add_days(o, 1);
  return o;
}

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
  for (int k = kSlices; k-- > 0;) {
    const size_t r = slice_radix(k), x = c % r;
    c /= r;
    const uint8_t m = s.v[kDimSt + k];
    if (kDimDefs[kDimSt + k].kind == kKindFilter ? (m & x) != 0 : m && !(m >> x & 1))
      return false;
  }
  return true;
}

void Orders::resize(size_t R, size_t A) { seq.resize(R * A), len.resize(R), n.resize(R * kClasses), pos.resize(R * A); }

Order Orders::row(const Strip &t, size_t r) const {
  const size_t A = t.A();
  assert(r < rows() && r < t.days());
  return {t.cls.data() + r * A, t.cnt.data() + r * kClasses, seq.data() + r * A, len[r], n.data() + r * kClasses, pos.data() + r * A, A};
}

size_t Strip::row_of(misc::Ymd d) const {
  const auto it = std::ranges::lower_bound(dates, d);
  assert(it != dates.end() && *it == d && "日不在原料带上");
  return static_cast<size_t>(it - dates.begin());
}

misc::Ymd Strip::holding_after(size_t r) const {
  assert(r < days());
  return r + 1 < days() ? dates[r + 1] : next_day(dates, dates[r]);
}

Order Strip::row(size_t r) const {
  assert(r < days());
  const size_t n = A();
  return {cls.data() + r * n, cnt.data() + r * kClasses, nullptr, 0, nullptr, nullptr, n};
}

float Strip::omega(size_t r, size_t q, size_t a, Weight w, Drift d) const {
  assert(r <= q && q < days());
  const size_t n = A();
  const std::vector<float> &g = d == Drift::Price ? grow : gain;
  return static_cast<float>(pool::omega(w, mcap[r * n + a], static_cast<double>(g[q * n + a]) / g[r * n + a]));
}

std::shared_ptr<const Strip> strip(const FeatureLib &lib, const Cancel *cancel, const Tick &tick) {
  assert(lib.id == kAllBar && lib.ready() && "原料带的源 = 已建的 all.bar 库");
  static misc::Epochal<Shelf<Strip>> shelf;
  static std::atomic<uint64_t> lines{0};
  auto &[mu, cur] = shelf.get();
  const std::lock_guard<std::mutex> lock(mu);
  std::vector<misc::Ymd> rows = landed(lib);
  if (rows.empty()) // 库已判废删除 (调用方持的是删前的清单): 同 stale 收工, 调用方下趟按新库再来
    return nullptr;
  const bool keep = cur && cur->axis.hash == lib.axis.hash && extends(cur->dates, rows);
  if (keep && cur->days() == rows.size())
    return cur;
  const size_t A = lib.axis.size(), lo = keep ? cur->days() : 0, R = rows.size();
  assert(A > 0 && A <= UINT16_MAX && R > lo);
  auto t = keep ? std::make_shared<Strip>(*cur) : std::make_shared<Strip>();
  t->axis = lib.axis, t->dates = std::move(rows);
  if (!keep)
    t->line = ++lines;
  t->cls.resize(R * A), t->cnt.resize(R * kClasses), t->ind.resize(R * A), t->mcap.resize(R * A), t->grow.resize(R * A), t->gain.resize(R * A), t->eo.resize(R * A);
  t->t1.lv.resize(R * A), t->t1.nv.resize(R * A), t->t1.m.resize(R * A), t->t1.mk.resize(R), t->t1.cap.resize(R);
  t->pre.resize(A);
  std::vector<float> pre((R - lo) * A), close((R - lo) * A); // [新行 · A]
  const FeatureRead rd(lib);
  const std::vector<size_t> cols = day_cols();
  misc::exec::ForkJoin ex(misc::exec::fit(R - lo));
  std::vector<FeatureRead::DayColumns> dc(static_cast<size_t>(ex.threads()));
  for (FeatureRead::DayColumns &c : dc)
    c.preallocate(A, L_1day, cols.size());
  begin(tick, R - lo);
  ex.tasks(static_cast<int>(R - lo), [&](int i, int tid) {
    if (cancelled(cancel))
      return;
    FeatureRead::DayColumns &c = dc[static_cast<size_t>(tid)];
    const size_t k = static_cast<size_t>(i);
    rd.load_day_columns(t->dates[lo + k], cols, c);
    fill_row(c, *t, lo + k, pre.data() + k * A, close.data() + k * A);
    step(tick);
  });
  if (cancelled(cancel) || rd.stale())
    return nullptr;
  for (size_t r = lo; r < R; ++r) {
    float *g = t->grow.data() + r * A;
    if (r == 0) {
      std::fill(g, g + A, 1.0f);
      continue;
    }
    const float *p = r == lo ? t->pre.data() : pre.data() + (r - 1 - lo) * A, *c = close.data() + (r - lo) * A, *g0 = g - A;
    for (size_t a = 0; a < A; ++a)
      g[a] = p[a] > 0.0f && c[a] > 0.0f ? static_cast<float>(static_cast<double>(g0[a]) * c[a] / p[a]) : g0[a];
  }
  // 池基准: 新行 + 前一行 (其续持前看到了新行)
  factor::stat::pool_mean(ex, t->t1, t->eo.data(), hold_on(kHoldT1, geo_of(1)), static_cast<int>(lo > 0 ? lo - 1 : 0), static_cast<int>(R));
  // 账的增长: 行 r−1 的续持收益 (行 r 价点可卖 → 做多值, 否则名义值; 与账集前一批的漂移同式)
  for (size_t r = lo; r < R; ++r) {
    float *g = t->gain.data() + r * A;
    if (r == 0) {
      std::fill(g, g + A, 1.0f);
      continue;
    }
    const float *g0 = g - A;
    const size_t p = (r - 1) * A;
    for (size_t a = 0; a < A; ++a) {
      const float y[2] = {factor::cpu::stat::h2f(t->t1.lv[p + a]), factor::cpu::stat::h2f(t->t1.nv[p + a])};
      g[a] = t->t1.m[p + a] ? static_cast<float>(static_cast<double>(g0[a]) * (1.0 + factor::stat::hold_ret(true, t->eo[r * A + a], y))) : g0[a];
    }
  }
  t->pre.assign(pre.end() - static_cast<std::ptrdiff_t>(A), pre.end());
  cur = std::move(t);
  return cur;
}

bool rank(const FeatureLib &lib, const Strip &t, Style st, misc::exec::ForkJoin &ex, const Cancel *cancel, const Tick &tick, Orders &rk) {
  const size_t lo = rk.rows(), A = t.A();
  rk.resize(t.days(), A);
  if (sweep(lib, t, lo, {&st, 1}, ex, cancel, tick, [&](size_t r, Style, const Order &o, int) {
        std::copy_n(o.seq, o.len, rk.seq.data() + r * A);
        std::copy_n(o.n, kClasses, rk.n.data() + r * kClasses);
        std::copy_n(o.pos, A, rk.pos.data() + r * A);
        rk.len[r] = static_cast<uint16_t>(o.len);
      }))
    return true;
  rk.resize(lo, A);
  return false;
}

Selector::Selector(const Spec &s) : s_(s), in_(kClasses + 1, 0) {
  for (size_t c = 0; c < kClasses; ++c)
    if (covers(s, c))
      in_[c] = 1, cls_.push_back(static_cast<uint16_t>(c));
}

void Selector::at(const Order &o, std::vector<Aid> &out) const {
  out.clear();
  if (s_.style() == kStyleMkt) {
    for (size_t a = 0; a < o.A; ++a)
      if (in_[o.cls[a]])
        out.push_back(static_cast<Aid>(a));
    return;
  }
  const auto sum = [&](const uint16_t *p) {
    size_t n = 0;
    for (const uint16_t c : cls_)
      n += p[c];
    return n;
  };
  const size_t n_in = sum(o.cnt), n = sum(o.n);
  if (n_in == 0 || static_cast<float>(n) < kCover * static_cast<float>(n_in))
    return;
  const bool high = s_.end() == kEndHigh;
  const size_t want = std::min<size_t>(static_cast<size_t>(s_.n()), n);
  // 收拢 ≈ 3n; 沿次序扫 ≈ want · len / n
  if (3 * n * n < want * o.len) {
    for (size_t c = 0, b = 0; c < kClasses; b += o.n[c++])
      if (in_[c])
        out.insert(out.end(), o.pos + b, o.pos + b + o.n[c]);
    if (want < n) {
      const auto cut = high ? out.end() - static_cast<std::ptrdiff_t>(want) : out.begin() + static_cast<std::ptrdiff_t>(want);
      std::nth_element(out.begin(), cut, out.end());
      if (high)
        out.erase(out.begin(), cut);
      else
        out.erase(cut, out.end());
    }
    for (Aid &x : out)
      x = o.seq[x];
  } else {
    const auto in = [&](Aid a) { return in_[o.cls[a]] != 0; };
    const Aid *b = o.seq, *e = o.seq + o.len;
    if (high) {
      while (e != b && out.size() < want)
        if (in(*--e))
          out.push_back(*e);
    } else
      for (; b != e && out.size() < want; ++b)
        if (in(*b))
          out.push_back(*b);
  }
  std::ranges::sort(out);
}

namespace {

// 选池账集 (Stat/Book.hpp): 本 = 权重 w; 行 r 的成员 mem (源行 src[r] 选出) 按 Strip::omega (账的漂移) 定目标权, 一律按真持有权和归一
struct PickBooks {
  const Strip &t;
  const uint8_t *mem;
  const size_t *src;
  int A = 0;
  size_t size() const { return kWeights; }
  double cap(int, size_t) const { return 0.0; }
  template <class Put>
  void each(int r, uint32_t a, Put &&put) const {
    const size_t i = static_cast<size_t>(r) * A + a;
    if (!mem[i])
      return;
    for (int w = 0; w < kWeights; ++w)
      if (const float om = t.omega(src[r], static_cast<size_t>(r), a, static_cast<Weight>(w), Drift::Book); om > 0.0f)
        put(static_cast<uint32_t>(w), om, factor::stat::InMask{mem + i});
  }
};

} // namespace

void perf(const Strip &t, Rebal rb, const Pick &pick, Perf &b) {
  namespace st = factor::stat;
  const size_t D = t.days(), A = t.A();
  b.first = D;
  // 批 = 行 r 收盘建仓 (持仓日 r + 1): 成员位 mem / 源行 src
  std::vector<uint8_t> mem(D * A, 0);
  std::vector<size_t> src(D, SIZE_MAX);
  std::vector<Aid> o;
  for (size_t r = 0; r + 1 < D; ++r) {
    src[r] = src_of(rb, t.dates, t.dates[r + 1]);
    if (src[r] == SIZE_MAX)
      continue;
    if (b.first == D)
      b.first = r + 1;
    if (r == 0 || src[r] != src[r - 1])
      pick(src[r], o);
    for (const Aid a : o)
      mem[r * A + a] = 1;
  }
  // 篮子账 (键 = 权重; Stat 账集同一核): 收益 (持仓日 r + 1) / 流水; 只付固定冲击
  //   日层每行一批 (h̄ = 1), 期线 Lines [字段 · B + 本] 与行情篮子账同一排布 (收益记平仓日 ⇄ 流水记建仓日, 协议表 2【记日】)
  const st::Span sp{static_cast<int>(D), static_cast<int>(A), geo_of(1)};
  const Hold hd = hold_on(kHoldT1, sp.g);
  const st::Label lb = st::label_of(t.t1);
  misc::exec::Serial ex;
  constexpr int B = kWeights, nf = kFlowImp;
  st::Lines ln;
  ln.reset((1 + nf) * B, 1, 1, static_cast<int>(D), [](int) { return kHoldT1; }, [](int i) { return i < B; });
  st::book_rows(ex, sp, hd, lb, t.eo.data(), PickBooks{t, mem.data(), src.data(), static_cast<int>(A)}, st::Cost{}, [&](int, int r, const double *ret, const TurnFlow *flow) {
    for (int w = 0; w < B; ++w) {
      ln.add(w, r, ret[w]);
      for (int f = 0; f < nf; ++f)
        ln.add((1 + f) * B + w, r, flow[w][f]);
    }
  });
  b.take(ln.out, B, nf);
}

size_t Picks::src(PoolId p, misc::Ymd d) const {
  assert(p != kAllPool && p < pool_count() && rows.size() + 1 == pool_count() && "选股表: 不是子池");
  return src_of(pool_def(p).spec.rebal(), strip->dates, d);
}

std::span<const Aid> Picks::row(PoolId p, size_t r) const { return rows[p - 1][r]; }

std::span<const Aid> Picks::of(PoolId p, misc::Ymd d) const {
  const size_t r = src(p, d);
  return r == SIZE_MAX ? std::span<const Aid>{} : row(p, r);
}

void Picks::pools(misc::Ymd d, std::span<PoolMask> in) const {
  assert(in.size() == strip->A());
  for (PoolId p = 1; p < pool_count(); ++p)
    for (const Aid a : of(p, d))
      in[a] |= pool_bit(p);
}

std::shared_ptr<const Picks> picks(const FeatureLib &lib, const Cancel *cancel, const Tick &tick) {
  static misc::Epochal<Shelf<Picks>> shelf;
  auto &[mu, cur] = shelf.get();
  const std::lock_guard<std::mutex> lock(mu);
  std::shared_ptr<const Strip> t = strip(lib, cancel, tick);
  if (!t)
    return nullptr;
  if (cur && cur->strip == t)
    return cur;
  const bool keep = cur && cur->strip->line == t->line;
  const size_t lo = keep ? cur->strip->days() : 0, R = t->days(), NP = pool_count();
  auto pk = keep ? std::make_shared<Picks>(*cur) : std::make_shared<Picks>(Picks{{}, std::vector<Csr<Aid>>(NP - 1)});
  pk->strip = t;

  // 子池按风格分组 (同风格共用一次排序)
  std::vector<Selector> sel;
  std::vector<PoolId> by[kStyles];
  for (PoolId p = 1; p < NP; ++p)
    by[sel.emplace_back(pool_def(p).spec).style()].push_back(p);
  std::vector<Style> sts;
  for (int st = 0; st < kStyles; ++st)
    if (!by[st].empty())
      sts.push_back(static_cast<Style>(st));

  misc::exec::ForkJoin ex(misc::exec::fit(R - lo));
  std::vector<std::vector<std::vector<Aid>>> got(R - lo, std::vector<std::vector<Aid>>(NP - 1)); // [新行][p − 1]
  if (!sweep(lib, *t, lo, sts, ex, cancel, tick, [&](size_t r, Style st, const Order &o, int) {
        for (const PoolId p : by[st])
          sel[p - 1u].at(o, got[r - lo][p - 1u]);
      }))
    return nullptr;
  for (size_t p = 0; p + 1 < NP; ++p) {
    Csr<Aid> &c = pk->rows[p];
    for (size_t i = 0; i < R - lo; ++i) {
      c.v.insert(c.v.end(), got[i][p].begin(), got[i][p].end());
      c.off.push_back(static_cast<uint32_t>(c.v.size()));
    }
  }
  cur = std::move(pk);
  return cur;
}

} // namespace pool
