#include "gui/task_beta/services/PoolSelect.hpp"

#include "features/Panel/Fund.hpp" // fund::Board

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <limits>
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
      const float px = v(kClose), sh = v(kShares), mc = px > 0.0f && sh > 0.0f ? px * sh : 0.0f, bd = v(kBoardCol), lvl = v(kStLevel);
      t.mcap[row] = mc;
      pre[row] = v(kPre) > 0.0f ? v(kPre) : 0.0f;
      close[row] = v(kTs) > 0.5f && px > 0.0f ? px : 0.0f;
      if (!(mc > 0.0f && v(kListAge) >= 0.0f && !(v(kDelistAge) >= 0.0f) && bd >= 1.0f && bd < static_cast<float>(kBoards)))
        continue;
      assert(lvl >= 0.0f && lvl < static_cast<float>(kSts - 1) && "all.bar: 在市资产的 st_level 须是 0..3");
      unsigned flt = 0;
      for (int k = 0; k < kFilters; ++k)
        flt |= (v(static_cast<TapeCol>(kFlt0 + k)) > 0.5f ? 1u : 0u) << k;
      const uint16_t c = class_of(static_cast<int>(lvl), static_cast<int>(bd), v(kMarginCol) > 0.5f ? 0 : 1, flt);
      t.cls[row] = c;
      ++t.cnt[static_cast<size_t>(i) * kClasses + c];
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
    const float *k = key.data() + r * A;
    uint32_t *o = rk.ord.data() + rk.off[r];
    uint16_t *fin = rk.fin.data() + r * kClasses;
    const uint16_t *cl = t.cls.data() + r * A;
    size_t n = 0;
    for (size_t a = 0; a < A; ++a)
      if (std::isfinite(k[a])) {
        o[n++] = static_cast<uint32_t>(a);
        if (cl[a] != kOut)
          ++fin[cl[a]];
      }
    std::sort(o, o + n, [&](uint32_t a, uint32_t b) { return k[a] < k[b] || (k[a] == k[b] && a < b); });
    tick();
  });
  return !cancel.load(std::memory_order_relaxed);
}

Selector::Selector(const Tape &t, const Ranks *rk, const Spec &s) : t_(t), rk_(rk), s_(s), in_(kClasses + 1, 0) {
  for (size_t c = 0; c < kClasses; ++c)
    if (covers(s, c))
      in_[c] = 1, cls_.push_back(static_cast<uint16_t>(c));
}

void Selector::at(size_t r, std::vector<uint32_t> &out) const {
  out.clear();
  const auto sum = [&](const std::vector<uint16_t> &cnt) {
    const uint16_t *p = cnt.data() + r * kClasses;
    size_t n = 0;
    for (const uint16_t c : cls_)
      n += p[c];
    return n;
  };
  const size_t A = t_.A, n_in = sum(t_.cnt);
  if (n_in == 0)
    return;
  const uint16_t *cl = t_.cls.data() + r * A;
  const auto in = [&](uint32_t a) { return in_[cl[a]] != 0; };
  const size_t n_fin = s_.n == 0 ? 0 : sum(rk_->fin);
  if (s_.n == 0 || static_cast<float>(n_fin) < kCover * static_cast<float>(n_in)) {
    for (size_t a = 0; a < A; ++a)
      if (in(static_cast<uint32_t>(a)))
        out.push_back(static_cast<uint32_t>(a));
    return;
  }
  const size_t want = std::min<size_t>(s_.n, n_fin);
  const uint32_t *o = rk_->ord.data() + rk_->off[r], *e = rk_->ord.data() + rk_->off[r + 1];
  if (s_.side == kSideLow) {
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
  const Selector sel(t, rk, s);
  const std::vector<uint8_t> &reb = t.rebal[s.rebal];
  std::vector<uint32_t> mem, nx; // 当前 / 新选的成员 (资产下标升序)
  // [Weight][成员] 持仓额 (调仓行 = 目标权, 其后各乘 1 + r; 漂移后的权 = 额 / 总额) / 新目标权; [Weight] 总额
  std::vector<double> v[kWeights], tw[kWeights];
  double sum[kWeights] = {};
  for (size_t d = 1; d < D; ++d) {
    if (reb[d - 1]) {
      sel.at(d - 1, nx);
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

} // namespace pool
