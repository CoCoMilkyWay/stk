// Probe — 探针层 (一个根的一种输出; 双层严格对仗生命周期): Load / Book 之上, Engine 之下.
//   趟级: init(PassCtx) ⇄ finish(PassCtx)    (首块前一次定形/置初值, 末块后好根一次收尾)
//   块级: open(Ctx)     ⇄ close(Ctx, RootView) (中间夹逐统计行 row(hi, t, r, ext))
//   落地三形 (行轴 = 评估期 rows_stat): RowsP<D, Got> (逐行暂存 → 折) / LandCellsP<V> (逐格) / LinesP<K> (净值线 → fold_book).
//   通用六探针: HoldP / CostP / CoverP / AlphaP / IcPdfP / YearP → EvalProbes<X...> → fill_eval.
#pragma once

#include "factor/Search/Contract.hpp" // Tick / DaySum / roll_of
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp" // Workspace / NoExt
#include "gui/engine/Book.hpp" // RowLines / fold_book
#include "gui/engine/Load.hpp"
#include "math/distribution/PdfSnap.hpp" // AggPdf / KLLcache
#include "misc/date.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace GUI::Engine {

// ---- 根上的统计核 ----
struct Cover {
  uint64_t on = 0, all = 0;
  double sum = 0.0;
  long long rows = 0;
  void merge(const Cover &o) { on += o.on, all += o.all, sum += o.sum, rows += o.rows; }
};
void cover_rows(misc::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int t0, int t1, int A, Cover &o);
void alpha_dist(misc::exec::ForkJoin &ex, const float *v, const uint8_t *m, const uint16_t *rx, int T, int A, int d, AlphaDist &o);
MiniPdf mini_pdf_of(const AggPdf &p);
inline double group_abs(const factor::stat::Row &r, int k, double cap) {
  return r.ok ? factor::stat::book_abs(r.grp[k], r.cnt[k], r.mkt, cap) : 0.0;
}

// ---- 尾部分层 (Inspect 净值线与换手 / Agent 观察共用) ----
struct TailThr {
  int v[factor::stat::kEnds][kTails] = {};
};
inline TailThr tail_thresholds() {
  using factor::stat::kBot, factor::stat::kTop;
  TailThr o;
  for (int j = 0; j < kTails; ++j) {
    const double q = static_cast<double>(kTailOfGroup[j]) / factor::stat::kGroups, span = factor::stat::kRankMax + 1;
    o.v[kTop][j] = static_cast<int>(std::ceil((1.0 - q) * span));
    o.v[kBot][j] = static_cast<int>(std::ceil(q * span));
    assert(o.v[kBot][j] >= 1 && o.v[kTop][j] <= factor::stat::kRankMax && o.v[kBot][j] < o.v[kTop][j]);
  }
  return o;
}
inline const TailThr kTailThr = tail_thresholds();
inline bool in_tail(int re, int r16, int thr) { return re == factor::stat::kTop ? r16 >= thr : r16 < thr; }
inline uint8_t tail_code(uint16_t r16, int j, int d) {
  if (r16 == factor::stat::kRankNone)
    return factor::stat::kEcVoid;
  const int et = factor::stat::kTop ^ d, eb = factor::stat::kBot ^ d;
  return in_tail(et, r16, kTailThr.v[et][j])   ? factor::stat::kEcTop
         : in_tail(eb, r16, kTailThr.v[eb][j]) ? factor::stat::kEcBot
                                               : factor::stat::kEcMid;
}

struct TailExt {
  const int (*thr)[kTails];
  double cap;
  int d = factor::stat::kFwd;
  double s[factor::stat::kEnds][kTails] = {};
  int n[factor::stat::kEnds][kTails] = {};
  void cell(const factor::cpu::stat::RxCell &cl) {
    const int x = *cl.code;
    for (int e = 0; e < factor::stat::kEnds; ++e)
      for (int j = 0; j < kTails; ++j) {
        const int re = e ^ d, th = thr[re][j];
        if (!in_tail(re, x, th))
          continue;
        const auto in = [re, th](uint16_t v) { return v != factor::stat::kRankNone && in_tail(re, v, th); };
        if (!cl.held(in))
          continue;
        ++n[e][j];
        s[e][j] += cl.ret(cl.continues(in));
      }
  }
  void finish(const factor::stat::Row &r, TailRow &o) const {
    o = TailRow{};
    if (!r.ok)
      return;
    for (int j = 0; j < kTails; ++j)
      for (int e = 0; e < factor::stat::kEnds; ++e) {
        o.cnt[e][j] = static_cast<uint16_t>(n[e][j]);
        o.sum[e][j] = static_cast<float>(factor::stat::book_excess(s[e][j], n[e][j], r.mkt, cap * kTailOfGroup[j]));
      }
  }
};

// ---- 探针契约与折叠容器 Probes<P...> ----
enum Need : uint8_t {
  kNeedRows = 1,
  kNeedRx = 2,
  kNeedEc = 4,
  kNeedMask = 8,
  kNeedAlpha = 16,
  kNeedX = 32,
  kNeedTicks = 64
};

struct RootView {
  factor::stat::Plane x;
  factor::stat::Plane alpha;
  factor::stat::Frame f = factor::stat::Frame::CS;
  const uint16_t *rx = nullptr;
  const uint8_t *ec = nullptr;
  const factor::stat::Accum *acc = nullptr;
  const factor::search::Tick *ticks = nullptr; // [H][T] 逐行 Tick (kNeedTicks; 正向, 只统计行有值; Search 契约【逐行 Tick】)
};

// 趟级上下文 (init / finish 共用)
struct PassCtx {
  misc::exec::ForkJoin &ex;
  const Domain &dm;
  Holds hd;
  Amts ad;
  int seg = 0, A = 0;
  int days_stat = 0, rows_stat = 0;
  factor::stat::Dir d = factor::stat::kFwd;
};

// 块级上下文 (open / close 共用)
struct Ctx {
  misc::exec::ForkJoin &ex;
  const Block &c;
  const factor::stat::Labels &L;
  factor::cpu::stat::Workspace *w = nullptr;
  factor::stat::Dir d = factor::stat::kFwd;
  bool last = false;
  factor::stat::Span s() const { return c.span(); }
};

template <class... P>
struct Probes {
  std::tuple<P...> p;
  static constexpr uint8_t need_each = (uint8_t{0} | ... | P::need);
  static constexpr uint8_t need_last = (uint8_t{0} | ... | P::need_last);
  static constexpr uint8_t need = need_each | need_last;
  static constexpr uint8_t need_at(bool last) { return last ? need : need_each; }
  static constexpr Segs segs = std::max({Segs::None, P::segs...});
  static constexpr bool cells = (false || ... || !std::is_same_v<typename P::Ext, factor::cpu::stat::NoExt>);
  struct Ext {
    std::tuple<typename P::Ext...> e;
    void cell(const factor::cpu::stat::RxCell &cl) {
      std::apply([&](auto &...q) { (q.cell(cl), ...); }, e);
    }
  };
  template <class Q>
  Q &at() { return std::get<Q>(p); }
  template <class Q>
  const Q &at() const { return std::get<Q>(p); }
  static double bytes(const Shape &sh, int H) { return (0.0 + ... + P::bytes(sh, H)); }
  static double scratch(const Shape &sh, int H) { return (0.0 + ... + P::scratch(sh, H)); }
  Ext ext() const {
    return std::apply([](const P &...q) { return Ext{std::tuple<typename P::Ext...>{q.ext()...}}; }, p);
  }
  void init(const PassCtx &c) {
    std::apply([&](P &...q) { (q.init(c), ...); }, p);
  }
  void open(const Ctx &c) {
    std::apply([&](P &...q) { (q.open(c), ...); }, p);
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &e) {
    [&]<size_t... I>(std::index_sequence<I...>) { (std::get<I>(p).row(hi, t, r, std::get<I>(e.e)), ...); }(std::index_sequence_for<P...>{});
  }
  void close(const Ctx &c, const RootView &v) {
    std::apply([&](P &...q) { (q.close(c, v), ...); }, p);
  }
  void finish(const PassCtx &c) {
    std::apply([&](P &...q) { (q.finish(c), ...); }, p);
  }
};

template <class Ps>
struct ProbeSink {
  Ps *ps;
  factor::stat::Dir d;
  using Ext = typename Ps::Ext;
  Ext ext() const { return ps->ext(); }
  void row(int i, int t, const factor::stat::Row &r, const Ext &e) const { ps->row(i, t, factor::stat::oriented(r, d), e); }
};

struct ProbeBase {
  using Ext = factor::cpu::stat::NoExt;
  static constexpr uint8_t need = 0;
  static constexpr uint8_t need_last = 0;
  static constexpr Segs segs = Segs::None;
  static double bytes(const Shape &, int) { return 0.0; }
  static double scratch(const Shape &, int) { return 0.0; }
  Ext ext() const { return {}; }
  void init(const PassCtx &) {}
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &, const Ext &) {}
  void close(const Ctx &, const RootView &) {}
  void finish(const PassCtx &) {}
};

// ---- 通用探针与三种落地基类 ----

// 二级 HoldStat[H] (毛): 块间 Accum 续攒 → finish_all
struct HoldP : ProbeBase {
  factor::stat::Accum acc[factor::stat::kMaxHold];
  int n_hold = 0;
  factor::stat::HoldStat hold[factor::stat::kMaxHold];
  void close(const Ctx &c, const RootView &v) {
    for (int i = 0; i < c.L.hd.n; ++i)
      acc[i].merge(v.acc[i]);
  }
  void finish(const PassCtx &p) {
    n_hold = p.hd.n;
    factor::stat::finish_all(acc, p.hd, p.seg, hold);
  }
};

// 换手成本 CostTier[C]: init 定段, close 按块续攒
struct CostP : ProbeBase {
  static constexpr uint8_t need = kNeedEc;
  static constexpr Segs segs = Segs::All;
  std::vector<CostTier> costs;
  void init(const PassCtx &p) {
    costs.assign(static_cast<size_t>(p.ad.n), CostTier{});
    for (int k = 0; k < p.ad.n; ++k)
      costs[static_cast<size_t>(k)].amt = p.ad.k[k];
  }
  void close(const Ctx &c, const RootView &v);
};

// 池内有效率 / 可秩率
struct CoverP : ProbeBase {
  static constexpr uint8_t need = kNeedRx | kNeedMask;
  Cover cv;
  float valid_pct = 0.f, rank_pct = 0.f;
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    cover_rows(c.ex, v.rx, v.x.m, c.c.y.gate, s.t_lo, s.t_hi(), s.A, cv);
  }
  void finish(const PassCtx &) {
    assert(cv.all > 0 && "截面池全空: 区间内无在池格");
    valid_pct = static_cast<float>(100.0 * static_cast<double>(cv.on) / static_cast<double>(cv.all));
    rank_pct = cv.rows > 0 ? static_cast<float>(100.0 * cv.sum / static_cast<double>(cv.rows)) : 0.f;
  }
};

// alpha 分布 (只末块取统计行)
struct AlphaP : ProbeBase {
  static constexpr uint8_t need_last = kNeedAlpha | kNeedRx;
  AlphaDist d;
  void close(const Ctx &c, const RootView &v) {
    if (!c.last)
      return;
    assert(c.c.ck.d1 == c.c.nd && c.c.t_back == 0 && "末块止于评估期末");
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    alpha_dist(c.ex, v.alpha.v + o, v.alpha.m + o, v.rx + o, s.t_hi() - s.t_lo, s.A, c.d, d);
  }
};

// 落地基类 1: 逐统计行暂存 → close 折进派生状态 (末块即释放暂存)
template <class D, class Got>
struct RowsP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  int t_lo = 0, n = 0;
  std::vector<Got> got;
  static double scratch(const Shape &sh, int H) { return sizeof(Got) * static_cast<double>(H) * sh.T; }
  void open(const Ctx &c) {
    const factor::stat::Span s = c.s();
    t_lo = s.t_lo, n = s.t_hi() - s.t_lo;
    got.assign(static_cast<size_t>(c.L.hd.n) * n, Got{});
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) { got[static_cast<size_t>(hi) * n + (t - t_lo)] = D::of(r); }
  void close(const Ctx &c, const RootView &) {
    const int u0 = c.c.land0() + t_lo;
    for (int hi = 0; hi < c.L.hd.n; ++hi)
      static_cast<D &>(*this).fold(c, hi, u0, std::span<const Got>(got).subspan(static_cast<size_t>(hi) * n, static_cast<size_t>(n)));
    if (c.last)
      std::vector<Got>().swap(got);
  }
};

// 落地基类 2: 逐格 [rows_stat][A] 进 dst (init 一次定形, 各块按 land0 写)
template <class V>
struct LandCellsP : ProbeBase {
  std::vector<V> *dst = nullptr;
  static double bytes(const Shape &sh, int) { return sizeof(V) * static_cast<double>(sh.n()); }
  void init(const PassCtx &p) { dst->assign(static_cast<size_t>(p.rows_stat) * p.A, V{}); }
  V *at(const Ctx &c) const { return dst->data() + static_cast<size_t>(c.c.land0() + c.c.t_lo) * c.s().A; }
};

// 落地基类 3: 净值线 [hd.n · K] (init 一次定形, finish 一次 fold_book)
template <int K>
struct LinesP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  RowLines x;
  BookLines out;
  int stride = 1, t0 = 0;
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(K) * H * sh.T; }
  void init(const PassCtx &p) {
    x.reset(p.hd.n * K, p.rows_stat);
    for (int i = 0; i < x.n; ++i)
      x.hold[static_cast<size_t>(i)] = p.hd.k[i / K];
  }
  void open(const Ctx &c) {
    assert(x.n == c.L.hd.n * K && x.T == c.c.rows_stat() && "净值线与作用域不符");
    t0 = c.c.land0();
  }
  float &at(int hi, int k, int t) { return x.x[static_cast<size_t>(hi * K + k) * x.T + static_cast<size_t>(t0 + t)]; }
  void finish(const PassCtx &p) {
    out = fold_book(p.ex, p.seg, x, stride);
    x = RowLines{};
  }
};

inline float ric_of(const factor::stat::Row &r) { return r.ok ? r.ric : std::numeric_limits<float>::quiet_NaN(); }

// 逐行 rIC 分布
struct IcPdfP : RowsP<IcPdfP, float> {
  std::vector<KLLcache> kll;
  AggPdf pdf[factor::stat::kMaxHold];
  static float of(const factor::stat::Row &r) { return ric_of(r); }
  void init(const PassCtx &p) {
    kll.clear();
    kll.reserve(static_cast<size_t>(p.hd.n));
    for (int i = 0; i < p.hd.n; ++i)
      kll.emplace_back(kAggKllCapacity, kAggKllResolution);
  }
  void fold(const Ctx &, int hi, int, std::span<const float> g) {
    std::vector<float> ok;
    ok.reserve(g.size());
    for (const float v : g)
      if (!std::isnan(v))
        ok.push_back(v);
    kll[static_cast<size_t>(hi)].addBatch(ok);
  }
  void finish(const PassCtx &p) {
    for (int i = 0; i < p.hd.n; ++i)
      pdf[i].fill(kll[static_cast<size_t>(i)], 3);
    std::vector<KLLcache>().swap(kll);
  }
};

// 分年分层统计 + 滚动窗统计 (同一份逐日和; 行已按所评方向, 见 ProbeSink)
struct YearRow {
  bool ok = false;
  float ric = 0.f, ls = 0.f, g[factor::stat::kGroups] = {};
};
struct YearP : RowsP<YearP, YearRow> {
  struct Day {
    int n = 0;
    double ric = 0.0, ls = 0.0, g[factor::stat::kGroups] = {};
  };
  int seg = 0;
  std::vector<std::vector<Day>> day;
  std::vector<YearStat> years[factor::stat::kMaxHold];
  factor::search::Roll roll[factor::stat::kMaxHold];
  std::vector<float> day_ric[factor::stat::kMaxHold]; // [日] 当日 ok 行 rIC 均值 (无 ok 行 = NaN)
  static double bytes(const Shape &sh, int H) { return sizeof(Day) * static_cast<double>(H) * sh.days; }
  static YearRow of(const factor::stat::Row &r) {
    YearRow x{r.ok != 0, r.ric, factor::stat::ls_row(r), {}};
    std::copy_n(r.grp, factor::stat::kGroups, x.g);
    return x;
  }
  void init(const PassCtx &p) {
    seg = p.seg;
    day.assign(static_cast<size_t>(p.hd.n), std::vector<Day>(static_cast<size_t>(p.days_stat)));
  }
  void fold(const Ctx &, int hi, int u0, std::span<const YearRow> g) {
    for (size_t i = 0; i < g.size(); ++i) {
      const YearRow &x = g[i];
      if (!x.ok)
        continue;
      Day &d = day[static_cast<size_t>(hi)][static_cast<size_t>((u0 + static_cast<int>(i)) / seg)];
      ++d.n, d.ric += x.ric, d.ls += x.ls;
      for (int k = 0; k < factor::stat::kGroups; ++k)
        d.g[k] += x.g[k];
    }
  }
  void finish(const PassCtx &p) {
    const std::vector<misc::Ymd> dates = p.dm.stat_dates();
    for (int hi = 0; hi < p.hd.n; ++hi) {
      const std::vector<Day> &ds = day[static_cast<size_t>(hi)];
      assert(ds.size() == dates.size() && "逐日累加与评估期日期不齐");
      std::vector<factor::search::DaySum> sums(ds.size());
      day_ric[hi].resize(ds.size());
      for (size_t i = 0; i < ds.size(); ++i) {
        sums[i] = {ds[i].n, ds[i].ric, ds[i].ls};
        day_ric[hi][i] = ds[i].n > 0 ? static_cast<float>(ds[i].ric / ds[i].n) : std::numeric_limits<float>::quiet_NaN();
      }
      roll[hi] = factor::search::roll_of(sums, p.hd.k[hi], seg);
      std::vector<YearStat> &out = years[hi];
      out.clear();
      for (size_t a = 0; a < ds.size();) {
        const int y = dates[a] / 10000;
        Day s;
        size_t b = a;
        for (; b < ds.size() && dates[b] / 10000 == y; ++b) {
          s.n += ds[b].n, s.ric += ds[b].ric, s.ls += ds[b].ls;
          for (int k = 0; k < factor::stat::kGroups; ++k)
            s.g[k] += ds[b].g[k];
        }
        a = b;
        if (s.n < 3)
          continue;
        out.push_back({y, s.n, static_cast<float>(factor::stat::mono_of(s.g, factor::stat::kGroups)), static_cast<float>(s.ls / s.n), static_cast<float>(s.ric / s.n)});
      }
    }
    std::vector<std::vector<Day>>().swap(day);
  }
};

// 分层 / 尾部 / LS 的逐行批收益 → fold_book 日净值线 (Inspect: 全部持有期 × kNavLines; agent eval_factor: 一个持有期 × kNavExcess)
inline double tail_abs(const factor::stat::Row &r, const TailRow &tr, int e, int j, double cap) {
  return factor::stat::book_abs(tr.sum[e][j], tr.cnt[e][j], r.mkt, cap * kTailOfGroup[j]);
}
// 线 i ∈ [0, kNavLines) 在行 r 的批收益 (线序见 Table.hpp kNavLines)
inline double nav_value(const factor::stat::Row &r, const TailRow &tr, int i, double cap) {
  if (!r.ok)
    return 0.0;
  using factor::stat::kBot, factor::stat::kTop, factor::stat::end_group;
  if (i >= 2 * kNavLayers) {
    const bool absolute = i < 2 * kNavLayers + kNavLs;
    const int j = i - 2 * kNavLayers - (absolute ? 0 : kNavLs);
    if (j == 0)
      return absolute ? 0.5 * (group_abs(r, end_group(kTop), cap) - group_abs(r, end_group(kBot), cap)) : factor::stat::ls_row(r);
    return absolute ? 0.5 * (tail_abs(r, tr, kTop, j - 1, cap) - tail_abs(r, tr, kBot, j - 1, cap)) : 0.5 * (tr.sum[kTop][j - 1] - tr.sum[kBot][j - 1]);
  }
  const bool absolute = i < kNavLayers;
  const int k = absolute ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return absolute ? group_abs(r, k, cap) : r.grp[k];
  const int e = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  return absolute ? tail_abs(r, tr, e, j, cap) : tr.sum[e][j];
}
// 超额线 i ∈ [0, kNavExcess) 在 kNavLines 线序里的号
inline constexpr int nav_excess(int i) { return i < kNavLayers ? kNavLayers + i : kNavLayers + kNavLs + i; }

// 持有期键 only 在 hd 里的下标; only < 0 → −1
inline int only_of(const Holds &hd, int only) {
  if (only < 0)
    return -1;
  for (int i = 0; i < hd.n; ++i)
    if (hd.k[i] == only)
      return i;
  assert(false && "only 须是本层标签组之一");
  return -1;
}

struct NavP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  using Ext = TailExt;
  int only = -1; // 调用方给: 持有期键 → 只出它的 kNavExcess 条超额线; −1 = 全部持有期 × kNavLines
  int sel = -1, t0 = 0, d = factor::stat::kFwd;
  double cap = 0.0;
  RowLines x;
  BookLines out; // [线][日]; 全部持有期时线 = hi · kNavLines + i
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(kNavLines) * H * sh.T; }
  Ext ext() const { return {kTailThr.v, cap, d}; }
  void init(const PassCtx &p) {
    d = p.d, sel = only_of(p.hd, only);
    x.reset(only < 0 ? p.hd.n * kNavLines : kNavExcess, p.rows_stat);
    for (int i = 0; i < x.n; ++i)
      x.hold[static_cast<size_t>(i)] = only < 0 ? p.hd.k[i / kNavLines] : only;
  }
  void open(const Ctx &c) {
    assert(x.T == c.c.rows_stat() && "净值线与作用域不符");
    t0 = c.c.land0(), cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &e) {
    if (only >= 0 && hi != sel)
      return;
    TailRow tr;
    e.finish(r, tr);
    const size_t at = static_cast<size_t>(t0 + t);
    if (only < 0)
      for (int i = 0; i < kNavLines; ++i)
        x.x[static_cast<size_t>(hi * kNavLines + i) * x.T + at] = static_cast<float>(nav_value(r, tr, i, cap));
    else
      for (int i = 0; i < kNavExcess; ++i)
        x.x[static_cast<size_t>(i) * x.T + at] = static_cast<float>(nav_value(r, tr, nav_excess(i), cap));
  }
  void finish(const PassCtx &p) {
    out = fold_book(p.ex, p.seg, x, p.seg);
    x = RowLines{};
  }
};

// 各级尾部 LS 的两端换手 (固定段, 无冲击): acc[j][hi]; only 同 NavP (只算该持有期, 放 acc[j][0])
struct TurnP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  int only = -1;
  factor::stat::CostAcc acc[kTails][factor::stat::kMaxHold] = {};
  static double scratch(const Shape &sh, int) { return static_cast<double>(sh.n()); }
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    factor::stat::Labels L = c.L;
    if (const int i = only_of(c.L.hd, only); i >= 0) {
      L.hd.n = 1, L.hd.k[0] = only;
      L.l[0] = c.L.l[i];
    }
    std::vector<uint8_t> ec(s.n());
    for (int j = 0; j < kTails; ++j) {
      c.ex.rows(s.T, [&](int t0, int t1, int) {
        for (size_t k = static_cast<size_t>(t0) * s.A, e = static_cast<size_t>(t1) * s.A; k < e; ++k)
          ec[k] = tail_code(v.rx[k], j, c.d);
      });
      const factor::stat::Cost flat{};
      factor::stat::CostAcc out[factor::stat::kMaxHold];
      factor::cpu::stat::cost_rows(c.ex, ec.data(), s, L, L.cap * kTailOfGroup[j], &flat, 1, *c.w, out);
      for (int hi = 0; hi < L.hd.n; ++hi)
        acc[j][hi].merge(out[hi]);
    }
  }
};

template <class... X>
using EvalProbes = Probes<HoldP, CostP, CoverP, AlphaP, IcPdfP, YearP, X...>;

template <class Ps>
void fill_eval(const Ps &p, const StatScope &scope, double eval_ms, EvalStat &st) {
  const HoldP &h = p.template at<HoldP>();
  const CostP &c = p.template at<CostP>();
  const IcPdfP &ic = p.template at<IcPdfP>();
  const CoverP &cv = p.template at<CoverP>();
  const YearP &yr = p.template at<YearP>();
  st.scope = scope;
  st.eval_ms = eval_ms;
  st.valid_pct = cv.valid_pct;
  st.rank_pct = cv.rank_pct;
  st.n_hold = h.n_hold;
  std::copy_n(h.hold, h.n_hold, st.hold);
  st.costs = c.costs;
  for (int hi = 0; hi < h.n_hold; ++hi) {
    st.ic_pdf[hi] = mini_pdf_of(ic.pdf[hi]);
    st.years[hi] = yr.years[hi];
    st.roll[hi] = yr.roll[hi];
  }
  st.alpha_pdf = mini_pdf_of(p.template at<AlphaP>().d.pdf);
}

} // namespace GUI::Engine
