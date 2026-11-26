// Probe — 探针层 (一个根的一种输出; 双层严格对仗生命周期): Load / Ledger 之上, Engine 之下.
//   趟级: init(PassCtx) ⇄ finish(PassCtx)    (首块前一次定形/置初值, 末块后好根一次收尾)
//   块级: open(Ctx)     ⇄ close(Ctx, RootView) (中间夹逐统计行 row(hi, t, r))
//   落地三形 (行轴 = 评估期 rows_stat): RowsP<D, Got> (逐行暂存 → 折) / LandCellsP<V> (逐格) / LinesP (净值线 → fold_ledger).
//   通用六探针: HoldP (三级 + 逐日 + 滚动窗) / TurnP (账集流水) / CoverP / AlphaP / IcPdfP / GrpP (逐日组和) → EvalProbes<X...> → fill_eval;
//   分年一律 by_year (逐日原料取 HoldP / GrpP / TurnP 三处, 只在 fill_eval 汇成 YearStat).
#pragma once

#include "factor/Stat/Book.hpp" // book_rows / FactorBooks
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp" // Workspace
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Roll.hpp"  // Day / roll_of
#include "gui/engine/Ledger.hpp" // RowLines / fold_ledger
#include "gui/engine/Load.hpp"
#include "math/distribution/PdfSnap.hpp" // AggPdf / KLLcache
#include "misc/date.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <span>
#include <tuple>
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

// ---- 探针契约与折叠容器 Probes<P...> ----
enum Need : uint8_t {
  kNeedRows = 1,
  kNeedRx = 2,
  kNeedMask = 4,
  kNeedAlpha = 8,
  kNeedX = 16
};

struct RootView {
  factor::stat::Plane x;
  factor::stat::Plane alpha;
  factor::stat::Frame f = factor::stat::Frame::CS;
  const uint16_t *rx = nullptr;
  const factor::stat::Accum *acc = nullptr; // [H] 本块三级 (已定向)
  const factor::stat::Day *day = nullptr;   // [H][D] 本块二级 (已定向, D = 本块统计日数)
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
  template <class Q>
  Q &at() { return std::get<Q>(p); }
  template <class Q>
  const Q &at() const { return std::get<Q>(p); }
  static double bytes(const Shape &sh, int H) { return (0.0 + ... + P::bytes(sh, H)); }
  static double scratch(const Shape &sh, int H) { return (0.0 + ... + P::scratch(sh, H)); }
  void init(const PassCtx &c) {
    std::apply([&](P &...q) { (q.init(c), ...); }, p);
  }
  void open(const Ctx &c) {
    std::apply([&](P &...q) { (q.open(c), ...); }, p);
  }
  void row(int hi, int t, const factor::stat::Row &r) {
    std::apply([&](P &...q) { (q.row(hi, t, r), ...); }, p);
  }
  void close(const Ctx &c, const RootView &v) {
    std::apply([&](P &...q) { (q.close(c, v), ...); }, p);
  }
  void finish(const PassCtx &c) {
    std::apply([&](P &...q) { (q.finish(c), ...); }, p);
  }
};

// 逐行收进探针的 sink (行已按所评方向: rx 源头定向)
template <class Ps>
struct ProbeSink {
  Ps *ps;
  void row(int i, int t, const factor::stat::Row &r) const { ps->row(i, t, r); }
};

struct ProbeBase {
  static constexpr uint8_t need = 0;
  static constexpr uint8_t need_last = 0;
  static constexpr Segs segs = Segs::None;
  static double bytes(const Shape &, int) { return 0.0; }
  static double scratch(const Shape &, int) { return 0.0; }
  void init(const PassCtx &) {}
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &) {}
  void close(const Ctx &, const RootView &) {}
  void finish(const PassCtx &) {}
};

// ---- 通用探针与三种落地基类 ----

// 分年: 评估期日 dates 按自然年切段, 逐段 fn(年, [a, b))
template <class Fn>
void by_year(const std::vector<misc::Ymd> &dates, Fn &&fn) {
  for (size_t a = 0, b; a < dates.size(); a = b) {
    const int y = dates[a] / 10000;
    for (b = a; b < dates.size() && dates[b] / 10000 == y; ++b)
      ;
    fn(y, a, b);
  }
}

// 三级 HoldStat[H] (毛) + 逐日 Day[H][评估期日] + 滚动窗: 块间 Accum 续攒, 各块二级 Day 拼回评估期 →
//   日净值 LS (Fold ls_daily: 钱按平仓日) → finish_all; 滚动窗 roll_of (正向, 行已按所评方向) + 收尾日的窗 rIC wins
struct HoldP : ProbeBase {
  int seg = 0, n_hold = 0;
  factor::stat::Accum acc[kHoldCount];
  std::vector<factor::stat::Day> day[kHoldCount]; // [hd][评估期日]
  factor::stat::HoldStat hold[kHoldCount];
  factor::stat::Roll roll[kHoldCount];
  std::vector<double> wins[kHoldCount]; // [hd][评估期日] 收尾日的窗 rIC (不计入 = NaN)
  static double bytes(const Shape &sh, int H) { return sizeof(factor::stat::Day) * static_cast<double>(H) * sh.days; }
  void init(const PassCtx &p) {
    seg = p.seg;
    for (int i = 0; i < p.hd.n; ++i)
      day[i].assign(static_cast<size_t>(p.days_stat), {});
  }
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const int D = s.days(), d0 = (c.c.land0() + s.t_lo) / seg;
    for (int i = 0; i < c.L.hd.n; ++i) {
      acc[i].merge(v.acc[i]);
      std::copy_n(v.day + static_cast<size_t>(i) * D, D, day[i].begin() + d0);
    }
  }
  void finish(const PassCtx &p) {
    n_hold = p.hd.n;
    Daily dl[kHoldCount];
    for (int i = 0; i < n_hold; ++i) {
      dl[i] = factor::stat::ls_daily(day[i], p.hd.k[i], seg);
      roll[i] = factor::stat::roll_of(day[i], p.hd.k[i], seg, 1, nullptr, &wins[i]);
    }
    factor::stat::finish_all(acc, dl, p.hd, p.seg, hold);
  }
  // 持有期 hi 第 i 日 ok 行 rIC 均值 (无 ok 行 = NaN)
  float day_ric(int hi, size_t i) const {
    const factor::stat::Day &d = day[hi][i];
    return d.n > 0 ? static_cast<float>(d.ric / d.n) : std::numeric_limits<float>::quiet_NaN();
  }
};

// 换手 (Stat/Book.hpp 因子账集, 每块每持有期一趟 book_rows): 各持有期 × 层级 × 两端的流水 turn[H] (各金额段同一份量, 盘口段另各一份冲击);
//   另按日攒整组两端买量 day_buy → fill_eval 折成分年年换手 (YearStat::turn)
struct TurnP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  static constexpr Segs segs = Segs::All;
  int seg = 0;
  Amts amts;
  TurnStat turn[kHoldCount];
  std::vector<double> row_buy;              // [块行] 整组两端买量之和 (close 内暂存)
  std::vector<std::vector<double>> day_buy; // [hd][评估期日] 同上按日合
  static double bytes(const Shape &sh, int H) { return sizeof(double) * static_cast<double>(H) * sh.days; }
  static double scratch(const Shape &sh, int) { return sizeof(double) * static_cast<double>(sh.T); }
  void init(const PassCtx &p) {
    seg = p.seg;
    amts = p.ad;
    day_buy.assign(static_cast<size_t>(p.hd.n), std::vector<double>(static_cast<size_t>(p.days_stat), 0.0));
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
  void row(int hi, int t, const factor::stat::Row &r) { got[static_cast<size_t>(hi) * n + (t - t_lo)] = D::of(r); }
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

// 落地基类 3: 净值线 (init 由派生类 lines() 定形, finish 一次 fold_ledger; stride = 每几行并一点)
struct LinesP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  RowLines x;
  LedgerLines out;
  int stride = 1, t0 = 0;
  // n 条线, 线 i 的持有期 hold_of(i)
  template <class HoldOf>
  void lines(const PassCtx &p, int n, HoldOf &&hold_of) {
    x.reset(n, p.rows_stat);
    for (int i = 0; i < n; ++i)
      x.hold[static_cast<size_t>(i)] = hold_of(i);
  }
  void open(const Ctx &c) {
    assert(x.T == c.c.rows_stat() && "净值线与作用域不符");
    t0 = c.c.land0();
  }
  float &at(int i, int t) { return x.x[static_cast<size_t>(i) * x.T + static_cast<size_t>(t0 + t)]; }
  void finish(const PassCtx &p) {
    out = fold_ledger(p.ex, p.seg, x, stride);
    x = RowLines{};
  }
};

inline float ric_of(const factor::stat::Row &r) { return r.ok ? r.ric : std::numeric_limits<float>::quiet_NaN(); }

// 逐行 rIC 分布
struct IcPdfP : RowsP<IcPdfP, float> {
  std::vector<KLLcache> kll;
  AggPdf pdf[kHoldCount];
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

// 逐日组和 (分年 mono 的原料; 行已按所评方向, 见 ProbeSink): ok 行的 Row::grp 按日合
struct GrpRow {
  bool ok = false;
  float g[factor::stat::kGroups] = {};
};
struct GrpP : RowsP<GrpP, GrpRow> {
  int seg = 0;
  std::vector<std::vector<std::array<double, factor::stat::kGroups>>> day; // [hd][评估期日][组]
  static double bytes(const Shape &sh, int H) { return sizeof(double) * factor::stat::kGroups * static_cast<double>(H) * sh.days; }
  static GrpRow of(const factor::stat::Row &r) {
    GrpRow x{r.ok != 0, {}};
    std::copy_n(r.grp, factor::stat::kGroups, x.g);
    return x;
  }
  void init(const PassCtx &p) {
    seg = p.seg;
    day.assign(static_cast<size_t>(p.hd.n), std::vector<std::array<double, factor::stat::kGroups>>(static_cast<size_t>(p.days_stat)));
  }
  void fold(const Ctx &, int hi, int u0, std::span<const GrpRow> g) {
    for (size_t i = 0; i < g.size(); ++i)
      if (g[i].ok) {
        std::array<double, factor::stat::kGroups> &d = day[static_cast<size_t>(hi)][static_cast<size_t>((u0 + static_cast<int>(i)) / seg)];
        for (int k = 0; k < factor::stat::kGroups; ++k)
          d[static_cast<size_t>(k)] += g[i].g[k];
      }
  }
};

// 分层 / 尾部 / LS 的逐行批收益 → fold_ledger 日净值线 (Inspect: 全部持有期 × kNavLines; agent eval_factor: 一个持有期 × kNavExcess)
inline double tail_abs(const factor::stat::Row &r, int e, int j, double cap) {
  return r.ok ? factor::stat::book_abs(r.tail[e][j], r.tail_n[e][j], r.mkt, cap * factor::stat::tail_frac(j)) : 0.0;
}
// 线 i ∈ [0, kNavLines) 在行 r 的批收益 (线序见 Table.hpp kNavLines)
inline double nav_value(const factor::stat::Row &r, int i, double cap) {
  if (!r.ok)
    return 0.0;
  using factor::stat::kBot, factor::stat::kTop, factor::stat::end_group;
  if (i >= 2 * kNavLayers) {
    const bool absolute = i < 2 * kNavLayers + kNavLs;
    const int j = i - 2 * kNavLayers - (absolute ? 0 : kNavLs);
    if (j == 0)
      return absolute ? 0.5 * (group_abs(r, end_group(kTop), cap) - group_abs(r, end_group(kBot), cap)) : factor::stat::ls_row(r);
    return absolute ? 0.5 * (tail_abs(r, kTop, j - 1, cap) - tail_abs(r, kBot, j - 1, cap)) : 0.5 * (r.tail[kTop][j - 1] - r.tail[kBot][j - 1]);
  }
  const bool absolute = i < kNavLayers;
  const int k = absolute ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return absolute ? group_abs(r, k, cap) : r.grp[k];
  const int e = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  return absolute ? tail_abs(r, e, j, cap) : r.tail[e][j];
}
// 超额线 i ∈ [0, kNavExcess) 在 kNavLines 线序里的号
inline constexpr int nav_excess(int i) { return i < kNavLayers ? kNavLayers + i : kNavLayers + kNavLs + i; }

// 持有期 only 在 hd 里的下标; only < 0 (全部持有期) → −1
inline int only_of(const Holds &hd, int only) {
  if (only < 0)
    return -1;
  const int i = hd.at(static_cast<HoldId>(only));
  assert(i >= 0 && "only 须是本层标签组之一");
  return i;
}

struct NavP : LinesP {
  int only = -1; // 调用方给: HoldId → 只出它的 kNavExcess 条超额线; −1 = 全部持有期 × kNavLines
  int sel = -1;
  double cap = 0.0;
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(kNavLines) * H * sh.T; }
  void init(const PassCtx &p) {
    sel = only_of(p.hd, only);
    stride = p.seg;
    lines(p, only < 0 ? p.hd.n * kNavLines : kNavExcess, [&](int i) { return only < 0 ? p.hd.k[i / kNavLines] : static_cast<HoldId>(only); });
  }
  void open(const Ctx &c) {
    LinesP::open(c);
    cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r) {
    if (only >= 0 && hi != sel)
      return;
    if (only < 0)
      for (int i = 0; i < kNavLines; ++i)
        at(hi * kNavLines + i, t) = static_cast<float>(nav_value(r, i, cap));
    else
      for (int i = 0; i < kNavExcess; ++i)
        at(i, t) = static_cast<float>(nav_value(r, nav_excess(i), cap));
  }
};

template <class... X>
using EvalProbes = Probes<HoldP, TurnP, CoverP, AlphaP, IcPdfP, GrpP, X...>;

// 一个持有期的分年 (评估期日 dates 按自然年; 只列 ok 行 ≥ 3 的年): 逐日 Day + 窗 rIC (HoldP) / 组和 (GrpP) / 整组两端买量 (TurnP)
std::vector<YearStat> years_of(const std::vector<misc::Ymd> &dates, const HoldP &h, const GrpP &g, const TurnP &c, int hi, HoldId hold);

template <class Ps>
void fill_eval(const Ps &p, const PassCtx &pc, const StatScope &scope, double eval_ms, EvalStat &st) {
  const HoldP &h = p.template at<HoldP>();
  const TurnP &c = p.template at<TurnP>();
  const IcPdfP &ic = p.template at<IcPdfP>();
  const CoverP &cv = p.template at<CoverP>();
  const GrpP &g = p.template at<GrpP>();
  const std::vector<misc::Ymd> dates = pc.dm.stat_dates();
  st.scope = scope;
  st.eval_ms = eval_ms;
  st.valid_pct = cv.valid_pct;
  st.rank_pct = cv.rank_pct;
  st.n_hold = h.n_hold;
  std::copy_n(h.hold, h.n_hold, st.hold);
  st.amts = c.amts;
  std::copy_n(c.turn, h.n_hold, st.turn);
  for (int hi = 0; hi < h.n_hold; ++hi) {
    st.ic_pdf[hi] = mini_pdf_of(ic.pdf[hi]);
    st.years[hi] = years_of(dates, h, g, c, hi, pc.hd.k[hi]);
    st.roll[hi] = h.roll[hi];
  }
  st.alpha_pdf = mini_pdf_of(p.template at<AlphaP>().d.pdf);
}

} // namespace GUI::Engine
