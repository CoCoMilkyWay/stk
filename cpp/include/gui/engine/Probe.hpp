// Probe — 探针层 (一个根的一种输出; 双层严格对仗生命周期): Load / Ledger 之上, Engine 之下.
//   趟级: init(PassCtx) ⇄ finish(PassCtx)    (PassCtx = pass_ctx(作业); 首块前一次定形/置初值, 末块后好根一次收尾; 常驻 bytes)
//   块级: open(Ctx)     ⇄ close(Ctx, RootView) (Ctx 带块上标签 + 逐根评估口径 Mode; 中间夹逐统计行 row(hi, t, r); 暂存 scratch)
//   落地三形 (行轴 = 评估期 rows_stat): RowsP<D, Got> (逐行暂存 → 折) / LandCellsP<V> (逐格) / LinesP (净值线: RowLines).
//   通用五探针: HoldP (逐日 Sum + 三级 + 滚动窗) ⇄ TurnP (逐日账集流水) / CoverP / AlphaP / IcPdfP → EvalProbes<X...> → fill_eval;
//   全期 / 分年同一 finish: 逐日 Sum 之和 stat_of ⇄ 逐日流水之和 turn_annual (分年一律 by_year, 只在 fill_eval 汇成 YearStat).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp" // Workspace
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Roll.hpp"  // Day / roll_of
#include "gui/engine/Ledger.hpp" // Realize
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
// 行 r (组 / 尾部按口径 from 存) 的第 k 组持仓收益改读成口径 to; 不 ok → 0
inline double group_as(const factor::stat::Row &r, int k, factor::stat::Ret from, factor::stat::Ret to) {
  return r.ok ? factor::stat::book_as(r.grp[k], r.cnt[k], r.mkt, r.cap, from, to) : 0.0;
}

// ---- 探针契约与折叠容器 Probes<P...> ----
enum NeedBit : uint8_t {
  kNeedRows = 1,
  kNeedRx = 2,
  kNeedMask = 4,
  kNeedAlpha = 8,
  kNeedX = 16
};

struct RootView {
  factor::stat::Plane x;
  factor::stat::Plane alpha;
  const uint16_t *rx = nullptr;
  const factor::stat::Sum *day = nullptr; // [H][D] 本块逐日 Sum (已定向, D = 本块统计日数)
};

// 趟级上下文 (init / finish 共用)
struct PassCtx {
  misc::exec::ForkJoin &ex;
  const Domain &dm;
  Holds hd;
  Amts ad;
  int seg = 0, A = 0;
  int days_stat = 0, rows_stat = 0;
};
inline PassCtx pass_ctx(const Job &j, misc::exec::ForkJoin &ex) {
  const int seg = j.ft->seg, ds = static_cast<int>(j.dm.stat_days());
  return {ex, j.dm, j.spec.y.hd, j.spec.y.ad, seg, static_cast<int>(j.dm.rs.axis.size()), ds, ds * seg};
}

// 块级上下文 (open / close 共用): 块上的标签 L 同块各根共用, 评估口径 m 逐根
struct Ctx {
  misc::exec::ForkJoin &ex;
  const Block &c;
  const factor::stat::Labels &L;
  factor::cpu::stat::Workspace *w = nullptr;
  factor::stat::Mode m;
  bool last = false;
  factor::stat::Span s() const { return c.span(); }
};

// 探针组 (本身即 Stat 的行 sink: row(hi, t, r) 逐行收, 行已按所评方向)
template <class... P>
struct Probes {
  std::tuple<P...> p;
  static constexpr uint8_t need_each = (uint8_t{0} | ... | P::need);
  static constexpr uint8_t need_last = (uint8_t{0} | ... | P::need_last);
  static constexpr uint8_t need = need_each | need_last;
  static constexpr uint8_t need_at(bool last) { return last ? need : need_each; }
  static constexpr Segs segs = std::max({Segs::None, P::segs...});
  static constexpr bool kCells = (false || ... || P::kCells); // 有探针要真持有格 (只 CPU 路径交; Stat/Cpu.hpp sink.cell)
  template <class Q>
  Q &at() { return std::get<Q>(p); }
  template <class Q>
  const Q &at() const { return std::get<Q>(p); }
  static constexpr Bytes bytes(int H) { return (Bytes{} + ... + P::bytes(H)); }
  static constexpr Bytes scratch(int H) { return (Bytes{} + ... + P::scratch(H)); }
  void init(const PassCtx &c) {
    std::apply([&](P &...q) { (q.init(c), ...); }, p);
  }
  void open(const Ctx &c) {
    std::apply([&](P &...q) { (q.open(c), ...); }, p);
  }
  void row(int hi, int t, const factor::stat::Row &r) {
    std::apply([&](P &...q) { (q.row(hi, t, r), ...); }, p);
  }
  void cell(int hi, int t, int a, int g, const factor::stat::Pos &x, const float *y) {
    std::apply([&](P &...q) { (q.cell(hi, t, a, g, x, y), ...); }, p);
  }
  void close(const Ctx &c, const RootView &v) {
    std::apply([&](P &...q) { (q.close(c, v), ...); }, p);
  }
  void finish(const PassCtx &c) {
    std::apply([&](P &...q) { (q.finish(c), ...); }, p);
  }
};

// bytes = 趟级常驻 (init 定形, 跨块) / scratch = 块级暂存 (open 定形); 都是线性字节式
struct ProbeBase {
  static constexpr uint8_t need = 0;
  static constexpr uint8_t need_last = 0;
  static constexpr Segs segs = Segs::None;
  static constexpr bool kCells = false;
  static constexpr Bytes bytes(int) { return {}; }
  static constexpr Bytes scratch(int) { return {}; }
  void init(const PassCtx &) {}
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &) {}
  void cell(int, int, int, int, const factor::stat::Pos &, const float *) {}
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

// 逐日 Sum[H][评估期日] (各块按绝对日拼回) → 三级 HoldStat[H] (毛; Fold stat_of: 全期之和 + 日净值 LS) + 滚动窗 roll_of (正向, 行已按所评方向) + 收尾日的窗 mono wins
struct HoldP : ProbeBase {
  int seg = 0, n_hold = 0;
  std::vector<factor::stat::Sum> day[kHoldCount]; // [hd][评估期日]
  factor::stat::HoldStat hold[kHoldCount];
  factor::stat::Roll roll[kHoldCount];
  std::vector<double> wins[kHoldCount]; // [hd][评估期日] 收尾日的窗 mono (不计入 = NaN)
  static constexpr Bytes bytes(int H) { return {.day = static_cast<double>(sizeof(factor::stat::Sum)) * H}; }
  void init(const PassCtx &p) {
    seg = p.seg;
    for (int i = 0; i < p.hd.n; ++i)
      day[i].assign(static_cast<size_t>(p.days_stat), {});
  }
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const int D = s.days(), d0 = (c.c.land0() + s.t_lo) / seg;
    for (int i = 0; i < c.L.hd.n; ++i)
      std::copy_n(v.day + static_cast<size_t>(i) * D, D, day[i].begin() + d0);
  }
  void finish(const PassCtx &p) {
    n_hold = p.hd.n;
    for (int i = 0; i < n_hold; ++i) {
      hold[i] = factor::stat::stat_of(day[i], p.hd.k[i], seg);
      roll[i] = factor::stat::roll_of(day[i], p.hd.k[i], seg, 1, &wins[i]);
    }
  }
  // 持有期 hi 第 i 日 ok 行 rIC 均值 (无 ok 行 = NaN)
  float day_ric(int hi, size_t i) const { return static_cast<float>(day[hi][i].ric_mean()); }
};

// 换手 (Stat/Book.hpp 因子账集, 每块每持有期一趟 book_rows): 逐日 (建仓日) 各层级 × 两端的流水 day[H][评估期日] (各金额段同一份量, 盘口段另各一份冲击)
//   → 全期 turn[H] = 逐日之和 (与 HoldP 的逐日 Sum 对仗; 分年 = 年内之和)
struct TurnP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  static constexpr Segs segs = Segs::All;
  int seg = 0;
  Amts amts;
  std::vector<TurnStat> day[kHoldCount]; // [hd][评估期日]
  TurnStat turn[kHoldCount];
  static constexpr Bytes bytes(int H) { return {.day = static_cast<double>(sizeof(TurnStat)) * H}; }
  void init(const PassCtx &p) {
    seg = p.seg;
    amts = p.ad;
    for (int i = 0; i < p.hd.n; ++i)
      day[i].assign(static_cast<size_t>(p.days_stat), TurnStat{});
  }
  void close(const Ctx &c, const RootView &v);
  void finish(const PassCtx &p) {
    for (int i = 0; i < p.hd.n; ++i) {
      turn[i] = TurnStat{};
      for (const TurnStat &d : day[i])
        turn[i].merge(d);
    }
  }
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
    alpha_dist(c.ex, v.alpha.v + o, v.alpha.m + o, v.rx + o, s.t_hi() - s.t_lo, s.A, c.m.d, d);
  }
};

// 落地基类 1: 逐统计行暂存 → close 折进派生状态 (末块即释放暂存)
template <class D, class Got>
struct RowsP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  int t_lo = 0, n = 0;
  std::vector<Got> got;
  static constexpr Bytes scratch(int H) { return {.row = static_cast<double>(sizeof(Got)) * H}; }
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
  static constexpr Bytes bytes(int) { return {.cell = sizeof(V)}; }
  void init(const PassCtx &p) { dst->assign(static_cast<size_t>(p.rows_stat) * p.A, V{}); }
  V *at(const Ctx &c) const { return dst->data() + static_cast<size_t>(c.c.land0() + c.c.t_lo) * c.s().A; }
};

// 落地基类 3: 净值线 (init 由派生类 lines() 定形; 块内逐行暂存 at(线, 行), close 时本块逐线折进 RowLines; finish 交出期线)
struct LinesP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  RowLines rl;
  LedgerLines out;
  // n 条线, 线 i 的持有期 hold_of(i) / 是否平仓行实现 at_exit(i); stride = 每几行并一期
  template <class HoldOf, class AtExit>
  void lines(const PassCtx &p, int n, int stride, HoldOf &&hold_of, AtExit &&at_exit) {
    std::vector<factor::stat::Realize> rz;
    for (int i = 0; i < n; ++i)
      rz.emplace_back(hold_of(i), p.seg, stride, p.rows_stat, at_exit(i));
    rl.reset(std::move(rz));
  }
  void open(const Ctx &c) {
    assert(rl.rz[0].T == c.c.rows_stat() && "净值线与作用域不符");
    const factor::stat::Span s = c.s();
    rl.open(s.t_lo, s.t_hi(), c.c.land0() + s.t_lo);
  }
  float &at(int i, int t) { return rl.at(i, t); }
  void close(const Ctx &c, const RootView &) {
    rl.close(c.ex);
    if (c.last)
      rl.drop();
  }
  void finish(const PassCtx &) {
    out = std::move(rl.out);
    rl = RowLines{};
  }
};

inline float ric_of(const factor::stat::Row &r) { return r.ok ? r.ric : std::numeric_limits<float>::quiet_NaN(); }

// 逐行 rIC 分布; dst 非空 → 另把逐行 rIC 落地 [hd][rows_stat] (无 ok = NaN; Inspect 的 rIC 时序)
struct IcPdfP : RowsP<IcPdfP, float> {
  std::vector<KLLcache> kll;
  AggPdf pdf[kHoldCount];
  std::vector<float> *dst = nullptr;
  static float of(const factor::stat::Row &r) { return ric_of(r); }
  void init(const PassCtx &p) {
    kll.clear();
    kll.reserve(static_cast<size_t>(p.hd.n));
    for (int i = 0; i < p.hd.n; ++i)
      kll.emplace_back(kAggKllCapacity, kAggKllResolution);
    if (dst)
      dst->assign(static_cast<size_t>(p.hd.n) * static_cast<size_t>(p.rows_stat), std::numeric_limits<float>::quiet_NaN());
  }
  void fold(const Ctx &c, int hi, int u0, std::span<const float> g) {
    if (dst)
      std::copy(g.begin(), g.end(), dst->begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(hi) * static_cast<size_t>(c.c.rows_stat()) + static_cast<size_t>(u0)));
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

// 分层 / 尾部 / LS 的逐行批收益 → Realize 日净值线 (Inspect: 全部持有期 × kNavLines; agent eval_factor: 一个持有期 × kNavExcess)
inline double tail_as(const factor::stat::Row &r, int e, int j, factor::stat::Ret from, factor::stat::Ret to) {
  return r.ok ? factor::stat::book_as(r.tail[e][j], r.tail_n[e][j], r.mkt, r.cap * factor::stat::tail_frac(j), from, to) : 0.0;
}
// 线 i ∈ [0, kNavLines) 在行 r 的批收益 (线序见 Table.hpp kNavLines; 行的组 / 尾部按因子的收益口径 ret 存, 线按自己的口径改读)
inline double nav_value(const factor::stat::Row &r, int i, factor::stat::Ret ret) {
  if (!r.ok)
    return 0.0;
  using factor::stat::kBot, factor::stat::kTop, factor::stat::end_group, factor::stat::Ret;
  if (i >= 2 * kNavLayers) {
    const Ret to = i < 2 * kNavLayers + kNavLs ? Ret::Abs : Ret::Excess;
    const int j = i - 2 * kNavLayers - (to == Ret::Abs ? 0 : kNavLs);
    if (j == 0)
      return 0.5 * (group_as(r, end_group(kTop), ret, to) - group_as(r, end_group(kBot), ret, to));
    return 0.5 * (tail_as(r, kTop, j - 1, ret, to) - tail_as(r, kBot, j - 1, ret, to));
  }
  const Ret to = i < kNavLayers ? Ret::Abs : Ret::Excess;
  const int k = to == Ret::Abs ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return group_as(r, k, ret, to);
  const int e = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  return tail_as(r, e, j, ret, to);
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
  factor::stat::Ret ret = factor::stat::Ret::Excess; // 行里组 / 尾部的收益口径 (= 根的)
  static constexpr Bytes bytes(int H) { return {.day = static_cast<double>(sizeof(float)) * kNavLines * H}; }
  static constexpr Bytes scratch(int H) { return {.row = static_cast<double>(sizeof(float)) * kNavLines * H}; }
  void init(const PassCtx &p) {
    sel = only_of(p.hd, only);
    lines(p, only < 0 ? p.hd.n * kNavLines : kNavExcess, p.seg, [&](int i) { return only < 0 ? p.hd.k[i / kNavLines] : static_cast<HoldId>(only); }, [](int) { return true; });
  }
  void open(const Ctx &c) {
    LinesP::open(c);
    ret = c.m.r;
  }
  void row(int hi, int t, const factor::stat::Row &r) {
    if (only >= 0 && hi != sel)
      return;
    if (only < 0)
      for (int i = 0; i < kNavLines; ++i)
        at(hi * kNavLines + i, t) = static_cast<float>(nav_value(r, i, ret));
    else
      for (int i = 0; i < kNavExcess; ++i)
        at(i, t) = static_cast<float>(nav_value(r, nav_excess(i), ret));
  }
};

template <class... X>
using EvalProbes = Probes<HoldP, TurnP, CoverP, AlphaP, IcPdfP, X...>;

// 一个持有期的分年 (评估期日 dates 按自然年; 只列 ok 行 ≥ 3 的年): 年内逐日 Sum 之和 stat_of + 窗 mono (HoldP) / 年内逐日流水之和 (TurnP)
std::vector<YearStat> years_of(const std::vector<misc::Ymd> &dates, const HoldP &h, const TurnP &c, int hi, HoldId hold);

template <class Ps>
void fill_eval(const Ps &p, const PassCtx &pc, const StatScope &scope, double eval_ms, EvalStat &st) {
  const HoldP &h = p.template at<HoldP>();
  const TurnP &c = p.template at<TurnP>();
  const IcPdfP &ic = p.template at<IcPdfP>();
  const CoverP &cv = p.template at<CoverP>();
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
    st.years[hi] = years_of(dates, h, c, hi, pc.hd.k[hi]);
    st.roll[hi] = h.roll[hi];
  }
  st.alpha_pdf = mini_pdf_of(p.template at<AlphaP>().d.pdf);
}

} // namespace GUI::Engine
