// Probe — 探针层 (一个根的一种输出; 双层严格对仗生命周期): Load / Ledger 之上, Engine 之下.
//   趟级: init(PassCtx) ⇄ finish(PassCtx)    (PassCtx = pass_ctx(作业); 首块前一次定形/置初值, 末块后好根一次收尾; 常驻 bytes)
//   块级: open(Ctx)     ⇄ close(Ctx, RootView) (Ctx 带块上标签 + 逐根评估口径 Mode; 中间夹逐统计行 row(hi, t, r); 暂存 scratch)
//   落地两形 (行轴 = 评估期 rows_stat): LandCellsP<V> (逐格) / LinesP (期线: Lines 直落, 按日切线程).
//   通用五探针: HoldP (逐日 Sum + 三级 + 滚动窗) ⇄ TurnP (逐日账集流水 = BookP 逐日形; 逐行形 FlowP 给组合) / CoverP / AlphaP / IcPdfP → EvalProbes<X...> → fill_eval;
//   全期 / 分年同一 finish: 逐日 Sum 之和 stat_of ⇄ 逐日流水之和 turn_annual (分年一律 by_year, 只在 fill_eval 汇成 YearStat).
#pragma once

#include "factor/Stat/Book.hpp" // book_rows / FactorBooks
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp" // Workspace
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Roll.hpp"  // Day / roll_of
#include "gui/engine/Ledger.hpp" // Lines
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

// ---- 探针契约与折叠容器 Probes<P...> ----
// 根要交给探针的东西: 一级整行 (含 rIC) / 只逐行 rIC / 已定向秩 / 因子掩码 / alpha 平面 / 因子值
enum NeedBit : uint8_t {
  kNeedRows = 1,
  kNeedRx = 2,
  kNeedMask = 4,
  kNeedAlpha = 8,
  kNeedX = 16,
  kNeedRic = 32
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
  LobMask lob = 0;
  int seg = 0, A = 0;
  int days_stat = 0, rows_stat = 0;
};
inline PassCtx pass_ctx(const Job &j, misc::exec::ForkJoin &ex) {
  const int seg = j.ft->seg, ds = static_cast<int>(j.dm.stat_days());
  return {ex, j.dm, j.spec.y.hd, j.spec.y.lob, seg, static_cast<int>(j.dm.rs.axis.size()), ds, ds * seg};
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

// 探针组 (本身即 Stat 的行 sink: row(hi, t, r) 逐行收, 行已按所评方向; 逐行 rIC 另经 ric(hi, t, v) 交, 整行时由 row 顺带)
template <class... P>
struct Probes {
  std::tuple<P...> p;
  static constexpr uint8_t need_each = (uint8_t{0} | ... | P::need);
  static constexpr uint8_t need_last = (uint8_t{0} | ... | P::need_last);
  static constexpr uint8_t need = need_each | need_last;
  static constexpr uint8_t need_at(bool last) { return last ? need : need_each; }
  static constexpr Segs segs = std::max({Segs::None, P::segs...});
  static constexpr bool kCells = (false || ... || P::kCells); // 有探针要真持有格 (只 CPU 路径交; Stat/Cpu.hpp sink.cell)
  // 要 Stat 行核 (逐日 Sum / 一级行 / 逐行 rIC); 否则一个根只秩化 (组合账只要已定向秩)
  static constexpr bool kStat = (false || ... || P::kStat) || (need & (kNeedRows | kNeedRic)) != 0;
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
    ric(hi, t, r.ok ? r.ric : std::numeric_limits<float>::quiet_NaN());
  }
  void ric(int hi, int t, float v) {
    std::apply([&](P &...q) { (q.ric(hi, t, v), ...); }, p);
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
  static constexpr bool kStat = false;
  static constexpr Bytes bytes(int) { return {}; }
  static constexpr Bytes scratch(int) { return {}; }
  void init(const PassCtx &) {}
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &) {}
  void ric(int, int, float) {}
  void cell(int, int, int, int, const factor::stat::Pos &, const float *) {}
  void close(const Ctx &, const RootView &) {}
  void finish(const PassCtx &) {}
};

// ---- 通用探针与两种落地基类 ----

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
  static constexpr bool kStat = true;
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

// 落地基类 1: 逐格 [rows_stat][A] 进 dst (init 一次定形, 各块按 land0 写)
template <class V>
struct LandCellsP : ProbeBase {
  std::vector<V> *dst = nullptr;
  static constexpr Bytes bytes(int) { return {.cell = sizeof(V)}; }
  void init(const PassCtx &p) { dst->assign(static_cast<size_t>(p.rows_stat) * p.A, V{}); }
  V *at(const Ctx &c) const { return dst->data() + static_cast<size_t>(c.c.land0() + c.c.t_lo) * c.s().A; }
};

// 落地基类 2: 期线 (init 由派生类 lines() 定形; 块内逐行 add(线, 块内行, 量) 直落 Lines (按日切线程); finish 交出期线)
struct LinesP : ProbeBase {
  Lines ln;
  LedgerLines out;
  int u0 = 0; // 本块行 0 的评估期行
  // n 条线, 线 i 的持有期 hold_of(i) / 记在平仓行 exit_of(i); stride = 每几行并一期
  template <class HoldOf, class ExitOf>
  void lines(const PassCtx &p, int n, int stride, HoldOf &&hold_of, ExitOf &&exit_of) {
    ln.reset(n, p.seg, stride, p.rows_stat, hold_of, exit_of);
  }
  void open(const Ctx &c) {
    assert(ln.rz[0].T == c.c.rows_stat() && "期线与作用域不符");
    u0 = c.c.land0();
  }
  void add(int i, int t, double x) { ln.add(i, u0 + t, x); }
  void finish(const PassCtx &) {
    out = std::move(ln.out);
    ln = Lines{};
  }
};

// 因子账集流水 (Stat/Book.hpp FactorBooks, 每块每持有期一趟 book_rows) → 期线 [持有期][本][字段]; 都已实现 (÷ h̄, 与 Realize 同约). 两形对仗:
//     逐日 Days (Run / Inspect / Agent): 两端 × 层级全部本, 全部盘口段; 字段 = 流水字段 f (建仓行即付; 收益由行核出, 不重记) → 逐日 TurnStat (全期 / 分年 = 日段之和)
//     逐行 Rows (组合): 只算顶端整组一本, 口径那一段; 字段 0 = 收益 (平仓行实现), 1 + f = 流水字段 f → 账线 (Ledger 收益 + 流水)
enum class BookForm : uint8_t { Days,
                                Rows };
template <BookForm F>
struct BookP : LinesP {
  static constexpr bool kDays = F == BookForm::Days;
  static constexpr uint8_t need = kNeedRx;
  static constexpr Segs segs = kDays ? Segs::All : Segs::Spec;
  static constexpr int kEnds = kDays ? factor::stat::kEnds : 1, kLevels = kDays ? factor::stat::kLevels : 1; // 逐行形 = 顶端整组
  static constexpr int kBooks = kEnds * kLevels;
  static constexpr int kRet = kDays ? 0 : 1; // 收益字段数 (流水字段自 kRet 起)
  static constexpr int kFields = kRet + kFlowFields;
  static constexpr int line(int hi, int b, int f) { return (hi * kBooks + b) * kFields + f; }
  static constexpr int flow_line(int hi, int b, int f) { return line(hi, b, kRet + f); }
  static constexpr Bytes bytes(int H) {
    const double n = static_cast<double>(sizeof(float)) * kBooks * kFields * H;
    return kDays ? Bytes{.day = n} : Bytes{.row = n};
  }
  LobMask lob = 0;
  void init(const PassCtx &p) {
    lob = p.lob;
    lines(p, p.hd.n * kBooks * kFields, kDays ? p.seg : 1, [&](int i) { return p.hd.k[i / (kBooks * kFields)]; }, [](int i) { return i % kFields < kRet; });
  }
  void close(const Ctx &c, const RootView &v) {
    namespace st = factor::stat;
    const st::Span s = c.s();
    for (int hi = 0; hi < c.L.hd.n; ++hi)
      st::book_rows(c.ex, s, hold_on(c.L.hd.k[hi], s.g), c.L.l[hi], c.L.eo, st::FactorBooks{v.rx, s.A, c.L.l[hi].cap, kEnds, kLevels}, c.c.y.cost,
                    [&](int, int t, const double *ret, const TurnFlow *flow) {
                      for (int b = 0; b < kBooks; ++b) {
                        if constexpr (!kDays)
                          add(line(hi, b, 0), t, ret[b]);
                        for (int f = 0; f < kFlowFields; ++f)
                          add(flow_line(hi, b, f), t, flow[b][f]);
                      }
                    });
  }
  // 逐日形: 持有期 hi 第 [d0, d1) 统计日的流水之和 (各层级 × 两端)
  TurnStat turn(int hi, size_t d0, size_t d1) const {
    static_assert(kDays);
    TurnStat o;
    for (int b = 0; b < kBooks; ++b)
      for (int f = 0; f < kFlowFields; ++f) {
        const float *x = out.at(flow_line(hi, b, f));
        double v = 0.0;
        for (size_t d = d0; d < d1; ++d)
          v += x[d];
        o.lv[b % factor::stat::kLevels].e[b / factor::stat::kLevels][f] += v;
      }
    return o;
  }
};
using TurnP = BookP<BookForm::Days>;
using FlowP = BookP<BookForm::Rows>;

// 逐行 rIC 分布 (块内逐行暂存, close 折进 KLL; 末块即释放暂存); dst 非空 → 另把逐行 rIC 落地 [hd][rows_stat] (无 ok = NaN; Inspect 的 rIC 时序)
struct IcPdfP : ProbeBase {
  static constexpr uint8_t need = kNeedRic;
  static constexpr Bytes scratch(int H) { return {.row = static_cast<double>(sizeof(float)) * H}; }
  int t_lo = 0, n = 0;
  std::vector<float> got; // [hd][本块统计行]
  std::vector<KLLcache> kll;
  AggPdf pdf[kHoldCount];
  std::vector<float> *dst = nullptr;
  void init(const PassCtx &p) {
    kll.clear();
    kll.reserve(static_cast<size_t>(p.hd.n));
    for (int i = 0; i < p.hd.n; ++i)
      kll.emplace_back(kAggKllCapacity, kAggKllResolution);
    if (dst)
      dst->assign(static_cast<size_t>(p.hd.n) * static_cast<size_t>(p.rows_stat), std::numeric_limits<float>::quiet_NaN());
  }
  void open(const Ctx &c) {
    const factor::stat::Span s = c.s();
    t_lo = s.t_lo, n = s.t_hi() - s.t_lo;
    got.assign(static_cast<size_t>(c.L.hd.n) * static_cast<size_t>(n), std::numeric_limits<float>::quiet_NaN());
  }
  void ric(int hi, int t, float v) { got[static_cast<size_t>(hi) * static_cast<size_t>(n) + static_cast<size_t>(t - t_lo)] = v; }
  void close(const Ctx &c, const RootView &) {
    const int u0 = c.c.land0() + t_lo;
    for (int hi = 0; hi < c.L.hd.n; ++hi) {
      const std::span<const float> g = std::span<const float>(got).subspan(static_cast<size_t>(hi) * static_cast<size_t>(n), static_cast<size_t>(n));
      if (dst)
        std::copy(g.begin(), g.end(), dst->begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(hi) * static_cast<size_t>(c.c.rows_stat()) + static_cast<size_t>(u0)));
      std::vector<float> ok;
      ok.reserve(g.size());
      for (const float v : g)
        if (!std::isnan(v))
          ok.push_back(v);
      kll[static_cast<size_t>(hi)].addBatch(ok);
    }
    if (c.last)
      std::vector<float>().swap(got);
  }
  void finish(const PassCtx &p) {
    for (int i = 0; i < p.hd.n; ++i)
      pdf[i].fill(kll[static_cast<size_t>(i)], 3);
    std::vector<KLLcache>().swap(kll);
  }
};

// 分层 / 尾部 / LS 的逐行批收益 → Realize 日净值线 (Inspect: 全部持有期 × kNavLines; agent eval_factor: 一个持有期 × kNavExcess)
// 线 i ∈ [0, kNavLines) 在行 r 的批收益 (线序见 Table.hpp kNavLines; Row 恒绝对, 线按自己的口径经契约 grp_as / tail_as / ls_row 读); 不 ok → 0
inline double nav_value(const factor::stat::Row &r, int i) {
  if (!r.ok)
    return 0.0;
  using factor::stat::kBot, factor::stat::kTop, factor::stat::Ret;
  if (i >= 2 * kNavLayers) {
    const Ret to = i < 2 * kNavLayers + kNavLs ? Ret::Abs : Ret::Excess;
    const int j = i - 2 * kNavLayers - (to == Ret::Abs ? 0 : kNavLs);
    return j == 0 ? factor::stat::ls_row(r, to) : 0.5 * (factor::stat::tail_as(r, kTop, j - 1, to) - factor::stat::tail_as(r, kBot, j - 1, to));
  }
  const Ret to = i < kNavLayers ? Ret::Abs : Ret::Excess;
  const int k = to == Ret::Abs ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return factor::stat::grp_as(r, k, to);
  return factor::stat::tail_as(r, (k - factor::stat::kGroups) / kTails, (k - factor::stat::kGroups) % kTails, to);
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
  static constexpr uint8_t need = kNeedRows;
  int only = -1; // 调用方给: HoldId → 只出它的 kNavExcess 条超额线; −1 = 全部持有期 × kNavLines
  int sel = -1;
  static constexpr Bytes bytes(int H) { return {.day = static_cast<double>(sizeof(float)) * kNavLines * H}; }
  void init(const PassCtx &p) {
    sel = only_of(p.hd, only);
    lines(p, only < 0 ? p.hd.n * kNavLines : kNavExcess, p.seg, [&](int i) { return only < 0 ? p.hd.k[i / kNavLines] : static_cast<HoldId>(only); }, [](int) { return true; });
  }
  void row(int hi, int t, const factor::stat::Row &r) {
    if (only >= 0 && hi != sel)
      return;
    if (only < 0)
      for (int i = 0; i < kNavLines; ++i)
        add(hi * kNavLines + i, t, nav_value(r, i));
    else
      for (int i = 0; i < kNavExcess; ++i)
        add(i, t, nav_value(r, nav_excess(i)));
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
  st.lob = c.lob;
  for (int hi = 0; hi < h.n_hold; ++hi) {
    st.turn[hi] = c.turn(hi, 0, dates.size());
    st.ic_pdf[hi] = mini_pdf_of(ic.pdf[hi]);
    st.years[hi] = years_of(dates, h, c, hi, pc.hd.k[hi]);
    st.roll[hi] = h.roll[hi];
  }
  st.alpha_pdf = mini_pdf_of(p.template at<AlphaP>().d.pdf);
}

} // namespace GUI::Engine
