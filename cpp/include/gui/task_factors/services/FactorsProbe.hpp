// FactorsProbe — 探针层 (一个根的一种输出; 引擎 FactorsEngine 按 need 备原料, 逐钩子调): 装载层 (FactorsLoad) / 净值引擎 (FactorsBook) 之上, 引擎之下.
//   原料 RootView / 上下文 Ctx / 抽样行 Ranks 两后端同形; Probes<P...> 逐钩子折叠;
//   通用探针 Hold / Cost / Cover / Alpha / IcPdf 四服务共用, 服务私有探针 (Search 抽样 / Inspect 净值线 / 秩 / 尾部换手 / Portfolio 出参线)
//   在各自 .cpp; 落地探针的三种形 LandRowsP (逐行) / LandCellsP (逐格) / LinesP (净值线: 逐行批收益 → finish 折成 BookLines);
//   根上的统计核: cover_rows 有效率 / 可秩率 (一遍, 块间可加); 换手成本直接走 Stat cost_rows (CostP / TurnP / CurveP 平级); alpha_dist alpha 分布
//   (Inspect 大图与因子表小图同一份); group_abs 组的绝对持仓收益 (Inspect 分层 / LS 绝对 / Portfolio 曲线).
// include Stat CPU 流程头 (受控浮点): 只给 CMake PRECISE_MATH 源.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"                       // Workspace / NoExt
#include "gui/task_factors/services/FactorsBook.hpp" // RowLines / fold_book
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"
#include "shared/Analysis.hpp" // AggPdf / KLLcache

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace GUI::Factors {

// ---- 根上的统计核 ----
// 覆盖 (块间可加; gate = 截面池 cs_valid, 池外格不进分子分母):
//   有效率 on / all = Σ(m ∧ gate) / Σgate;
//   可秩率 sum / rows: 逐行 = 所在并列块 (同 r16) 不超过 ⌈nv / kGroups⌉ 只的格数 / nv (nv = 该行 m ∧ gate 格数; 整行 kRankNone → 0), 只计 nv ≥ 1 的行
struct Cover {
  uint64_t on = 0, all = 0;
  double sum = 0.0;
  long long rows = 0;
  void merge(const Cover &o) { on += o.on, all += o.all, sum += o.sum, rows += o.rows; }
};
// 行 [t0, t1) 的覆盖折进 o (条带并行; 并列块用每线程一张 r16 计数表, 每行线性). rx = 该因子的 r16 [T][A] (rank_x 出)
void cover_rows(misc::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int t0, int t1, int A, Cover &o);
// alpha 分布 (AlphaDist, FactorsTable.hpp) 两趟, 每趟条带并行: 每线程一份 KLL + 缓冲批量 addBatch, 末尾合并进线程 0 那份.
//   趟 1: 全体样本 → 矩 + 截尾范围 [lo, hi]; 同趟逐行: 截面均值 (series) + 各组 min / max → 相邻组边界中点, 各组和 / 数
//   趟 2: 只收 [lo, hi] 内样本 → pdf. d = 所评方向 (反向: 值取负, 组倒序; Stat 契约【方向 Dir】)
void alpha_dist(misc::exec::ForkJoin &ex, const float *v, const uint8_t *m, const uint16_t *rx, int T, int A, int d, AlphaDist &o);
// 因子表迷你 PDF: AggPdf → MiniPdf (n_pts < 2 → 空)
MiniPdf mini_pdf_of(const analysis::AggPdf &p);
// 组 k 的绝对持仓收益 (超额 + 只数 · 基准 / 持仓分母; Stat 契约【持仓口径】; 不 ok → 0)
inline double group_abs(const factor::stat::Row &r, int k, double cap) {
  return r.ok ? factor::stat::book_abs(r.grp[k], r.cnt[k], r.mkt, cap) : 0.0;
}

// ---- 探针 (一个根的一种输出; 钩子同形, Probes<P...> 逐钩子折叠) ----
//   need / need_last  原料位: 每块要的 / 只末块另要的 (引擎据此算 / 拷回; 本块给 Probes::need_at(last))
//   segs              要装的成本段 (Segs; Probes 取各探针之最, 管线据此定 YSpec::ad); 持有期在根上 (Root::hd)
//   Ext ext()         行的逐格扩展 (accum_rows 的 label_row 逐格; NoExt = 无; 有则只 CPU)
//   open(c)           本块评前 (单线程; 落地平面在评估域首块定形)
//   row(hi, t, r, e)  一级行 (accum_rows 线程内并发, 行不相交; t = 块内统计行 ∈ [t_lo, t_hi), hi = 根的 hd 下标); r 已按根的方向定向 (oriented)
//   root(c, v)        本块原料就绪 (全核); v.acc / v.ec 已按根的方向, v.rx 是原值的秩 (按 c.d 自取)
//   finish(c, v)      末块 root 之后 (c / v 同 root, v 另有 need_last)
//   bytes(sh, H)      宿主常驻 (字节; 每根一份, 留到 finish; 预算按同时在评的根数计)
//   scratch(sh, H)    宿主暂存 (字节; 一个根 open → root 内, 根间不共存; 预算计一份)
// 小输出存成员 (调用方 at<P>() 取), 大平面落调用方给的缓冲 (指针; 跨块按绝对行 t0_abs + t)
enum Need : uint8_t {
  kNeedRows = 1,   // 一级 Row 进 row 钩子
  kNeedRx = 2,     // r16(x) [T][A]
  kNeedEc = 4,     // 端码 [T][A]
  kNeedMask = 8,   // 根掩码 [T][A]
  kNeedAlpha = 16, // 根的输入平面
  kNeedX = 32,     // 根值 [T][A]
  kNeedRanks = 64  // r16 抽样行 (Ranks; GPU 批核也给)
};
// 根的 r16 抽样行 (CPU 拷宿主 rx / GPU 批核 search_ranks): 行 t0 + i·stride (i < n_rows) → out[n_rows][A]
struct Ranks {
  void *src = nullptr;
  int k = 0;
  void (*fn)(void *src, int k, int t0, int stride, int n_rows, uint16_t *out) = nullptr;
  void operator()(int t0, int stride, int n_rows, uint16_t *out) const {
    assert(fn && "本路径无抽样行");
    fn(src, k, t0, stride, n_rows, out);
  }
};
// 一个根在本块的原料 (两后端同形; 块内 [T][A]); 缺的 (need 没要) 为空
struct RootView {
  factor::stat::Plane x;     // 根: v 要 kNeedX, m 要 kNeedMask
  factor::stat::Plane alpha; // 根的输入 (kNeedAlpha)
  factor::stat::Frame f = factor::stat::Frame::CS;
  const uint16_t *rx = nullptr;             // r16(x) (kNeedRx)
  const uint8_t *ec = nullptr;              // 端码 (kNeedEc)
  const factor::stat::Accum *acc = nullptr; // [L.hd.n] 本块累加
  Ranks ranks;                              // (kNeedRanks)
};
// 探针的上下文 (一块; 引擎 EngineBase::roots 建, 同根的 open / root / finish 同一份)
struct Ctx {
  misc::exec::ForkJoin &ex;
  const Block &c;
  const factor::stat::Labels &L;             // 根所评持有期 (Root::hd) 的标签 + cap (引擎会话那份: CPU 的 ry 指块上的标签秩)
  factor::cpu::stat::Workspace *w = nullptr; // Stat 宿主工作区 (CPU / GPU 逐根时有; 批核空)
  factor::stat::Dir d = factor::stat::kFwd;  // 根的方向 (Root::d)
  factor::stat::Span s() const { return c.span(); }
};
// 落地平面: 评估域首块 (t0_abs = 0) 定形填 fill, 其后各块按绝对行写
template <class T>
void land_open(std::vector<T> &v, size_t n, const factor::stat::Span &s, T fill = T{}) {
  if (s.t0_abs == 0)
    v.assign(n, fill);
  assert(v.size() == n && "落地平面与作用域不符");
}

template <class... P>
struct Probes {
  std::tuple<P...> p;
  static constexpr uint8_t need_each = (uint8_t{0} | ... | P::need);
  static constexpr uint8_t need_last = (uint8_t{0} | ... | P::need_last);
  static constexpr uint8_t need = need_each | need_last; // 全部可能要的 (定后端路径)
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
  void open(const Ctx &c) {
    std::apply([&](P &...q) { (q.open(c), ...); }, p);
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &e) {
    [&]<size_t... I>(std::index_sequence<I...>) { (std::get<I>(p).row(hi, t, r, std::get<I>(e.e)), ...); }(std::index_sequence_for<P...>{});
  }
  void root(const Ctx &c, const RootView &v) {
    std::apply([&](P &...q) { (q.root(c, v), ...); }, p);
  }
  void finish(const Ctx &c, const RootView &v) {
    std::apply([&](P &...q) { (q.finish(c, v), ...); }, p);
  }
};
// accum_rows 的 sink (Stat/Cpu.hpp): 行按根的方向定向后进探针
template <class Ps>
struct ProbeSink {
  Ps *ps;
  factor::stat::Dir d;
  using Ext = typename Ps::Ext;
  Ext ext() const { return ps->ext(); }
  void row(int i, int t, const factor::stat::Row &r, const Ext &e) const { ps->row(i, t, factor::stat::oriented(r, d), e); }
};

// 探针的空钩子 (各探针按需遮蔽)
struct ProbeBase {
  using Ext = factor::cpu::stat::NoExt;
  static constexpr uint8_t need_last = 0;
  static constexpr Segs segs = Segs::None;
  static double bytes(const Shape &, int) { return 0.0; }
  static double scratch(const Shape &, int) { return 0.0; }
  Ext ext() const { return {}; }
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &, const Ext &) {}
  void root(const Ctx &, const RootView &) {}
  void finish(const Ctx &, const RootView &) {}
};
// 二级 HoldStat[H] (毛): 块间 Accum 续攒 → finish_all
struct HoldP : ProbeBase {
  static constexpr uint8_t need = 0;
  factor::stat::Accum acc[factor::stat::kMaxHold];
  int n_hold = 0;
  factor::stat::HoldStat hold[factor::stat::kMaxHold];
  void root(const Ctx &c, const RootView &v) {
    for (int i = 0; i < c.L.hd.n; ++i)
      acc[i].merge(v.acc[i]);
  }
  void finish(const Ctx &c, const RootView &) {
    n_hold = c.L.hd.n;
    factor::stat::finish_all(acc, c.L.hd, c.c.y.sh.seg, hold);
  }
};
// 换手成本 CostTier[C] (契约【换手成本】; C ≥ 1, 换手量各段同一份): Stat cost_rows (端码 ec × 全部金额段 costs_of(y)) 块间续攒.
// 成本三探针同一个核: CostP 分段攒 / Inspect TurnP 尾部端码 (一个固定段) / Portfolio CurveP 另落逐行 (land)
struct CostP : ProbeBase {
  static constexpr uint8_t need = kNeedEc;
  static constexpr Segs segs = Segs::All;
  std::vector<CostTier> costs; // [amt] = y.ad 序 (首块按 y.ad 建)
  void root(const Ctx &c, const RootView &v);
};
// 池内有效率 valid% / 可秩率 rank% (统计行; 一遍 cover_rows). 因子在池内全无效 → rank% = 0
struct CoverP : ProbeBase {
  static constexpr uint8_t need = kNeedRx | kNeedMask;
  Cover cv;
  float valid_pct = 0.f, rank_pct = 0.f;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    cover_rows(c.ex, v.rx, v.x.m, c.c.y.gate.data(), s.t_lo, s.t_hi(), s.A, cv);
  }
  void finish(const Ctx &, const RootView &) {
    assert(cv.all > 0 && "截面池全空: 区间内无在池格");
    valid_pct = static_cast<float>(100.0 * static_cast<double>(cv.on) / static_cast<double>(cv.all));
    rank_pct = cv.rows > 0 ? static_cast<float>(100.0 * cv.sum / static_cast<double>(cv.rows)) : 0.f;
  }
};
// alpha 分布 (与持有期无关; 两趟扫张量, 不跨块合并 → 只取末块的统计行: 整期一块 = 全期, 分块 = 最近一块)
struct AlphaP : ProbeBase {
  static constexpr uint8_t need = 0;
  static constexpr uint8_t need_last = kNeedAlpha | kNeedRx;
  AlphaDist d;
  void finish(const Ctx &c, const RootView &v) {
    assert(c.c.d1 == c.c.nd && c.c.t_back == 0 && "末块止于评估期末");
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    alpha_dist(c.ex, v.alpha.v + o, v.alpha.m + o, v.rx + o, s.t_hi() - s.t_lo, s.A, c.d, d);
  }
};
// 逐行 rIC 分布 (ok 行; 每持有期一份 KLL, 块间续攒) → 因子表小图 / Inspect 左下左半 (同一份成品; n < 3 与 Accum::finish 同口径: 不画)
struct IcPdfP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  int T = 0;
  std::vector<float> ric;    // 本块 [hi][T] (非 ok = NaN), root 收进 kll 即放
  std::vector<KLLcache> kll; // [hi]
  analysis::AggPdf pdf[factor::stat::kMaxHold];
  static double scratch(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(H) * sh.T; }
  void open(const Ctx &c) {
    T = c.s().T;
    ric.assign(static_cast<size_t>(c.L.hd.n) * T, std::numeric_limits<float>::quiet_NaN());
    while (kll.size() < static_cast<size_t>(c.L.hd.n))
      kll.emplace_back(analysis::kAggKllCapacity, analysis::kAggKllResolution);
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    if (r.ok)
      ric[static_cast<size_t>(hi) * T + t] = r.ric;
  }
  void root(const Ctx &c, const RootView &) {
    std::vector<float> ok;
    ok.reserve(static_cast<size_t>(T));
    for (int i = 0; i < c.L.hd.n; ++i) {
      ok.clear();
      for (int t = 0; t < T; ++t)
        if (const float v = ric[static_cast<size_t>(i) * T + t]; !std::isnan(v))
          ok.push_back(v);
      kll[static_cast<size_t>(i)].addBatch(ok);
    }
    ric = {};
  }
  void finish(const Ctx &c, const RootView &) {
    for (int i = 0; i < c.L.hd.n; ++i)
      pdf[i].fill(kll[static_cast<size_t>(i)], 3);
  }
};
// 落地探针的三种形 (探针基类; open 在评估域首块定形, 其后各块按绝对行写):
//   LandRowsP<R>  逐行 [hd.n][rows_all] 进 dst (调用方的缓冲; Inspect RicP): at(hi, t) = 块内行 t 的格
//   LandCellsP<V> 逐格 [rows_all][A] 进 dst (Inspect RxP / RxAllP): stat_at(s) = 本块统计首行的首格
//   LinesP<K>     净值线 (FactorsBook RowLines, 线 hi · K + k; Inspect NavP / Portfolio CurveP): at(hi, k, t) = 块内行 t 这一批的收益;
//                 finish 折成 out (fold_book; stride / at_exit 由派生 open 定: 缺省逐行 / 全收益)
template <class R>
struct LandRowsP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  std::vector<R> *dst = nullptr;
  int T = 0, t0 = 0; // 行轴 rows_all / 本块绝对首行
  static double bytes(const Shape &sh, int H) { return sizeof(R) * static_cast<double>(H) * sh.T; }
  void open(const Ctx &c, R fill = R{}) {
    const factor::stat::Span s = c.s();
    T = c.c.rows_all(), t0 = s.t0_abs;
    land_open(*dst, static_cast<size_t>(c.L.hd.n) * T, s, fill);
  }
  R &at(int hi, int t) const { return (*dst)[static_cast<size_t>(hi) * T + t0 + t]; }
};
template <class V>
struct LandCellsP : ProbeBase {
  std::vector<V> *dst = nullptr;
  static double bytes(const Shape &sh, int) { return sizeof(V) * static_cast<double>(sh.n()); }
  void open(const Ctx &c) { land_open(*dst, static_cast<size_t>(c.c.rows_all()) * c.s().A, c.s()); }
  V *stat_at(const factor::stat::Span &s) const { return dst->data() + static_cast<size_t>(s.t0_abs + s.t_lo) * s.A; }
};
template <int K>
struct LinesP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  RowLines x;
  BookLines out;
  int stride = 1, t0 = 0; // 折点行数 / 本块绝对首行
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(K) * H * sh.T; }
  void open(const Ctx &c) {
    const factor::stat::Span s = c.s();
    if (s.t0_abs == 0) {
      x.reset(c.L.hd.n * K, c.c.rows_all());
      for (int i = 0; i < x.n; ++i)
        x.hold[static_cast<size_t>(i)] = c.L.hd.k[i / K];
    }
    assert(x.n == c.L.hd.n * K && x.T == c.c.rows_all() && "净值线与作用域不符");
    t0 = s.t0_abs;
  }
  float &at(int hi, int k, int t) { return x.x[static_cast<size_t>(hi * K + k) * x.T + t0 + t]; }
  void finish(const Ctx &c, const RootView &) {
    out = fold_book(c.ex, c.c.y.sh.seg, x, stride);
    x = {};
  }
};
// EvalStat 的探针组 (Run 行 / Inspect 结果同一份): 通用五探针在前, X = 服务私有探针
template <class... X>
using EvalProbes = Probes<HoldP, CostP, CoverP, AlphaP, IcPdfP, X...>;
// 作用域头 / 耗时 / 二级 / 换手成本 / 有效率 / 可秩率 / 小图 → EvalStat (EvalProbes 的成品)
template <class Ps>
void fill_eval(const Ps &p, const StatScope &scope, double eval_ms, EvalStat &st) {
  const HoldP &h = p.template at<HoldP>();
  const CostP &c = p.template at<CostP>();
  const IcPdfP &ic = p.template at<IcPdfP>();
  const CoverP &cv = p.template at<CoverP>();
  st.scope = scope;
  st.eval_ms = eval_ms;
  st.valid_pct = cv.valid_pct;
  st.rank_pct = cv.rank_pct;
  st.n_hold = h.n_hold;
  std::copy_n(h.hold, h.n_hold, st.hold);
  st.costs = c.costs;
  for (int hi = 0; hi < h.n_hold; ++hi)
    st.ic_pdf[hi] = mini_pdf_of(ic.pdf[hi]);
  st.alpha_pdf = mini_pdf_of(p.template at<AlphaP>().d.pdf);
}

} // namespace GUI::Factors
