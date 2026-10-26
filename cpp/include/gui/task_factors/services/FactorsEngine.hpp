// FactorsEngine — 因子评估引擎 (四服务同一条管线): 作用域块 (FactorsLoad ScopeCache) → 森林 (Dag) → 逐根原料 (RootView) → 探针元组 (Probes) → 出参.
//   探针 = 一个根的一种输出, 钩子同形 (见 Probes); 四服务的差别只在探针元组 (Run / Search / Inspect / Portfolio 各自声明), 引擎与管线同一份.
//   引擎 Engine<Cpu | Gpu> 同一份接口 (后端只在构造处分叉: with_engine):
//     bind(c)                                绑作用域块: 块 (或其代 gen) 变 → 会话作废, 下次用时重开 (Y 一个块只进会话一次)
//     walk(d, need, visit(i, ms))            顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//     plant<Ps>(cands, C)                    一批候选的共享森林 (C 出 = 实际收的前 C 个): Ps::need = 0 → 只要 Accum, CPU ≤ batch / GPU 再按显存减半;
//                                            否则全收
//     forest(d, need, f, ps, node, done)     走森林, 每个同根首个 k (Dag::first_root) 评一次: 原料按 Ps::need 备齐 → ps(k).root → done(k);
//                                            node(i, ms) 同 walk 的 visit. CPU: 根即算即放 (rank_x → accum_rows 进探针行钩子 → 腿码);
//                                            GPU: Ps::need = 0 → 一批根同在显存一次 search_eval (keep_roots), 否则逐根 stat_eval 只拷回要的原料
//   管线 eval_job (Run / Inspect / Portfolio 同一份; Search 分块自走 plant / forest): 作用域 → 开库 → 预算 → 装块 → plan_forest (值域, 逐根归因) → forest.
// 只给服务 .cpp include (EvalCpu → TS / CS Cpu.hpp, 受控浮点: CMake PRECISE_MATH 源).
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp" // Result
#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace GUI::Factors {

// ---- 探针 (一个根的一种输出; 钩子同形, Probes<P...> 逐钩子折叠) ----
//   need              原料位 (引擎据此算 / 拷回)
//   Ext ext()         行的逐格扩展 (accum_rows 的 label_row 逐格; NoExt = 无; 有则只 CPU)
//   open(c)           本块评前 (单线程; 落地平面在作用域首块定形)
//   row(hi, t, r, e)  一级行 (accum_rows 线程内并发, 行不相交; t = 块内行 ∈ [t_lo, T), hi = Probes::hd 下标)
//   root(c, v)        本块原料就绪 (全核)
//   finish(c)         全部块之后 (c = 末块)
//   bytes(sh, H)      宿主另占 (字节; 预算)
// 小输出存成员 (调用方 at<P>() 取), 大平面落调用方给的缓冲 (指针; 跨块按绝对行 t0_abs + t)
enum Need : uint8_t {
  kNeedRows = 1,  // 一级 Row 进 row 钩子
  kNeedRx = 2,    // r16(x) [T][A]
  kNeedLc = 4,    // 腿码 [T][A]
  kNeedMask = 8,  // 根掩码 [T][A]
  kNeedAlpha = 16 // 根的输入平面
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
  factor::stat::Plane x;     // 根: m 要 kNeedMask; v 只 CPU
  factor::stat::Plane alpha; // 根的输入 (kNeedAlpha)
  factor::stat::Frame f = factor::stat::Frame::CS;
  const uint16_t *rx = nullptr;             // r16(x) (kNeedRx)
  const uint8_t *lc = nullptr;              // 腿码 (kNeedLc)
  const factor::stat::Accum *acc = nullptr; // [L.hd.n] 本块累加
  Ranks ranks;
};
// 探针的上下文 (一块)
struct Ctx {
  misc::exec::ForkJoin &ex;
  const ScopeCache &c;
  const factor::stat::Labels &L;             // 所评持有期 (Probes::hd) 的标签 + cap
  factor::cpu::stat::Workspace *w = nullptr; // Stat 宿主工作区 (CPU / GPU 逐根时有; 批核 / finish 空)
  factor::stat::Span s() const { return c.span(); }
};
// 落地平面: 作用域首块 (t0_abs = 0) 定形清零, 其后各块按绝对行写
template <class T>
void land_open(std::vector<T> &v, size_t n, const factor::stat::Span &s) {
  if (s.t0_abs == 0)
    v.assign(n, T{});
  assert(v.size() == n && "落地平面与作用域不符");
}

template <class... P>
struct Probes {
  std::tuple<P...> p;
  factor::stat::Holds hd; // 只评这些持有期 (n = 0 → Y 全部); 钩子的 hi 按它
  static constexpr uint8_t need = (uint8_t{0} | ... | P::need);
  static constexpr bool cells = (false || ... || !std::is_same_v<typename P::Ext, factor::cpu::stat::NoExt>);
  struct Ext {
    std::tuple<typename P::Ext...> e;
    void cell(unsigned x, bool ok, const float *y) {
      std::apply([&](auto &...q) { (q.cell(x, ok, y), ...); }, e);
    }
  };
  template <class Q>
  Q &at() { return std::get<Q>(p); }
  template <class Q>
  const Q &at() const { return std::get<Q>(p); }
  static double bytes(const Shape &sh, int H) { return (0.0 + ... + P::bytes(sh, H)); }
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
  void finish(misc::exec::ForkJoin &ex, const ScopeCache &c) {
    const factor::stat::Labels L = labels_pick(labels_of(c.y), hd);
    const Ctx cx{ex, c, L, nullptr};
    std::apply([&](P &...q) { (q.finish(cx), ...); }, p);
  }
};
// accum_rows 的 sink (Stat/Cpu.hpp): 行进探针
template <class Ps>
struct ProbeSink {
  Ps *ps;
  using Ext = typename Ps::Ext;
  Ext ext() const { return ps->ext(); }
  void row(int i, int t, const factor::stat::Row &r, const Ext &e) const { ps->row(i, t, r, e); }
};

// 探针的空钩子 (各探针按需遮蔽)
struct ProbeBase {
  using Ext = factor::cpu::stat::NoExt;
  static double bytes(const Shape &, int) { return 0.0; }
  Ext ext() const { return {}; }
  void open(const Ctx &) {}
  void row(int, int, const factor::stat::Row &, const Ext &) {}
  void root(const Ctx &, const RootView &) {}
  void finish(const Ctx &) {}
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
  void finish(const Ctx &c) {
    n_hold = c.L.hd.n;
    factor::stat::finish_all(acc, c.L.hd, c.c.y.sh.seg, hold);
  }
};
// 换手成本 CostTier[C] (契约【换手成本】): 块间续攒
struct CostP : ProbeBase {
  static constexpr uint8_t need = kNeedLc;
  std::vector<CostTier> costs;
  void root(const Ctx &c, const RootView &v) { cost_tiers(c.ex, v.lc, c.s(), c.L, c.c.y, *c.w, costs); }
};
// 池内有效率 valid% (统计行)
struct ValidP : ProbeBase {
  static constexpr uint8_t need = kNeedMask;
  uint64_t on = 0, all = 0;
  float pct = 0.f;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    valid_count(v.x.m, c.c.y.gate.data(), static_cast<size_t>(s.t_lo) * s.A, s.n(), on, all);
  }
  void finish(const Ctx &) {
    assert(all > 0 && "截面池全空: 区间内无在池格");
    pct = 100.f * static_cast<float>(on) / static_cast<float>(all);
  }
};
// 可秩率 rank% (统计行)
struct RankP : ProbeBase {
  static constexpr uint8_t need = kNeedRx | kNeedMask;
  double sum = 0.0;
  long long rows = 0;
  float pct = 0.f;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    rank_sum(c.ex, v.rx, v.x.m, c.c.y.gate.data(), s.t_lo, s.T, s.A, sum, rows);
  }
  void finish(const Ctx &) {
    assert(rows > 0 && "截面池全空: 区间内无在池格");
    pct = static_cast<float>(100.0 * sum / static_cast<double>(rows));
  }
};
// alpha 分布 (与持有期无关; 两趟扫整张量 → 只整块)
struct AlphaP : ProbeBase {
  static constexpr uint8_t need = kNeedAlpha | kNeedRx;
  AlphaDist d;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    assert(s.t0_abs == 0 && s.t_lo == 0 && s.T == c.c.rows_all() && "alpha 分布只整块");
    alpha_dist(c.ex, v.alpha.v, v.alpha.m, v.rx, s.T, s.A, d);
  }
};
// 逐行 rIC 分布 (ok 行; 每持有期一份 KLL, 块间续攒) → 因子表小图 (与 Inspect 左下左半同一组样本)
struct IcPdfP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  int T = 0;
  std::vector<float> ric;    // 本块 [hi][T] (非 ok = NaN), root 收进 kll
  std::vector<KLLcache> kll; // [hi]
  MiniPdf pdf[factor::stat::kMaxHold];
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(H) * sh.T; }
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
  void finish(const Ctx &c) {
    for (int i = 0; i < c.L.hd.n; ++i) {
      analysis::AggPdf p;
      p.fill(kll[static_cast<size_t>(i)], 3);
      pdf[i] = mini_pdf_of(p);
    }
  }
};
// 一级 Row 落地 [hd.n][rows_all]
struct RowsP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  std::vector<factor::stat::Row> *dst = nullptr;
  int T = 0, t0 = 0; // 行轴 rows_all / 本块绝对首行
  static double bytes(const Shape &sh, int H) { return sizeof(factor::stat::Row) * static_cast<double>(H) * sh.T; }
  void open(const Ctx &c) {
    const factor::stat::Span s = c.s();
    T = c.c.rows_all(), t0 = s.t0_abs;
    land_open(*dst, static_cast<size_t>(c.L.hd.n) * T, s);
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) { (*dst)[static_cast<size_t>(hi) * T + t0 + t] = r; }
};
// 二级 / 成本 / 有效率 / 可秩率 → EvalStat (Run 行 / Inspect 结果同一份; scope / eval_ms 归调用方)
template <class Ps>
void fill_eval(const Ps &p, EvalStat &st) {
  const HoldP &h = p.template at<HoldP>();
  st.valid_pct = p.template at<ValidP>().pct;
  st.rank_pct = p.template at<RankP>().pct;
  st.n_hold = h.n_hold;
  std::copy_n(h.hold, h.n_hold, st.hold);
  st.costs = p.template at<CostP>().costs;
}

struct Cpu {};
struct Gpu {};
template <class B>
class Engine;

template <>
class Engine<Cpu> {
public:
  static constexpr const char *kName = "cpu";
  Engine(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, int batch) : ex_(ex), cancel_(cancel), batch_(batch) { assert(batch >= 1); }

  void bind(const ScopeCache &c) {
    if (c_ == &c && gen_ == c.gen)
      return;
    c_ = &c, gen_ = c.gen;
    stat_on_ = false;
  }
  // 宿主另占 (字节 / 格): 活槽 + 标签秩 2H + rx 2 + lc 1
  static double cell_bytes(const factor::Dag &d, int H) { return kPlaneBytes * d.n_slots + 2.0 * H + 3.0; }

  template <class Visit>
  bool walk(const factor::Dag &d, const uint8_t *need, Visit &&visit) {
    assert(c_ && "先 bind");
    in_ = leaves(*c_, d.feats);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::cpu::walk(ex_, d, in_, hp_, sc_, sh.T, sh.A, sh.seg, c_->y.gate.data(), need,
                             [&](int i, const auto &, double ms) { return visit(i, ms); });
  }
  // walk 的 visit 内: 节点 i 的平面 (i 及其输入仍在槽里)
  const factor::check::Plane *plane(int i) const { return factor::cpu::plane_at(*d_, in_, hp_, i); }

  template <class Ps>
  factor::Dag plant(const std::vector<const factor::expr::Expr *> &cands, int &C) const {
    assert(!cands.empty());
    C = Ps::need == 0 ? std::min(batch_, static_cast<int>(cands.size())) : static_cast<int>(cands.size());
    return factor::build_forest(std::vector<const factor::expr::Expr *>(cands.begin(), cands.begin() + C), /*keep_roots=*/false);
  }
  template <class Slot, class Node, class Done>
  bool forest(const factor::Dag &d, const uint8_t *need, const factor::stat::Frame *f, Slot &&ps, Node &&node, Done &&done) {
    using Ps = std::remove_cvref_t<decltype(ps(0))>;
    factor::cpu::stat::StatSession &ss = session();
    const factor::stat::Span s = c_->span();
    rx_.resize(s.n());
    if constexpr ((Ps::need & kNeedLc) != 0)
      lc_.resize(s.n());
    return walk(d, need, [&](int i, double ms) {
      if (!node(i, ms))
        return false;
      for (int k = 0; k < static_cast<int>(d.roots.size()); ++k) {
        if (d.roots[static_cast<size_t>(k)] != i || d.first_root(k) != k)
          continue;
        const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
        assert(nd.op >= 0 && nd.n_in >= 1 && "根是归一算子, 至少一元");
        Ps &p = ps(k);
        const factor::stat::Labels L = labels_pick(ss.L, p.hd);
        const Ctx cx{ex_, *c_, L, &ss.w};
        p.open(cx);
        const factor::check::Plane *rp = plane(i), *ap = plane(nd.in[0]); // alpha = 根的输入: 根的 visit 内仍在槽里
        const factor::stat::Plane x{rp->v.data(), rp->m.data()};
        factor::stat::Accum acc[factor::stat::kMaxHold];
        factor::cpu::stat::rank_x(ex_, x, c_->y.gate.data(), f[k], s, rx_.data(), ss.w);
        factor::cpu::stat::accum_rows(ex_, s, rx_.data(), L, ss.w, acc, ProbeSink<Ps>{&p});
        if constexpr ((Ps::need & kNeedLc) != 0)
          factor::cpu::stat::leg_codes(ex_, rx_.data(), s, lc_.data());
        p.root(cx, RootView{x, {ap->v.data(), ap->m.data()}, f[k], rx_.data(), (Ps::need & kNeedLc) ? lc_.data() : nullptr, acc, Ranks{this, 0, &ranks_of}});
        done(k);
        break;
      }
      return !cancelled();
    });
  }

private:
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }
  // Stat 会话 (Y 的标签秩 + 工作区, 绑定内一次)
  factor::cpu::stat::StatSession &session() {
    assert(c_ && "先 bind");
    if (!stat_on_) {
      const YPlanes &y = c_->y;
      factor::cpu::stat::stat_open(ex_, ss_, y.sh.T, y.sh.A, y.sh.seg, labels_of(y), y.ad.n);
      stat_on_ = true;
    }
    return ss_;
  }
  static void ranks_of(void *src, int, int t0, int stride, int n_rows, uint16_t *out) {
    const Engine &e = *static_cast<const Engine *>(src);
    const int A = e.c_->y.sh.A;
    assert(t0 >= 0 && stride >= 1 && n_rows >= 0 && t0 + (n_rows - 1) * stride < e.c_->y.sh.T);
    for (int i = 0; i < n_rows; ++i)
      std::copy_n(e.rx_.data() + static_cast<size_t>(t0 + i * stride) * A, A, out + static_cast<size_t>(i) * A);
  }

  misc::exec::ForkJoin &ex_;
  const std::atomic<bool> &cancel_;
  int batch_ = 1;
  const ScopeCache *c_ = nullptr;
  uint64_t gen_ = 0;
  const factor::Dag *d_ = nullptr;
  std::vector<const factor::check::Plane *> in_; // = leaves(c, d.feats)
  factor::cpu::HostPool hp_;                     // 峰值活槽 × n × 5B
  std::vector<factor::check::Arena> sc_;
  factor::cpu::stat::StatSession ss_;
  bool stat_on_ = false;
  std::vector<uint16_t> rx_; // 根的 r16
  std::vector<uint8_t> lc_;  // 根的腿码 (kNeedLc)
};

template <>
class Engine<Gpu> {
public:
  static constexpr const char *kName = "gpu";
  Engine(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, int batch) : ex_(ex), cancel_(cancel), batch_(batch) {
    assert(factor::gpu::available() && "GPU 后端不可用");
    assert(batch >= 1 && batch <= factor::gpu::search_max_batch());
  }
  ~Engine() { close(); }
  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;

  void bind(const ScopeCache &c) {
    if (c_ == &c && gen_ == c.gen)
      return;
    close();
    c_ = &c, gen_ = c.gen;
  }

  // 宿主另占 (字节 / 格): 拷回的 根掩码 1 + rx 2 + lc 1 + alpha 5
  static double cell_bytes(const factor::Dag &, int) { return 4.0 + kPlaneBytes; }

  template <class Visit>
  bool walk(const factor::Dag &d, const uint8_t *need, Visit &&visit) {
    inputs(d.feats);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::gpu::walk(sess_, d, dev_, dp_, sh.T, sh.A, sh.seg, gate_, need, [&](int i, const auto &, double ms) { return visit(i, ms); });
  }

  // Ps::need = 0: 森林按实际槽数试装, 新增槽的显存不够就减半 (一个候选都装不下 = 块太大, 断言); 否则全收 (逐根评, 根即算即放)
  template <class Ps>
  factor::Dag plant(const std::vector<const factor::expr::Expr *> &cands, int &C) {
    assert(!cands.empty());
    if constexpr (Ps::need != 0) {
      C = static_cast<int>(cands.size());
      return factor::build_forest(cands, /*keep_roots=*/false);
    }
    open_search();
    C = std::min(batch_, static_cast<int>(cands.size()));
    while (true) {
      factor::Dag d = factor::build_forest(std::vector<const factor::expr::Expr *>(cands.begin(), cands.begin() + C), /*keep_roots=*/true);
      inputs(d.feats);
      size_t vfree = 0, vtotal = 0;
      factor::gpu::mem_info(&vfree, &vtotal);
      const size_t grow = d.n_slots > static_cast<int>(dp_.slots.size()) ? static_cast<size_t>(d.n_slots) - dp_.slots.size() : 0;
      if (static_cast<double>(grow) * kPlaneBytes * static_cast<double>(c_->y.sh.n()) <= 0.9 * static_cast<double>(vfree))
        return d;
      assert(C > 1 && "显存不够评一个候选: 块太大");
      C /= 2;
    }
  }
  template <class Slot, class Node, class Done>
  bool forest(const factor::Dag &d, const uint8_t *need, const factor::stat::Frame *f, Slot &&ps, Node &&node, Done &&done) {
    using Ps = std::remove_cvref_t<decltype(ps(0))>;
    static_assert(!Ps::cells, "逐格扩展只 CPU");
    const factor::stat::Span s = c_->span();
    if constexpr (Ps::need == 0) { // 批核: 一批根同在显存, 一次 search_eval
      open_search();
      if (!walk(d, need, [&](int i, double ms) { return node(i, ms) && !cancelled(); }))
        return false;
      std::vector<int> ks;
      std::vector<const factor::gpu::DevPlane *> roots;
      for (int k = 0; k < static_cast<int>(d.roots.size()); ++k) {
        const int ri = d.roots[static_cast<size_t>(k)];
        if (d.first_root(k) != k || (need && !need[static_cast<size_t>(ri)]))
          continue;
        assert(d.nodes[static_cast<size_t>(ri)].op >= 0 && d.nodes[static_cast<size_t>(ri)].slot >= 0);
        assert(f[k] == f[ks.empty() ? k : ks.front()] && "一批同口径");
        ks.push_back(k);
        roots.push_back(at(ri));
      }
      const int C = static_cast<int>(ks.size());
      assert(C >= 1 && "一批至少一个好根");
      std::vector<factor::search::Result> out(static_cast<size_t>(C));
      factor::gpu::search_eval(search_, roots.data(), C, gate_, f[ks.front()], s.t0_abs, s.t_lo, out.data());
      const factor::stat::Labels L0 = labels_of(c_->y);
      for (int j = 0; j < C; ++j) {
        Ps &p = ps(ks[static_cast<size_t>(j)]);
        const factor::stat::Labels L = labels_pick(L0, p.hd);
        const Ctx cx{ex_, *c_, L, nullptr};
        p.open(cx);
        RootView v;
        v.f = f[ks[static_cast<size_t>(j)]];
        v.acc = out[static_cast<size_t>(j)].acc;
        v.ranks = Ranks{search_, j, &ranks_of};
        p.root(cx, v);
        done(ks[static_cast<size_t>(j)]);
      }
      return true;
    } else { // 逐根: stat_eval, 只拷回要的原料
      const YPlanes &y = c_->y;
      assert(s.t0_abs == 0 && s.t_lo == 0 && "GPU Stat 会话只整块");
      if (!stat_) {
        stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
        w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
      }
      const size_t n = s.n();
      constexpr uint8_t need_p = Ps::need;
      if constexpr ((need_p & kNeedRows) != 0)
        rows_.resize(static_cast<size_t>(y.hd.n) * s.T);
      if constexpr ((need_p & kNeedRx) != 0)
        rx_.resize(n);
      if constexpr ((need_p & kNeedLc) != 0)
        lc_.resize(n);
      if constexpr ((need_p & kNeedMask) != 0)
        mask_.resize(n);
      if constexpr ((need_p & kNeedAlpha) != 0)
        av_.resize(n), am_.resize(n);
      const factor::stat::Labels L = labels_of(y);
      return walk(d, need, [&](int i, double ms) {
        if (!node(i, ms))
          return false;
        for (int k = 0; k < static_cast<int>(d.roots.size()); ++k) {
          if (d.roots[static_cast<size_t>(k)] != i || d.first_root(k) != k)
            continue;
          const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
          assert(nd.op >= 0 && nd.n_in >= 1 && "根是归一算子, 至少一元");
          Ps &p = ps(k);
          assert(p.hd.n == 0 && "GPU Stat 会话按 Y 全部持有期");
          const Ctx cx{ex_, *c_, L, &w_};
          p.open(cx);
          const factor::gpu::DevPlane *rp = at(i); // Stat 只看池内 (g 在算子内部)
          RootView v;
          v.f = f[k];
          if constexpr ((need_p & kNeedMask) != 0) {
            factor::gpu::download(sess_, rp, nullptr, mask_.data());
            v.x.m = mask_.data();
          }
          factor::stat::Accum acc[factor::stat::kMaxHold];
          factor::gpu::stat_eval(stat_, rp, gate_, f[k], acc, (need_p & kNeedRows) ? rows_.data() : nullptr, (need_p & kNeedLc) ? lc_.data() : nullptr,
                                 (need_p & kNeedRx) ? rx_.data() : nullptr);
          v.acc = acc;
          v.rx = (need_p & kNeedRx) ? rx_.data() : nullptr;
          v.lc = (need_p & kNeedLc) ? lc_.data() : nullptr;
          if constexpr ((need_p & kNeedRows) != 0) {
            const auto e = p.ext();
            ex_.rows(s.T, [&](int t0, int t1, int) {
              for (int hi = 0; hi < L.hd.n; ++hi)
                for (int t = t0; t < t1; ++t)
                  p.row(hi, t, rows_[static_cast<size_t>(hi) * s.T + t], e);
            });
          }
          if constexpr ((need_p & kNeedAlpha) != 0) { // alpha = 根的输入: 叶 = 宿主平面, 否则拷回
            const factor::DagNode &an = d.nodes[static_cast<size_t>(nd.in[0])];
            if (an.op < 0) {
              const factor::check::Plane *q = c_->x.find(d.feats[static_cast<size_t>(an.feat)]);
              v.alpha = {q->v.data(), q->m.data()};
            } else {
              factor::gpu::download(sess_, at(nd.in[0]), av_.data(), am_.data());
              v.alpha = {av_.data(), am_.data()};
            }
          }
          p.root(cx, v);
          done(k);
          break;
        }
        return !cancelled();
      });
    }
  }

private:
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }
  static void ranks_of(void *src, int k, int t0, int stride, int n_rows, uint16_t *out) {
    factor::gpu::search_ranks(static_cast<factor::gpu::SearchSession *>(src), k, t0, stride, n_rows, out);
  }
  const factor::gpu::DevPlane *at(int i) const { return factor::gpu::plane_at(*d_, dev_, dp_, i); }
  // 输入 = X 叶 (codes 序) + 截面池掩码; 叶集不变不重传
  void inputs(const std::vector<std::string> &codes) {
    assert(c_ && "先 bind");
    if (sess_ && codes == codes_)
      return;
    close_inputs();
    sess_ = factor::gpu::session_open(c_->y.sh.n());
    codes_ = codes;
    const std::vector<const factor::check::Plane *> in = leaves(*c_, codes);
    dev_.resize(in.size());
    for (size_t i = 0; i < in.size(); ++i)
      dev_[i] = factor::gpu::upload(sess_, in[i]->v.data(), in[i]->m.data());
    gate_ = factor::gpu::upload_mask(sess_, c_->y.gate.data());
  }
  void open_search() {
    assert(c_ && "先 bind");
    if (search_)
      return;
    const YPlanes &y = c_->y;
    factor::stat::Label lab[factor::stat::kMaxHold];
    for (int hi = 0; hi < y.hd.n; ++hi)
      lab[hi] = label_of(y.labels[static_cast<size_t>(hi)], nullptr);
    search_ = factor::gpu::search_open(y.sh.T, y.sh.A, y.sh.seg, batch_, y.hd, y.n_full);
    factor::gpu::search_load(search_, lab);
  }
  void close_inputs() {
    dp_.release();
    if (sess_)
      factor::gpu::session_close(sess_); // 上传的输入平面随会话释放
    sess_ = nullptr, gate_ = nullptr;
    dev_.clear(), codes_.clear();
  }
  void close() {
    dp_.release();
    if (stat_)
      factor::gpu::stat_close(stat_);
    if (search_)
      factor::gpu::search_close(search_);
    stat_ = nullptr, search_ = nullptr;
    close_inputs();
  }

  misc::exec::ForkJoin &ex_;
  const std::atomic<bool> &cancel_;
  int batch_ = 1;
  const ScopeCache *c_ = nullptr;
  uint64_t gen_ = 0;
  const factor::Dag *d_ = nullptr;
  factor::gpu::Session *sess_ = nullptr;
  std::vector<std::string> codes_;                 // 已上传的叶 (= dev_ 序)
  std::vector<const factor::gpu::DevPlane *> dev_; // [leaf]
  const factor::gpu::DevPlane *gate_ = nullptr;    // 截面池掩码 (随会话释放)
  factor::gpu::DevPool dp_;                        // 中间量常驻显存, 不回宿主
  factor::gpu::StatSession *stat_ = nullptr;
  factor::gpu::SearchSession *search_ = nullptr;
  factor::cpu::stat::Workspace w_;      // 宿主探针 (cost_rows)
  std::vector<factor::stat::Row> rows_; // 一级 Row 拷回 [hd.n][T] (kNeedRows)
  std::vector<uint16_t> rx_;            // 根的 r16 拷回 (kNeedRx)
  std::vector<uint8_t> lc_;             // 根的腿码拷回 (kNeedLc)
  std::vector<uint8_t> mask_;           // 根掩码拷回 (kNeedMask)
  std::vector<float> av_;               // alpha 拷回 (kNeedAlpha; 叶直接用宿主平面)
  std::vector<uint8_t> am_;
};

// 后端只在这里分叉一次: fn(Engine<Cpu | Gpu> &)
template <class Fn>
bool with_engine(bool gpu, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, int batch, Fn &&fn) {
  if (gpu) {
    Engine<Gpu> e(ex, cancel, batch);
    return fn(e);
  }
  Engine<Cpu> e(ex, cancel, batch);
  return fn(e);
}

// ---- 管线 (Run / Inspect / Portfolio 同一份; Search 分块自走 plant / forest) ----
// worker 私有常驻件 (跨请求存活): 各服务的 Flow 由它派生, 各加自己的引擎 / 缓冲
struct FactorFlow {
  misc::exec::ForkJoin ex;
  LibReaders lr; // 已建的库 (清单没变的复用)
  ScopeCache c;  // 作用域块 (X 留法见 eval_job 的 retain)
  FactorFlow() : ex(misc::exec::hw()) {}
};
// 一次评估的出参
struct JobOut {
  EvalScope es;
  factor::Dag d;                // 森林: roots[k] ↔ exprs[k]
  std::vector<double> ms;       // [node] 算子耗时 (叶 / 没走的 0)
  std::vector<std::string> why; // [k] 非空 = 该根 BROKEN (特征叶值域越界), 不评
  std::string empty;            // Rc::Empty 的原因 (所用库未建 / 无共同落盘日)
  double eval_ms(int k) const { // 根 k 子树的算子 wall 之和 (共享节点算给每个用它的根 = 单独算它要多久)
    double s = 0.0;
    for (const int i : d.subtree(d.roots[static_cast<size_t>(k)]))
      s += ms[static_cast<size_t>(i)];
    return s;
  }
};
// 一个因子的规范串 → Expr (Factors 表 / 组合条目的规范串已校验, 必可再解析)
inline factor::expr::Expr parse_job(const FactorJob &j) {
  factor::expr::Expr e;
  std::string err;
  const bool ok = factor::expr::parse(j.expr, j.ft->lookup(), j.ft->seg, e, err);
  assert(ok && "Factors 表的规范串必可再解析");
  (void)ok;
  return e;
}
// 森林 (eng.plant) → eval_scope → open_libs → assert_fits → ensure_xy → plan_forest (on_plan(): o.why 已定) → eng.forest:
// 每个好根的同根首个 k0, ps(k0) 的钩子全部走完 (单块: root 后即 finish) → done(k0). o 须新建 (每次调用一个).
// X: retain = 只留本森林的叶 / 否则作用域内读过的全留. 宿主峰值 = Y + X + 引擎 (E::cell_bytes) / 格 + 探针 (Ps::bytes).
// 进度: Loading 期 = 天; Running 期 = 好根的算子节点 + 好根数. 结局 Ok / Empty (o.empty) / Broken (全部根坏, o.why) / Cancel / Stale
template <class E, class Slot, class OnPlan, class Done>
Rc eval_job(E &eng, FactorFlow &F, const FeatureTable &ft, const FactorScope &sc, const std::vector<const factor::expr::Expr *> &exprs,
            const factor::stat::Frame *frames, const YSpec &ys, bool retain, const char *who, Slot &&ps, std::atomic<bool> &cancel, Progress &prog,
            JobOut &o, OnPlan &&on_plan, Done &&done) {
  using Ps = std::remove_cvref_t<decltype(ps(0))>;
  assert(sc.seg == ft.seg && "请求层与字段表层不符");
  assert(o.d.nodes.empty() && "o 须是新的");
  int C = 0;
  o.d = eng.template plant<Ps>(exprs, C);
  assert(C == static_cast<int>(exprs.size()));
  const factor::Dag &d = o.d;
  const auto cancelled = [&] { return cancel.load(std::memory_order_relaxed); };

  // ---- S: 作用域 ----
  if (!eval_scope(ft, sc, d.feats, o.es, o.empty))
    return Rc::Empty;
  open_libs(sc, F.lr, &cancel);
  const size_t nd = o.es.days.size();
  { // 宿主峰值: X = retain ? 本森林 : 缓存 ∪ 本森林
    const bool hit = F.c.key == scope_key(ft, o.es, 0, nd, ys);
    size_t nx = d.feats.size();
    if (hit && !retain) {
      nx = F.c.x.codes.size();
      for (const std::string &code : d.feats)
        nx += !F.c.x.find(code);
    }
    const int days = static_cast<int>(nd), A = static_cast<int>(o.es.rs.axis.size()), H = ps(0).hd.n ? ps(0).hd.n : ys.hd.n;
    const Shape sh{days * ft.seg, A, days, ft.seg};
    const double per = cell_bytes(ys) + kPlaneBytes * static_cast<double>(nx) + E::cell_bytes(d, ys.hd.n);
    assert_fits(who, static_cast<double>(sh.n()) * per + Ps::bytes(sh, H), days, A, ft.seg);
  }

  // ---- Y / X: 装载 → 叶值域, 逐根归因 ----
  if (const Rc rc = ensure_xy(F.c, ft, o.es, F.lr, 0, nd, ys, d.feats, retain, F.ex, cancel, prog); rc != Rc::Ok)
    return rc;
  std::vector<uint8_t> need;
  if (plan_forest(F.ex, F.c, d, o.why, need) == 0) {
    on_plan();
    return Rc::Broken;
  }
  on_plan();
  if (cancelled())
    return Rc::Cancel;

  // ---- 森林: 后序顺序走, 每节点全核; 好根即评 ----
  size_t n_run = 0;
  for (size_t i = 0; i < d.nodes.size(); ++i)
    n_run += need[i] && d.nodes[i].op >= 0;
  for (int k = 0; k < static_cast<int>(d.roots.size()); ++k)
    n_run += d.first_root(k) == k && o.why[static_cast<size_t>(k)].empty();
  prog.begin(Phase::Running, n_run);
  o.ms.assign(d.nodes.size(), 0.0);
  eng.bind(F.c);
  const bool ok = eng.forest(
      d, need.data(), frames, ps,
      [&](int i, double ms) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0) {
          o.ms[static_cast<size_t>(i)] = ms;
          prog.publish(1);
        }
        return !cancelled();
      },
      [&](int k0) {
        ps(k0).finish(F.ex, F.c);
        prog.publish(1);
        done(k0);
      });
  return ok && !cancelled() ? Rc::Ok : Rc::Cancel;
}

} // namespace GUI::Factors
