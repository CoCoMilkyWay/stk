// FactorsEngine — 因子评估引擎 (四服务同一条管线): 根集 (RootSet) → 块 (FactorsLoad Block) → 森林 (Forest) → 逐根原料 (RootView) → 探针元组 (FactorsProbe) → 出参.
//   探针 = 一个根的一种输出 (Probes / ProbeBase), 钩子 = 一趟作业的事件 (Hooks / HookBase), 两者同形; 四服务的差别只在探针元组与钩子
//   (Run / Search / Inspect / Portfolio 各自声明), 引擎与管线同一份.
//   引擎 Engine<Cpu | Gpu> 同一份接口 (共有部分在 EngineBase; FactorFlow 持有, 懒建; 后端只在 FactorFlow::with_engine<Ps> 分叉):
//     budget<Ps>(d, H) / batch_of(P, b, per_day)  预算 (字节 / 格: 宿主 work + 显存 dev) / 批核实际批 (kBatch = 预算按的同批候选数)
//     bind(c)                                绑块: 块 (或其代 gen) 变 → 会话作废 (unbind), 下次用时重开 (Y 一个块只进会话一次)
//     labels()                               会话的标签 (Y 全部持有期; CPU = Stat 会话预处理过的, GPU = 宿主视图): 探针 Ctx::L / 原料同一份
//     walk(d, need, visit(i, ms))            顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//     plant<Ps>(roots, batch, C)             一批根 (Root = 式 + 口径) 的共享森林 Forest (C 出 = 实际收的前 C 个, ≤ batch): GPU 批核再按显存减半
//     eval(F, need, ps, last, node, done)    走森林, 每个根 k 评一次: 选取 Pick → 探针 open → 原料 → root (→ last: finish) → done(k). 原料 (后端 view<Ps>):
//                                            CPU 恒逐根 Stat (run → accum_rows 进探针行钩子 → 腿码; 抽样行拷 rx), 走到根即评;
//                                            GPU 逐根 = stat_eval 只拷回 Ps::need 要的; 批核 (kBatched<Ps>: 只累加 + 抽样行) = kDefer 走完森林一批根同在显存一次 search_eval
//   管线 (四服务同一份): Job (规格 → 评估域 / 计划 / 块) — open_job (评估域 → 开库) → plan_job (budget_of → 分块 + 多块满仓只数) → pass (一趟: 逐块
//   load_block (ensure_block) → hk.block → 逐批 plant → plan_forest (值域逐根归因, 坏根跨块粘住) → hk.plan → eval (hk.node / hk.done); 末块根即 finish).
//   eval_job = 一趟一张森林 (Run / Inspect / Portfolio; 根可空 = 只块钩子: Portfolio 池篮子; 探针 whole 或无根 → 整期一块 (装不下截最近的日), 否则按预算分块);
//   Search 自己 plan_job, 每趟一次 pass. 结局统一 FactorWorker::settle.
// 只给服务 .cpp include (EvalCpu → TS / CS Cpu.hpp, 受控浮点: CMake PRECISE_MATH 源).
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp" // Result / max_lag
#include "factor/Search/Space.hpp"    // lookback
#include "gui/task_factors/services/FactorsProbe.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FactorWorker (成员定义在此)
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

namespace GUI::Factors {

// 森林的根 = (具体式, 口径): 同式异口径 = 同一节点上的两个根 (子树共享, 各评一次). 同式同口径的去重在上游 RootSet
struct Root {
  const factor::expr::Expr *e = nullptr;
  factor::stat::Frame f = factor::stat::Frame::CS;
};
// 根集 (四服务同一份): 条目 (规范串, 口径) → 根; 同串同口径的条目共一个根 (只评一次). 根 k ↔ roots()[k] / 探针 ps(k); 条目 i 的根 = of[i]
struct RootSet {
  std::vector<factor::expr::Expr> es;  // [k] 式
  std::vector<factor::stat::Frame> fs; // [k] 口径
  std::vector<int> of;                 // [条目] → 根
  void add(const FeatureTable &ft, const std::string &canon, factor::stat::Frame f) {
    size_t k = 0;
    while (k < es.size() && (es[k].canon != canon || fs[k] != f))
      ++k;
    if (k == es.size())
      es.push_back(ft.reparse(canon)), fs.push_back(f);
    of.push_back(static_cast<int>(k));
  }
  size_t size() const { return es.size(); }
  std::vector<Root> roots() const { // 指 es: add 完再取
    std::vector<Root> rs;
    for (size_t k = 0; k < es.size(); ++k)
      rs.push_back({&es[k], fs[k]});
    return rs;
  }
};
struct Forest {
  factor::Dag d;                      // d.roots[k] ↔ 根 k
  std::vector<factor::stat::Frame> f; // [k] 口径
  size_t size() const { return f.size(); }
};
inline Forest plant_forest(std::span<const Root> rs, bool keep_roots) {
  assert(!rs.empty());
  std::vector<const factor::expr::Expr *> es;
  Forest o;
  for (const Root &r : rs)
    es.push_back(r.e), o.f.push_back(r.f);
  o.d = factor::build_forest(es, keep_roots);
  return o;
}
// 分块前带 (日): 回看 look 行 + rAC lag 行 (统计首行的 lag 行也须整窗; Search 契约【分块流式】)
inline int carry_days(int look, const factor::stat::Holds &hd, int seg) { return (look + factor::search::max_lag(hd, seg) + seg - 1) / seg; }

// 探针组 Ps 只要累加 (+ 抽样行) = 批核: GPU 一批根同在显存一次 search_eval (出 Y 全部持有期); 否则逐根 Stat
template <class Ps>
inline constexpr bool kBatched = (Ps::need & ~kNeedRanks) == 0;

struct Cpu {};
struct Gpu {};
template <class B>
class Engine;

// 两后端共有: 线程池 / 取消旗 / 绑定的块 (块或其代 gen 变 → D::unbind 作废会话) / 当前 Dag; 森林骨架 eval (标签 D::labels / 原料 D::ready / D::view)
template <class D>
class EngineBase {
public:
  void bind(const Block &c) {
    if (c_ == &c && gen_ == c.gen)
      return;
    self().unbind();
    c_ = &c, gen_ = c.gen;
  }
  misc::exec::ForkJoin &ex() const { return ex_; }
  const Block &block() const {
    assert(c_ && "先 bind");
    return *c_;
  }
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }
  // 走森林 F (须已 bind; 两后端同一份): 每个根 k 评一次: ps(k).open → 原料 → ps(k).root (→ last: ps(k).finish) → done(k).
  // D::kDefer<Ps> → 走完森林一批评, 否则走到根即评. node(i, ms) = 每走完一个节点 (ms 同 walk). last = 末块. false = 取消
  template <class Slot, class Node, class Done>
  bool eval(const Forest &F, const uint8_t *need, Slot &&ps, bool last, Node &&node, Done &&done) {
    using Ps = std::remove_cvref_t<decltype(ps(0))>;
    constexpr bool defer = D::template kDefer<Ps>;
    std::vector<int> ks;
    const bool ok = self().walk(F.d, need, [&](int i, double ms) {
      node(i, ms);
      for (size_t k = 0; k < F.size(); ++k)
        if (F.d.roots[k] == i) {
          assert(F.d.nodes[static_cast<size_t>(i)].op >= 0 && F.d.nodes[static_cast<size_t>(i)].n_in >= 1 && "根是归一算子, 至少一元");
          ks.push_back(static_cast<int>(k));
        }
      if (!defer && !ks.empty()) {
        roots<Ps>(F, ks, ps, last, done);
        ks.clear();
      }
      return !cancelled();
    });
    if (defer && ok && !ks.empty())
      roots<Ps>(F, ks, ps, last, done);
    return ok;
  }

protected:
  EngineBase(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel) : ex_(ex), cancel_(cancel) {}
  D &self() { return static_cast<D &>(*this); }

  misc::exec::ForkJoin &ex_;
  const std::atomic<bool> &cancel_;
  const Block *c_ = nullptr;
  uint64_t gen_ = 0;
  const factor::Dag *d_ = nullptr;

private:
  // 根 ks (都已走到): D::ready 备好 (GPU 批核一次 search_eval) → 逐个: 选取 pk (探针 hd → 会话下标) → open → D::view 原料 → root → (last) finish → done
  template <class Ps, class Slot, class Done>
  void roots(const Forest &F, const std::vector<int> &ks, Slot &ps, bool last, Done &done) {
    factor::cpu::stat::Workspace *w = self().template ready<Ps>(F, ks);
    const factor::stat::Labels L0 = self().labels();
    for (size_t j = 0; j < ks.size(); ++j) {
      const int k = ks[j];
      Ps &p = ps(k);
      const Pick pk = pick_of(L0.hd, p.hd);
      const factor::stat::Labels L = labels_pick(L0, pk);
      const Ctx cx{ex_, block(), L, w};
      p.open(cx);
      factor::stat::Accum acc[factor::stat::kMaxHold];
      p.root(cx, self().template view<Ps>(F, k, static_cast<int>(j), cx, pk, p, acc));
      if (last)
        p.finish(cx);
      done(k);
    }
  }
};

template <>
class Engine<Cpu> : public EngineBase<Engine<Cpu>> {
  friend class EngineBase<Engine<Cpu>>;

public:
  static constexpr const char *kName = "cpu";
  template <class Ps>
  static constexpr bool kDefer = false; // 走到根即评 (根平面在 visit 内仍在槽里, 不留根)
  static constexpr int kBatch = 64;     // 批核一张森林的候选数 (只为公共前缀, 根即算即放)
  Engine(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel) : EngineBase(ex, cancel) {}

  // 预算 (字节 / 格): 宿主 = 活槽 + 标签秩 2H + rx 2 + lc 1, 无显存. 批核的 d = 一个候选, kBatch 个同结构候选的森林按它另 2 槽余量
  template <class Ps>
  static Budget budget(const factor::Dag &d, int H) {
    return {0.0, kPlaneBytes * (d.n_slots + (kBatched<Ps> ? 2 : 0)) + 2.0 * H + 3.0, 0.0, 0.0};
  }
  static int batch_of(const Plan &, const Budget &, double) { return kBatch; }

  template <class Visit>
  bool walk(const factor::Dag &d, const uint8_t *need, Visit &&visit) {
    in_ = leaves(block(), d.feats);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::cpu::walk(ex_, d, in_, hp_, sc_, sh.T, sh.A, sh.seg, c_->y.gate.data(), need,
                             [&](int i, const auto &, double ms) { return visit(i, ms); });
  }
  // walk 的 visit 内: 节点 i 的平面 (i 及其输入仍在槽里)
  const factor::check::Plane *plane(int i) const { return factor::cpu::plane_at(*d_, in_, hp_, i); }

  template <class Ps>
  Forest plant(std::span<const Root> rs, int batch, int &C) const {
    assert(!rs.empty() && batch >= 1);
    C = std::min(batch, static_cast<int>(rs.size()));
    return plant_forest(rs.first(static_cast<size_t>(C)), /*keep_roots=*/false);
  }

private:
  void unbind() { stat_on_ = false; }
  factor::stat::Labels labels() { return stat_session().L; }
  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &, const std::vector<int> &) {
    return &stat_session().w;
  }
  // 根 k 的原料 (walk 的 visit 内): run (标签 cx.L = 会话预处理的那几组) → accum_rows 进探针行钩子 → 腿码; 原料按 Ps::need 给, 抽样行恒可取 (拷 rx)
  template <class Ps>
  RootView view(const Forest &F, int k, int, const Ctx &cx, const Pick &, Ps &p, factor::stat::Accum *acc) {
    constexpr uint8_t need_p = Ps::need;
    const int i = F.d.roots[static_cast<size_t>(k)];
    const factor::stat::Frame f = F.f[static_cast<size_t>(k)];
    const factor::stat::Span s = c_->span();
    const factor::check::Plane *rp = plane(i);
    rx_.resize(s.n());
    factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate.data(), f, s, cx.L, rx_.data(), ss_.w, acc, ProbeSink<Ps>{&p});
    RootView v;
    v.f = f;
    v.acc = acc;
    v.x = {(need_p & kNeedX) != 0 ? rp->v.data() : nullptr, (need_p & kNeedMask) != 0 ? rp->m.data() : nullptr};
    v.rx = (need_p & kNeedRx) != 0 ? rx_.data() : nullptr;
    v.ranks = Ranks{this, 0, &ranks_of};
    if constexpr ((need_p & kNeedLc) != 0) {
      lc_.resize(s.n());
      factor::cpu::stat::leg_codes(ex_, rx_.data(), s, lc_.data());
      v.lc = lc_.data();
    }
    if constexpr ((need_p & kNeedAlpha) != 0) {
      const factor::check::Plane *ap = plane(F.d.nodes[static_cast<size_t>(i)].in[0]); // alpha = 根的输入: 根的 visit 内仍在槽里
      v.alpha = {ap->v.data(), ap->m.data()};
    }
    return v;
  }
  // Stat 会话 (Y 的标签秩 + 工作区, 绑定内一次)
  factor::cpu::stat::StatSession &stat_session() {
    if (!stat_on_) {
      const YPlanes &y = block().y;
      factor::cpu::stat::stat_open(ex_, ss_, y.sh.T, y.sh.A, y.sh.seg, labels_of(y), y.ad.n);
      stat_on_ = true;
    }
    return ss_;
  }
  static void ranks_of(void *src, int, int t0, int stride, int n_rows, uint16_t *out) {
    const Engine &e = *static_cast<const Engine *>(src);
    const size_t A = static_cast<size_t>(e.c_->y.sh.A);
    for (int r = 0; r < n_rows; ++r)
      std::copy_n(e.rx_.data() + static_cast<size_t>(t0 + r * stride) * A, A, out + static_cast<size_t>(r) * A);
  }

  std::vector<const factor::check::Plane *> in_; // = leaves(c, d.feats)
  factor::cpu::HostPool hp_;                     // 峰值活槽 × n × 5B
  std::vector<factor::check::Arena> sc_;
  factor::cpu::stat::StatSession ss_;
  bool stat_on_ = false;
  std::vector<uint16_t> rx_; // 根的 r16 (逐根)
  std::vector<uint8_t> lc_;  // 根的腿码 (kNeedLc)
};

template <>
class Engine<Gpu> : public EngineBase<Engine<Gpu>> {
  friend class EngineBase<Engine<Gpu>>;

public:
  static constexpr const char *kName = "gpu";
  template <class Ps>
  static constexpr bool kDefer = kBatched<Ps>; // 批核走完森林再一次 search_eval (keep_roots: 一批根同在显存)
  static constexpr int kBatch = 8;             // 批核预算按至少这么多候选同在显存 (核内 8 候选共用标签行)
  Engine(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel) : EngineBase(ex, cancel) { assert(factor::gpu::available() && "GPU 后端不可用"); }
  ~Engine() { close(); }
  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;

  // 预算 (字节 / 格): 显存 = 特征 5·nf + 门控 1 + 会话标签 (lv, sv, m, ry) 7·H + 活槽 5·n_slots.
  // 逐根: 宿主 = 拷回的 根掩码 1 + rx 2 + lc 1 + alpha 5. 批核 (d = 一个候选): 宿主不拷回; 活槽 ×2 (keep_roots) + kBatch 根各 kDevRootBytes
  template <class Ps>
  static Budget budget(const factor::Dag &d, int H) {
    constexpr bool batch = kBatched<Ps>;
    const double dev = kPlaneBytes * static_cast<double>(d.feats.size()) + 1.0 + 7.0 * H + kPlaneBytes * d.n_slots * (batch ? 2 : 1);
    return {0.0, batch ? 0.0 : 4.0 + kPlaneBytes, 0.0, batch ? dev + kDevRootBytes * kBatch : dev};
  }
  // 批核实际批: 最长一块的格数下, 显存预算扣掉非根部分后能同在的根数 (b = budget 出的, ≤ search_max_batch)
  static int batch_of(const Plan &P, const Budget &b, double per_day) {
    const double nmax = static_cast<double>(P.max_days()) * per_day, dv = b.dev - kDevRootBytes * kBatch;
    return std::clamp(static_cast<int>((P.vram - nmax * dv) / (kDevRootBytes * nmax)), 1, factor::gpu::search_max_batch());
  }
  static constexpr double kDevRootBytes = kPlaneBytes + 2.0; // 批核根平面 5 + rx 2

  template <class Visit>
  bool walk(const factor::Dag &d, const uint8_t *need, Visit &&visit) {
    inputs(d.feats);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::gpu::walk(sess_, d, dev_, dp_, sh.T, sh.A, sh.seg, gate_, need, [&](int i, const auto &, double ms) { return visit(i, ms); });
  }

  // 批核 (须已 bind; Search 会话按批开): 森林按实际槽数试装, 新增槽的显存不够就减半 (一个候选都装不下 = 块太大, 断言);
  // 否则收前 batch 个 (逐根评, 根即算即放)
  template <class Ps>
  Forest plant(std::span<const Root> rs, int batch, int &C) {
    assert(!rs.empty() && batch >= 1);
    C = std::min(batch, static_cast<int>(rs.size()));
    if constexpr (!kBatched<Ps>)
      return plant_forest(rs.first(static_cast<size_t>(C)), /*keep_roots=*/false);
    search_session(batch);
    while (true) {
      Forest F = plant_forest(rs.first(static_cast<size_t>(C)), /*keep_roots=*/true);
      inputs(F.d.feats);
      size_t vfree = 0, vtotal = 0;
      factor::gpu::mem_info(&vfree, &vtotal);
      const size_t grow = F.d.n_slots > static_cast<int>(dp_.slots.size()) ? static_cast<size_t>(F.d.n_slots) - dp_.slots.size() : 0;
      if (static_cast<double>(grow) * kPlaneBytes * static_cast<double>(c_->y.sh.n()) <= 0.9 * static_cast<double>(vfree))
        return F;
      assert(C > 1 && "显存不够评一个候选: 块太大");
      C /= 2;
    }
  }

private:
  void unbind() { close(); }
  factor::stat::Labels labels() const { return labels_of(c_->y); }
  // 逐根: Stat 会话 → 宿主探针工作区; 批核: 走完的森林里根 ks 同在显存, 一次 search_eval → out_[ks.size()] (无工作区)
  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &F, const std::vector<int> &ks) {
    if constexpr (!kBatched<Ps>) {
      return &stat_session();
    } else {
      assert(search_ && "批核先 plant");
      const factor::stat::Frame f = F.f[static_cast<size_t>(ks.front())];
      std::vector<const factor::gpu::DevPlane *> roots;
      for (const int k : ks) {
        const int r = F.d.roots[static_cast<size_t>(k)];
        assert(F.d.nodes[static_cast<size_t>(r)].slot >= 0);
        assert(F.f[static_cast<size_t>(k)] == f && "一批同口径");
        roots.push_back(at(r));
      }
      const factor::stat::Span s = c_->span();
      out_.assign(ks.size(), {});
      factor::gpu::search_eval(search_, roots.data(), static_cast<int>(ks.size()), gate_, f, s.t0_abs, s.t_lo, out_.data());
      return nullptr;
    }
  }
  // 根 k (批内第 j 个) 的原料: 会话按 Y 全部持有期出, 取 pk 那几组. 批核: acc ← out_[j], 抽样行 search_ranks; 逐根: stat_root
  template <class Ps>
  RootView view(const Forest &F, int k, int j, const Ctx &, const Pick &pk, Ps &p, factor::stat::Accum *acc) {
    if constexpr (kBatched<Ps>) {
      for (int hi = 0; hi < pk.n; ++hi)
        acc[hi] = out_[static_cast<size_t>(j)].acc[pk.at[hi]];
      RootView v;
      v.f = F.f[static_cast<size_t>(k)];
      v.acc = acc;
      v.ranks = Ranks{search_, j, &ranks_of};
      return v;
    } else {
      return stat_root(F, k, pk, p, acc);
    }
  }
  // 逐根: stat_eval 只拷回 Ps::need 要的原料
  template <class Ps>
  RootView stat_root(const Forest &F, int k, const Pick &pk, Ps &p, factor::stat::Accum *acc) {
    constexpr uint8_t need_p = Ps::need;
    static_assert(!Ps::cells && (need_p & kNeedRanks) == 0, "逐格扩展只 CPU; 抽样行只批核");
    RootView v;
    v.f = F.f[static_cast<size_t>(k)];
    v.acc = acc;
    const YPlanes &y = c_->y;
    const factor::stat::Span s = c_->span();
    const size_t n = s.n();
    const int i = F.d.roots[static_cast<size_t>(k)];
    if constexpr ((need_p & kNeedRows) != 0)
      rows_.resize(static_cast<size_t>(y.hd.n) * s.T);
    if constexpr ((need_p & kNeedRx) != 0)
      rx_.resize(n);
    if constexpr ((need_p & kNeedLc) != 0)
      lc_.resize(n);
    if constexpr ((need_p & kNeedX) != 0)
      xv_.resize(n);
    if constexpr ((need_p & kNeedMask) != 0)
      mask_.resize(n);
    if constexpr ((need_p & kNeedAlpha) != 0)
      av_.resize(n), am_.resize(n);
    const factor::gpu::DevPlane *rp = at(i); // Stat 只看池内 (g 在算子内部)
    if constexpr ((need_p & (kNeedX | kNeedMask)) != 0) {
      float *xv = (need_p & kNeedX) != 0 ? xv_.data() : nullptr;
      uint8_t *xm = (need_p & kNeedMask) != 0 ? mask_.data() : nullptr;
      factor::gpu::download(sess_, rp, xv, xm);
      v.x = {xv, xm};
    }
    factor::stat::Accum all[factor::stat::kMaxHold];
    factor::gpu::stat_eval(stat_, rp, gate_, v.f, s.t0_abs, s.t_lo, all, (need_p & kNeedRows) ? rows_.data() : nullptr,
                           (need_p & kNeedLc) ? lc_.data() : nullptr, (need_p & kNeedRx) ? rx_.data() : nullptr);
    for (int hi = 0; hi < pk.n; ++hi)
      acc[hi] = all[pk.at[hi]];
    v.rx = (need_p & kNeedRx) ? rx_.data() : nullptr;
    v.lc = (need_p & kNeedLc) ? lc_.data() : nullptr;
    if constexpr ((need_p & kNeedRows) != 0) {
      const auto e = p.ext();
      ex_.rows(s.T - s.t_lo, [&](int r0, int r1, int) {
        for (int hi = 0; hi < pk.n; ++hi)
          for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t)
            p.row(hi, t, rows_[static_cast<size_t>(pk.at[hi]) * s.T + t], e);
      });
    }
    if constexpr ((need_p & kNeedAlpha) != 0) { // alpha = 根的输入: 叶 = 宿主平面, 否则拷回
      const int in0 = F.d.nodes[static_cast<size_t>(i)].in[0];
      const factor::DagNode &an = F.d.nodes[static_cast<size_t>(in0)];
      if (an.op < 0) {
        const factor::check::Plane *q = c_->x.find(ids_[static_cast<size_t>(an.feat)]);
        v.alpha = {q->v.data(), q->m.data()};
      } else {
        factor::gpu::download(sess_, at(in0), av_.data(), am_.data());
        v.alpha = {av_.data(), am_.data()};
      }
    }
    return v;
  }
  // Stat 会话 (逐根; 绑定内一次) → 宿主探针工作区
  factor::cpu::stat::Workspace &stat_session() {
    if (!stat_) {
      const YPlanes &y = block().y;
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
      w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
    }
    return w_;
  }
  // Search 会话 (批核; 批变了重开, 绑定内一次)
  void search_session(int batch) {
    assert(batch >= 1 && batch <= factor::gpu::search_max_batch());
    if (search_ && batch == batch_)
      return;
    if (search_)
      factor::gpu::search_close(search_);
    batch_ = batch;
    const YPlanes &y = block().y;
    search_ = factor::gpu::search_open(y.sh.T, y.sh.A, y.sh.seg, batch_, y.hd, y.n_full);
    factor::gpu::search_load(search_, labels_of(y).l);
  }
  static void ranks_of(void *src, int k, int t0, int stride, int n_rows, uint16_t *out) {
    factor::gpu::search_ranks(static_cast<factor::gpu::SearchSession *>(src), k, t0, stride, n_rows, out);
  }
  const factor::gpu::DevPlane *at(int i) const { return factor::gpu::plane_at(*d_, dev_, dp_, i); }
  // 输入 = X 叶 (feats 序) + 截面池掩码; 叶集不变不重传
  void inputs(const std::vector<std::string> &feats) {
    assert(c_ && c_->ft && "先 bind");
    std::vector<LeafId> ids = c_->ft->x.ids(feats);
    if (sess_ && ids == ids_)
      return;
    close_inputs();
    sess_ = factor::gpu::session_open(c_->y.sh.n());
    ids_ = std::move(ids);
    for (const factor::check::Plane *p : leaves(*c_, feats))
      dev_.push_back(factor::gpu::upload(sess_, p->v.data(), p->m.data()));
    gate_ = factor::gpu::upload_mask(sess_, c_->y.gate.data());
  }
  void close_inputs() {
    dp_.release();
    if (sess_)
      factor::gpu::session_close(sess_); // 上传的输入平面随会话释放
    sess_ = nullptr, gate_ = nullptr;
    dev_.clear(), ids_.clear();
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

  int batch_ = 1; // 批核会话的批 (search_session 定)
  factor::gpu::Session *sess_ = nullptr;
  std::vector<LeafId> ids_;                        // 已上传的叶 (= dev_ 序)
  std::vector<const factor::gpu::DevPlane *> dev_; // [feat]
  const factor::gpu::DevPlane *gate_ = nullptr;    // 截面池掩码 (随会话释放)
  factor::gpu::DevPool dp_;                        // 中间量常驻显存, 不回宿主
  factor::gpu::StatSession *stat_ = nullptr;
  factor::gpu::SearchSession *search_ = nullptr;
  std::vector<factor::search::Result> out_; // 批核一批的结果 (ready 出, view 取)
  factor::cpu::stat::Workspace w_;          // 宿主探针 (cost_rows)
  std::vector<factor::stat::Row> rows_;     // 一级 Row 拷回 [hd.n][T] (kNeedRows)
  std::vector<uint16_t> rx_;                // 根的 r16 拷回 (kNeedRx)
  std::vector<uint8_t> lc_;                 // 根的腿码拷回 (kNeedLc)
  std::vector<float> xv_;                   // 根值拷回 (kNeedX)
  std::vector<uint8_t> mask_;               // 根掩码拷回 (kNeedMask)
  std::vector<float> av_;                   // alpha 拷回 (kNeedAlpha; 叶直接用宿主平面)
  std::vector<uint8_t> am_;
};

// ---- 管线 (四服务同一份: open_job → plan_job → pass) ----
// worker 私有常驻件 (跨请求存活; 各服务一份): 线程池 / 库 / 块 / worker 的取消旗与进度面 / 引擎 (懒建; 后端只在 with_engine 分叉)
struct FactorFlow {
  misc::exec::ForkJoin ex;
  LibReaders lr;             // 已建的库 (清单没变的复用)
  Block c;                   // worker 的块 (X 留法见 Job::retain)
  std::atomic<bool> &cancel; // worker 的取消旗 (库判废时 open_libs 也拉它)
  Progress &prog;            // worker 的进度面
  FactorFlow(std::atomic<bool> &cancel, Progress &prog) : ex(misc::exec::hw()), cancel(cancel), prog(prog) {}
  Io io(const FeatureTable &ft, const Domain &dm) { return {ft, dm, lr, ex, cancel, prog}; }
  template <class B>
  Engine<B> &engine() {
    std::optional<Engine<B>> &e = slot<B>();
    if (!e)
      e.emplace(ex, cancel);
    return *e;
  }
  // fn(Engine<Cpu | Gpu> &): 探针组 Ps 有逐格扩展 (cells) → 恒 CPU, 否则按 gpu
  template <class Ps, class Fn>
  auto with_engine(bool gpu, Fn &&fn) {
    if constexpr (!Ps::cells) {
      if (gpu)
        return fn(engine<Gpu>());
    }
    return fn(engine<Cpu>());
  }
  // 弃引擎 (每个请求用完: 宿主工作区 / 显存不跨请求占) / 再弃块 (Run / Search: Y / X 也不跨请求留)
  void drop_engines() { cpu_.reset(), gpu_.reset(); }
  void drop() {
    drop_engines();
    c.reset();
  }

private:
  template <class B>
  std::optional<Engine<B>> &slot() {
    if constexpr (std::is_same_v<B, Gpu>)
      return gpu_;
    else
      return cpu_;
  }
  std::optional<Engine<Cpu>> cpu_;
  std::optional<Engine<Gpu>> gpu_;
};
template <class D, class Req>
FactorFlow &FactorWorker<D, Req>::flow() {
  if (!flow_)
    flow_ = std::make_unique<FactorFlow>(this->cancel_, board());
  return *flow_;
}
template <class D, class Req>
bool FactorWorker<D, Req>::abort_out(Rc rc) {
  assert(aborted(rc) && "只收中断结局");
  if (rc == Rc::Cancel)
    return false;
  flow().c.reset(); // 库换了, 缓存的一切不可信
  std::lock_guard<std::mutex> lock(mutex);
  message = kStaleMsg;
  return false;
}
template <class D, class Req>
template <class Fail>
bool FactorWorker<D, Req>::settle(Rc rc, const std::string &why, Fail &&fail) {
  if (aborted(rc))
    return abort_out(rc);
  if (rc != Rc::Ok)
    fail(why);
  return true;
}
// 作业的块位 (plan_job 定): shared (F.c, 跨请求留 Y / X; 整期一块且非 Job::own) 或作业自有 own (用完即弃; 按 P.cache_all 建 每块一个 / 一个复用)
struct Blocks {
  Block *shared = nullptr;
  std::vector<Block> own;
  Block &of(size_t k) {
    if (shared)
      return *shared;
    assert(!own.empty() && "先 plan_job");
    return own.size() == 1 ? own[0] : own[k];
  }
};
// 一次作业 (四服务同一份): 规格 (调用方填 spec.y / spec.b / retain / own / who; carry 由 eval_job 或调用方定) → open_job (评估域 dm + 叶 spec.x)
// → plan_job (计划 P + 块位) → pass
struct Job {
  BlockSpec spec;                   // 块三侧: x = 叶 (open_job 定) / y / b (调用方填)
  bool retain = true;               // 块上只留 spec.x / 否则评估域内读过的全留
  bool own = false;                 // 块归作业 (用完即弃, 不占 F.c)
  int carry = -1;                   // 前带日 (< 0 = 整期一块)
  const char *who = "";             // 整期截近期时的消息
  const FeatureTable *ft = nullptr; // open_job 定
  Domain dm;
  Plan P;
  Blocks blocks;
  std::string why; // Rc::Empty (所用库未建 / 无共同落盘日) / Rc::Broken (首个坏根: 特征叶值域越界) 的原因
  Io io(FactorFlow &F) const { return F.io(*ft, dm); }
  Chunk whole() const { return {0, 0, dm.days.size()}; } // 整期
};
// 一次评估 (eval_job) 的出参: 作业 + 森林 + 节点耗时
struct JobOut {
  Job job;
  Forest fr;                    // 根 k ↔ eval_job 的 roots[k]
  std::vector<double> ms;       // [node] 算子耗时 (各块之和; 叶 / 没走的 0)
  double eval_ms(int k) const { // 根 k 子树的算子 wall 之和 (共享节点算给每个用它的根 = 单独算它要多久)
    double s = 0.0;
    for (const int i : fr.d.subtree(fr.d.roots[static_cast<size_t>(k)]))
      s += ms[static_cast<size_t>(i)];
    return s;
  }
};
// 评估域 (叶 feats (+ j.spec.b: 篮子原料) 所用库 + 评估期) → 开库; j.spec.x = feats 的 LeafId. false = 评估域不成 (j.why; 正常输入)
inline bool open_job(FactorFlow &F, const FactorScope &sc, const std::vector<std::string> &feats, Job &j) {
  assert(sc.ft && sc.lv == sc.ft->lv && "作用域层与字段表层不符");
  j.ft = sc.ft.get();
  j.spec.x = sc.ft->x.ids(feats);
  if (!domain_of(*sc.ft, sc, j.spec.x, j.spec.b, SIZE_MAX, j.dm, j.why))
    return false;
  open_libs(sc, F.lr, &F.cancel);
  return true;
}
// 整期一块 (j.carry < 0) 装不下宿主预算 → 评估期只留最近的日 (按超出比例缩, 重解析评估域, 至放得下; 单日都放不下 → assert).
// bud() = 当前评估域的作业预算. 共享块 F.c 在本作业里要么命中要么被换掉, 它占的内存算回预算
template <class B>
void fit_recent(FactorFlow &F, const FactorScope &sc, Job &j, B &&bud) {
  const size_t all = j.dm.days.size();
  const double held = j.own ? 0.0 : F.c.bytes();
  for (;;) {
    const double bytes = whole_bytes(j.dm, j.ft->seg, bud()), budget = host_budget() + held;
    if (bytes <= budget)
      break;
    const size_t n = j.dm.days.size();
    assert(n > 1 && "单日的整张量装载都放不下内存 (因子层太细)");
    const size_t keep = std::clamp(static_cast<size_t>(static_cast<double>(n) * budget / bytes), size_t{1}, n - 1);
    std::string why;
    const bool ok = domain_of(*sc.ft, sc, j.spec.x, j.spec.b, keep, j.dm, why);
    assert(ok && "近期子期是整期的后缀, 评估域必成");
  }
  if (j.dm.days.size() < all)
    std::fprintf(stderr, "[factors] %s: 整期 %zu 天装不下内存预算, 只评最近 %zu 天 (%d - %d)\n", j.who, all, j.dm.days.size(), j.dm.dates.front(),
                 j.dm.dates.back());
}
// 作业预算 (字节 / 格): load = 块三侧 (nx 个叶 + Y + B); work / dev = 引擎 (E::budget, d = 一张森林 / 批核的一个候选; 空 = 无森林);
// fixed = 探针 (Ps::bytes, H 个持有期)
template <class E, class Ps>
Budget budget_of(const Job &j, const factor::Dag &d, size_t nx, int H) {
  const int seg = j.ft->seg, days = static_cast<int>(j.dm.days.size()), A = static_cast<int>(j.dm.rs.axis.size());
  Budget b = d.nodes.empty() ? Budget{} : E::template budget<Ps>(d, j.spec.y.hd.n);
  b.load = cell_bytes(*j.ft, j.spec, nx);
  b.fixed = Ps::bytes(Shape{days * seg, A, days, seg}, H);
  return b;
}
// 预算 b → 分块 (j.carry; plan_chunks) → 块位 (整期一块 ∧ 非 own → F.c, 否则作业自有); 多块 → 先扫全期门控定满仓只数 j.spec.y.cap
// (各块同一个 = 整期一块的值). Ok / Cancel / Stale
inline Rc plan_job(FactorFlow &F, Job &j, const Budget &b) {
  j.P = plan_chunks(j.dm, j.ft->seg, b, j.carry);
  const size_t n = j.P.chunks.size();
  j.blocks = Blocks{};
  if (n == 1 && !j.own)
    j.blocks.shared = &F.c;
  else
    j.blocks.own.resize(j.P.cache_all ? n : 1);
  if (n == 1)
    return Rc::Ok;
  return rc_of(n_full_of(j.io(F), j.whole(), j.spec.y.cap), F.lr);
}
// 块 k 就位 (ensure_block: j.spec / j.retain → j.blocks.of(k)). Ok / Cancel / Stale
inline Rc load_block(FactorFlow &F, Job &j, size_t k) {
  assert(k < j.P.chunks.size());
  return ensure_block(j.blocks.of(k), j.io(F), j.P.chunks[k], j.spec, j.retain);
}

// 森林的评估计划 (四服务同一份): 算子节点的特征叶元按 OpTable in 列的严格域逐元查块上数据 (c.dom 缓存);
// why[k] = 根 k 子树里首个越界 (非空 = BROKEN, 不评; 入参非空 = 已知坏根, 原样留), need = 好根子树之并 (引擎 walk 掩码). 返回好根数
int plan_forest(misc::exec::ForkJoin &ex, const Block &c, const factor::Dag &d, std::vector<std::string> &why, std::vector<uint8_t> &need);

// ---- 钩子 (一趟作业的事件; 与探针 ProbeBase 对仗: 探针 = 一个根的输出, 钩子 = 一趟的事件). 各服务派生 HookBase, 按需遮蔽 ----
//   block(c, ci, load_s)     块 ci 就位 (load_s = 装载秒): 块钩子 = 只随评估域变的产出 (池等权线 / 池篮子)
//   plan(b0, Fr, why, need)  一批森林 (首根 b0) 值域已查: why[k] 非空 = 根 b0 + k BROKEN, 不评 (每块一次, 坏根只增); need = 要走的节点
//   node(b0, Fr, i, ms)      走完节点 i (ms = 算子耗时, 叶 0)
//   done(k)                  根 k 的探针钩子全部走完 (末块 finish 后); k = roots 下标
struct HookBase {
  void block(const Block &, size_t, double) {}
  void plan(size_t, const Forest &, const std::vector<std::string> &, const std::vector<uint8_t> &) {}
  void node(size_t, const Forest &, int, double) {}
  void done(int) {}
};
// 一趟 (四服务同一份; j 已 plan_job, eng 与 F.cancel 同旗): 根按 batch 一批一张森林; 逐块: load_block → hk.block → 逐批:
// eng.plant → plan_forest (叶值域逐根归因) → hk.plan → eng.eval (hk.node; 末块根评完即 finish) → hk.done.
// 坏根跨块粘住 (某块越界 = 整根作废: 各块同一份探针, 不收半截). 无根 = 只块钩子.
// Ok / Broken (全部根坏, j.why = 首个坏因) / Cancel / Stale
template <class E, class Slot, class H>
Rc pass(E &eng, FactorFlow &F, Job &j, std::span<const Root> roots, int batch, Slot &&ps, H &&hk) {
  using Ps = std::remove_cvref_t<decltype(ps(0))>;
  using Clock = std::chrono::steady_clock;
  const size_t n = j.P.chunks.size(), R = roots.size();
  std::vector<std::string> dead(R); // [k] 坏因 (跨块粘住)
  for (size_t ci = 0; ci < n; ++ci) {
    const Clock::time_point t_load = Clock::now();
    if (const Rc rc = load_block(F, j, ci); rc != Rc::Ok)
      return rc;
    const Block &c = j.blocks.of(ci);
    hk.block(c, ci, std::chrono::duration<double>(Clock::now() - t_load).count());
    eng.bind(c);
    for (size_t b0 = 0; b0 < R;) {
      int C = 0;
      const Forest Fr = eng.template plant<Ps>(roots.subspan(b0), batch, C);
      const auto at = [&](int k) -> Ps & { return ps(static_cast<int>(b0) + k); };
      std::vector<std::string> why(dead.begin() + static_cast<long>(b0), dead.begin() + static_cast<long>(b0) + C);
      std::vector<uint8_t> need;
      const int good = plan_forest(eng.ex(), c, Fr.d, why, need);
      std::copy(why.begin(), why.end(), dead.begin() + static_cast<long>(b0));
      hk.plan(b0, Fr, why, need);
      if (good > 0 && !eng.eval(Fr, need.data(), at, ci + 1 == n, [&](int i, double ms) { hk.node(b0, Fr, i, ms); }, [&](int k) { hk.done(static_cast<int>(b0) + k); }))
        return Rc::Cancel;
      b0 += static_cast<size_t>(C);
    }
    if (R > 0 && std::none_of(dead.begin(), dead.end(), [](const std::string &s) { return s.empty(); })) {
      j.why = dead[0];
      return Rc::Broken;
    }
    if (eng.cancelled())
      return Rc::Cancel;
  }
  return Rc::Ok;
}
// eval_job 包在调用方钩子 h 外的一层: 进度 (Running 期 = 好根的算子节点 + 好根数, 每块重开) + 节点耗时 o.ms, 再转给 h
template <class H>
struct JobHooks : HookBase {
  H &h;
  JobOut &o;
  Progress &prog;
  void block(const Block &c, size_t ci, double s) { h.block(c, ci, s); }
  void plan(size_t b0, const Forest &Fr, const std::vector<std::string> &why, const std::vector<uint8_t> &need) {
    size_t n_run = 0;
    for (size_t i = 0; i < Fr.d.nodes.size(); ++i)
      n_run += need[i] && Fr.d.nodes[i].op >= 0;
    for (const std::string &w : why)
      n_run += w.empty();
    prog.begin(Phase::Running, n_run);
    h.plan(b0, Fr, why, need);
  }
  void node(size_t b0, const Forest &Fr, int i, double ms) {
    if (Fr.d.nodes[static_cast<size_t>(i)].op >= 0) {
      o.ms[static_cast<size_t>(i)] += ms;
      prog.publish(1);
    }
    h.node(b0, Fr, i, ms);
  }
  void done(int k) {
    prog.publish(1);
    h.done(k);
  }
};
// 一次作业 = 一趟, 根全进一张森林 (Run / Inspect / Portfolio; 根可空, 来自 RootSet 即两两不同): open_job → plan_job (探针要整期 (Ps::whole)
// 或无根 → 整期一块, 装不下则评估期截最近的日 (fit_recent); 否则前带 = 根的最长回看 + lag, 按预算分块) → pass (钩子 hk 外包 JobHooks). o 须新建, 调用方先填 o.job 的 spec.y / spec.b /
// retain / own / who. X: retain = 只留本森林的叶 / 否则评估域内读过的全留 (预算按缓存 ∪ 本森林). 块钩子只整期一块的作业用.
// 进度: Loading 期 = 天; Running 期 (每块) = 好根的算子节点 + 好根数. 结局 Ok / Empty / Broken (全部根坏) / Cancel / Stale (Empty / Broken: o.job.why)
template <class E, class Slot, class H>
Rc eval_job(E &eng, FactorFlow &F, const FactorScope &sc, const std::vector<Root> &roots, Slot &&ps, JobOut &o, H &&hk) {
  using Ps = std::remove_cvref_t<decltype(ps(0))>;
  const FeatureTable &ft = *sc.ft;
  assert(o.fr.size() == 0 && "o 须是新的");
  const int C = static_cast<int>(roots.size());
  if (C > 0)
    o.fr = plant_forest(roots, /*keep_roots=*/false);
  const factor::Dag &d = o.fr.d;
  Job &j = o.job;

  // ---- 评估域 → 前带 → 预算 → 分块 (探针按最多持有期的根) ----
  if (!open_job(F, sc, d.feats, j))
    return Rc::Empty;
  if (C > 0 && !Ps::whole) {
    int look = 0;
    for (const Root &r : roots)
      look = std::max(look, factor::search::space::lookback(*r.e));
    j.carry = carry_days(look, j.spec.y.hd, ft.seg);
  }
  {
    int nh = 0;
    for (int k = 0; k < C; ++k)
      nh = std::max(nh, ps(k).hd.n);
    const auto bud = [&] {
      const size_t nx = j.own ? j.spec.x.size() : resident_leaves(F.c, block_key(ft, j.dm, j.whole(), j.spec), j.spec.x, j.retain);
      return budget_of<E, Ps>(j, d, nx, nh);
    };
    if (j.carry < 0)
      fit_recent(F, sc, j, bud);
    if (const Rc rc = plan_job(F, j, bud()); rc != Rc::Ok)
      return rc;
  }

  // ---- 一趟: 逐块 → 块钩子 → 森林 (后序顺序走, 每节点全核; 好根即评) ----
  o.ms.assign(d.nodes.size(), 0.0);
  return pass(eng, F, j, roots, std::max(C, 1), ps, JobHooks<std::remove_reference_t<H>>{{}, hk, o, F.prog});
}

} // namespace GUI::Factors
