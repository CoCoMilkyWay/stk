// FactorsEngine — 因子评估引擎 (四服务同一条管线): 作用域块 (FactorsLoad ScopeCache) → 森林 (Dag) → 逐根原料 (RootView) → 探针元组 (FactorsProbe) → 出参.
//   探针 = 一个根的一种输出, 钩子同形 (见 Probes); 四服务的差别只在探针元组 (Run / Search / Inspect / Portfolio 各自声明), 引擎与管线同一份.
//   引擎 Engine<Cpu | Gpu> 同一份接口 (后端只在构造处分叉: with_engine):
//     host_bytes(n_slots, H)                 宿主另占 (字节 / 格; 预算); GPU 另有 dev_bytes (显存)
//     bind(c)                                绑作用域块: 块 (或其代 gen) 变 → 会话作废, 下次用时重开 (Y 一个块只进会话一次)
//     walk(d, need, visit(i, ms))            顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//     plant<Ps>(cands, C)                    一批候选的共享森林 (C 出 = 实际收的前 C 个): Ps::need = 0 → 只要 Accum, CPU ≤ batch / GPU 再按显存减半;
//                                            否则全收
//     forest(d, need, f, ps, node, done)     走森林, 每个同根首个 k (Dag::first_root_at) 评一次: 原料按 Ps::need 备齐 → ps(k).root → done(k);
//                                            node(i, ms) 同 walk 的 visit. CPU: 根即算即放 (Stat run → accum_rows 进探针行钩子 → 腿码);
//                                            GPU: Ps::need = 0 → 一批根同在显存一次 search_eval (keep_roots), 否则逐根 stat_eval 只拷回要的原料
//                                            (会话按 Y 全部持有期, 取 ps(k).hd 那几组)
//   管线 open_job (作用域 → 开库; 四服务同一份) + eval_job (Run / Inspect / Portfolio 同一份; Search 分块自走 plant / forest):
//   open_job → 预算 → 装块 → plan_forest (值域, 逐根归因) → forest.
// 只给服务 .cpp include (EvalCpu → TS / CS Cpu.hpp, 受控浮点: CMake PRECISE_MATH 源).
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp" // Result
#include "gui/task_factors/services/FactorsProbe.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <string>
#include <type_traits>
#include <vector>

namespace GUI::Factors {

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
  static double host_bytes(int n_slots, int H) { return kPlaneBytes * n_slots + 2.0 * H + 3.0; }

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
      if (const int k = d.first_root_at(i); k >= 0) {
        const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
        assert(nd.op >= 0 && nd.n_in >= 1 && "根是归一算子, 至少一元");
        Ps &p = ps(k);
        const factor::stat::Labels L = labels_pick(ss.L, p.hd);
        const Ctx cx{ex_, *c_, L, &ss.w};
        p.open(cx);
        const factor::check::Plane *rp = plane(i), *ap = plane(nd.in[0]); // alpha = 根的输入: 根的 visit 内仍在槽里
        const factor::stat::Plane x{rp->v.data(), rp->m.data()};
        factor::stat::Accum acc[factor::stat::kMaxHold];
        factor::cpu::stat::run(ex_, x, c_->y.gate.data(), f[k], s, L, rx_.data(), ss.w, acc, ProbeSink<Ps>{&p});
        if constexpr ((Ps::need & kNeedLc) != 0)
          factor::cpu::stat::leg_codes(ex_, rx_.data(), s, lc_.data());
        p.root(cx, RootView{x, {ap->v.data(), ap->m.data()}, f[k], rx_.data(), (Ps::need & kNeedLc) ? lc_.data() : nullptr, acc, Ranks{this, 0, &ranks_of}});
        done(k);
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
  static double host_bytes(int, int) { return 4.0 + kPlaneBytes; }
  // 显存常驻 (字节 / 格): 特征 5·nf + 门控 1 + 搜索会话标签 (lv, sv, m, ry) 7·H + 活槽 5·n_slots; 每个批核根另加 kDevRootBytes (根平面 5 + rx 2)
  static double dev_bytes(int nf, int n_slots, int H) { return kPlaneBytes * nf + 1.0 + 7.0 * H + kPlaneBytes * n_slots; }
  static constexpr double kDevRootBytes = kPlaneBytes + 2.0;

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
      const factor::stat::Labels L0 = labels_of(y);
      return walk(d, need, [&](int i, double ms) {
        if (!node(i, ms))
          return false;
        if (const int k = d.first_root_at(i); k >= 0) {
          const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
          assert(nd.op >= 0 && nd.n_in >= 1 && "根是归一算子, 至少一元");
          Ps &p = ps(k);
          const factor::stat::Labels L = labels_pick(L0, p.hd); // 会话按 Y 全部持有期出, 取 p.hd 那几组: pick[hi] = 会话下标
          int pick[factor::stat::kMaxHold];
          for (int hi = 0; hi < L.hd.n; ++hi) {
            pick[hi] = 0;
            while (L0.hd.h[pick[hi]] != L.hd.h[hi])
              ++pick[hi];
          }
          const Ctx cx{ex_, *c_, L, &w_};
          p.open(cx);
          const factor::gpu::DevPlane *rp = at(i); // Stat 只看池内 (g 在算子内部)
          RootView v;
          v.f = f[k];
          if constexpr ((need_p & kNeedMask) != 0) {
            factor::gpu::download(sess_, rp, nullptr, mask_.data());
            v.x.m = mask_.data();
          }
          factor::stat::Accum all[factor::stat::kMaxHold], acc[factor::stat::kMaxHold];
          factor::gpu::stat_eval(stat_, rp, gate_, f[k], all, (need_p & kNeedRows) ? rows_.data() : nullptr, (need_p & kNeedLc) ? lc_.data() : nullptr,
                                 (need_p & kNeedRx) ? rx_.data() : nullptr);
          for (int hi = 0; hi < L.hd.n; ++hi)
            acc[hi] = all[pick[hi]];
          v.acc = acc;
          v.rx = (need_p & kNeedRx) ? rx_.data() : nullptr;
          v.lc = (need_p & kNeedLc) ? lc_.data() : nullptr;
          if constexpr ((need_p & kNeedRows) != 0) {
            const auto e = p.ext();
            ex_.rows(s.T, [&](int t0, int t1, int) {
              for (int hi = 0; hi < L.hd.n; ++hi)
                for (int t = t0; t < t1; ++t)
                  p.row(hi, t, rows_[static_cast<size_t>(pick[hi]) * s.T + t], e);
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
// 作用域 (叶 feats 所用库 + 评估期) → 开库 (四服务同一份). false = 作用域不成 (why; 正常输入)
inline bool open_job(FactorFlow &F, const FeatureTable &ft, const FactorScope &sc, const std::vector<std::string> &feats, std::atomic<bool> &cancel,
                     EvalScope &es, std::string &why) {
  if (!eval_scope(ft, sc, feats, es, why))
    return false;
  open_libs(sc, F.lr, &cancel);
  return true;
}
// 森林 (eng.plant) → open_job → assert_fits → ensure_xy → plan_forest (on_plan(): o.why 已定) → eng.forest:
// 每个好根的同根首个 k0, ps(k0) 的钩子全部走完 (单块: root 后即 finish) → done(k0). o 须新建 (每次调用一个).
// X: retain = 只留本森林的叶 / 否则作用域内读过的全留. 宿主峰值 = Y + X + 引擎 (E::host_bytes) / 格 + 探针 (Ps::bytes).
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
  if (!open_job(F, ft, sc, d.feats, cancel, o.es, o.empty))
    return Rc::Empty;
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
    const double per = cell_bytes(ys) + kPlaneBytes * static_cast<double>(nx) + E::host_bytes(d.n_slots, ys.hd.n);
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
