// FactorsEngine — 因子评估引擎: 作用域块 (FactorsLoad ScopeCache) 上走 DAG + Stat / Search 会话, CPU / GPU 两个特化同一份接口
// (三服务的评估流程按 E 写一遍, 后端只在构造处分叉: with_engine / Engine<Cpu | Gpu>):
//   bind(c)          绑作用域块: 块 (或其代 gen) 变 → 会话作废, 下次用时重开 (Y 一个作用域只进会话一次)
//   walk(d, need, visit(i, ms))     顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//   stat(root, f, rows, st, adist)   visit 内调: 该根的 Stat (一级 Row 非空再落地 [hd.n][T]) + 宿主收尾 finish_root → st / adist
//   forest(cands, C)                 一批候选的共享森林 (C 出 = 实际收的前 C 个: CPU ≤ batch, GPU 再按显存减半)
//   search(d, f, t0_abs, t_lo, done(k, r, ranks))   一批候选评完逐个回调; ranks(t0, stride, n_rows, out) = 该候选 r16 抽样行
// CPU: 根即算即放 (Stat / Search 都在根的 visit 里评); GPU: 输入 (X 叶 + 门控) 按 d.feats 上传, 叶集不变不重传; Search 森林 keep_roots, 一批一次 search_eval.
// 单因子流程 (Inspect / Portfolio 同一份, 只 CPU): FactorFlow 常驻件 + factor_pass 前缀 (S → Y → X → R); 净值 = Stat 逐行结果 → fold_exit (FactorsLoad)
// 只给服务 .cpp include (EvalCpu → TS / CS Cpu.hpp, 受控浮点: CMake PRECISE_MATH 源).
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Cpu.hpp"
#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <string>
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
    stat_on_ = search_on_ = false;
  }

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

  // Stat 会话 (Y 的标签秩 + 工作区, 绑定内一次); Inspect 直接拿来走自己的 sink
  factor::cpu::stat::StatSession &stat_session() {
    assert(c_ && "先 bind");
    if (!stat_on_) {
      const YPlanes &y = c_->y;
      factor::cpu::stat::stat_open(ex_, ss_, y.sh.T, y.sh.A, y.sh.seg, labels_of(y), y.ad.n);
      stat_on_ = true;
    }
    return ss_;
  }
  void stat(int root, factor::stat::Frame f, factor::stat::Row *rows, EvalStat &st, AlphaDist &adist) {
    factor::cpu::stat::StatSession &ss = stat_session();
    const YPlanes &y = c_->y;
    const size_t n = y.sh.n();
    rx_.resize(n);
    lc_.resize(n);
    const factor::check::Plane *rp = plane(root);                                       // Stat 只看池内 (g 在算子内部)
    const factor::check::Plane *ap = plane(d_->nodes[static_cast<size_t>(root)].in[0]); // alpha = 根的输入: 根的槽先于输入释放分配, 此刻仍在
    const factor::stat::Plane x{rp->v.data(), rp->m.data()};
    factor::stat::Accum acc[factor::stat::kMaxHold];
    if (rows)
      factor::cpu::stat::stat_eval(ex_, ss, x, y.gate.data(), f, acc, factor::cpu::stat::LandRows{rows, y.sh.T}, lc_.data(), rx_.data());
    else
      factor::cpu::stat::stat_eval(ex_, ss, x, y.gate.data(), f, acc, factor::cpu::stat::DropRows{}, lc_.data(), rx_.data());
    finish_root(ex_, y, RootRaw{rp->m.data(), rx_.data(), lc_.data(), acc, {ap->v.data(), ap->m.data()}}, ss.w, st, adist);
  }

  factor::Dag forest(const std::vector<const factor::expr::Expr *> &cands, int &C) const {
    assert(!cands.empty());
    C = std::min(batch_, static_cast<int>(cands.size()));
    return factor::build_forest(std::vector<const factor::expr::Expr *>(cands.begin(), cands.begin() + C), /*keep_roots=*/false);
  }
  template <class Done>
  bool search(const factor::Dag &d, factor::stat::Frame f, int t0_abs, int t_lo, Done &&done) {
    assert(c_ && "先 bind");
    const YPlanes &y = c_->y;
    if (!search_on_) {
      factor::stat::Label lab[factor::stat::kMaxHold];
      for (int hi = 0; hi < y.hd.n; ++hi)
        lab[hi] = label_of(y.labels[static_cast<size_t>(hi)], nullptr);
      factor::cpu::search::search_open(ex_, sr_, y.sh.T, y.sh.A, y.sh.seg, y.hd, y.n_full);
      factor::cpu::search::search_load(ex_, sr_, lab);
      search_on_ = true;
    }
    return walk(d, nullptr, [&](int i, double) {
      if (!d.is_root(i))
        return !cancelled();
      assert(d.nodes[static_cast<size_t>(i)].op >= 0); // 根是归一算子, 不会是特征叶
      const factor::check::Plane *rp = plane(i);
      const factor::stat::Plane x{rp->v.data(), rp->m.data()};
      factor::search::Result out;
      factor::cpu::search::search_eval(ex_, sr_, &x, 1, y.gate.data(), f, t0_abs, t_lo, &out);
      for (size_t k = 0; k < d.roots.size(); ++k)
        if (d.roots[k] == i)
          done(static_cast<int>(k), out, [&](int t0, int stride, int n_rows, uint16_t *o) { factor::cpu::search::search_ranks(sr_, 0, t0, stride, n_rows, o); });
      return !cancelled();
    });
  }

private:
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }

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
  factor::cpu::search::SearchSession sr_;
  bool stat_on_ = false, search_on_ = false;
  std::vector<uint16_t> rx_; // 根的 r16 (stat)
  std::vector<uint8_t> lc_;  // 根的腿码 (stat)
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

  template <class Visit>
  bool walk(const factor::Dag &d, const uint8_t *need, Visit &&visit) {
    inputs(d.feats);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::gpu::walk(sess_, d, dev_, dp_, sh.T, sh.A, sh.seg, gate_, need, [&](int i, const auto &, double ms) { return visit(i, ms); });
  }

  void stat(int root, factor::stat::Frame f, factor::stat::Row *rows, EvalStat &st, AlphaDist &adist) {
    const YPlanes &y = c_->y;
    if (!stat_) {
      const size_t n = y.sh.n();
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
      w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
      rx_.resize(n), lc_.resize(n), mask_.resize(n), av_.resize(n), am_.resize(n);
    }
    const factor::gpu::DevPlane *rp = at(root); // Stat 只看池内 (g 在算子内部)
    factor::gpu::download(sess_, rp, nullptr, mask_.data());
    factor::stat::Accum acc[factor::stat::kMaxHold];
    factor::gpu::stat_eval(stat_, rp, gate_, f, acc, rows, lc_.data(), rx_.data());
    const int ai = d_->nodes[static_cast<size_t>(root)].in[0]; // alpha = 根的输入: 叶 = 宿主平面, 否则拷回
    const factor::DagNode &an = d_->nodes[static_cast<size_t>(ai)];
    factor::stat::Plane alpha{av_.data(), am_.data()};
    if (an.op < 0) {
      const factor::check::Plane *p = c_->x.find(d_->feats[static_cast<size_t>(an.feat)]);
      alpha = {p->v.data(), p->m.data()};
    } else {
      factor::gpu::download(sess_, at(ai), av_.data(), am_.data());
    }
    finish_root(ex_, y, RootRaw{mask_.data(), rx_.data(), lc_.data(), acc, alpha}, w_, st, adist);
  }

  // 森林按实际槽数试装: 新增槽的显存不够就减半 (一个候选都装不下 = 块太大, 断言)
  factor::Dag forest(const std::vector<const factor::expr::Expr *> &cands, int &C) {
    assert(!cands.empty());
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
  template <class Done>
  bool search(const factor::Dag &d, factor::stat::Frame f, int t0_abs, int t_lo, Done &&done) {
    open_search();
    if (!walk(d, nullptr, [&](int, double) { return !cancel_.load(std::memory_order_relaxed); }))
      return false;
    const int C = static_cast<int>(d.roots.size());
    std::vector<const factor::gpu::DevPlane *> roots(static_cast<size_t>(C));
    for (int k = 0; k < C; ++k) {
      const int ri = d.roots[static_cast<size_t>(k)];
      assert(d.nodes[static_cast<size_t>(ri)].op >= 0 && d.nodes[static_cast<size_t>(ri)].slot >= 0);
      roots[static_cast<size_t>(k)] = at(ri);
    }
    std::vector<factor::search::Result> out(static_cast<size_t>(C));
    factor::gpu::search_eval(search_, roots.data(), C, gate_, f, t0_abs, t_lo, out.data());
    for (int k = 0; k < C; ++k)
      done(k, out[static_cast<size_t>(k)], [&](int t0, int stride, int n_rows, uint16_t *o) { factor::gpu::search_ranks(search_, k, t0, stride, n_rows, o); });
    return true;
  }

private:
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
  factor::cpu::stat::Workspace w_; // 宿主收尾 (cost_rows)
  std::vector<uint16_t> rx_;       // 根的 r16 拷回 (rank% / 成本)
  std::vector<uint8_t> lc_;        // 根的腿码拷回
  std::vector<uint8_t> mask_;      // 根掩码拷回 (valid% / rank%)
  std::vector<float> av_;          // alpha 拷回 (alpha_dist; 叶直接用宿主平面)
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

// ---- 单因子流程 (Inspect / Portfolio 同一份) ----
// worker 私有常驻件 (跨请求存活): 两服务的 Flow 由它派生, 各加自己的缓冲
struct FactorFlow {
  misc::exec::ForkJoin ex;
  LibReaders lr; // 已建的库 (清单没变的复用)
  ScopeCache c;  // 作用域块 (X 留法见 PassSpec::retain)
  Engine<Cpu> E; // 毛标签秩 (stat_session, 作用域内一次) + 活槽 (HostPool 常驻)
  explicit FactorFlow(const std::atomic<bool> &cancel) : ex(misc::exec::hw()), E(ex, cancel, 1) {}
};
// 前缀的口径 (调用方定)
struct PassSpec {
  YSpec ys;
  bool retain = false;     // X: true = 只留本因子的叶 (Portfolio) / false = 作用域内读过的全留 (Inspect)
  const char *who = "";    // assert_fits 消息
  double cell_extra = 0.0; // 调用方另占 (字节 / 格)
  double row_extra = 0.0;  // 调用方另占 (字节 / 行)
  int tail = 0;            // Running 期进度: 算子节点 + 秩之后调用方 publish 的步数
};
enum class Pass : uint8_t { Ok,
                            Empty,  // 作用域不成 (所用库未建 / 无共同落盘日), why
                            Broken, // 特征叶值域越界, why (Y 已装)
                            Cancel,
                            Stale }; // 库判废 (块已 reset)
struct FactorPass {
  factor::Dag d;
  EvalScope es;
  const factor::check::Plane *root = nullptr; // 根平面 (Stat 只看池内); 下次 walk 前有效
  factor::stat::Plane alpha;                  // 根的输入: 根的槽先于输入释放分配, walk 后仍在
  double eval_ms = 0.0;                       // 算子节点 wall 之和
  std::string why;                            // Empty / Broken 的原因
};
// 规范串 → Dag → eval_scope → open_libs → assert_fits → ensure_xy → check_leaves → walk (全核) → rank_x (g = Y 门控) 进 rx. o 须新建 (每次调用一个).
// 宿主峰值 = Y + X + 活槽 + 标签秩 2H + rx 2 + cell_extra (/ 格) + row_extra (/ 行). 进度: Loading 期 = 天; Running 期 = 算子节点 + 秩 + tail
inline Pass factor_pass(FactorFlow &F, const FactorJob &j, const PassSpec &ps, std::atomic<bool> &cancel, Progress &prog, std::vector<uint16_t> &rx,
                        FactorPass &o) {
  const FeatureTable &ft = *j.ft;
  const FactorScope &sc = j.scope;
  assert(sc.seg == ft.seg && "请求层与字段表层不符");
  assert(!o.root && o.why.empty() && o.d.nodes.empty() && "o 须是新的");
  {
    factor::expr::Expr e;
    std::string err;
    const bool ok = factor::expr::parse(j.expr, ft.lookup(), ft.seg, e, err);
    assert(ok && "Factors 表的规范串必可再解析");
    (void)ok;
    o.d = factor::build(e);
  }
  const factor::Dag &d = o.d;
  const auto cancelled = [&] { return cancel.load(std::memory_order_relaxed); };

  // ---- S: 作用域 ----
  if (!eval_scope(ft, sc, d.feats, o.es, o.why))
    return Pass::Empty;
  open_libs(sc, F.lr, &cancel);
  const EvalScope &es = o.es;
  const size_t nd = es.days.size();
  { // 宿主峰值: X = retain ? 本因子 : 缓存 ∪ 本因子
    const bool hit = F.c.key == scope_key(ft, es, 0, nd, ps.ys);
    size_t nx = d.feats.size();
    if (hit && !ps.retain) {
      nx = F.c.x.codes.size();
      for (const std::string &c : d.feats)
        nx += !F.c.x.find(c);
    }
    const int days = static_cast<int>(nd), A = static_cast<int>(es.rs.axis.size());
    const double cells = static_cast<double>(days) * ft.seg * A;
    const double per = cell_bytes(ps.ys) + kPlaneBytes * static_cast<double>(nx + static_cast<size_t>(d.n_slots)) + 2.0 * ps.ys.hd.n + 2.0 + ps.cell_extra;
    assert_fits(ps.who, cells * per + ps.row_extra * days * ft.seg, days, A, ft.seg);
  }

  // ---- Y / X: 装载 + 叶值域 ----
  const Load ld = ensure_xy(F.c, ft, es, F.lr, 0, nd, ps.ys, d.feats, ps.retain, F.ex, cancel, prog);
  if (ld == Load::Cancel)
    return Pass::Cancel;
  if (ld == Load::Stale)
    return Pass::Stale;
  check_leaves(F.ex, d, F.c, o.why);
  if (!o.why.empty())
    return Pass::Broken;
  F.E.bind(F.c);
  factor::cpu::stat::StatSession &ss = F.E.stat_session();
  if (cancelled())
    return Pass::Cancel;

  // ---- X: Dag 后序顺序走, 每节点全核 ----
  prog.begin(Phase::Running, static_cast<size_t>(d.n_ops + 1 + ps.tail));
  if (!F.E.walk(d, nullptr, [&](int i, double ms) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0) {
          o.eval_ms += ms;
          prog.publish(1);
        }
        return !cancelled();
      }))
    return Pass::Cancel;
  const int root = d.root(), ai = d.nodes[static_cast<size_t>(root)].in[0];
  assert(ai >= 0 && "根是归一算子, 至少一元");
  o.root = F.E.plane(root);
  const factor::check::Plane *ap = F.E.plane(ai);
  o.alpha = {ap->v.data(), ap->m.data()};

  // ---- R: 池内秩 ----
  const YPlanes &y = F.c.y;
  rx.resize(y.sh.n());
  factor::cpu::stat::rank_x(F.ex, factor::stat::Plane{o.root->v.data(), o.root->m.data()}, y.gate.data(), j.frame, ss.s, rx.data(), ss.w);
  prog.publish(1);
  return cancelled() ? Pass::Cancel : Pass::Ok;
}

} // namespace GUI::Factors
