// FactorsEngine — 因子评估引擎: 作用域块 (FactorsLoad ScopeCache) 上走 DAG + Stat / Search 会话, CPU / GPU 两个特化同一份接口
// (三服务的评估流程按 E 写一遍, 后端只在构造处分叉: with_engine / Engine<Cpu | Gpu>):
//   bind(c)          绑作用域块: 块 (或其代 gen) 变 → 会话作废, 下次用时重开 (Y 一个作用域只进会话一次)
//   walk(d, need, visit(i, ms))     顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//   stat(root, f, rows, st, xd)      visit 内调: 该根的 Stat (一级 Row 非空再落地 [hd.n][T]) + 宿主收尾 finish_root → st / xd
//   forest(cands, C)                 一批候选的共享森林 (C 出 = 实际收的前 C 个: CPU ≤ batch, GPU 再按显存减半)
//   search(d, f, t0_abs, t_lo, done(k, r, ranks))   一批候选评完逐个回调; ranks(t0, stride, n_rows, out) = 该候选 r16 抽样行
// CPU: 根即算即放 (Stat / Search 都在根的 visit 里评); GPU: 输入 (X 叶 + 门控) 按 d.feats 上传, 叶集不变不重传; Search 森林 keep_roots, 一批一次 search_eval.
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
  void stat(int root, factor::stat::Frame f, factor::stat::Row *rows, EvalStat &st, XDist &xd) {
    factor::cpu::stat::StatSession &ss = stat_session();
    const YPlanes &y = c_->y;
    const size_t n = y.sh.n();
    rx_.resize(n);
    lc_.resize(n);
    const factor::check::Plane *rp = plane(root);                                       // Stat 只看池内 (g 在算子内部)
    const factor::check::Plane *xp = plane(d_->nodes[static_cast<size_t>(root)].in[0]); // 根输入: 根的槽先于输入释放分配, 此刻仍在
    const factor::stat::Plane x{rp->v.data(), rp->m.data()};
    factor::stat::Accum acc[factor::stat::kMaxHold];
    if (rows)
      factor::cpu::stat::stat_eval(ex_, ss, x, y.gate.data(), f, acc, factor::cpu::stat::LandRows{rows, y.sh.T}, lc_.data(), rx_.data());
    else
      factor::cpu::stat::stat_eval(ex_, ss, x, y.gate.data(), f, acc, factor::cpu::stat::DropRows{}, lc_.data(), rx_.data());
    finish_root(ex_, y, RootRaw{rp->m.data(), rx_.data(), lc_.data(), acc, {xp->v.data(), xp->m.data()}}, ss.w, st, xd);
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

  void stat(int root, factor::stat::Frame f, factor::stat::Row *rows, EvalStat &st, XDist &xd) {
    const YPlanes &y = c_->y;
    if (!stat_) {
      const size_t n = y.sh.n();
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
      w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
      rx_.resize(n), lc_.resize(n), mask_.resize(n), xv_.resize(n), xm_.resize(n);
    }
    const factor::gpu::DevPlane *rp = at(root); // Stat 只看池内 (g 在算子内部)
    factor::gpu::download(sess_, rp, nullptr, mask_.data());
    factor::stat::Accum acc[factor::stat::kMaxHold];
    factor::gpu::stat_eval(stat_, rp, gate_, f, acc, rows, lc_.data(), rx_.data());
    const int xi = d_->nodes[static_cast<size_t>(root)].in[0]; // 根输入: 叶 = 宿主平面, 否则拷回
    const factor::DagNode &xn = d_->nodes[static_cast<size_t>(xi)];
    factor::stat::Plane x{xv_.data(), xm_.data()};
    if (xn.op < 0) {
      const factor::check::Plane *p = c_->x.find(d_->feats[static_cast<size_t>(xn.feat)]);
      x = {p->v.data(), p->m.data()};
    } else {
      factor::gpu::download(sess_, at(xi), xv_.data(), xm_.data());
    }
    finish_root(ex_, y, RootRaw{mask_.data(), rx_.data(), lc_.data(), acc, x}, w_, st, xd);
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
  std::vector<float> xv_;          // 根输入拷回 (x_dist; 叶直接用宿主平面)
  std::vector<uint8_t> xm_;
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

} // namespace GUI::Factors
