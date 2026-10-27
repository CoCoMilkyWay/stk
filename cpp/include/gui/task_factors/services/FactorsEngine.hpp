// FactorsEngine — 因子评估引擎 (四服务同一条管线): 作用域块 (FactorsLoad ScopeCache) → 森林 (Dag) → 逐根原料 (RootView) → 探针元组 (FactorsProbe) → 出参.
//   探针 = 一个根的一种输出, 钩子同形 (见 Probes); 四服务的差别只在探针元组 (Run / Search / Inspect / Portfolio 各自声明), 引擎与管线同一份.
//   引擎 Engine<Cpu | Gpu> 同一份接口 (后端只在构造处分叉: with_engine):
//     host_bytes(n_slots, H)                 宿主另占 (字节 / 格; 预算); GPU 另有 dev_bytes (显存)
//     bind(c)                                绑作用域块: 块 (或其代 gen) 变 → 会话作废, 下次用时重开 (Y 一个块只进会话一次)
//     walk(d, need, visit(i, ms))            顺序走 Dag (叶 = 块里的 X 平面, 截面池 g = 块的 Y 门控); ms = 算子耗时 (叶 0), visit 返回 false → 停
//     plant<Ps>(cands, C)                    一批候选的共享森林 (C 出 = 实际收的前 C 个): Ps::need = 0 → 只要 Accum, CPU ≤ batch / GPU 再按显存减半;
//                                            否则全收
//     labels<B>() / work<B>()                会话的标签 (Y 全部持有期) / 探针工作区: B = 批核 (search_session) 或逐根 (stat_session)
//     root<Ps>(d, i, f, L, p, acc)           逐根 (Ps::need ≠ 0): walk 的 visit 内根 i 的原料, 按 Ps::need 备齐
//                                            (CPU: run → accum_rows 进探针行钩子 → 腿码; GPU: stat_eval 只拷回要的原料, 会话按 Y 全部持有期, 取 L 那几组)
//     batch(d, ks, f) / batch_view(f, r, j, L, acc)  批核 (Ps::need = 0): 根 ks 一次 search_eval; kDefer (GPU) 走完森林一批根同在显存 (keep_roots),
//                                            否则 (CPU) 走到根即评一个
//   森林 forest(eng, d, need, f, ps, node, done) 两后端同一份骨架: 每个同根首个 k (Dag::first_root_at) 评一次: 探针 open → 原料 → root → done(k).
//   管线 (四服务同一份): Job (作用域 / 计划 / 块) — open_job (作用域 → 开库) → plan_job (预算 → 分块, FactorsLoad) → 逐块 load_block (ensure_xy)
//   → eval_forest (plan_forest 值域逐根归因 → forest; 末块根即 finish). eval_job = 整期一块 (Run / Inspect / Portfolio); Search 逐块逐趟走同一组
//   load_block / eval_forest; eval_basket = Portfolio 池线 (无叶无森林, load_b).
// 只给服务 .cpp include (EvalCpu → TS / CS Cpu.hpp, 受控浮点: CMake PRECISE_MATH 源).
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp" // Result
#include "factor/Search/Cpu.hpp"      // SearchSession (CPU 批核)
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
  static constexpr bool kDefer = false; // 批核走到根即评 (Search 会话一次一个, 不留根平面)
  Engine(misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, int batch) : ex_(ex), cancel_(cancel), batch_(batch) { assert(batch >= 1); }

  void bind(const ScopeCache &c) {
    if (c_ == &c && gen_ == c.gen)
      return;
    c_ = &c, gen_ = c.gen;
    stat_on_ = search_on_ = false;
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
  // ---- forest 的后端原语 ----
  misc::exec::ForkJoin &ex() const { return ex_; }
  const ScopeCache &scope() const {
    assert(c_ && "先 bind");
    return *c_;
  }
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }
  template <bool Batch>
  factor::stat::Labels labels() {
    if constexpr (Batch)
      return search_session().L;
    else
      return stat_session().L;
  }
  template <bool Batch>
  factor::cpu::stat::Workspace *work() {
    if constexpr (Batch)
      return nullptr;
    else
      return &stat_session().w;
  }
  // 逐根 (Stat 会话): run → accum_rows 进探针行钩子 → 腿码; 原料按 Ps::need 给 (x.v 恒给)
  template <class Ps>
  RootView root(const factor::Dag &d, int i, factor::stat::Frame f, const factor::stat::Labels &L, Ps &p, factor::stat::Accum *acc) {
    constexpr uint8_t need_p = Ps::need;
    static_assert(need_p != 0, "批核走 batch");
    const factor::stat::Span s = c_->span();
    const factor::check::Plane *rp = plane(i);
    rx_.resize(s.n());
    factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate.data(), f, s, L, rx_.data(), stat_session().w, acc, ProbeSink<Ps>{&p});
    RootView v;
    v.f = f;
    v.acc = acc;
    v.x = {rp->v.data(), (need_p & kNeedMask) != 0 ? rp->m.data() : nullptr};
    v.rx = (need_p & kNeedRx) != 0 ? rx_.data() : nullptr;
    if constexpr ((need_p & kNeedLc) != 0) {
      lc_.resize(s.n());
      factor::cpu::stat::leg_codes(ex_, rx_.data(), s, lc_.data());
      v.lc = lc_.data();
    }
    if constexpr ((need_p & kNeedAlpha) != 0) {
      const factor::check::Plane *ap = plane(d.nodes[static_cast<size_t>(i)].in[0]); // alpha = 根的输入: 根的 visit 内仍在槽里
      v.alpha = {ap->v.data(), ap->m.data()};
    }
    return v;
  }
  // 批核 (Search 会话): 根 ks 一次 search_eval → [ks.size()]. kDefer 假: forest 走到根即评, ks = {该根} (根平面在 visit 内仍在槽里)
  std::vector<factor::search::Result> batch(const factor::Dag &d, const std::vector<int> &ks, const factor::stat::Frame *f) {
    const int C = static_cast<int>(ks.size());
    assert(C >= 1 && "一批至少一个好根");
    std::vector<factor::stat::Plane> roots;
    for (const int k : ks) {
      assert(f[k] == f[ks.front()] && "一批同口径");
      const factor::check::Plane *rp = plane(d.roots[static_cast<size_t>(k)]);
      roots.push_back({rp->v.data(), rp->m.data()});
    }
    const factor::stat::Span s = c_->span();
    std::vector<factor::search::Result> out(static_cast<size_t>(C));
    factor::cpu::search::search_eval(ex_, search_session(), roots.data(), C, c_->y.gate.data(), f[ks.front()], s.t0_abs, s.t_lo, out.data());
    return out;
  }
  // 批核第 j 个根的原料: acc ← r.acc (L = Y 全部持有期)
  RootView batch_view(factor::stat::Frame f, const factor::search::Result &r, int j, const factor::stat::Labels &L, factor::stat::Accum *acc) {
    std::copy_n(r.acc, L.hd.n, acc);
    RootView v;
    v.f = f;
    v.acc = acc;
    v.ranks = Ranks{&sr_, j, &ranks_of};
    return v;
  }

private:
  // Stat 会话 (Y 的标签秩 + 工作区, 绑定内一次)
  factor::cpu::stat::StatSession &stat_session() {
    assert(c_ && "先 bind");
    if (!stat_on_) {
      const YPlanes &y = c_->y;
      factor::cpu::stat::stat_open(ex_, ss_, y.sh.T, y.sh.A, y.sh.seg, labels_of(y), y.ad.n);
      stat_on_ = true;
    }
    return ss_;
  }
  // Search 会话 (批核; 与 GPU search_session 同一组原语, 绑定内一次)
  factor::cpu::search::SearchSession &search_session() {
    assert(c_ && "先 bind");
    if (!search_on_) {
      const YPlanes &y = c_->y;
      factor::cpu::search::search_open(ex_, sr_, y.sh.T, y.sh.A, y.sh.seg, y.hd, y.n_full);
      factor::cpu::search::search_load(ex_, sr_, labels_of(y).l);
      search_on_ = true;
    }
    return sr_;
  }
  static void ranks_of(void *src, int k, int t0, int stride, int n_rows, uint16_t *out) {
    factor::cpu::search::search_ranks(*static_cast<const factor::cpu::search::SearchSession *>(src), k, t0, stride, n_rows, out);
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
  factor::cpu::search::SearchSession sr_;
  bool stat_on_ = false, search_on_ = false;
  std::vector<uint16_t> rx_; // 根的 r16 (逐根)
  std::vector<uint8_t> lc_;  // 根的腿码 (kNeedLc)
};

template <>
class Engine<Gpu> {
public:
  static constexpr const char *kName = "gpu";
  static constexpr bool kDefer = true; // 批核走完森林再一次 search_eval (keep_roots: 一批根同在显存)
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
    search_session();
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
  // ---- forest 的后端原语 ----
  misc::exec::ForkJoin &ex() const { return ex_; }
  const ScopeCache &scope() const {
    assert(c_ && "先 bind");
    return *c_;
  }
  bool cancelled() const { return cancel_.load(std::memory_order_relaxed); }
  template <bool Batch>
  factor::stat::Labels labels() {
    if constexpr (Batch)
      search_session();
    else
      stat_session();
    return labels_of(c_->y);
  }
  template <bool Batch>
  factor::cpu::stat::Workspace *work() {
    if constexpr (Batch)
      return nullptr;
    else
      return &stat_session();
  }
  // 逐根 (Stat 会话): stat_eval 只拷回 Ps::need 要的原料; 会话按 Y 全部持有期, 取 L 那几组
  template <class Ps>
  RootView root(const factor::Dag &d, int i, factor::stat::Frame f, const factor::stat::Labels &L, Ps &p, factor::stat::Accum *acc) {
    constexpr uint8_t need_p = Ps::need;
    static_assert(need_p != 0 && !Ps::cells, "批核走 batch; 逐格扩展只 CPU");
    assert(stat_ && "先 labels<false>");
    const YPlanes &y = c_->y;
    const factor::stat::Span s = c_->span();
    assert(s.t0_abs == 0 && s.t_lo == 0 && "GPU Stat 会话只整块");
    const size_t n = s.n();
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
    const factor::stat::Labels L0 = labels_of(y); // 会话按 Y 全部持有期出, 取 L 那几组: pick[hi] = 会话下标
    int pick[factor::stat::kMaxHold];
    for (int hi = 0; hi < L.hd.n; ++hi)
      pick[hi] = factor::stat::hold_index(L0.hd, L.hd.h[hi]);
    const factor::gpu::DevPlane *rp = at(i); // Stat 只看池内 (g 在算子内部)
    RootView v;
    v.f = f;
    if constexpr ((need_p & kNeedMask) != 0) {
      factor::gpu::download(sess_, rp, nullptr, mask_.data());
      v.x.m = mask_.data();
    }
    factor::stat::Accum all[factor::stat::kMaxHold];
    factor::gpu::stat_eval(stat_, rp, gate_, f, all, (need_p & kNeedRows) ? rows_.data() : nullptr, (need_p & kNeedLc) ? lc_.data() : nullptr,
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
      const int in0 = d.nodes[static_cast<size_t>(i)].in[0];
      const factor::DagNode &an = d.nodes[static_cast<size_t>(in0)];
      if (an.op < 0) {
        const factor::check::Plane *q = c_->x.find(d.feats[static_cast<size_t>(an.feat)]);
        v.alpha = {q->v.data(), q->m.data()};
      } else {
        factor::gpu::download(sess_, at(in0), av_.data(), am_.data());
        v.alpha = {av_.data(), am_.data()};
      }
    }
    return v;
  }
  // 批核: 走完的森林里根 ks 同在显存, 一次 search_eval → [ks.size()]
  std::vector<factor::search::Result> batch(const factor::Dag &d, const std::vector<int> &ks, const factor::stat::Frame *f) {
    const int C = static_cast<int>(ks.size());
    assert(C >= 1 && "一批至少一个好根");
    std::vector<const factor::gpu::DevPlane *> roots;
    for (const int k : ks) {
      assert(d.nodes[static_cast<size_t>(d.roots[static_cast<size_t>(k)])].slot >= 0);
      assert(f[k] == f[ks.front()] && "一批同口径");
      roots.push_back(at(d.roots[static_cast<size_t>(k)]));
    }
    const factor::stat::Span s = c_->span();
    std::vector<factor::search::Result> out(static_cast<size_t>(C));
    factor::gpu::search_eval(search_, roots.data(), C, gate_, f[ks.front()], s.t0_abs, s.t_lo, out.data());
    return out;
  }
  // 批核第 j 个根的原料: acc ← r.acc (L = Y 全部持有期)
  RootView batch_view(factor::stat::Frame f, const factor::search::Result &r, int j, const factor::stat::Labels &L, factor::stat::Accum *acc) {
    std::copy_n(r.acc, L.hd.n, acc);
    RootView v;
    v.f = f;
    v.acc = acc;
    v.ranks = Ranks{search_, j, &ranks_of};
    return v;
  }

private:
  // Stat 会话 (逐根; 绑定内一次) → 宿主探针工作区
  factor::cpu::stat::Workspace &stat_session() {
    assert(c_ && "先 bind");
    if (!stat_) {
      const YPlanes &y = c_->y;
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
      w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
    }
    return w_;
  }
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
  // Search 会话 (批核; 与 CPU search_session 同一组原语, 绑定内一次)
  void search_session() {
    assert(c_ && "先 bind");
    if (search_)
      return;
    const YPlanes &y = c_->y;
    search_ = factor::gpu::search_open(y.sh.T, y.sh.A, y.sh.seg, batch_, y.hd, y.n_full);
    factor::gpu::search_load(search_, labels_of(y).l);
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

// 走森林 (两后端同一份骨架; eng 须已 bind): 每个同根首个 k (Dag::first_root_at) 评一次: ps(k).open → 原料 → ps(k).root → done(k).
// Ps::need = 0 → 批核 batch / batch_view (出 Y 全部持有期, ps(k).hd 须空): kDefer 后端走完森林一批, 否则走到根即评 (一批一个);
// 否则逐根 (eng.root 按 need 备原料). node(i, ms) 同 walk 的 visit. false = 取消
template <class E, class Slot, class Node, class Done>
bool forest(E &eng, const factor::Dag &d, const uint8_t *need, const factor::stat::Frame *f, Slot &&ps, Node &&node, Done &&done) {
  using Ps = std::remove_cvref_t<decltype(ps(0))>;
  constexpr bool batch = Ps::need == 0, defer = batch && E::kDefer;
  const factor::stat::Labels L0 = eng.template labels<batch>();
  factor::cpu::stat::Workspace *w = eng.template work<batch>();
  const auto eval = [&](int k, auto &&view) {
    Ps &p = ps(k);
    assert((!batch || p.hd.n == 0) && "批核出 Y 全部持有期");
    const factor::stat::Labels L = labels_pick(L0, p.hd);
    const Ctx cx{eng.ex(), eng.scope(), L, w};
    p.open(cx);
    factor::stat::Accum acc[factor::stat::kMaxHold];
    p.root(cx, view(L, p, acc));
    done(k);
  };
  std::vector<int> ks; // defer: 走完再批评的根
  const bool ok = eng.walk(d, need, [&](int i, double ms) {
    if (!node(i, ms))
      return false;
    if (const int k = d.first_root_at(i); k >= 0) {
      const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
      assert(nd.op >= 0 && nd.n_in >= 1 && "根是归一算子, 至少一元");
      if constexpr (defer)
        ks.push_back(k);
      else if constexpr (batch) {
        const std::vector<factor::search::Result> out = eng.batch(d, {k}, f);
        eval(k, [&](const factor::stat::Labels &L, Ps &, factor::stat::Accum *acc) { return eng.batch_view(f[k], out[0], 0, L, acc); });
      } else
        eval(k, [&](const factor::stat::Labels &L, Ps &p, factor::stat::Accum *acc) { return eng.template root<Ps>(d, i, f[k], L, p, acc); });
    }
    return !eng.cancelled();
  });
  if constexpr (defer) {
    if (!ok)
      return false;
    const std::vector<factor::search::Result> out = eng.batch(d, ks, f);
    for (size_t j = 0; j < ks.size(); ++j)
      eval(ks[j], [&](const factor::stat::Labels &L, Ps &, factor::stat::Accum *acc) { return eng.batch_view(f[ks[j]], out[j], static_cast<int>(j), L, acc); });
  }
  return ok;
}

// ---- 管线 (四服务同一份: open_job → plan_job → 逐块 load_block → eval_forest) ----
// worker 私有常驻件 (跨请求存活): 各服务的 Flow 由它派生, 各加自己的引擎 / 缓冲
struct FactorFlow {
  misc::exec::ForkJoin ex;
  LibReaders lr; // 已建的库 (清单没变的复用)
  ScopeCache c;  // 作用域块 (X 留法见 Job::retain)
  FactorFlow() : ex(misc::exec::hw()) {}
};
// 一次作业 (四服务同一份): 作用域 es → 计划 P (分块) → 块. own 空 = 一块即 F.c (跨请求留 Y / X); 多块 (Search) / 用完即弃 (池篮子)
// 的块在 own (P.cache_all ? 每块一个 : 一个复用)
struct Job {
  EvalScope es;
  Plan P;
  YSpec ys;
  std::vector<std::string> codes; // X 叶 (open_job 的 feats; 平面下标 = 此序)
  bool retain = true;             // 块上只留 codes 的叶 / 否则作用域内读过的全留
  std::vector<ScopeCache> own;
  std::string why; // Rc::Empty (所用库未建 / 无共同落盘日) / Rc::Broken (首个坏根: 特征叶值域越界) 的原因
};
// 一次评估 (eval_job) 的出参
struct JobOut : Job {
  factor::Dag d;                // 森林: roots[k] ↔ exprs[k]
  std::vector<double> ms;       // [node] 算子耗时 (叶 / 没走的 0)
  double eval_ms(int k) const { // 根 k 子树的算子 wall 之和 (共享节点算给每个用它的根 = 单独算它要多久)
    double s = 0.0;
    for (const int i : d.subtree(d.roots[static_cast<size_t>(k)]))
      s += ms[static_cast<size_t>(i)];
    return s;
  }
};
// 作业 j 的块 k
inline ScopeCache &block_of(FactorFlow &F, Job &j, size_t k) {
  assert(k < j.P.chunks.size() && (!j.own.empty() || j.P.chunks.size() == 1) && "多块的作业块在 own");
  return j.own.empty() ? F.c : j.own[j.P.cache_all ? k : 0];
}
// 作用域 (叶 feats 所用库 + 评估期) → 开库; j.codes = feats. false = 作用域不成 (j.why; 正常输入)
inline bool open_job(FactorFlow &F, const FactorScope &sc, const std::vector<std::string> &feats, std::atomic<bool> &cancel, Job &j) {
  assert(sc.ft && sc.seg == sc.ft->seg && "作用域层与字段表层不符");
  j.codes = feats;
  if (!eval_scope(*sc.ft, sc, feats, j.es, j.why))
    return false;
  open_libs(sc, F.lr, &cancel);
  return true;
}
// 块 k 就位 (ensure_xy: j.ys / j.codes / j.retain → block_of(F, j, k)). Ok / Cancel / Stale
inline Rc load_block(FactorFlow &F, const FeatureTable &ft, Job &j, size_t k, const std::atomic<bool> &cancel, Progress &prog) {
  return ensure_xy(block_of(F, j, k), ft, j.es, F.lr, j.P.chunks[k], j.ys, j.codes, j.retain, F.ex, cancel, prog);
}
// 块 c 上评一张森林 (四服务同一份; eng 与 cancel 同旗): plan_forest (叶值域, 逐根归因) → on_plan(why[k], need) → forest;
// last (末块) → 根评完即 ps(k).finish, 再 done(k). 无好根 → Broken (why = 首个坏根的原因). Ok / Broken / Cancel
template <class E, class Slot, class OnPlan, class Node, class Done>
Rc eval_forest(E &eng, const ScopeCache &c, const factor::Dag &d, const factor::stat::Frame *frames, bool last, Slot &&ps, std::string &why,
               OnPlan &&on_plan, Node &&node, Done &&done) {
  std::vector<std::string> bad;
  std::vector<uint8_t> need;
  const int good = plan_forest(eng.ex(), c, d, bad, need);
  on_plan(bad, need);
  if (good == 0) {
    why = bad[0];
    return Rc::Broken;
  }
  if (eng.cancelled())
    return Rc::Cancel;
  eng.bind(c);
  const bool ok = forest(eng, d, need.data(), frames, ps, node, [&](int k0) {
    if (last)
      ps(k0).finish(eng.ex(), c);
    done(k0);
  });
  return ok && !eng.cancelled() ? Rc::Ok : Rc::Cancel;
}
// 森林 (eng.plant) → open_job → plan_job (整期一块, 块即 F.c) → load_block → eval_forest (on_plan(why): why[k] 非空 = 根 k BROKEN, 不评):
// 每个好根的同根首个 k0, ps(k0) 的钩子全部走完 (单块 = 末块: root 后即 finish) → done(k0). o 须新建 (每次调用一个).
// X: retain = 只留本森林的叶 / 否则作用域内读过的全留. 预算 load = Y + X, work = 引擎 (E::host_bytes), fixed = 探针 (Ps::bytes).
// 进度: Loading 期 = 天; Running 期 = 好根的算子节点 + 好根数. 结局 Ok / Empty / Broken (全部根坏) / Cancel / Stale (Empty / Broken: o.why)
template <class E, class Slot, class OnPlan, class Done>
Rc eval_job(E &eng, FactorFlow &F, const FactorScope &sc, const std::vector<const factor::expr::Expr *> &exprs, const factor::stat::Frame *frames,
            const YSpec &ys, bool retain, const char *who, Slot &&ps, std::atomic<bool> &cancel, Progress &prog, JobOut &o, OnPlan &&on_plan,
            Done &&done) {
  using Ps = std::remove_cvref_t<decltype(ps(0))>;
  const FeatureTable &ft = *sc.ft;
  assert(o.d.nodes.empty() && "o 须是新的");
  int C = 0;
  o.d = eng.template plant<Ps>(exprs, C);
  assert(C == static_cast<int>(exprs.size()));
  const factor::Dag &d = o.d;
  const auto cancelled = [&] { return cancel.load(std::memory_order_relaxed); };

  // ---- S: 作用域 → 预算 (整期一块) ----
  if (!open_job(F, sc, d.feats, cancel, o))
    return Rc::Empty;
  o.ys = ys, o.retain = retain;
  { // X = retain ? 本森林 : 缓存 ∪ 本森林
    const int days = static_cast<int>(o.es.days.size()), A = static_cast<int>(o.es.rs.axis.size()), H = ps(0).hd.n ? ps(0).hd.n : ys.hd.n;
    const bool hit = F.c.key == scope_key(ft, o.es, 0, static_cast<size_t>(days), ys);
    size_t nx = d.feats.size();
    if (hit && !retain) {
      nx = F.c.x.codes.size();
      for (const std::string &code : d.feats)
        nx += !F.c.x.find(code);
    }
    const Budget b{cell_bytes(ys) + kPlaneBytes * static_cast<double>(nx), E::host_bytes(d.n_slots, ys.hd.n),
                   Ps::bytes(Shape{days * ft.seg, A, days, ft.seg}, H)};
    o.P = plan_job(o.es, ft.seg, b, -1, who);
  }

  // ---- 块 → 森林: 后序顺序走, 每节点全核; 好根即评 ----
  if (const Rc rc = load_block(F, ft, o, 0, cancel, prog); rc != Rc::Ok)
    return rc;
  o.ms.assign(d.nodes.size(), 0.0);
  return eval_forest(
      eng, block_of(F, o, 0), d, frames, /*last=*/true, ps, o.why,
      [&](const std::vector<std::string> &why, const std::vector<uint8_t> &need) {
        size_t n_run = 0;
        for (size_t i = 0; i < d.nodes.size(); ++i)
          n_run += need[i] && d.nodes[i].op >= 0;
        for (int k = 0; k < static_cast<int>(d.roots.size()); ++k)
          n_run += d.first_root(k) == k && why[static_cast<size_t>(k)].empty();
        prog.begin(Phase::Running, n_run);
        on_plan(why);
      },
      [&](int i, double ms) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0) {
          o.ms[static_cast<size_t>(i)] = ms;
          prog.publish(1);
        }
        return !cancelled();
      },
      [&](int k0) {
        prog.publish(1);
        done(k0);
      });
}
// 日层池篮子 (Portfolio 池线; 与 eval_job 对仗, 无叶无森林): open_job → plan_job (整期一块) → load_block (块 Y = 门控 + T+1C 标签) → load_b.
// 块在 j.own (用完即弃, 不占 F.c). 结局 Ok / Empty (j.why) / Cancel / Stale
inline Rc eval_basket(FactorFlow &F, const FactorScope &sc, std::atomic<bool> &cancel, Progress &prog, Job &j, BPlanes &B) {
  const FeatureTable &ft = *sc.ft;
  assert(ft.seg == 1 && "池篮子只日层");
  assert(find_leg(ft.y.labels, factor::stat::hold_close(1)) && "日层须有 T+1C 标签 (池指数 C(D) → C(D+1))");
  if (!open_job(F, sc, {}, cancel, j))
    return Rc::Empty;
  j.ys = YSpec{holds_of({factor::stat::hold_close(1)}), {}, 0.f};
  j.P = plan_job(j.es, ft.seg, Budget{cell_bytes(j.ys) + kBasketCellBytes}, -1, "Portfolio");
  j.own.resize(1);
  if (const Rc rc = load_block(F, ft, j, 0, cancel, prog); rc != Rc::Ok)
    return rc;
  return rc_of(load_b(ft, j.es, F.lr, j.own[0], B, F.ex, cancel, prog), F.lr);
}

} // namespace GUI::Factors
