// Engine — 因子评估引擎与统一管线 (Run / Search / Inspect / Portfolio / Agent 观察共用):
//   森林 Forest: 去重算子 DAG + 一次性解析的 LeafId 表 + 因子根 (经 Stat) + 可选纯表达式根 (直取平面, 不经 Stat).
//   双后端同构 Backend<Cpu | Gpu> (挂在进程唯一 Store::exec 上跨文件 / 跨趟常驻复用, 内存与显存纳入 Store 统一预算):
//     budget<Ps>(d, H) / batch_of(P, b) / kRanks
//     bind(blk) / release() / drop()
//     walk(cancel, F, need, on_node) / eval<Ps>(cancel, F, need, ps, last, on_node, on_step)
//   单一分块流式管线:
//     prepare  → 评估域 + 规格 + 前后带 + 预算 (fit_years / plan_chunks)
//     run_pass → init(PassCtx) → 逐块 [d_lo, d_hi) load_block → 逐批 plant + plan_forest + eval(open → row → close) → finish(PassCtx)
//     eval_forest  = 单森林单趟作业 (Run / Inspect / Portfolio)
//     eval_baskets = 池篮子与个股账作业 (Portfolio 行情两侧)
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp" // Result / hold_reach
#include "factor/Search/Space.hpp"    // lookback
#include "gui/engine/Probe.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Engine {

// ---- 森林: 因子根 + 可选纯表达式根 + LeafId 一步到位 ----
struct Root {
  const factor::expr::Expr *e = nullptr;
  factor::stat::Frame f = factor::stat::Frame::CS;
  factor::stat::Dir d = factor::stat::kFwd;
};

struct RootSet {
  Holds hd;
  std::deque<factor::expr::Expr> es;
  std::vector<Root> r;
  std::vector<int> of; // [条目] → 唯一根下标
  void add(const FeatureTable &ft, const std::string &canon, factor::stat::Frame f, factor::stat::Dir d) {
    size_t k = 0;
    while (k < r.size() && (r[k].e->canon != canon || r[k].f != f || r[k].d != d))
      ++k;
    if (k == r.size())
      r.push_back({&es.emplace_back(ft.reparse(canon)), f, d});
    of.push_back(static_cast<int>(k));
  }
  size_t size() const { return r.size(); }
};

struct Forest {
  factor::Dag d;
  std::vector<LeafId> leaves;       // d.feats 对应的 LeafId
  std::vector<Root> r;              // [k] 因子根 ↔ d.roots[k] (k < r.size()); 其后 d.roots[r.size()..] 为纯表达式根
  std::vector<std::vector<int>> at; // [node] → 因子根 k
  size_t size() const { return r.size(); }
};

inline Forest plant_forest(const FeatureTable &ft, std::span<const Root> rs, bool keep_roots,
                           std::span<const factor::expr::Expr *const> plain = {}) {
  assert(!rs.empty() || !plain.empty());
  std::vector<const factor::expr::Expr *> es;
  es.reserve(rs.size() + plain.size());
  for (const Root &x : rs)
    es.push_back(x.e);
  for (const factor::expr::Expr *e : plain)
    es.push_back(e);
  Forest o{factor::build_forest(es, keep_roots || !plain.empty()), ft.x.ids({}), {rs.begin(), rs.end()}, {}};
  o.leaves = ft.x.ids(o.d.feats);
  o.at.resize(o.d.nodes.size());
  for (size_t k = 0; k < o.r.size(); ++k) {
    const int i = o.d.roots[k];
    assert(o.d.nodes[static_cast<size_t>(i)].op >= 0 && o.d.nodes[static_cast<size_t>(i)].n_in >= 1 && "因子根须是归一算子");
    o.at[static_cast<size_t>(i)].push_back(static_cast<int>(k));
  }
  return o;
}

inline Bands bands_of(int look, const factor::stat::Holds &hd, int seg) {
  const int h = factor::search::hold_reach(hd, seg);
  return {(look + h + seg - 1) / seg + factor::stat::kChainDays, (h + seg - 1) / seg};
}

inline bool same_holds(const Holds &a, const Holds &b) { return a.n == b.n && std::equal(a.k, a.k + a.n, b.k); }

template <class Ps>
inline constexpr bool kBatched = (Ps::need & ~kNeedRanks) == 0;

// ---- 双后端同构: Backend<Cpu> ⇄ Backend<Gpu> ----
struct Cpu {};
struct Gpu {};
template <class B>
class Backend;

template <class D>
class BackendBase {
public:
  misc::exec::ForkJoin &ex() const { return ex_; }
  const Block &block() const {
    assert(c_ && "先 bind");
    return *c_;
  }
  template <class Ps, class Node, class Step>
  bool eval(const std::atomic<bool> &cancel, const Forest &F, const uint8_t *need, std::span<Ps> ps, bool last, Node &&on_node, Step &&on_step) {
    assert(ps.size() == F.size());
    constexpr bool defer = D::template kDefer<Ps>;
    std::vector<int> ks;
    const bool ok = self().walk_dag(cancel, F.d, F.leaves, need, [&](int i, double ms) {
      on_node(i, ms);
      const std::vector<int> &at = F.at[static_cast<size_t>(i)];
      ks.insert(ks.end(), at.begin(), at.end());
      if (!defer && !ks.empty()) {
        roots<Ps>(F, ks, ps, last, on_step);
        ks.clear();
      }
      return !cancel.load(std::memory_order_relaxed);
    });
    if (defer && ok && !ks.empty())
      roots<Ps>(F, ks, ps, last, on_step);
    return ok;
  }

protected:
  explicit BackendBase(misc::exec::ForkJoin &ex) : ex_(ex) {}
  D &self() { return static_cast<D &>(*this); }

  misc::exec::ForkJoin &ex_;
  const Block *c_ = nullptr;
  const factor::Dag *d_ = nullptr;

private:
  template <class Ps, class Step>
  void roots(const Forest &F, const std::vector<int> &ks, std::span<Ps> ps, bool last, Step &on_step) {
    factor::cpu::stat::Workspace *w = self().template ready<Ps>(F, ks);
    const factor::stat::Labels L = self().labels();
    const uint8_t need = Ps::need_at(last);
    for (size_t j = 0; j < ks.size(); ++j) {
      const int k = ks[j];
      Ps &p = ps[static_cast<size_t>(k)];
      const Ctx cx{ex_, block(), L, w, F.r[static_cast<size_t>(k)].d, last};
      p.open(cx);
      factor::stat::Accum acc[factor::stat::kMaxHold];
      const RootView v = self().template view<Ps>(F, k, static_cast<int>(j), cx, need, p, acc);
      for (int hi = 0; hi < L.hd.n; ++hi)
        acc[hi] = factor::stat::oriented(acc[hi], cx.d);
      p.close(cx, v);
      on_step(k);
    }
  }
};

template <>
class Backend<Cpu> : public BackendBase<Backend<Cpu>> {
  friend class BackendBase<Backend<Cpu>>;

public:
  static constexpr const char *kName = "cpu";
  template <class Ps>
  static constexpr bool kDefer = false;
  static constexpr int kBatch = 64;
  static constexpr bool kRanks = true;

  explicit Backend(misc::exec::ForkJoin &ex) : BackendBase(ex) {}

  template <class Ps>
  static Budget budget(const factor::Dag &d, int H) {
    return {.load = 2.0 * H, .work = kPlaneBytes * (d.n_slots + (kBatched<Ps> ? 2 : 0)) + 3.0};
  }
  static int batch_of(const Plan &, const Budget &) { return kBatch; }

  void bind(const Block &c) { c_ = &c; }
  void drop() {
    hp_ = {}, sc_ = {}, w_ = {};
    in_ = {}, rx_ = {}, ec_ = {};
  }
  void release() {
    drop();
    c_ = nullptr, d_ = nullptr;
  }
  double host_bytes() const {
    double b = static_cast<double>(hp_.bytes()) + static_cast<double>(rx_.capacity()) * sizeof(uint16_t) + static_cast<double>(ec_.capacity());
    for (const factor::check::Arena &a : sc_)
      b += static_cast<double>(a.buf.capacity());
    return b;
  }

  template <class Visit>
  bool walk_dag(const std::atomic<bool> &cancel, const factor::Dag &d, std::span<const LeafId> leaves, const uint8_t *need, Visit &&visit) {
    in_ = leaves_of(block(), leaves);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::cpu::walk(ex_, d, in_, hp_, sc_, sh.T, sh.A, sh.seg, c_->y.gate, need, [&](int i, const auto &, double ms) {
      return visit(i, ms) && !cancel.load(std::memory_order_relaxed);
    });
  }
  const factor::check::Plane *plane(int i) const { return factor::cpu::plane_at(*d_, in_, hp_, i); }

  // 供流式观察直接跑一次单因子 Stat, 复用常驻 rx_ / w_
  template <class Sink>
  uint16_t *stat_run(const factor::check::Plane *rp, factor::stat::Frame f, const factor::stat::Span &s, const factor::stat::Labels &L,
                     factor::stat::Accum *acc, const Sink &sink) {
    w_.ensure(s.A, L.hd.n, 0, ex_.threads());
    rx_.resize(s.n());
    factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, f, s, L, rx_.data(), w_, acc, sink);
    return rx_.data();
  }

  template <class Ps>
  Forest plant(const FeatureTable &ft, std::span<const Root> rs, int batch, int &C) const {
    assert(!rs.empty() && batch >= 1);
    C = std::min(batch, static_cast<int>(rs.size()));
    return plant_forest(ft, rs.first(static_cast<size_t>(C)), /*keep_roots=*/false);
  }

private:
  factor::stat::Labels labels() const {
    const factor::stat::Labels L = labels_of(c_->y);
    factor::stat::assert_labels(L, c_->y.sh.seg, false);
    return L;
  }
  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &, const std::vector<int> &) {
    const YPlanes &y = block().y;
    w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
    return &w_;
  }
  template <class Ps>
  RootView view(const Forest &F, int k, int, const Ctx &cx, uint8_t need, Ps &p, factor::stat::Accum *acc) {
    const int i = F.d.roots[static_cast<size_t>(k)];
    const factor::stat::Frame f = F.r[static_cast<size_t>(k)].f;
    const factor::stat::Span s = c_->span();
    const factor::check::Plane *rp = plane(i);
    rx_.resize(s.n());
    if ((need & kNeedRows) != 0 || Ps::cells)
      factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, f, s, cx.L, rx_.data(), w_, acc, ProbeSink<Ps>{&p, cx.d});
    else
      factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, f, s, cx.L, rx_.data(), w_, acc, factor::cpu::stat::DropRows{});
    RootView v;
    v.f = f;
    v.acc = acc;
    v.x = {(need & kNeedX) != 0 ? rp->v.data() : nullptr, (need & kNeedMask) != 0 ? rp->m.data() : nullptr};
    v.rx = (need & kNeedRx) != 0 ? rx_.data() : nullptr;
    v.ranks = Ranks{this, 0, &ranks_of};
    if ((need & kNeedEc) != 0) {
      ec_.resize(s.n());
      factor::cpu::stat::end_codes(ex_, rx_.data(), s, cx.d, ec_.data());
      v.ec = ec_.data();
    }
    if ((need & kNeedAlpha) != 0) {
      const factor::check::Plane *ap = plane(F.d.nodes[static_cast<size_t>(i)].in[0]);
      v.alpha = {ap->v.data(), ap->m.data()};
    }
    return v;
  }
  static void ranks_of(void *src, int, int t0, int stride, int n_rows, uint16_t *out) {
    const Backend &e = *static_cast<const Backend *>(src);
    const size_t A = static_cast<size_t>(e.c_->y.sh.A);
    for (int r = 0; r < n_rows; ++r)
      std::copy_n(e.rx_.data() + static_cast<size_t>(t0 + r * stride) * A, A, out + static_cast<size_t>(r) * A);
  }

  std::vector<const factor::check::Plane *> in_;
  factor::cpu::HostPool hp_;
  std::vector<factor::check::Arena> sc_;
  factor::cpu::stat::Workspace w_;
  std::vector<uint16_t> rx_;
  std::vector<uint8_t> ec_;
};

template <>
class Backend<Gpu> : public BackendBase<Backend<Gpu>> {
  friend class BackendBase<Backend<Gpu>>;

public:
  static constexpr const char *kName = "gpu";
  template <class Ps>
  static constexpr bool kDefer = kBatched<Ps>;
  static constexpr int kBatch = 8;
  static constexpr bool kRanks = false;
  static constexpr double kDevRootBytes = kPlaneBytes + 2.0;

  explicit Backend(misc::exec::ForkJoin &ex) : BackendBase(ex) { assert(factor::gpu::available() && "GPU 后端不可用"); }
  ~Backend() { release_vram(); }
  Backend(const Backend &) = delete;
  Backend &operator=(const Backend &) = delete;

  template <class Ps>
  static Budget budget(const factor::Dag &d, int H) {
    constexpr bool batch = kBatched<Ps>;
    const double dev = kPlaneBytes * static_cast<double>(d.feats.size()) + 2.0 + 7.0 * H + kPlaneBytes * d.n_slots * (batch ? 2 : 1);
    return {.work = batch ? 0.0 : 4.0 + kPlaneBytes, .dev = batch ? dev + kDevRootBytes * kBatch : dev + 3.0};
  }
  static int batch_of(const Plan &P, const Budget &b) {
    const double nmax = P.max_cells(), dv = b.dev - kDevRootBytes * kBatch;
    return std::clamp(static_cast<int>((P.vram - nmax * dv) / (kDevRootBytes * nmax)), 1, factor::gpu::search_max_batch());
  }

  void bind(const Block &c) {
    c_ = &c;
    const size_t n = c.y.sh.n();
    if (n != n_) {
      release_vram();
      sess_ = factor::gpu::session_open(n);
      n_ = n;
    }
    if (x_gen_ != c.x_gen) {
      for (auto &[_, e] : x_cache_)
        free_x_.push_back(e.p);
      x_cache_.clear();
      if (!gate_)
        gate_ = factor::gpu::mask_new(sess_);
      factor::gpu::upload_to(sess_, gate_, nullptr, c.y.gate);
      x_gen_ = c.x_gen;
    }
  }
  void drop() {
    w_ = {}, out_ = {}, rows_ = {}, rx_ = {}, ec_ = {};
    xv_ = {}, mask_ = {}, av_ = {}, am_ = {};
  }
  void release() {
    release_vram();
    drop();
    c_ = nullptr, d_ = nullptr;
  }
  double host_bytes() const {
    return static_cast<double>(rows_.capacity()) * sizeof(factor::stat::Row) + static_cast<double>(rx_.capacity()) * sizeof(uint16_t) +
           static_cast<double>(ec_.capacity()) + static_cast<double>(xv_.capacity() + av_.capacity()) * sizeof(float) +
           static_cast<double>(mask_.capacity() + am_.capacity());
  }
  double vram_bytes() const {
    if (!sess_)
      return 0.0;
    return static_cast<double>(factor::gpu::session_bytes(sess_) + dp_.bytes(n_) + (gate_ ? n_ : 0) +
                               (x_cache_.size() + free_x_.size()) * (sizeof(float) + 1) * n_ + factor::gpu::stat_bytes(stat_) +
                               factor::gpu::search_bytes(search_));
  }

  template <class Visit>
  bool walk_dag(const std::atomic<bool> &cancel, const factor::Dag &d, std::span<const LeafId> leaves, const uint8_t *need, Visit &&visit) {
    inputs(leaves);
    if (d.n_slots > static_cast<int>(dp_.slots.size()))
      evict_unused_x((static_cast<size_t>(d.n_slots) - dp_.slots.size()) * (sizeof(float) + 1) * n_);
    d_ = &d;
    const Shape &sh = c_->y.sh;
    return factor::gpu::walk(sess_, d, dev_, dp_, sh.T, sh.A, sh.seg, gate_, need, [&](int i, const auto &, double ms) {
      return visit(i, ms) && !cancel.load(std::memory_order_relaxed);
    });
  }

  template <class Ps>
  Forest plant(const FeatureTable &ft, std::span<const Root> rs, int batch, int &C) {
    assert(!rs.empty() && batch >= 1);
    C = std::min(batch, static_cast<int>(rs.size()));
    if constexpr (!kBatched<Ps>)
      return plant_forest(ft, rs.first(static_cast<size_t>(C)), /*keep_roots=*/false);
    search_session(batch);
    while (true) {
      Forest F = plant_forest(ft, rs.first(static_cast<size_t>(C)), /*keep_roots=*/true);
      inputs(F.leaves);
      size_t vfree = 0, vtotal = 0;
      factor::gpu::mem_info(&vfree, &vtotal);
      const size_t grow = F.d.n_slots > static_cast<int>(dp_.slots.size()) ? static_cast<size_t>(F.d.n_slots) - dp_.slots.size() : 0;
      const double need_b = static_cast<double>(grow) * kPlaneBytes * static_cast<double>(n_);
      if (need_b <= 0.9 * (static_cast<double>(vfree) + static_cast<double>(reclaimable_x_bytes()))) {
        if (grow > 0)
          evict_unused_x(grow * (sizeof(float) + 1) * n_);
        return F;
      }
      assert(C > 1 && "显存不够评一个候选: 块太大");
      C /= 2;
    }
  }

private:
  struct LeafEntry {
    factor::gpu::DevPlane *p = nullptr;
    uint64_t tick = 0;
    bool pinned = false;
  };

  size_t reclaimable_x_bytes() const {
    size_t k = free_x_.size();
    for (const auto &[_, e] : x_cache_)
      k += !e.pinned;
    return k * (sizeof(float) + 1) * n_;
  }
  void evict_unused_x(size_t need_bytes) {
    if (!sess_ || need_bytes == 0)
      return;
    size_t vfree = 0, vtotal = 0;
    factor::gpu::mem_info(&vfree, &vtotal);
    const size_t unit = (sizeof(float) + 1) * n_;
    while (static_cast<double>(need_bytes) > 0.9 * static_cast<double>(vfree)) {
      if (!free_x_.empty()) {
        factor::gpu::plane_del(sess_, free_x_.back());
        free_x_.pop_back();
        vfree += unit;
        continue;
      }
      auto lru = x_cache_.end();
      for (auto it = x_cache_.begin(); it != x_cache_.end(); ++it)
        if (!it->second.pinned && (lru == x_cache_.end() || it->second.tick < lru->second.tick))
          lru = it;
      if (lru == x_cache_.end())
        break;
      factor::gpu::plane_del(sess_, lru->second.p);
      x_cache_.erase(lru);
      vfree += unit;
    }
  }
  void inputs(std::span<const LeafId> leaves) {
    assert(c_ && sess_ && "先 bind");
    for (auto &[_, e] : x_cache_)
      e.pinned = false;
    size_t miss = 0;
    for (const LeafId id : leaves)
      if (const auto it = x_cache_.find(id); it != x_cache_.end())
        it->second.pinned = true, it->second.tick = ++x_tick_;
      else
        ++miss;
    if (miss > free_x_.size()) {
      size_t vfree = 0, vtotal = 0;
      factor::gpu::mem_info(&vfree, &vtotal);
      const size_t unit = (sizeof(float) + 1) * n_;
      while (miss > free_x_.size() && static_cast<double>((miss - free_x_.size()) * unit) > 0.9 * static_cast<double>(vfree)) {
        auto lru = x_cache_.end();
        for (auto it = x_cache_.begin(); it != x_cache_.end(); ++it)
          if (!it->second.pinned && (lru == x_cache_.end() || it->second.tick < lru->second.tick))
            lru = it;
        if (lru == x_cache_.end())
          break;
        free_x_.push_back(lru->second.p);
        x_cache_.erase(lru);
      }
      while (free_x_.size() < miss)
        free_x_.push_back(factor::gpu::plane_new(sess_));
    }
    dev_.clear();
    dev_.reserve(leaves.size());
    for (const LeafId id : leaves) {
      if (const auto it = x_cache_.find(id); it != x_cache_.end()) {
        dev_.push_back(it->second.p);
        continue;
      }
      factor::gpu::DevPlane *p = free_x_.back();
      free_x_.pop_back();
      const Leaf *lf = c_->x.find(id);
      assert(lf && "叶不在块上 (BlockSpec::x)");
      factor::gpu::upload_to(sess_, p, lf->v.data(), lf->m.data());
      x_cache_.emplace(id, LeafEntry{p, ++x_tick_, true});
      dev_.push_back(p);
    }
  }
  void release_vram() {
    dp_.release();
    if (stat_)
      factor::gpu::stat_close(stat_);
    if (search_)
      factor::gpu::search_close(search_);
    stat_ = nullptr, search_ = nullptr;
    st_gen_ = 0, sr_gen_ = 0;
    if (sess_) {
      if (gate_)
        factor::gpu::plane_del(sess_, gate_);
      for (auto &[_, e] : x_cache_)
        factor::gpu::plane_del(sess_, e.p);
      for (factor::gpu::DevPlane *p : free_x_)
        factor::gpu::plane_del(sess_, p);
      factor::gpu::session_close(sess_);
    }
    sess_ = nullptr, gate_ = nullptr;
    x_cache_.clear(), free_x_.clear(), dev_.clear();
    n_ = 0, x_gen_ = 0;
  }

  factor::stat::Labels labels() const { return labels_of(c_->y); }
  const factor::gpu::DevPlane *at(int i) const { return factor::gpu::plane_at(*d_, dev_, dp_, i); }

  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &F, const std::vector<int> &ks) {
    if constexpr (!kBatched<Ps>) {
      return &stat_session();
    } else {
      assert(search_ && "批核先 plant");
      const factor::stat::Frame f = F.r[static_cast<size_t>(ks.front())].f;
      std::vector<const factor::gpu::DevPlane *> roots;
      roots.reserve(ks.size());
      for (const int k : ks) {
        const int r = F.d.roots[static_cast<size_t>(k)];
        assert(F.d.nodes[static_cast<size_t>(r)].slot >= 0);
        assert(F.r[static_cast<size_t>(k)].f == f && "一批同口径");
        roots.push_back(at(r));
      }
      const factor::stat::Span s = c_->span();
      out_.assign(ks.size(), {});
      factor::gpu::search_eval(search_, roots.data(), static_cast<int>(ks.size()), gate_, f, s.t0_abs, s.t_lo, s.t_back, out_.data());
      return nullptr;
    }
  }
  template <class Ps>
  RootView view(const Forest &F, int k, int j, const Ctx &cx, uint8_t need, Ps &p, factor::stat::Accum *acc) {
    if constexpr (kBatched<Ps>) {
      assert(cx.d == factor::stat::kFwd && "批核的根恒正向 (Search 打分时自己定向)");
      std::copy_n(out_[static_cast<size_t>(j)].acc, cx.L.hd.n, acc);
      RootView v;
      v.f = F.r[static_cast<size_t>(k)].f;
      v.acc = acc;
      v.ranks = Ranks{search_, j, &ranks_of};
      return v;
    } else {
      return stat_root(F, k, cx, need, p, acc);
    }
  }
  template <class Ps>
  RootView stat_root(const Forest &F, int k, const Ctx &cx, uint8_t need, Ps &p, factor::stat::Accum *acc) {
    static_assert(!Ps::cells && (Ps::need & kNeedRanks) == 0, "逐格扩展只 CPU; 抽样行只批核");
    RootView v;
    v.f = F.r[static_cast<size_t>(k)].f;
    v.acc = acc;
    const factor::stat::Span s = c_->span();
    const size_t n = s.n();
    const int i = F.d.roots[static_cast<size_t>(k)], H = cx.L.hd.n;
    const bool rows = (need & kNeedRows) != 0, rx = (need & kNeedRx) != 0, ec = (need & kNeedEc) != 0;
    if (rows)
      rows_.resize(static_cast<size_t>(H) * s.T);
    if (rx)
      rx_.resize(n);
    if (ec)
      ec_.resize(n);
    if ((need & kNeedX) != 0)
      xv_.resize(n);
    if ((need & kNeedMask) != 0)
      mask_.resize(n);
    if ((need & kNeedAlpha) != 0)
      av_.resize(n), am_.resize(n);
    const factor::gpu::DevPlane *rp = at(i);
    if ((need & (kNeedX | kNeedMask)) != 0) {
      float *xv = (need & kNeedX) != 0 ? xv_.data() : nullptr;
      uint8_t *xm = (need & kNeedMask) != 0 ? mask_.data() : nullptr;
      factor::gpu::download(sess_, rp, xv, xm);
      v.x = {xv, xm};
    }
    factor::gpu::stat_eval(stat_, rp, gate_, v.f, cx.d, s.t0_abs, s.t_lo, s.t_back, acc, rows ? rows_.data() : nullptr, ec ? ec_.data() : nullptr,
                           rx ? rx_.data() : nullptr);
    v.rx = rx ? rx_.data() : nullptr;
    v.ec = ec ? ec_.data() : nullptr;
    if (rows) {
      const ProbeSink<Ps> sink{&p, cx.d};
      ex_.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int) {
        for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t)
          for (int hi = 0; hi < H; ++hi) {
            const typename ProbeSink<Ps>::Ext e = sink.ext();
            sink.row(hi, t, rows_[static_cast<size_t>(hi) * s.T + t], e);
          }
      });
    }
    if ((need & kNeedAlpha) != 0) {
      const int in0 = F.d.nodes[static_cast<size_t>(i)].in[0];
      const factor::DagNode &an = F.d.nodes[static_cast<size_t>(in0)];
      if (an.op < 0) {
        const factor::check::Plane *q = c_->x.find(F.leaves[static_cast<size_t>(an.feat)]);
        v.alpha = {q->v.data(), q->m.data()};
      } else {
        factor::gpu::download(sess_, at(in0), av_.data(), am_.data());
        v.alpha = {av_.data(), am_.data()};
      }
    }
    return v;
  }
  factor::cpu::stat::Workspace &stat_session() {
    if (search_) {
      factor::gpu::search_close(search_);
      search_ = nullptr, sr_gen_ = 0;
    }
    const YPlanes &y = block().y;
    const bool same_shape = stat_ && st_T_ == y.sh.T && st_A_ == y.sh.A && st_seg_ == y.sh.seg && same_holds(st_hd_, y.hd);
    if (!same_shape) {
      if (stat_)
        factor::gpu::stat_close(stat_);
      evict_unused_x(static_cast<size_t>((7 * y.hd.n + 4) * n_));
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, labels_of(y), nullptr);
      st_T_ = y.sh.T, st_A_ = y.sh.A, st_seg_ = y.sh.seg, st_hd_ = y.hd, st_gen_ = c_->gen;
    } else if (st_gen_ != c_->gen) {
      factor::gpu::stat_load(stat_, labels_of(y), nullptr);
      st_gen_ = c_->gen;
    }
    w_.ensure(y.sh.A, y.hd.n, y.ad.n, ex_.threads());
    return w_;
  }
  void search_session(int batch) {
    assert(batch >= 1 && batch <= factor::gpu::search_max_batch());
    if (stat_) {
      factor::gpu::stat_close(stat_);
      stat_ = nullptr, st_gen_ = 0;
    }
    const YPlanes &y = block().y;
    const bool same_shape = search_ && sr_T_ == y.sh.T && sr_A_ == y.sh.A && sr_seg_ == y.sh.seg && sr_batch_ == batch &&
                            same_holds(sr_hd_, y.hd) && sr_cap_ == y.n_full;
    if (!same_shape) {
      if (search_)
        factor::gpu::search_close(search_);
      evict_unused_x(static_cast<size_t>((7 * y.hd.n + 1 + 2 * batch) * n_));
      search_ = factor::gpu::search_open(y.sh.T, y.sh.A, y.sh.seg, batch, y.hd, y.n_full);
      factor::gpu::search_load(search_, labels_of(y).l);
      sr_T_ = y.sh.T, sr_A_ = y.sh.A, sr_seg_ = y.sh.seg, sr_batch_ = batch, sr_hd_ = y.hd, sr_cap_ = y.n_full, sr_gen_ = c_->gen;
    } else if (sr_gen_ != c_->gen) {
      factor::gpu::search_load(search_, labels_of(y).l);
      sr_gen_ = c_->gen;
    }
  }
  static void ranks_of(void *src, int k, int t0, int stride, int n_rows, uint16_t *out) {
    factor::gpu::search_ranks(static_cast<factor::gpu::SearchSession *>(src), k, t0, stride, n_rows, out);
  }

  size_t n_ = 0;
  uint64_t x_gen_ = 0, x_tick_ = 0;
  factor::gpu::Session *sess_ = nullptr;
  factor::gpu::DevPlane *gate_ = nullptr;
  std::map<LeafId, LeafEntry> x_cache_;
  std::vector<factor::gpu::DevPlane *> free_x_;
  std::vector<const factor::gpu::DevPlane *> dev_;
  factor::gpu::DevPool dp_;

  factor::gpu::StatSession *stat_ = nullptr;
  int st_T_ = 0, st_A_ = 0, st_seg_ = 0;
  Holds st_hd_{};
  uint64_t st_gen_ = 0;

  factor::gpu::SearchSession *search_ = nullptr;
  int sr_T_ = 0, sr_A_ = 0, sr_seg_ = 0, sr_batch_ = 0;
  double sr_cap_ = 0.0;
  Holds sr_hd_{};
  uint64_t sr_gen_ = 0;

  std::vector<factor::search::Result> out_;
  factor::cpu::stat::Workspace w_;
  std::vector<factor::stat::Row> rows_;
  std::vector<uint16_t> rx_;
  std::vector<uint8_t> ec_;
  std::vector<float> xv_;
  std::vector<uint8_t> mask_;
  std::vector<float> av_;
  std::vector<uint8_t> am_;
};

// Store 内常驻的双后端环境 (须在 Lease 内访问)
struct EngineSpace final : ExecSpace {
  Backend<Cpu> cpu;
  std::optional<Backend<Gpu>> gpu;
  explicit EngineSpace(misc::exec::ForkJoin &ex) : cpu(ex) {}
  Backend<Gpu> &gpu_of(misc::exec::ForkJoin &ex) {
    if (!gpu)
      gpu.emplace(ex);
    return *gpu;
  }
  double host_bytes() const override { return cpu.host_bytes() + (gpu ? gpu->host_bytes() : 0.0); }
  double vram_bytes() const override { return gpu ? gpu->vram_bytes() : 0.0; }
  void drop_host() override {
    cpu.drop();
    if (gpu)
      gpu->drop();
  }
  void clear() override {
    cpu.release();
    if (gpu)
      gpu->release();
  }
};

EngineSpace &engine_space(Store &S);

template <class Ps, class Fn>
auto with_backend(FactorFlow &F, bool use_gpu, Fn &&fn) {
  EngineSpace &es = engine_space(F.S);
  if constexpr (!Ps::cells) {
    if (use_gpu)
      return fn(es.gpu_of(F.S.ex));
  }
  return fn(es.cpu);
}

// ---- 统一评估管线: Job → prepare → run_pass (eval_forest / eval_baskets) ----
struct Job {
  const char *who = "";
  const FeatureTable *ft = nullptr;
  BlockSpec spec;
  Bands bands;
  Budget b;
  Domain dm;
  Plan P;
  double n_full = 0.0;
  std::string why;
  Io io(FactorFlow &F) const { return F.io(*ft, dm); }
  Chunk whole() const { return {0, dm.warm, dm.days.size(), dm.days.size()}; }
  double reserve() const { return P.max_cells() * b.work + b.fixed; }
  PassCtx pass_ctx(misc::exec::ForkJoin &ex, factor::stat::Dir d = factor::stat::kFwd) const {
    const int seg = ft->seg, ds = static_cast<int>(dm.stat_days());
    return {ex, dm, spec.y.hd, spec.y.ad, seg, static_cast<int>(dm.rs.axis.size()), ds, ds * seg, d};
  }
  StatScope scope(const FactorScope &sc, const char *backend) const { return stat_scope(sc, dm, ft->seg, n_full, backend); }
};

inline bool open_job(FactorFlow &F, const FactorScope &sc, std::span<const LeafId> leaves, Job &j) {
  assert(sc.ft && sc.lv == sc.ft->lv && "作用域层与字段表层不符");
  j.ft = sc.ft.get();
  j.spec.x.assign(leaves.begin(), leaves.end());
  if (!domain_of(*sc.ft, sc, j.spec.x, SIZE_MAX, j.dm, j.why))
    return false;
  open_libs(sc, F.S.lr);
  return true;
}

template <class B>
bool fit_years(FactorFlow &F, const FactorScope &sc, Job &j, B &&bud) {
  const double host = F.S.budget();
  const double vram = bud().dev > 0.0 ? F.S.vram() : 0.0;
  const auto fits = [&] {
    const Budget b = bud();
    const double n = cells_of(j.dm, j.ft->seg);
    return b.host(n) <= host && (b.dev <= 0.0 || b.vram(n) <= vram);
  };
  if (fits())
    return true;
  assert(sc.years > 0 && "全期整期一块装不下内存 / 显存预算");
  const size_t days = j.dm.stat_days();
  FactorScope s = sc;
  for (s.years = sc.years - 1; s.years >= 1; --s.years) {
    std::string why;
    [[maybe_unused]] const bool ok = domain_of(*sc.ft, s, j.spec.x, SIZE_MAX, j.dm, why);
    assert(ok && "近期子期是整期的后缀, 评估域必成");
    if (fits())
      break;
  }
  assert(s.years >= 1 && "1 年的整期装载都放不下内存 / 显存 (因子层太细)");
  cap_years(sc.pool, sc.lv, s.years);
  j.why = "评估期近 " + std::to_string(sc.years) + " 年 (" + std::to_string(days) + " 天) 整段装不下内存 / 显存预算, 本作用域最多 " + std::to_string(s.years) + " 年";
  std::fprintf(stderr, "[factors] %s: %s, 评估年数封顶 %d 年\n", j.who, j.why.c_str(), s.years);
  return false;
}

template <class E, class Ps>
Budget budget_of(const Job &j, const factor::Dag &d, size_t nx, int H, int R) {
  const int seg = j.ft->seg, days = static_cast<int>(j.dm.days.size()), A = static_cast<int>(j.dm.rs.axis.size());
  const Shape sh{days * seg, A, days, seg};
  Budget b = E::template budget<Ps>(d, j.spec.y.hd.n);
  b.load += cell_bytes(*j.ft, j.spec, nx);
  b.fixed = R * Ps::bytes(sh, H) + Ps::scratch(sh, H);
  return b;
}

inline Rc load_block(FactorFlow &F, Job &j, size_t k, Block &c) {
  assert(k < j.P.chunks.size());
  const Rc rc = F.S.pin(c, j.io(F), j.P.chunks[k], j.spec, j.reserve());
  if (rc == Rc::Ok)
    j.n_full = c.y.n_full;
  return rc;
}

template <class E, class Ps>
Rc prepare(FactorFlow &F, const FactorScope &sc, const factor::Dag &d, std::span<const LeafId> leaves, int look, const Holds &hd, int R, Job &j) {
  if (!open_job(F, sc, leaves, j))
    return Rc::Empty;
  j.spec.y = y_spec(*j.ft, j.dm, hd, Ps::segs, sc.max_impact);
  j.spec.ry = E::kRanks;
  if (look >= 0)
    j.bands = bands_of(look, hd, j.ft->seg);
  const auto bud = [&] { return budget_of<E, Ps>(j, d, j.spec.x.size(), hd.n, R); };
  if (j.bands.front < 0 && !fit_years(F, sc, j, bud))
    return Rc::Empty;
  j.b = bud();
  j.P = plan_chunks(j.dm, j.ft->seg, j.b, j.bands, F.S.budget(), j.b.dev > 0.0 ? F.S.vram() : 0.0);
  return Rc::Ok;
}

int plan_forest(misc::exec::ForkJoin &ex, const Block &c, const Forest &F, std::vector<std::string> &why, std::vector<uint8_t> &need);

struct PassHooks {
  size_t d_lo = 0, d_hi = SIZE_MAX; // 只执行统计段与 [d_lo, d_hi) 相交的块
  void block(const Block &, size_t, double) {}
  void plan(size_t, const Forest &, const std::vector<std::string> &, const std::vector<uint8_t> &) {}
  void node(size_t, const Forest &, int, double) {}
  void step(int) {}
  void root(int) {}
};

// 单趟分块流式执行: init → 逐块 load_block → 逐批 plant + plan_forest + eval(open → row → close) → 好根 finish
template <class E, class Plant, class Ps, class H>
Rc run_pass(E &eng, FactorFlow &F, Job &j, std::span<const Root> roots, Plant &&plant, std::span<Ps> ps, std::vector<std::string> &dead, H &&hk) {
  using Clock = std::chrono::steady_clock;
  const size_t n = j.P.chunks.size(), R = roots.size();
  assert(ps.size() == R);
  dead.assign(R, {});
  for (size_t k = 0; k < R; ++k)
    ps[k].init(j.pass_ctx(eng.ex(), roots[k].d));

  size_t last_ci = n;
  for (size_t ci = 0; ci < n; ++ci) {
    const Chunk &ck = j.P.chunks[ci];
    if (ck.s1 > hk.d_lo && ck.s0 < hk.d_hi)
      last_ci = ci;
  }
  assert(last_ci < n && "分块与目标日段无交集");

  for (size_t ci = 0; ci < n; ++ci) {
    const Chunk &ck = j.P.chunks[ci];
    if (ck.s1 <= hk.d_lo || ck.s0 >= hk.d_hi)
      continue;
    const bool last = ci == last_ci;
    const Clock::time_point t_load = Clock::now();
    Block c;
    if (const Rc rc = load_block(F, j, ci, c); rc != Rc::Ok)
      return rc;
    hk.block(c, ci, std::chrono::duration<double>(Clock::now() - t_load).count());
    eng.bind(c);
    for (size_t b0 = 0; b0 < R;) {
      int C = 0;
      const Forest &Fr = plant(b0, C);
      assert(C >= 1 && b0 + static_cast<size_t>(C) <= R && Fr.size() == static_cast<size_t>(C));
      std::vector<std::string> why(dead.begin() + static_cast<ptrdiff_t>(b0), dead.begin() + static_cast<ptrdiff_t>(b0) + C);
      std::vector<uint8_t> need;
      const int good = plan_forest(eng.ex(), c, Fr, why, need);
      std::copy_n(why.begin(), C, dead.begin() + static_cast<ptrdiff_t>(b0));
      hk.plan(b0, Fr, why, need);
      if (good > 0 && !eng.eval(F.cancel, Fr, need.data(), ps.subspan(b0, static_cast<size_t>(C)), last, [&](int i, double ms) { hk.node(b0, Fr, i, ms); }, [&](int k) { hk.step(static_cast<int>(b0) + k); }))
        return Rc::Cancel;
      b0 += static_cast<size_t>(C);
    }
    if (R > 0 && std::none_of(dead.begin(), dead.end(), [](const std::string &s) { return s.empty(); })) {
      j.why = dead[0];
      return Rc::Broken;
    }
    if (F.cancel.load(std::memory_order_relaxed))
      return Rc::Cancel;
  }

  for (size_t k = 0; k < R; ++k)
    if (dead[k].empty()) {
      ps[k].finish(j.pass_ctx(eng.ex(), roots[k].d));
      hk.root(static_cast<int>(k));
    }
  return Rc::Ok;
}

// 单森林单趟作业 (Run / Inspect / Portfolio 共用): 根去重共享一张森林, 未中断时每条目恰回调一次 on_entry(e, k, why)
struct ForestOut {
  Job job;
  Forest fr;
  std::vector<double> ms;
  std::vector<std::string> dead;
  const char *backend = "";
  double eval_ms(int k) const {
    double s = 0.0;
    for (const int i : fr.d.subtree(fr.d.roots[static_cast<size_t>(k)]))
      s += ms[static_cast<size_t>(i)];
    return s;
  }
  StatScope scope(const FactorScope &sc) const { return job.scope(sc, backend); }
};

struct NoOp {
  template <class... A>
  void operator()(A &&...) const {}
};

template <class E, class Ps, class OnEntry = NoOp, class OnBlock = NoOp, class OnPlan = NoOp>
Rc eval_forest(E &eng, FactorFlow &F, const FactorScope &sc, const RootSet &rs, std::span<Ps> ps, ForestOut &o, bool whole,
               OnEntry &&on_entry = {}, OnBlock &&on_block = {}, OnPlan &&on_plan = {}) {
  static_assert(!kBatched<Ps>, "eval_forest 逐根评 (批核只 Search)");
  assert(rs.size() > 0 && rs.hd.n > 0 && ps.size() == rs.size() && o.fr.size() == 0);
  o.fr = plant_forest(*sc.ft, rs.r, /*keep_roots=*/false);
  o.backend = E::kName;
  int look = whole ? -1 : 0;
  if (!whole)
    for (const Root &r : rs.r)
      look = std::max(look, factor::search::space::lookback(*r.e, sc.ft->seg));
  std::vector<uint8_t> fin(rs.of.size(), 0);
  const auto emit = [&](size_t e, const std::string &why) {
    if (!fin[e])
      fin[e] = 1, on_entry(static_cast<int>(e), rs.of[e], why);
  };
  Rc rc = prepare<E, Ps>(F, sc, o.fr.d, o.fr.leaves, look, rs.hd, static_cast<int>(rs.size()), o.job);
  if (rc == Rc::Ok) {
    o.ms.assign(o.fr.d.nodes.size(), 0.0);
    struct Hooks : PassHooks {
      Progress &prog;
      const RootSet &rs;
      ForestOut &o;
      decltype(emit) &emit_fn;
      OnBlock &blk_fn;
      OnPlan &plan_fn;
      void block(const Block &c, size_t ci, double s) { blk_fn(c, ci, s); }
      void plan(size_t, const Forest &Fr, const std::vector<std::string> &why, const std::vector<uint8_t> &need) {
        size_t n_run = 0;
        for (size_t i = 0; i < Fr.d.nodes.size(); ++i)
          n_run += need[i] && Fr.d.nodes[i].op >= 0;
        for (const std::string &w : why)
          n_run += w.empty();
        prog.begin(Phase::Running, n_run);
        for (size_t e = 0; e < rs.of.size(); ++e)
          if (const std::string &w = why[static_cast<size_t>(rs.of[e])]; !w.empty())
            emit_fn(e, w);
        plan_fn();
      }
      void node(size_t, const Forest &Fr, int i, double ms) {
        if (Fr.d.nodes[static_cast<size_t>(i)].op >= 0) {
          o.ms[static_cast<size_t>(i)] += ms;
          prog.publish(1);
        }
      }
      void step(int) { prog.publish(1); }
      void root(int k) {
        for (size_t e = 0; e < rs.of.size(); ++e)
          if (rs.of[e] == k)
            emit_fn(e, {});
      }
    } hk{{}, F.prog, rs, o, emit, on_block, on_plan};
    const auto plant_one = [&](size_t b0, int &C) -> const Forest & {
      assert(b0 == 0);
      C = static_cast<int>(o.fr.size());
      return o.fr;
    };
    rc = run_pass(eng, F, o.job, std::span<const Root>(rs.r), plant_one, ps, o.dead, hk);
  }
  if (!aborted(rc))
    for (size_t e = 0; e < rs.of.size(); ++e) {
      assert((rc != Rc::Ok || fin[e]) && "Ok: 条目都已有终局");
      emit(e, o.job.why);
    }
  return rc;
}

Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out);

} // namespace GUI::Engine
