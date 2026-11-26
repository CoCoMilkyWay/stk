// Engine — 因子评估引擎与统一管线 (Run / Search / Inspect / Portfolio / 篮子 / Agent 观察共用):
//   森林 Forest: 去重算子 DAG + 一次性解析的 LeafId 表 + 因子根 (经 Stat) + 可选纯表达式根 (直取平面, 不经 Stat).
//   双后端同构 Backend<Cpu | Gpu> (挂在进程唯一 Store::exec 上跨文件 / 跨趟常驻复用, 内存与显存纳入 Store 统一预算):
//     budget<Ps>(d, H) / batch_of(P, b) / kRanks
//     bind(blk) / release() / drop()
//     walk_dag(cancel, d, leaves, need, visit) / eval<Ps>(cancel, F, need, ps, last, on_node, on_step)
//   单一分块流式管线 (作业层 ⇄ 块层):
//     Need       = 一个作业对装载与预算的全部要求: 森林作业 forest_need ⇄ 篮子作业 basket_need
//     open_job   → 评估域 + 两侧规格 + 前后带 + 预算 (整期一块装不下即封顶年数) + 分块计划 (唯一的作业入口)
//     for_blocks → 逐块 load_block → on_block (唯一的块循环)
//     eval_block → 一块一森林: 值域核 → 走 DAG + 各因子根 Stat 进探针 (唯一的块内评估: run_pass / Agent 观察流同一条)
//     run_pass   → init → for_blocks { on_block → bind → 逐批 plant + eval_block } → 好根 finish
//     eval_forest  = 单森林单趟作业 (Run / Inspect / Portfolio): open_job → run_pass → 逐条目回调
//     eval_baskets = 池篮子与个股账作业 (Portfolio 行情两侧): open_job → for_blocks { BasketPass }
#pragma once

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Space.hpp" // lookback
#include "gui/engine/Probe.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <map>
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
  factor::stat::Ret r = factor::stat::Ret::Excess; // 收益口径 (因子文件 type; Stat 契约【收益 Ret】)
};

struct RootSet {
  Holds hd;
  std::deque<factor::expr::Expr> es;
  std::vector<Root> r;
  std::vector<int> of; // [条目] → 唯一根下标
  void add(const FeatureTable &ft, const std::string &canon, factor::stat::Frame f, factor::stat::Dir d, factor::stat::Ret ret) {
    size_t k = 0;
    while (k < r.size() && (r[k].e->canon != canon || r[k].f != f || r[k].d != d || r[k].r != ret))
      ++k;
    if (k == r.size())
      r.push_back({&es.emplace_back(ft.reparse(canon)), f, d, ret});
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
                           std::span<const factor::expr::Expr *const> plain = {}, bool fuse = true) {
  assert(!rs.empty() || !plain.empty());
  std::vector<const factor::expr::Expr *> es;
  es.reserve(rs.size() + plain.size());
  for (const Root &x : rs)
    es.push_back(x.e);
  for (const factor::expr::Expr *e : plain)
    es.push_back(e);
  Forest o{factor::build_forest(es, keep_roots || !plain.empty(), fuse), {}, {rs.begin(), rs.end()}, {}};
  o.leaves = ft.x.ids(o.d.feats);
  o.at.resize(o.d.nodes.size());
  for (size_t k = 0; k < o.r.size(); ++k) {
    const int i = o.d.roots[k];
    assert(o.d.nodes[static_cast<size_t>(i)].op >= 0 && o.d.nodes[static_cast<size_t>(i)].n_in >= 1 && "因子根须是归一算子");
    o.at[static_cast<size_t>(i)].push_back(static_cast<int>(k));
  }
  return o;
}

inline Bands bands_of(int look, const Holds &hd, int seg) {
  const int h = factor::stat::hold_reach(hd, seg);
  return {(look + h + seg - 1) / seg + kChainDays, (h + seg - 1) / seg};
}

// 探针只要三级 + 二级 (Search) → GPU 走批核 (逐候选并行)
template <class Ps>
inline constexpr bool kBatched = Ps::need == 0;

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
    factor::stat::Labels L = self().labels();
    const uint8_t need = Ps::need_at(last);
    for (size_t j = 0; j < ks.size(); ++j) {
      const int k = ks[j];
      Ps &p = ps[static_cast<size_t>(k)];
      L.ret = F.r[static_cast<size_t>(k)].r; // 标签同块共用, 收益口径逐根
      const Ctx cx{ex_, block(), L, w, F.r[static_cast<size_t>(k)].d, last};
      p.open(cx);
      const RootView v = self().template view<Ps>(F, k, static_cast<int>(j), cx, need, p);
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
    hp_ = factor::cpu::HostPool{};
    w_ = factor::cpu::stat::Workspace{};
    std::vector<factor::check::Arena>().swap(sc_);
    std::vector<const factor::check::Plane *>().swap(in_);
    std::vector<uint16_t>().swap(rx_);
    std::vector<factor::stat::Day>().swap(day_);
  }
  void release() {
    drop();
    c_ = nullptr, d_ = nullptr;
  }
  double host_bytes() const {
    double b = static_cast<double>(hp_.bytes()) + static_cast<double>(rx_.capacity()) * sizeof(uint16_t) +
               static_cast<double>(day_.capacity()) * sizeof(factor::stat::Day);
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
  // 一个根平面的 Stat (rx_ / day_ / w_ 常驻复用): → 已定向的 rx_ (块形), acc[H], 逐日 day_[H][D]
  template <class Sink>
  void run_root(const factor::check::Plane *rp, factor::stat::Frame f, factor::stat::Dir d, const factor::stat::Span &s, const factor::stat::Labels &L,
                factor::stat::Accum *acc, const Sink &sink) {
    w_.ensure(s.A, L.hd.n, ex_.threads());
    rx_.resize(s.n());
    day_.resize(static_cast<size_t>(L.hd.n) * s.days());
    factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, f, d, s, L, rx_.data(), w_, acc, day_.data(), sink);
  }
  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &, const std::vector<int> &) {
    const YPlanes &y = block().y;
    w_.ensure(y.sh.A, y.hd.n, ex_.threads());
    return &w_;
  }
  template <class Ps>
  RootView view(const Forest &F, int k, int, const Ctx &cx, uint8_t need, Ps &p) {
    const int i = F.d.roots[static_cast<size_t>(k)];
    const factor::stat::Frame f = F.r[static_cast<size_t>(k)].f;
    const factor::stat::Span s = c_->span();
    const factor::check::Plane *rp = plane(i);
    if ((need & kNeedRows) != 0)
      run_root(rp, f, cx.d, s, cx.L, acc_, ProbeSink<Ps>{&p});
    else
      run_root(rp, f, cx.d, s, cx.L, acc_, factor::cpu::stat::DropRows{});
    RootView v;
    v.f = f;
    v.acc = acc_;
    v.day = day_.data();
    v.x = {(need & kNeedX) != 0 ? rp->v.data() : nullptr, (need & kNeedMask) != 0 ? rp->m.data() : nullptr};
    v.rx = (need & kNeedRx) != 0 ? rx_.data() : nullptr;
    if ((need & kNeedAlpha) != 0) {
      const factor::check::Plane *ap = plane(F.d.nodes[static_cast<size_t>(i)].in[0]);
      v.alpha = {ap->v.data(), ap->m.data()};
    }
    return v;
  }

  std::vector<const factor::check::Plane *> in_;
  factor::cpu::HostPool hp_;
  std::vector<factor::check::Arena> sc_;
  factor::cpu::stat::Workspace w_;
  std::vector<uint16_t> rx_;
  std::vector<factor::stat::Day> day_; // [H][D]
  factor::stat::Accum acc_[kHoldCount];
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
    return std::clamp(static_cast<int>((P.vram - nmax * dv) / (kDevRootBytes * nmax)), 1, factor::gpu::stat_max_batch());
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
    w_ = factor::cpu::stat::Workspace{};
    std::vector<factor::stat::Accum>().swap(acc_);
    std::vector<factor::stat::Day>().swap(day_);
    std::vector<factor::stat::Row>().swap(rows_);
    std::vector<uint16_t>().swap(rx_);
    std::vector<float>().swap(xv_);
    std::vector<uint8_t>().swap(mask_);
    std::vector<float>().swap(av_);
    std::vector<uint8_t>().swap(am_);
  }
  void release() {
    release_vram();
    drop();
    c_ = nullptr, d_ = nullptr;
  }
  double host_bytes() const {
    return static_cast<double>(rows_.capacity()) * sizeof(factor::stat::Row) + static_cast<double>(rx_.capacity()) * sizeof(uint16_t) +
           static_cast<double>(xv_.capacity() + av_.capacity()) * sizeof(float) +
           static_cast<double>(mask_.capacity() + am_.capacity()) + static_cast<double>(day_.capacity()) * sizeof(factor::stat::Day) +
           static_cast<double>(acc_.capacity()) * sizeof(factor::stat::Accum);
  }
  double vram_bytes() const {
    if (!sess_)
      return 0.0;
    return static_cast<double>(factor::gpu::session_bytes(sess_) + dp_.bytes(n_) + (gate_ ? n_ : 0) + (x_cache_.size() + free_x_.size()) * (sizeof(float) + 1) * n_ +
                               factor::gpu::stat_bytes(stat_));
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
    session(batch);
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

  factor::gpu::DevPlane *pop_lru() {
    auto lru = x_cache_.end();
    for (auto it = x_cache_.begin(); it != x_cache_.end(); ++it)
      if (!it->second.pinned && (lru == x_cache_.end() || it->second.tick < lru->second.tick))
        lru = it;
    if (lru == x_cache_.end())
      return nullptr;
    factor::gpu::DevPlane *p = lru->second.p;
    x_cache_.erase(lru);
    return p;
  }
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
      factor::gpu::DevPlane *p = pop_lru();
      if (!p)
        break;
      factor::gpu::plane_del(sess_, p);
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
        factor::gpu::DevPlane *p = pop_lru();
        if (!p)
          break;
        free_x_.push_back(p);
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
    stat_ = nullptr, st_gen_ = 0;
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

  // Stat 常驻会话: 形状 / 持有期 / 满仓 / 批上限变了才重开, 块换了才重传标签
  void session(int cmax) {
    assert(cmax >= 1 && cmax <= factor::gpu::stat_max_batch());
    const YPlanes &y = block().y;
    const bool same = stat_ && st_T_ == y.sh.T && st_A_ == y.sh.A && st_seg_ == y.sh.seg && st_cmax_ == cmax && st_hd_ == y.hd && st_cap_ == y.n_full;
    if (!same) {
      if (stat_)
        factor::gpu::stat_close(stat_);
      evict_unused_x(static_cast<size_t>((7 * y.hd.n + 2 + 2 * cmax) * n_));
      stat_ = factor::gpu::stat_open(y.sh.T, y.sh.A, y.sh.seg, cmax, y.hd, y.n_full);
      st_T_ = y.sh.T, st_A_ = y.sh.A, st_seg_ = y.sh.seg, st_cmax_ = cmax, st_hd_ = y.hd, st_cap_ = y.n_full, st_gen_ = 0;
    }
    if (st_gen_ != c_->gen) {
      factor::gpu::stat_load(stat_, labels_of(y).l);
      st_gen_ = c_->gen;
    }
  }

  template <class Ps>
  factor::cpu::stat::Workspace *ready(const Forest &F, const std::vector<int> &ks) {
    const YPlanes &y = block().y;
    if constexpr (!kBatched<Ps>) {
      if (!stat_ || st_cmax_ < 1)
        session(1);
      else
        session(st_cmax_);
      w_.ensure(y.sh.A, y.hd.n, ex_.threads());
      return &w_;
    } else {
      assert(stat_ && "批核先 plant");
      const factor::stat::Frame f = F.r[static_cast<size_t>(ks.front())].f;
      const factor::stat::Dir d = F.r[static_cast<size_t>(ks.front())].d;
      const factor::stat::Ret ret = F.r[static_cast<size_t>(ks.front())].r;
      std::vector<const factor::gpu::DevPlane *> roots;
      roots.reserve(ks.size());
      for (const int k : ks) {
        const int r = F.d.roots[static_cast<size_t>(k)];
        assert(F.d.nodes[static_cast<size_t>(r)].slot >= 0);
        assert(F.r[static_cast<size_t>(k)].f == f && F.r[static_cast<size_t>(k)].d == d && F.r[static_cast<size_t>(k)].r == ret && "一批同口径同方向同收益口径");
        roots.push_back(at(r));
      }
      const factor::stat::Span s = c_->span();
      acc_.resize(ks.size() * static_cast<size_t>(y.hd.n));
      day_.resize(ks.size() * static_cast<size_t>(y.hd.n) * s.days());
      factor::gpu::stat_eval(stat_, roots.data(), static_cast<int>(ks.size()), gate_, f, d, ret, s.t0_abs, s.t_lo, s.t_back, acc_.data(), day_.data());
      return nullptr;
    }
  }
  template <class Ps>
  RootView view(const Forest &F, int k, int j, const Ctx &cx, uint8_t need, Ps &p) {
    if constexpr (kBatched<Ps>) {
      const size_t H = static_cast<size_t>(cx.L.hd.n);
      RootView v;
      v.f = F.r[static_cast<size_t>(k)].f;
      v.acc = acc_.data() + static_cast<size_t>(j) * H;
      v.day = day_.data() + static_cast<size_t>(j) * H * c_->span().days();
      return v;
    } else {
      return stat_root(F, k, cx, need, p);
    }
  }
  template <class Ps>
  RootView stat_root(const Forest &F, int k, const Ctx &cx, uint8_t need, Ps &p) {
    RootView v;
    v.f = F.r[static_cast<size_t>(k)].f;
    const factor::stat::Span s = c_->span();
    const size_t n = s.n();
    const int i = F.d.roots[static_cast<size_t>(k)], H = cx.L.hd.n;
    const bool rows = (need & kNeedRows) != 0, rx = (need & kNeedRx) != 0;
    acc_.resize(static_cast<size_t>(H));
    day_.resize(static_cast<size_t>(H) * s.days());
    if (rows)
      rows_.resize(static_cast<size_t>(H) * s.T);
    if (rx)
      rx_.resize(n);
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
    factor::gpu::stat_eval(stat_, &rp, 1, gate_, v.f, cx.d, cx.L.ret, s.t0_abs, s.t_lo, s.t_back, acc_.data(), day_.data(), rows ? rows_.data() : nullptr,
                           rx ? rx_.data() : nullptr);
    v.acc = acc_.data();
    v.day = day_.data();
    v.rx = rx ? rx_.data() : nullptr;
    if (rows) {
      const ProbeSink<Ps> sink{&p};
      ex_.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int) {
        for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t)
          for (int hi = 0; hi < H; ++hi)
            sink.row(hi, t, rows_[static_cast<size_t>(hi) * s.T + t]);
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

  size_t n_ = 0;
  uint64_t x_gen_ = 0, x_tick_ = 0;
  factor::gpu::Session *sess_ = nullptr;
  factor::gpu::DevPlane *gate_ = nullptr;
  std::map<LeafId, LeafEntry> x_cache_;
  std::vector<factor::gpu::DevPlane *> free_x_;
  std::vector<const factor::gpu::DevPlane *> dev_;
  factor::gpu::DevPool dp_;

  factor::gpu::StatSession *stat_ = nullptr;
  int st_T_ = 0, st_A_ = 0, st_seg_ = 0, st_cmax_ = 0;
  double st_cap_ = 0.0;
  Holds st_hd_{};
  uint64_t st_gen_ = 0;

  factor::cpu::stat::Workspace w_;
  std::vector<factor::stat::Accum> acc_; // [C][H]
  std::vector<factor::stat::Day> day_;   // [C][H][D]
  std::vector<factor::stat::Row> rows_;
  std::vector<uint16_t> rx_;
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
  if (use_gpu)
    return fn(es.gpu_of(F.S.ex));
  return fn(es.cpu);
}

// ---- 作业层: Need → open_job → Job ----
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
  double reserve() const { return P.max_cells() * b.work + b.fixed; }
  PassCtx pass_ctx(misc::exec::ForkJoin &ex, factor::stat::Dir d = factor::stat::kFwd) const {
    const int seg = ft->seg, ds = static_cast<int>(dm.stat_days());
    return {ex, dm, spec.y.hd, spec.y.ad, seg, static_cast<int>(dm.rs.axis.size()), ds, ds * seg, d};
  }
  StatScope scope(const FactorScope &sc, const char *backend) const { return stat_scope(sc, dm, ft->seg, n_full, backend); }
};

// 一个作业对装载与预算的全部要求 (森林作业 forest_need ⇄ 篮子作业 basket_need; 唯一消费方 open_job)
struct Need {
  std::vector<LeafId> x;                  // X 叶
  Holds hd;                               // Y 标签的持有期
  Segs segs = Segs::None;                 // Y 成本段
  bool ry = false;                        // Y 标签秩 (Stat 根 · CPU)
  Bands bands;                            // 前后带 (front < 0 = 整期一块: 装不下即封顶年数)
  std::function<Budget(const Job &)> bud; // 装载 load 之外的预算 (load 由两侧规格现推); 随评估域重算
};

// 调用方另加的前后带 (日) / 每格宿主工作量 (字节)
struct Extra {
  int front = 0, back = 0;
  double work = 0.0;
};

// 森林作业: R = 因子根数 (0 = 只有纯表达式根: 不装标签秩, 前带只够回看); look < 0 = 整期一块
template <class E, class Ps>
Need forest_need(const FeatureTable &ft, const factor::Dag &d, std::vector<LeafId> x, int look, const Holds &hd, int R, const Extra &e = {}) {
  Need n{std::move(x), hd, Ps::segs, E::kRanks && R > 0, {}, {}};
  if (look >= 0) {
    n.bands = R > 0 ? bands_of(look, hd, ft.seg) : Bands{(look + ft.seg - 1) / ft.seg, 0};
    n.bands.front = std::max(n.bands.front, e.front), n.bands.back = std::max(n.bands.back, e.back);
  }
  n.bud = [&d, H = hd.n, R, w = e.work](const Job &j) {
    const int seg = j.ft->seg, days = static_cast<int>(j.dm.days.size());
    const Shape sh{days * seg, static_cast<int>(j.dm.rs.axis.size()), days, seg};
    Budget b = E::template budget<Ps>(d, H);
    b.work += w;
    b.fixed = R * Ps::bytes(sh, H) + Ps::scratch(sh, H);
    return b;
  };
  return n;
}
// 篮子作业: all 池日层的估值七叶 + T+1 标签 (无因子根: 不装标签秩 / 盘口段)
Need basket_need(const FeatureTable &ft);

// 唯一的作业入口: 评估域 → 两侧规格 → 前后带 → 预算 (整期一块装不下即封顶年数, Empty) → 分块计划
Rc open_job(FactorFlow &F, const FactorScope &sc, Need n, Job &j);

inline Rc load_block(FactorFlow &F, Job &j, size_t k, Block &c) {
  assert(k < j.P.chunks.size());
  const Rc rc = F.S.pin(c, j.io(F), j.P.chunks[k], j.spec, j.reserve());
  if (rc == Rc::Ok)
    j.n_full = c.y.n_full;
  return rc;
}

// ---- 块层: for_blocks → eval_block → run_pass / eval_forest ----
struct NoOp {
  template <class... A>
  void operator()(A &&...) const {}
};
struct AllChunks {
  bool operator()(const Chunk &) const { return true; }
};

// 唯一的块循环: 计划里 want(块) 的块逐个 load_block → on_block(块, 块号, 末块, 装载秒) → Rc; 装载失败 / on_block 非 Ok / 取消即返回
template <class OnBlock, class Want = AllChunks>
Rc for_blocks(FactorFlow &F, Job &j, OnBlock &&on_block, Want &&want = {}) {
  using Clock = std::chrono::steady_clock;
  const size_t n = j.P.chunks.size();
  assert(n >= 1);
  for (size_t ci = 0; ci < n; ++ci) {
    if (!want(j.P.chunks[ci]))
      continue;
    const Clock::time_point t0 = Clock::now();
    Block c;
    if (const Rc rc = load_block(F, j, ci, c); rc != Rc::Ok)
      return rc;
    if (const Rc rc = on_block(c, ci, ci + 1 == n, std::chrono::duration<double>(Clock::now() - t0).count()); rc != Rc::Ok)
      return rc;
    if (F.cancel.load(std::memory_order_relaxed))
      return Rc::Cancel;
  }
  return Rc::Ok;
}

// 值域核: dead [d.roots] 进出 (已坏的根不再核, 新坏的填因); need [节点] = 好根子树; → 好根数
int plan_forest(misc::exec::ForkJoin &ex, const Block &c, const Forest &F, std::span<std::string> dead, std::vector<uint8_t> &need);

// 唯一的块内评估 (先 bind): 值域核 → 走 DAG + 各好因子根 Stat 进探针 ps [F.size()]; ms[k] += 因子根 k 子树算子 wall.
//   进度: 批作业 (Search) 由 run_pass 按 候选 × 块 计; 其余本批 = 要算的算子节点 + 好因子根
template <class E, class Ps>
Rc eval_block(E &eng, FactorFlow &F, const Block &c, const Forest &Fr, std::span<std::string> dead, std::span<Ps> ps, bool last, std::span<double> ms) {
  assert(ps.size() == Fr.size() && ms.size() == Fr.size() && dead.size() == Fr.d.roots.size());
  constexpr bool by_node = !kBatched<Ps>;
  const std::vector<factor::DagNode> &nd = Fr.d.nodes;
  std::vector<uint8_t> need;
  if (plan_forest(eng.ex(), c, Fr, dead, need) == 0)
    return Rc::Ok;
  if constexpr (by_node) {
    size_t n = 0;
    for (size_t i = 0; i < nd.size(); ++i)
      n += need[i] && nd[i].op >= 0;
    for (size_t k = 0; k < Fr.size(); ++k)
      n += dead[k].empty();
    F.prog.begin(Phase::Running, n);
  }
  std::vector<double> wall(nd.size(), 0.0);
  const bool ok = eng.eval(
      F.cancel, Fr, need.data(), ps, last,
      [&](int i, double t) {
        if (nd[static_cast<size_t>(i)].op < 0)
          return;
        wall[static_cast<size_t>(i)] += t;
        if (by_node)
          F.prog.publish(1);
      },
      [&](int) { F.prog.publish(1); });
  for (size_t k = 0; k < Fr.size(); ++k)
    if (dead[k].empty())
      for (const int i : Fr.d.subtree(Fr.d.roots[k]))
        ms[k] += wall[static_cast<size_t>(i)];
  return ok ? Rc::Ok : Rc::Cancel;
}

// 一趟各因子根的结局: dead[k] 空 = 好根 (已 finish); ms[k] = 子树算子 wall (各块各批累加)
struct Pass {
  std::vector<std::string> dead;
  std::vector<double> ms;
};

// 单趟分块流式执行: init → for_blocks { on_block(块, 块号, 装载秒) → bind → 逐批 plant + eval_block } → 好根 finish
template <class E, class Ps, class Plant, class OnBlock = NoOp>
Rc run_pass(E &eng, FactorFlow &F, Job &j, std::span<const Root> roots, Plant &&plant, std::span<Ps> ps, Pass &out, OnBlock &&on_block = {}) {
  const size_t R = roots.size(), n = j.P.chunks.size();
  assert(R >= 1 && ps.size() == R);
  out.dead.assign(R, {});
  out.ms.assign(R, 0.0);
  for (size_t k = 0; k < R; ++k)
    ps[k].init(j.pass_ctx(eng.ex(), roots[k].d));

  const Rc rc = for_blocks(F, j, [&](const Block &c, size_t ci, bool last, double load_s) {
    on_block(c, ci, load_s);
    eng.bind(c);
    if constexpr (kBatched<Ps>) {
      F.prog.begin(Phase::Running, R * n);
      F.prog.publish(R * ci);
    }
    for (size_t b0 = 0; b0 < R;) {
      int C = 0;
      const Forest &Fr = plant(b0, C);
      const size_t m = static_cast<size_t>(C);
      assert(C >= 1 && b0 + m <= R && Fr.size() == m);
      if (eval_block(eng, F, c, Fr, std::span(out.dead).subspan(b0, m), ps.subspan(b0, m), last, std::span(out.ms).subspan(b0, m)) != Rc::Ok)
        return Rc::Cancel;
      b0 += m;
    }
    if (std::none_of(out.dead.begin(), out.dead.end(), [](const std::string &s) { return s.empty(); })) {
      j.why = out.dead[0];
      return Rc::Broken;
    }
    return Rc::Ok;
  });
  if (rc != Rc::Ok)
    return rc;

  for (size_t k = 0; k < R; ++k)
    if (out.dead[k].empty())
      ps[k].finish(j.pass_ctx(eng.ex(), roots[k].d));
  return Rc::Ok;
}

// 单森林单趟作业 (Run / Inspect / Portfolio 共用): 根去重共享一张森林; 未中断时跑完逐条目回调 on_entry(e, k, why) (why 空 = 好根)
struct ForestOut {
  Job job;
  Forest fr;
  Pass pass;
  const char *backend = "";
  // 根 k 在结局 rc 下的坏因 (空 = 好根)
  std::string why(int k, Rc rc) const {
    const size_t i = static_cast<size_t>(k);
    return i < pass.dead.size() && !pass.dead[i].empty() ? pass.dead[i] : rc == Rc::Ok ? std::string()
                                                                                       : job.why;
  }
  double eval_ms(int k) const { return pass.ms[static_cast<size_t>(k)]; }
  StatScope scope(const FactorScope &sc) const { return job.scope(sc, backend); }
};

template <class E, class Ps, class OnEntry = NoOp, class OnBlock = NoOp>
Rc eval_forest(E &eng, FactorFlow &F, const FactorScope &sc, const RootSet &rs, std::span<Ps> ps, ForestOut &o, bool whole, OnEntry &&on_entry = {},
               OnBlock &&on_block = {}) {
  static_assert(!kBatched<Ps>, "eval_forest 逐根评 (批核只 Search)");
  assert(rs.size() > 0 && rs.hd.n > 0 && ps.size() == rs.size() && o.fr.size() == 0);
  o.fr = plant_forest(*sc.ft, rs.r, /*keep_roots=*/false);
  o.backend = E::kName;
  int look = whole ? -1 : 0;
  if (!whole)
    for (const Root &r : rs.r)
      look = std::max(look, factor::search::space::lookback(*r.e, sc.ft->seg));
  Rc rc = open_job(F, sc, forest_need<E, Ps>(*sc.ft, o.fr.d, o.fr.leaves, look, rs.hd, static_cast<int>(rs.size())), o.job);
  if (rc == Rc::Ok) {
    const auto plant = [&](size_t b0, int &C) -> const Forest & {
      assert(b0 == 0);
      C = static_cast<int>(o.fr.size());
      return o.fr;
    };
    rc = run_pass(eng, F, o.job, std::span<const Root>(rs.r), plant, ps, o.pass, on_block);
  }
  if (!aborted(rc))
    for (size_t e = 0; e < rs.of.size(); ++e)
      on_entry(static_cast<int>(e), rs.of[e], o.why(rs.of[e], rc));
  return rc;
}

Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out);

} // namespace GUI::Engine
