// Engine — 因子评估引擎 (Run / Search / Inspect / Portfolio / Agent 观察共用; 作业层 run_job 在 Load, 篮子作业在 Ledger):
//   森林 Forest: 去重算子 DAG + 一次性解析的 LeafId 表 + 因子根 (逐根评估口径 Mode, 经 Stat) + 可选纯表达式根 (直取平面, 不经 Stat).
//   双后端同构 Backend<Cpu | Gpu> (挂在进程唯一 Store::exec 上跨文件 / 跨趟常驻复用, 内存与显存纳入 Store 统一预算):
//     budget<Ps>(d, H) / batch_of(P, b) / kRanks
//     bind(blk) / release() / drop()
//     walk_dag(cancel, d, leaves, need, visit) / eval<Ps>(cancel, F, need, ps, last, on_node, on_step)
//   森林作业 (与篮子作业 Ledger eval_baskets 逐件对仗):
//     forest_need  → 对装载与预算的要求 (⇄ basket_need)
//     ForestPass   → 块消费者 (⇄ BasketPass; run_job 驱动): 块 = before → bind → 逐批种森林 + eval_block → after; 末块后好根 finish
//                    种法两种: 定森林 (一张种全部根, Run / Inspect / Portfolio / Agent 观察) ⇄ 逐批现种 (Search)
//     eval_block   → 一块一森林: 值域核 → 走 DAG + 各因子根 Stat 进探针 (唯一的块内评估)
//     eval_forest  = 单森林作业: plant → open_job → run_job(ForestPass) → 各条目结局 ForestOut::each
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
#include <cstdint>
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
  factor::stat::Mode m; // 评估口径 (口径 / 方向 / 收益口径 = 因子文件 type; Stat 契约【视图】)
};

struct RootSet {
  Holds hd;
  std::deque<factor::expr::Expr> es;
  std::vector<Root> r;
  std::vector<int> of; // [条目] → 唯一根下标
  void add(const FeatureTable &ft, const std::string &canon, const factor::stat::Mode &m) {
    size_t k = 0;
    while (k < r.size() && (r[k].e->canon != canon || r[k].m != m))
      ++k;
    if (k == r.size())
      r.push_back({&es.emplace_back(ft.reparse(canon)), m});
    of.push_back(static_cast<int>(k));
  }
  size_t size() const { return r.size(); }
};

struct Forest {
  factor::Dag d;
  std::vector<LeafId> leaves;        // d.feats 对应的 LeafId
  std::vector<Root> r;               // [k] 因子根 ↔ d.roots[k] (k < r.size()); 其后 d.roots[r.size()..] 为纯表达式根
  std::vector<std::vector<int>> at;  // [node] → 因子根 k
  std::vector<std::vector<int>> sub; // [根] → 子树节点 (种时一次; 值域核 / 子树 wall 逐块复用)
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
  Forest o{factor::build_forest(es, keep_roots || !plain.empty(), fuse), {}, {rs.begin(), rs.end()}, {}, {}};
  o.leaves = ft.x.ids(o.d.feats);
  o.at.resize(o.d.nodes.size());
  for (size_t k = 0; k < o.r.size(); ++k) {
    const int i = o.d.roots[k];
    assert(o.d.nodes[static_cast<size_t>(i)].op >= 0 && o.d.nodes[static_cast<size_t>(i)].n_in >= 1 && "因子根须是归一算子");
    o.at[static_cast<size_t>(i)].push_back(static_cast<int>(k));
  }
  for (const int i : o.d.roots)
    o.sub.push_back(o.d.subtree(i));
  return o;
}

// 回看行数 = 各根表达式 lookback 的最大值 (因子根 + 纯表达式根)
inline int look_of(std::span<const Root> rs, std::span<const factor::expr::Expr *const> plain, int seg) {
  int look = 0;
  for (const Root &r : rs)
    look = std::max(look, factor::search::space::lookback(*r.e, seg));
  for (const factor::expr::Expr *e : plain)
    look = std::max(look, factor::search::space::lookback(*e, seg));
  return look;
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
  // 块上的标签 (同块各根共用; 评估口径逐根在 Ctx::m)
  factor::stat::Labels labels() const {
    const factor::stat::Labels L = labels_of(block().y);
    factor::stat::assert_labels(L, block().y.sh.g.S, false);
    return L;
  }

  misc::exec::ForkJoin &ex_;
  const Block *c_ = nullptr;
  const factor::Dag *d_ = nullptr;

private:
  // 一批到根的因子根 ks: (批核先 stat_batch 一次评完整批) → 逐根 open → view (Stat 出 RootView) → close
  template <class Ps, class Step>
  void roots(const Forest &F, const std::vector<int> &ks, std::span<Ps> ps, bool last, Step &on_step) {
    if constexpr (D::template kDefer<Ps>)
      self().stat_batch(F, ks);
    const factor::stat::Labels L = labels();
    factor::cpu::stat::Workspace *w = self().template work<Ps>();
    const uint8_t need = Ps::need_at(last);
    for (size_t j = 0; j < ks.size(); ++j) {
      const int k = ks[j];
      Ps &p = ps[static_cast<size_t>(k)];
      const Ctx cx{ex_, block(), L, w, F.r[static_cast<size_t>(k)].m, last};
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
    std::vector<factor::stat::Sum>().swap(day_);
  }
  void release() {
    drop();
    c_ = nullptr, d_ = nullptr;
  }
  double host_bytes() const {
    double b = static_cast<double>(hp_.bytes()) + static_cast<double>(rx_.capacity()) * sizeof(uint16_t) +
               static_cast<double>(day_.capacity()) * sizeof(factor::stat::Sum);
    for (const factor::check::Arena &a : sc_)
      b += static_cast<double>(a.buf.capacity());
    return b;
  }

  template <class Visit>
  bool walk_dag(const std::atomic<bool> &cancel, const factor::Dag &d, std::span<const LeafId> leaves, const uint8_t *need, Visit &&visit) {
    in_ = leaves_of(block(), leaves);
    d_ = &d;
    const Span &sh = c_->y.sh;
    return factor::cpu::walk(ex_, d, in_, hp_, sc_, sh.T, sh.A, sh.g.S, c_->y.gate, need, [&](int i, const auto &, double ms) {
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
  template <class Ps>
  factor::cpu::stat::Workspace *work() {
    const YPlanes &y = block().y;
    w_.ensure(y.sh.A, ex_.threads());
    return &w_;
  }
  // 一个根平面的 Stat (rx_ / day_ / w_ 常驻复用; 探针组 p 即行 sink): → 已定向的 rx_ (块形), 逐日 day_[H][D]
  template <class Ps>
  RootView view(const Forest &F, int k, int, const Ctx &cx, uint8_t need, Ps &p) {
    const int i = F.d.roots[static_cast<size_t>(k)];
    const factor::stat::Span s = c_->span();
    const factor::check::Plane *rp = plane(i);
    rx_.resize(s.n());
    RootView v;
    if constexpr (Ps::kStat) {
      day_.resize(static_cast<size_t>(cx.L.hd.n) * s.days());
      factor::cpu::stat::run(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, cx.m, s, cx.L, rx_.data(), w_, day_.data(), p);
      v.day = day_.data();
    } else {
      factor::cpu::stat::rank_x(ex_, {rp->v.data(), rp->m.data()}, c_->y.gate, cx.m, s, rx_.data(), w_);
    }
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
  std::vector<factor::stat::Sum> day_; // [H][D]
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
    std::vector<factor::stat::Sum>().swap(day_);
    std::vector<factor::stat::Row>().swap(rows_);
    std::vector<float>().swap(ric_);
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
           static_cast<double>(xv_.capacity() + av_.capacity() + ric_.capacity()) * sizeof(float) +
           static_cast<double>(mask_.capacity() + am_.capacity()) + static_cast<double>(day_.capacity()) * sizeof(factor::stat::Sum);
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
    const Span &sh = c_->y.sh;
    return factor::gpu::walk(sess_, d, dev_, dp_, sh.T, sh.A, sh.g.S, gate_, need, [&](int i, const auto &, double ms) {
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

  const factor::gpu::DevPlane *at(int i) const { return factor::gpu::plane_at(*d_, dev_, dp_, i); }

  // Stat 常驻会话: 形状 / 持有期 / 批上限变了才重开, 块换了才重传标签
  void session(int cmax) {
    assert(cmax >= 1 && cmax <= factor::gpu::stat_max_batch());
    const YPlanes &y = block().y;
    const bool same = stat_ && st_T_ == y.sh.T && st_A_ == y.sh.A && st_seg_ == y.sh.g.S && st_cmax_ == cmax && st_hd_ == y.hd;
    if (!same) {
      if (stat_)
        factor::gpu::stat_close(stat_);
      evict_unused_x(static_cast<size_t>((7 * y.hd.n + 2 + 2 * cmax) * n_));
      stat_ = factor::gpu::stat_open(c_->span(), cmax, y.hd);
      st_T_ = y.sh.T, st_A_ = y.sh.A, st_seg_ = y.sh.g.S, st_cmax_ = cmax, st_hd_ = y.hd, st_gen_ = 0;
    }
    if (st_gen_ != c_->gen) {
      factor::gpu::stat_load(stat_, labels_of(y));
      st_gen_ = c_->gen;
    }
  }

  // 批核: 一批到根的候选 ks (同一评估口径) 一次 stat_eval → day_[C][H][D]; 各根的 view 只切片
  void stat_batch(const Forest &F, const std::vector<int> &ks) {
    assert(stat_ && "批核先 plant");
    const YPlanes &y = block().y;
    const factor::stat::Mode m = F.r[static_cast<size_t>(ks.front())].m;
    std::vector<const factor::gpu::DevPlane *> roots;
    roots.reserve(ks.size());
    for (const int k : ks) {
      const int r = F.d.roots[static_cast<size_t>(k)];
      assert(F.d.nodes[static_cast<size_t>(r)].slot >= 0);
      assert(F.r[static_cast<size_t>(k)].m == m && "一批同评估口径");
      roots.push_back(at(r));
    }
    const factor::stat::Span s = c_->span();
    day_.resize(ks.size() * static_cast<size_t>(y.hd.n) * s.days());
    factor::gpu::stat_eval(stat_, roots.data(), static_cast<int>(ks.size()), gate_, m, s, day_.data());
  }
  // 逐根的会话 + 宿主工作区 (批核无逐根宿主工作)
  template <class Ps>
  factor::cpu::stat::Workspace *work() {
    if constexpr (kBatched<Ps>) {
      return nullptr;
    } else {
      const YPlanes &y = block().y;
      session(stat_ ? st_cmax_ : 1);
      w_.ensure(y.sh.A, ex_.threads());
      return &w_;
    }
  }
  // 批核: 切 stat_batch 的第 j 份; 逐根: 一个根平面单候选 stat_eval (按 need 拷回一级 / rx / 平面; 一级逐行交探针组 p)
  template <class Ps>
  RootView view(const Forest &F, int k, int j, const Ctx &cx, uint8_t need, Ps &p) {
    static_assert(!Ps::kCells, "真持有格只在宿主行核里交 (Stat/Cpu.hpp sink.cell)");
    RootView v;
    const factor::stat::Span s = c_->span();
    const int H = cx.L.hd.n;
    if constexpr (kBatched<Ps>) {
      v.day = day_.data() + static_cast<size_t>(j) * H * s.days();
      return v;
    }
    const size_t n = s.n();
    const int i = F.d.roots[static_cast<size_t>(k)];
    const bool rows = (need & kNeedRows) != 0, ric = !rows && (need & kNeedRic) != 0, rx = (need & kNeedRx) != 0;
    if constexpr (Ps::kStat)
      day_.resize(static_cast<size_t>(H) * s.days());
    if (rows)
      rows_.resize(static_cast<size_t>(H) * s.T);
    if (ric)
      ric_.resize(static_cast<size_t>(H) * s.T);
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
    factor::gpu::stat_eval(stat_, &rp, 1, gate_, cx.m, s, Ps::kStat ? day_.data() : nullptr, rows ? rows_.data() : nullptr, ric ? ric_.data() : nullptr,
                           rx ? rx_.data() : nullptr);
    v.day = Ps::kStat ? day_.data() : nullptr;
    v.rx = rx ? rx_.data() : nullptr;
    if (rows || ric) // 与 CPU 行核同交法: 按日切线程, 同日的行同一线程行序
      ex_.rows(s.days(), [&](int d0, int d1, int) {
        for (int t = s.t_lo + d0 * s.g.S; t < s.t_lo + d1 * s.g.S; ++t)
          for (int hi = 0; hi < H; ++hi) {
            const size_t at = static_cast<size_t>(hi) * s.T + t;
            if (rows)
              p.row(hi, t, rows_[at]);
            else
              p.ric(hi, t, ric_[at]);
          }
      });
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
  Holds st_hd_{};
  uint64_t st_gen_ = 0;

  factor::cpu::stat::Workspace w_;
  std::vector<factor::stat::Sum> day_; // [C][H][D]
  std::vector<factor::stat::Row> rows_;
  std::vector<float> ric_;
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

template <class Fn>
auto with_backend(FactorFlow &F, bool use_gpu, Fn &&fn) {
  EngineSpace &es = engine_space(F.S);
  if (use_gpu)
    return fn(es.gpu_of(F.S.ex));
  return fn(es.cpu);
}

// ---- 森林作业: forest_need (⇄ basket_need) / ForestPass (⇄ BasketPass) / eval_forest (⇄ eval_baskets) ----
// R = 本作业的因子根数 (= 探针组数; Search = 候选数, fr 只是模板的占位; 0 = 只有纯表达式根: 不装标签秩, 前带只够回看); look < 0 = 整期一块
template <class E, class Ps>
Need forest_need(const FeatureTable &ft, const Forest &fr, int R, int look, const Holds &hd) {
  Need n{fr.leaves, hd, Ps::segs, E::kRanks && R > 0, {}, E::template budget<Ps>(fr.d, hd.n), Ps::bytes(hd.n) * R + Ps::scratch(hd.n)};
  if (look >= 0)
    n.bands = R > 0 ? bands_of(look, hd, ft.seg) : Bands{(look + ft.seg - 1) / ft.seg, 0};
  return n;
}

// 值域核: dead [d.roots] 进出 (已坏的根不再核, 新坏的填因); need [节点] = 好根子树; → 好根数
int plan_forest(misc::exec::ForkJoin &ex, const Block &c, const Forest &F, std::span<std::string> dead, std::vector<uint8_t> &need);

// 唯一的块内评估 (先 bind): 值域核 → 走 DAG + 各好因子根 Stat 进探针 ps [F.size()]; ms[k] += 因子根 k 子树算子 wall.
//   进度: 批作业 (Search) 由 ForestPass 按 候选 × 块 计; 其余本批 = 要算的算子节点 + 好因子根
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
      for (const int i : Fr.sub[k])
        ms[k] += wall[static_cast<size_t>(i)];
  return ok ? Rc::Ok : Rc::Cancel;
}

// 一趟各根的结局: dead[根] 空 = 好根 (根 = 因子根 + 纯表达式根; 好因子根已 finish); ms[因子根] = 子树算子 wall (各块各批累加)
struct Fate {
  std::vector<std::string> dead;
  std::vector<double> ms;
};

// 块级钩子 (各件可空): want 挑块 (空 = 全装) / before 评前 (块, 块号, 装载秒) ⇄ after 评后 (块)
struct Hooks {
  std::function<bool(const Chunk &)> want;
  std::function<void(const Block &, size_t, double)> before;
  std::function<void(const Block &)> after;
};

// 种法: 自因子根 b0 起种一批的森林, C = 本批因子根数
using Plant = std::function<const Forest &(size_t b0, int &C)>;

// 森林的块消费者 (⇄ BasketPass; run_job 驱动): init 各探针 ⇄ finish 好根; 块 = before → bind → 逐批 plant + eval_block → after; 因子根全坏 = Broken (j.why).
//   种法两种: 定森林 fr (一张种全部根, 含纯表达式根) ⇄ 逐批现种 (Search: 后端按批上限 / 显存定 C, 无纯表达式根)
template <class E, class Ps>
class ForestPass {
public:
  ForestPass(E &eng, FactorFlow &F, const Forest &fr, std::span<Ps> ps, Fate &out, Hooks hk = {})
      : ForestPass(eng, F, fr.r, planted(fr), ps, out, std::move(hk), fr.d.roots.size() - fr.size()) {}
  ForestPass(E &eng, FactorFlow &F, std::span<const Root> roots, Plant plant, std::span<Ps> ps, Fate &out, Hooks hk = {})
      : ForestPass(eng, F, roots, std::move(plant), ps, out, std::move(hk), 0) {}

  bool want(const Chunk &k) const { return !hk_.want || hk_.want(k); }
  void init(Job &j) {
    j_ = &j;
    n_ = j.P.chunks.size();
    out_.dead.assign(roots_.size() + plain_, {});
    out_.ms.assign(roots_.size(), 0.0);
    for (Ps &p : ps_)
      p.init(pass_ctx(j, eng_.ex()));
  }
  Rc block(const Block &c, size_t ci, bool last, double load_s) {
    if (hk_.before)
      hk_.before(c, ci, load_s);
    eng_.bind(c);
    const size_t R = roots_.size();
    if constexpr (kBatched<Ps>) {
      F_.prog.begin(Phase::Running, R * n_);
      F_.prog.publish(R * ci);
    }
    for (size_t b0 = 0;;) {
      int C = 0;
      const Forest &Fr = plant_(b0, C);
      const size_t m = static_cast<size_t>(C);
      assert((R == 0 || m >= 1) && b0 + m <= R && Fr.size() == m);
      if (!Fr.d.roots.empty() &&
          eval_block(eng_, F_, c, Fr, std::span(out_.dead).subspan(b0, Fr.d.roots.size()), ps_.subspan(b0, m), last, std::span(out_.ms).subspan(b0, m)) != Rc::Ok)
        return Rc::Cancel;
      if ((b0 += m) >= R)
        break;
    }
    if (R > 0 && std::none_of(out_.dead.begin(), out_.dead.begin() + static_cast<std::ptrdiff_t>(R), [](const std::string &s) { return s.empty(); })) {
      j_->why = out_.dead[0];
      return Rc::Broken;
    }
    if (hk_.after)
      hk_.after(c);
    return Rc::Ok;
  }
  void finish(const Job &j) {
    for (size_t k = 0; k < roots_.size(); ++k)
      if (out_.dead[k].empty())
        ps_[k].finish(pass_ctx(j, eng_.ex()));
  }

private:
  ForestPass(E &eng, FactorFlow &F, std::span<const Root> roots, Plant plant, std::span<Ps> ps, Fate &out, Hooks hk, size_t plain)
      : eng_(eng), F_(F), roots_(roots), plant_(std::move(plant)), ps_(ps), out_(out), hk_(std::move(hk)), plain_(plain) {
    assert(ps.size() == roots.size());
  }
  static Plant planted(const Forest &fr) {
    return [&fr](size_t b0, int &C) -> const Forest & {
      assert(b0 == 0);
      C = static_cast<int>(fr.size());
      return fr;
    };
  }

  E &eng_;
  FactorFlow &F_;
  std::span<const Root> roots_;
  Plant plant_;
  std::span<Ps> ps_;
  Fate &out_;
  Hooks hk_;
  size_t plain_ = 0, n_ = 0;
  Job *j_ = nullptr;
};

// 单森林作业的产出 (Run / Inspect / Portfolio): 根 k 的坏因 why (空 = 好根) / 子树 wall / 作用域; each = 各条目的结局 (中断不回调)
struct ForestOut {
  Job job;
  Forest fr;
  Fate fate;
  const char *backend = "";
  std::string why(int k, Rc rc) const {
    const size_t i = static_cast<size_t>(k);
    return i < fate.dead.size() && !fate.dead[i].empty() ? fate.dead[i] : rc == Rc::Ok ? std::string()
                                                                                       : job.why;
  }
  double eval_ms(int k) const { return fate.ms[static_cast<size_t>(k)]; }
  StatScope scope(const FactorScope &sc) const { return job.scope(sc, backend); }
  template <class Fn>
  void each(const RootSet &rs, Rc rc, Fn &&fn) const {
    if (aborted(rc))
      return;
    for (size_t e = 0; e < rs.of.size(); ++e)
      fn(static_cast<int>(e), rs.of[e], why(rs.of[e], rc));
  }
};

// 单森林作业 (⇄ eval_baskets): 根去重共享一张定森林 → open_job → run_job(ForestPass); whole = 整期一块 (Inspect)
template <class E, class Ps>
Rc eval_forest(E &eng, FactorFlow &F, const FactorScope &sc, const RootSet &rs, std::span<Ps> ps, ForestOut &o, bool whole, Hooks hk = {}) {
  static_assert(!kBatched<Ps>, "eval_forest 逐根评 (批核只 Search)");
  assert(rs.size() > 0 && rs.hd.n > 0 && ps.size() == rs.size() && o.fr.size() == 0);
  o.fr = plant_forest(*sc.ft, rs.r, /*keep_roots=*/false);
  o.backend = E::kName;
  const int look = whole ? -1 : look_of(rs.r, {}, sc.ft->seg);
  if (const Rc rc = open_job(F, sc, forest_need<E, Ps>(*sc.ft, o.fr, static_cast<int>(rs.size()), look, rs.hd), o.job); rc != Rc::Ok)
    return rc;
  return run_job(F, o.job, ForestPass<E, Ps>(eng, F, o.fr, ps, o.fate, std::move(hk)));
}

} // namespace GUI::Engine
