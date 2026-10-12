// SearchService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Search/Cpu.hpp → Stat/Cpu.hpp), 依赖受控浮点:
// CMake PRECISE_MATH 源.
#include "gui/task_factors/services/SearchService.hpp"

#include "factor/Dag.hpp"
#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/Exec.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Cpu.hpp"
#include "factor/Search/Grid.hpp"
#include "factor/Stat/Cpu.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <map>
#include <string>
#include <thread>

namespace GUI::Factors {

namespace {

namespace grid = factor::search::grid;
using factor::search::Result;
using factor::search::Score;
using Clock = std::chrono::steady_clock;

constexpr int kCarryDays = 5;      // 块首多带的交易日 (ROLL 预热 + rAC lag): 行数 = kMaxD = TS 归一窗 ≥ max_lag
constexpr int kCpuForest = 64;     // CPU 一张共享 DAG 的候选数 (只为公共前缀, 根即算即放)
constexpr int kGpuMinBatch = 8;    // 定块大小时按至少这么多候选同在显存算 (核内 8 候选共用标签行)
constexpr double kHostFrac = 0.6;  // 宿主可用内存的使用比例
constexpr double kVramFrac = 0.85; // 显存空闲量的使用比例
static_assert(kCarryDays * factor::kSegLen == factor::expr::kMaxD, "carry 行数须等于窗长上限 kMaxD");

// 一块的装载数据 (+ CPU 的 ry); 整轮缓存或单块复用
struct ChunkData {
  int idx = -1; // 块号; -1 = 空
  int t_lo = 0, t0_abs = 0;
  Loaded L;
  std::vector<std::vector<uint16_t>> ry; // [H][n] (CPU; 装载后清空, 首次评估时算)
};

// 一个候选 (评过 / 待评)
struct Entry {
  std::vector<double> u;
  std::vector<int> offs; // 粗格点偏移 (粗阶段的点才有)
  factor::expr::Expr e;
  Result r;
  Score sc;
  int pass = -1; // 评过的趟; -1 = 待评
};

// 分支 (粗网格后): 支内 h 固定
struct Branch {
  std::vector<double> center;
  int hi = -1;
  double best = 0.0, step = 0.5;
  bool alive = true;
};

enum class Rc : uint8_t { Ok,
                          Cancel, // 取消 / 库判废 → run 返回 false
                          Abort   // 正常输入问题, message 已填 → run 返回 true
};

} // namespace

// worker 私有的常驻件 (线程池 + CPU 工作区; 跨请求存活, ensure 幂等)
struct SearchService::Flow {
  factor::exec::ForkJoin ex;
  factor::cpu::Pool pool;
  std::vector<factor::cpu::ParScratch> sc;
  factor::cpu::search::Workspace w;
  Flow() : ex(static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))) {}
};

bool MakeSearchRequest(const SharedData &data, const FactorRow &row, bool gpu, int cost_idx, SearchRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Optimize 只接有效 alpha 行");
  req = SearchRequest{};
  req.factor_dir = data.config.factor_dir + "/" + data.config.universe;
  req.file = row.file;
  req.expr = row.expr;
  req.frame = row.frame;
  req.gpu = gpu;
  req.cost_idx = cost_idx;
  req.sell_impact = data.config.sell_impact;
  req.max_impact = data.config.max_impact;
  if (data.asset.items.empty())
    return false; // 资产轴未就绪 (数据库未扫), 读不了特征库
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return false;
  req.universe = data.config.universe;
  req.start_date = data.config.start_date;
  req.end_date = data.config.end_date;
  return true;
}

void SearchService::Request(const SearchRequest &req) {
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = req;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&SearchService::worker_loop, this);
  }
}

void SearchService::Stop() {
  if (!thread_.joinable())
    return;
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    stop_.store(true, std::memory_order_relaxed);
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  thread_.join();
}

void SearchService::worker_loop() {
  TraceThread("SearchWorker");
  Flow F;
  while (true) {
    SearchRequest req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = *pending_;
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed);
    }
    pass_.store(0, std::memory_order_relaxed);
    done_.store(0, std::memory_order_relaxed);
    total_.store(0, std::memory_order_relaxed);
    chunk_.store(0, std::memory_order_relaxed);
    chunks_.store(0, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(mutex);
      result = SearchResult{};
      result.file = req.file;
      result.expr0 = req.expr;
      result.backend = req.gpu ? "gpu" : "cpu";
    }
    status_.store(SearchStatus::Loading, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
    const bool ok = run(req, F);
    status_.store(ok ? SearchStatus::Done : SearchStatus::Cancelled, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
}

bool SearchService::run(const SearchRequest &req, Flow &F) {
  TraceN("SearchRun");
  namespace fs = factor::stat;
  const Clock::time_point t_start = Clock::now();
  const int nt = F.ex.threads();
  const int A = static_cast<int>(req.scope.uni.size());
  const int H = static_cast<int>(feats_.labels.size());
  assert(H >= 1 && H <= fs::kMaxHold);
  assert(req.cost_idx >= 0 && req.cost_idx < static_cast<int>(feats_.costs.size()) && "金额档下标越界");
  fs::Holds hd;
  hd.n = H;
  for (int hi = 0; hi < H; ++hi)
    hd.h[hi] = feats_.labels[static_cast<size_t>(hi)].hold;
  fs::assert_holds(hd);
  assert(factor::search::max_lag(hd) <= kCarryDays * factor::kSegLen && "carry 不够 rAC 的 lag");
  factor::search::Objective obj; // n_full 装了块 0 后按池宽填 (见下【尺度】前); hold 窗 / ric_pow 取契约默认
  obj.sell_impact = req.sell_impact;

  SearchResult res;
  res.file = req.file;
  res.expr0 = req.expr;
  res.backend = req.gpu ? "gpu" : "cpu";
  res.hd = hd;
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };

  // ---- 表达式 + 可搜槽 ----
  factor::expr::Expr e0;
  {
    std::string err;
    const bool ok = factor::expr::parse(req.expr, feats_.lookup(), e0, err);
    assert(ok && "Factors 表的规范串必可再解析");
    (void)ok;
  }
  std::vector<grid::Slot> slots = grid::slots_of(e0, req.frame == fs::Frame::TS);
  for (const grid::Slot &s : slots)
    res.slots.push_back(s.name);
  const factor::Dag d0 = factor::build(e0);
  const int nf = static_cast<int>(d0.feats.size()), s0 = d0.n_slots;

  // 候选表: 规范串去重
  std::vector<Entry> ents;
  std::map<std::string, int> seen;
  const auto add_cand = [&](const std::vector<double> &u, const std::vector<int> &offs) -> int {
    Entry en;
    if (!grid::apply(e0, slots, u, en.e))
      return -1;
    if (const auto it = seen.find(en.e.canon); it != seen.end())
      return it->second;
    en.u = u;
    en.offs = offs;
    const int id = static_cast<int>(ents.size());
    seen.emplace(en.e.canon, id);
    ents.push_back(std::move(en));
    return id;
  };
  const auto publish = [&] {
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    res.cands.clear();
    for (const Entry &en : ents) {
      if (en.pass < 0)
        continue;
      SearchCand c;
      c.expr = en.e.canon;
      c.u = en.u;
      c.pass = en.pass;
      c.best_hi = en.sc.best;
      c.score = en.sc.score;
      c.ls_net = en.sc.ls_net;
      c.util = en.sc.util;
      c.ric = en.sc.ric;
      res.cands.push_back(std::move(c));
    }
    std::stable_sort(res.cands.begin(), res.cands.end(), [](const SearchCand &a, const SearchCand &b) { return a.score > b.score; });
    res.best_expr = res.cands.empty() || !std::isfinite(res.cands.front().score) ? std::string() : res.cands.front().expr;
    if (const auto it = seen.find(e0.canon); it != seen.end() && ents[static_cast<size_t>(it->second)].pass >= 0)
      res.base_score = ents[static_cast<size_t>(it->second)].sc.score;
    std::lock_guard<std::mutex> lock(mutex);
    result = res;
  };
  const auto abort_with = [&](std::string msg) {
    res.message = std::move(msg);
    publish();
    return true;
  };
  if (slots.empty())
    return abort_with("无可搜参数项 (表达式没有 d / k; TS 根的 d 是口径定义不搜)");

  // ---- 作用域 / 日期 ----
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty())
    return abort_with("特征库在该区间无数据 (先算特征)");
  const int days = static_cast<int>(dates.size());

  // ---- 分块与预算 (字节 / 格; 见头文件【分块流式】) ----
  //   L0  宿主 Loaded: 特征 (v+m) 5·nf + 截面门控 5 + 标签 (lv,sv,m) 5·H + 成本 (全部金额档) 5·Cc
  //   Wc  CPU 工作区: ry 2·H + ws 2 + 池 (s0+2)·5
  //   F0  设备常驻: 特征 5·nf + 门控 1 + 搜索会话标签 (lv,sv,m,ry) 7·H + 一档成本 5; 每候选 7 (根平面 5 + rx 2); 中间量 2·s0 槽
  const int Cc = static_cast<int>(feats_.costs.size());
  const double per_day = static_cast<double>(factor::kSegLen) * A;
  const double L0 = 5.0 * nf + 5.0 + 5.0 * H + 5.0 * Cc;
  const double Wc = 2.0 * H + 2.0 + 5.0 * (s0 + 2);
  const double F0 = 5.0 * nf + 1.0 + 7.0 * H + 5.0;
  const MemoryUsage mu = memory_usage();
  const double host_budget = kHostFrac * static_cast<double>(mu.total_bytes - mu.used_bytes);
  int D = std::clamp(static_cast<int>(host_budget / (per_day * (L0 + (req.gpu ? 0.0 : Wc)))) - kCarryDays, 1, days);
  double vram_budget = 0.0;
  if (req.gpu) {
    assert(factor::gpu::available() && "GPU 后端不可用");
    size_t vfree = 0, vtotal = 0;
    factor::gpu::mem_info(&vfree, &vtotal);
    vram_budget = kVramFrac * static_cast<double>(vfree);
    const int Dg = std::clamp(static_cast<int>(vram_budget / (per_day * (F0 + 10.0 * s0 + 7.0 * kGpuMinBatch))) - kCarryDays, 1, days);
    D = std::min(D, Dg);
  }
  const int n_chunks = (days + D - 1) / D;
  const int Tmax = std::min(days, D + kCarryDays) * factor::kSegLen; // 最长一块的行数
  int batch = 1;
  if (req.gpu) {
    const double nmax = static_cast<double>(Tmax) * A;
    batch = std::clamp(static_cast<int>((vram_budget - nmax * (F0 + 10.0 * s0)) / (7.0 * nmax)), 1, factor::gpu::search_max_batch());
  }
  // 整轮缓存全部块 (装得下) 还是一块复用
  double cache_bytes = 0.0;
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - kCarryDays);
    cache_bytes += (day1 - load0) * per_day * (L0 + (req.gpu ? 0.0 : 2.0 * H));
  }
  const bool cache_all = cache_bytes + Tmax * static_cast<double>(A) * (req.gpu ? 0.0 : Wc) <= host_budget;
  res.n_chunk = n_chunks, res.chunk_days = D, res.batch = batch;
  chunks_.store(n_chunks, std::memory_order_relaxed);

  std::vector<ChunkData> chunks(cache_all ? static_cast<size_t>(n_chunks) : 1);
  bool domain_checked = false;
  // 装一块: 交易日 [day0, day1), 前带 carry; 首块顺带查特征叶值域
  const auto load_chunk = [&](int ci, ChunkData &cd) -> Rc {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - kCarryDays);
    const std::vector<std::string> cdates(dates.begin() + load0, dates.begin() + day1);
    cd.idx = -1;
    cd.ry.clear();
    cd.L = Loaded{};
    init_loaded(feats_, day1 - load0, A, cd.L);
    cd.L.max_impact = static_cast<float>(req.max_impact);
    cd.L.feat_codes = d0.feats;
    status_.store(SearchStatus::Loading, std::memory_order_release);
    total_.store(day1 - load0, std::memory_order_relaxed);
    done_.store(0, std::memory_order_relaxed);
    if (!load_planes(feats_, reader, cdates, cd.L, F.ex, cancel_, done_, /*with_labels=*/true, /*with_costs=*/true, /*with_index=*/false)) {
      if (reader.stale()) {
        res.message = "特征库判废 (子轴/字段表与当前不符), 需重算特征";
        publish();
        return Rc::Abort;
      }
      return Rc::Cancel;
    }
    cd.idx = ci;
    cd.t_lo = (day0 - load0) * factor::kSegLen;
    cd.t0_abs = load0 * factor::kSegLen;
    if (!domain_checked) {
      domain_checked = true;
      for (int i = 0; i < static_cast<int>(d0.nodes.size()); ++i) {
        std::string err;
        check_leaf_domains(d0, i, cd.L, err);
        if (!err.empty()) {
          res.message = err;
          publish();
          return Rc::Abort;
        }
      }
    }
    return Rc::Ok;
  };

  // ---- 一块上评一批候选 (CPU): 共享 DAG 顺序走, 根算完立刻 Search ----
  const auto cpu_chunk = [&](ChunkData &cd, const std::vector<int> &ids) -> bool {
    Loaded &L = cd.L;
    const int T = L.T;
    const size_t n = L.n();
    F.w.ensure(T, A, H, nt);
    if (cd.ry.size() != static_cast<size_t>(H)) {
      cd.ry.assign(static_cast<size_t>(H), std::vector<uint16_t>(n));
      for (int hi = 0; hi < H; ++hi) {
        const LabelPlane &p = L.labels[static_cast<size_t>(hi)];
        factor::cpu::stat::prep_label(F.ex, p.lv.data(), p.m.data(), T, A, cd.ry[static_cast<size_t>(hi)].data(), F.w.st);
      }
    }
    fs::Label lab[fs::kMaxHold];
    for (int hi = 0; hi < H; ++hi) {
      const LabelPlane &p = L.labels[static_cast<size_t>(hi)];
      lab[hi] = {p.lv.data(), p.sv.data(), p.m.data(), cd.ry[static_cast<size_t>(hi)].data()};
    }
    const CostPlane &cp = L.costs[static_cast<size_t>(req.cost_idx)];
    const factor::search::Cost cost{cp.buy.data(), cp.sell.data(), cp.m.data()};
    if (F.sc.size() < static_cast<size_t>(nt))
      F.sc.resize(static_cast<size_t>(nt));
    for (size_t b0 = 0; b0 < ids.size(); b0 += kCpuForest) {
      const size_t b1 = std::min(ids.size(), b0 + kCpuForest);
      std::vector<const factor::expr::Expr *> ptrs;
      for (size_t k = b0; k < b1; ++k)
        ptrs.push_back(&ents[static_cast<size_t>(ids[k])].e);
      const factor::Dag Fd = factor::build_forest(ptrs, /*keep_roots=*/false);
      assert(Fd.feats == d0.feats && "同结构候选的特征叶序必与基线一致 (平面下标按 d0.feats 装)");
      F.pool.prepare(Fd.n_slots, n);
      const auto plane_of = [&](int i) -> const factor::check::Plane * {
        const factor::DagNode &nd = Fd.nodes[static_cast<size_t>(i)];
        return nd.op < 0 ? &L.planes[static_cast<size_t>(nd.feat)] : &F.pool.slots[static_cast<size_t>(nd.slot)];
      };
      for (int i = 0; i < static_cast<int>(Fd.nodes.size()); ++i) {
        if (cancelled())
          return false;
        const factor::DagNode &nd = Fd.nodes[static_cast<size_t>(i)];
        assert(nd.op >= 0 || !Fd.is_root(i)); // 根是归一算子, 不会是特征叶
        if (nd.op < 0)
          continue;
        const factor::check::Plane *in[3] = {};
        for (int a = 0; a < factor::expr::kOps[nd.op].arity; ++a)
          in[a] = plane_of(nd.in[a]);
        factor::cpu::run_node(F.ex, nd, in, F.pool.slots[static_cast<size_t>(nd.slot)], T, A, F.sc, L.cs.m.data());
        if (!Fd.is_root(i))
          continue;
        const factor::check::Plane *rp = plane_of(i);
        const float *xv = rp->v.data();
        const uint8_t *xm = rp->m.data();
        for (size_t k = b0; k < b1; ++k) {
          if (Fd.roots[k - b0] != i)
            continue;
          Result out;
          factor::cpu::search::run(F.ex, 1, &xv, &xm, L.cs.m.data(), req.frame, T, A, cd.t0_abs, cd.t_lo, hd, lab, cost, F.w, &out);
          ents[static_cast<size_t>(ids[k])].r.merge(out, H);
          done_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    return true;
  };

  // ---- 一块上评一批候选 (GPU): keep_roots 森林, 一批根平面同在显存, 一次 search_eval ----
  const auto gpu_chunk = [&](ChunkData &cd, const std::vector<int> &ids) -> bool {
    Loaded &L = cd.L;
    const int T = L.T;
    const size_t n = L.n();
    factor::gpu::Session *sess = factor::gpu::session_open(n);
    std::vector<const factor::gpu::DevPlane *> dev(static_cast<size_t>(nf));
    for (int i = 0; i < nf; ++i)
      dev[static_cast<size_t>(i)] = factor::gpu::upload(sess, L.planes[static_cast<size_t>(i)].v.data(), L.planes[static_cast<size_t>(i)].m.data());
    const factor::gpu::DevPlane *cs_gate = factor::gpu::upload_mask(sess, L.cs.m.data());
    fs::Label lab[fs::kMaxHold];
    for (int hi = 0; hi < H; ++hi) {
      const LabelPlane &p = L.labels[static_cast<size_t>(hi)];
      lab[hi] = {p.lv.data(), p.sv.data(), p.m.data(), nullptr};
    }
    const CostPlane &cp = L.costs[static_cast<size_t>(req.cost_idx)];
    const factor::search::Cost cost{cp.buy.data(), cp.sell.data(), cp.m.data()};
    factor::gpu::SearchSession *ss = factor::gpu::search_open(T, A, batch, hd);
    factor::gpu::search_load(ss, lab, cost);
    factor::gpu::DevPool pool;
    std::vector<Result> out(static_cast<size_t>(batch));
    std::vector<const factor::gpu::DevPlane *> roots(static_cast<size_t>(batch));
    bool ok = true;
    int C = batch;
    for (size_t b0 = 0; ok && b0 < ids.size();) {
      C = std::min(C, static_cast<int>(ids.size() - b0));
      // 森林按实际槽数试装: 新增槽的显存不够就减半 (一个候选都装不下 = 块太大, 断言)
      factor::Dag Fd;
      while (true) {
        std::vector<const factor::expr::Expr *> ptrs;
        for (int k = 0; k < C; ++k)
          ptrs.push_back(&ents[static_cast<size_t>(ids[b0 + static_cast<size_t>(k)])].e);
        Fd = factor::build_forest(ptrs, /*keep_roots=*/true);
        assert(Fd.feats == d0.feats);
        size_t vfree = 0, vtotal = 0;
        factor::gpu::mem_info(&vfree, &vtotal);
        const size_t grow = Fd.n_slots > static_cast<int>(pool.slots.size()) ? static_cast<size_t>(Fd.n_slots) - pool.slots.size() : 0;
        if (static_cast<double>(grow) * 5.0 * static_cast<double>(n) <= 0.9 * static_cast<double>(vfree))
          break;
        assert(C > 1 && "显存不够评一个候选: 块太大");
        C /= 2;
      }
      pool.prepare(sess, Fd.n_slots);
      const auto plane_of = [&](int i) -> const factor::gpu::DevPlane * {
        const factor::DagNode &nd = Fd.nodes[static_cast<size_t>(i)];
        return nd.op < 0 ? dev[static_cast<size_t>(nd.feat)] : pool.slots[static_cast<size_t>(nd.slot)];
      };
      for (int i = 0; i < static_cast<int>(Fd.nodes.size()); ++i) {
        if (cancelled()) {
          ok = false;
          break;
        }
        const factor::DagNode &nd = Fd.nodes[static_cast<size_t>(i)];
        if (nd.op < 0)
          continue;
        const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
        const factor::gpu::DevPlane *in[3] = {};
        for (int a = 0; a < o.arity; ++a)
          in[a] = plane_of(nd.in[a]);
        factor::gpu::DevPlane *o_plane = pool.slots[static_cast<size_t>(nd.slot)];
        if (o.a == factor::A::SELF)
          factor::gpu::run_ts_dev(sess, o.name, in[0], in[1], in[2], o_plane, T, A, nd.p);
        else
          factor::gpu::run_cs_dev(sess, o.name, in[0], in[1], in[2], cs_gate, o_plane, T, A, nd.p);
      }
      if (!ok)
        break;
      for (int k = 0; k < C; ++k) {
        const factor::DagNode &rn = Fd.nodes[static_cast<size_t>(Fd.roots[static_cast<size_t>(k)])];
        assert(rn.op >= 0 && rn.slot >= 0);
        roots[static_cast<size_t>(k)] = pool.slots[static_cast<size_t>(rn.slot)];
      }
      factor::gpu::search_eval(ss, roots.data(), C, cs_gate, req.frame, cd.t0_abs, cd.t_lo, out.data());
      for (int k = 0; k < C; ++k)
        ents[static_cast<size_t>(ids[b0 + static_cast<size_t>(k)])].r.merge(out[static_cast<size_t>(k)], H);
      done_.fetch_add(C, std::memory_order_relaxed);
      b0 += static_cast<size_t>(C);
    }
    pool.release();
    factor::gpu::search_close(ss);
    factor::gpu::session_close(sess); // 上传的输入平面随会话释放
    return ok;
  };

  // ---- 一趟: ids 沿全部块流过, 收尾打分 + 发布 ----
  const auto run_pass = [&](const std::vector<int> &ids, int pass) -> Rc {
    if (ids.empty())
      return Rc::Ok;
    pass_.store(pass, std::memory_order_relaxed);
    chunk_.store(0, std::memory_order_relaxed);
    for (int id : ids)
      ents[static_cast<size_t>(id)].r = Result{};
    for (int ci = 0; ci < n_chunks; ++ci) {
      ChunkData &cd = chunks[cache_all ? static_cast<size_t>(ci) : 0];
      if (cd.idx != ci) {
        const Rc rc = load_chunk(ci, cd);
        if (rc != Rc::Ok)
          return rc;
      }
      status_.store(SearchStatus::Running, std::memory_order_release);
      total_.store(static_cast<int>(ids.size()) * n_chunks, std::memory_order_relaxed);
      done_.store(static_cast<int>(ids.size()) * ci, std::memory_order_relaxed);
      if (!(req.gpu ? gpu_chunk(cd, ids) : cpu_chunk(cd, ids)))
        return Rc::Cancel;
      chunk_.store(ci + 1, std::memory_order_relaxed);
    }
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      en.sc = factor::search::score_of(en.r, hd, obj);
      en.pass = pass;
    }
    res.n_pass = pass + 1;
    publish();
    epoch_.fetch_add(1, std::memory_order_relaxed);
    return Rc::Ok;
  };
  const auto handle = [&](Rc rc, bool &ret) {
    ret = rc != Rc::Cancel;
    return rc != Rc::Ok;
  };
  bool ret = false;

  // ---- 尺度: 先装块 0 (趟 0 反正要), 阈值槽 (ANY / GE0) 的坐标单位 = 基线 DAG 里该节点输入的池内有效格标准差 (CPU 走到该节点即止) ----
  if (handle(load_chunk(0, chunks[0]), ret))
    return ret;
  // ---- 满仓只数 n_full (目标的 util 分母; 契约【目标】): 块 0 池内 (cs_valid) 平均每行有效票数 / kGroups —— 与因子无关, 池宽决定 ----
  {
    const Loaded &L = chunks[0].L;
    const uint8_t *g = L.cs.m.data();
    long long on = 0;
    for (size_t k = 0, n = L.n(); k < n; ++k)
      on += g[k] != 0;
    obj.n_full = static_cast<double>(on) / static_cast<double>(L.T) / fs::kGroups;
    assert(obj.n_full > 0.0 && "块 0 池内无有效票");
    res.n_full = obj.n_full;
  }
  {
    std::vector<size_t> need; // 要尺度的槽
    std::vector<int> want;    // 对应输入节点在 d0 的下标
    for (size_t j = 0; j < slots.size(); ++j) {
      if (!grid::needs_scale(slots[j]))
        continue;
      const factor::expr::Node &nd = e0.nodes[static_cast<size_t>(slots[j].node)];
      assert(factor::expr::kOps[nd.op].arity >= 1);
      const std::string key = factor::expr::to_string(e0, nd.args[0]);
      const auto it = std::find(d0.keys.begin(), d0.keys.end(), key);
      assert(it != d0.keys.end() && "Expr 子树必在同一 Expr 的 Dag 里");
      need.push_back(j);
      want.push_back(static_cast<int>(it - d0.keys.begin()));
    }
    if (!need.empty()) {
      ChunkData &cd = chunks[0];
      Loaded &L = cd.L;
      const int T = L.T;
      const size_t n = L.n();
      const int last = *std::max_element(want.begin(), want.end());
      F.pool.prepare(d0.n_slots, n);
      if (F.sc.size() < static_cast<size_t>(nt))
        F.sc.resize(static_cast<size_t>(nt));
      const auto plane_of = [&](int i) -> const factor::check::Plane * {
        const factor::DagNode &nd = d0.nodes[static_cast<size_t>(i)];
        return nd.op < 0 ? &L.planes[static_cast<size_t>(nd.feat)] : &F.pool.slots[static_cast<size_t>(nd.slot)];
      };
      std::vector<double> sigma(need.size(), 0.0);
      for (int i = 0; i <= last; ++i) {
        if (cancelled())
          return false;
        const factor::DagNode &nd = d0.nodes[static_cast<size_t>(i)];
        if (nd.op >= 0) {
          const factor::check::Plane *in[3] = {};
          for (int a = 0; a < factor::expr::kOps[nd.op].arity; ++a)
            in[a] = plane_of(nd.in[a]);
          factor::cpu::run_node(F.ex, nd, in, F.pool.slots[static_cast<size_t>(nd.slot)], T, A, F.sc, L.cs.m.data());
        }
        for (size_t q = 0; q < need.size(); ++q) {
          if (want[q] != i)
            continue;
          const factor::check::Plane *p = plane_of(i);
          const uint8_t *gate = L.cs.m.data();
          double s1 = 0.0, s2 = 0.0;
          long long cnt = 0;
          for (size_t k = 0; k < n; ++k) {
            const bool on = p->m[k] && gate[k];
            const double v = on ? static_cast<double>(p->v[k]) : 0.0;
            s1 += v, s2 += v * v, cnt += on;
          }
          sigma[q] = cnt >= 2 ? std::sqrt(std::max(0.0, (s2 - s1 * s1 / static_cast<double>(cnt)) / static_cast<double>(cnt - 1))) : 0.0;
        }
      }
      std::vector<grid::Slot> kept;
      res.slots.clear();
      for (size_t j = 0, q = 0; j < slots.size(); ++j) {
        if (grid::needs_scale(slots[j])) {
          const double sd = sigma[q++];
          if (!(std::isfinite(sd) && sd > 0.0))
            continue; // 输入常量 → 阈值无意义, 不搜
          grid::set_scale(slots[j], sd);
        }
        res.slots.push_back(slots[j].name);
        kept.push_back(slots[j]);
      }
      slots = std::move(kept);
      if (slots.empty())
        return abort_with("可搜参数项只有阈值, 而其输入在块 0 上是常量 (σ = 0), 不搜");
    }
  }

  // ---- 趟 0: 粗网格 (含基线; 步长由预算反推) ----
  const grid::Coarse coarse = grid::coarse_lattice(slots);
  const std::vector<std::vector<int>> &lattice = coarse.offs;
  std::vector<int> lattice_id(lattice.size(), -1); // 粗格点 → 候选 (−1 = 该点参数不合法)
  std::vector<int> ids;
  for (size_t li = 0; li < lattice.size(); ++li) {
    const int id = add_cand(grid::coarse_u(slots, lattice[li], coarse.step), lattice[li]);
    lattice_id[li] = id;
    if (id >= 0 && std::find(ids.begin(), ids.end(), id) == ids.end())
      ids.push_back(id);
  }
  assert(seen.count(e0.canon) && "基线 (全 0 偏移) 必在粗网格里");
  if (handle(run_pass(ids, 0), ret))
    return ret;

  // ---- 分支: 互不相邻的 top-kBranches, 支内 h 固定 ----
  std::vector<double> lscore(lattice.size(), -std::numeric_limits<double>::infinity());
  for (size_t li = 0; li < lattice.size(); ++li)
    if (lattice_id[li] >= 0)
      lscore[li] = ents[static_cast<size_t>(lattice_id[li])].sc.score;
  std::vector<Branch> branches;
  for (int li : grid::pick_branches(lattice, lscore)) {
    const Entry &en = ents[static_cast<size_t>(lattice_id[static_cast<size_t>(li)])];
    Branch b;
    b.center = en.u;
    b.hi = en.sc.best;
    b.best = en.sc.score;
    b.step = coarse.step * 0.5;
    branches.push_back(std::move(b));
  }
  if (branches.empty())
    return abort_with("粗网格无一候选有正分 (净 LS ≤ 0 或 IC ≤ 0), 文件不动");

  // ---- 趟 1..: 每支围 top-1 细化 (点数 ≤ kLayerMax), 步长减半, 增益 < kMinGain 停 ----
  for (int level = 1; level <= grid::kMaxLevels; ++level) {
    std::vector<std::vector<int>> pts(branches.size()); // 每支本层的格点候选
    ids.clear();
    for (size_t bi = 0; bi < branches.size(); ++bi) {
      Branch &b = branches[bi];
      if (!b.alive)
        continue;
      for (const std::vector<double> &u : grid::refine_lattice(slots, b.center, b.step)) {
        const int id = add_cand(u, {});
        if (id < 0)
          continue;
        pts[bi].push_back(id);
        if (ents[static_cast<size_t>(id)].pass < 0 && std::find(ids.begin(), ids.end(), id) == ids.end())
          ids.push_back(id);
      }
    }
    if (handle(run_pass(ids, level), ret))
      return ret;
    bool any = false;
    for (size_t bi = 0; bi < branches.size(); ++bi) {
      Branch &b = branches[bi];
      if (!b.alive)
        continue;
      int best_id = -1;
      double best = -std::numeric_limits<double>::infinity();
      for (int id : pts[bi]) {
        const double sc = factor::search::score_at(ents[static_cast<size_t>(id)].sc, b.hi, obj);
        if (sc > best)
          best = sc, best_id = id;
      }
      const bool gained = best_id >= 0 && best > b.best * (1.0 + grid::kMinGain);
      if (best_id >= 0 && best > b.best) {
        b.center = ents[static_cast<size_t>(best_id)].u;
        b.best = best;
      }
      if (gained) {
        b.step *= 0.5;
        any = true;
      } else {
        b.alive = false;
      }
    }
    if (!any)
      break;
  }

  // ---- 终: 全部候选按自选 h* 的 score 取最优; 严格好于基线才落盘 (基线在粗网格里评过, 它的 score 就是门槛; 基线 −∞ 时任一有限分即过) ----
  int best_id = -1;
  for (size_t i = 0; i < ents.size(); ++i)
    if (ents[i].pass >= 0 && std::isfinite(ents[i].sc.score) && (best_id < 0 || ents[i].sc.score > ents[static_cast<size_t>(best_id)].sc.score))
      best_id = static_cast<int>(i);
  assert(best_id >= 0); // 分支非空 ⇒ 至少一个有限分
  const Entry &best = ents[static_cast<size_t>(best_id)];
  const Entry &base = ents[static_cast<size_t>(seen.at(e0.canon))];
  assert(base.pass >= 0 && "基线在粗网格 (趟 0) 必评过");
  if (!(best.sc.score > base.sc.score))
    return abort_with("当前参数已是最优 (无候选 score 严格高于基线), 文件不动");
  SearchMark mark;
  mark.expr = best.e.canon;
  mark.hold = hd.h[best.sc.best];
  mark.amt = feats_.costs[static_cast<size_t>(req.cost_idx)].amt;
  mark.sell_impact = req.sell_impact;
  mark.ls_net = best.sc.ls_net;
  mark.util = best.sc.util;
  mark.ric = best.sc.ric;
  mark.score = best.sc.score;
  mark.base_score = base.sc.score;
  mark.time = now_string();
  WriteFactorParams(req.factor_dir, req.file, best.e, mark);
  res.wrote = true;
  res.message = "params 已写入 (h* = " + fs::hold_name(mark.hold) + "), 自动 Run 重算 stat";
  publish();
  return true;
}

} // namespace GUI::Factors
