// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/EvalCpu.hpp"
#include "factor/Stat/Cpu.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <ctime>

namespace GUI::Factors {

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

std::string now_string() {
  const std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", std::localtime(&t));
  return buf;
}

} // namespace

// 跨请求缓存 (worker 私有): 同作用域下标签 + 冲击成本 + 毛标签 rank 常驻, 特征平面只保留当前因子的
struct InspectService::Cache {
  std::string key; // features_dir | 子轴 hash | 月份表
  std::vector<std::string> dates;
  Loaded L;                              // planes / feat_codes = 当前因子的特征 (Dag::feats 序); labels / costs 常驻
  std::vector<std::vector<uint16_t>> ry; // [H] 毛做多标签逐行截面 rank (prep_label, 与因子无关)
};

bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, int impact_amt, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.file = row.file;
  req.expr = row.expr;
  req.frame = row.frame;
  req.reload = reload;
  req.impact_amt = impact_amt;
  req.sell_impact = impact_amt > 0 ? data.config.sell_impact : 0.0;
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

void InspectService::Request(const InspectRequest &req) {
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = req;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&InspectService::worker_loop, this);
  }
}

void InspectService::Stop() {
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

void InspectService::worker_loop() {
  TraceThread("InspectWorker");
  Cache cache;
  while (true) {
    InspectRequest req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = *pending_;
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed);
    }
    done_.store(0, std::memory_order_relaxed);
    total_.store(0, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(mutex); // 旧 result 不清: 页面继续整页画旧因子 / 旧口径, 新结果发布时一次性覆盖 (不闪)
      message.clear();
    }
    status_.store(InspectStatus::Loading, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
    const bool ok = evaluate(req, cache);
    status_.store(ok ? InspectStatus::Done : InspectStatus::Cancelled, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
}

bool InspectService::evaluate(const InspectRequest &req, Cache &cache) {
  TraceN("InspectEvaluate");
  factor::expr::Expr e;
  {
    std::string err;
    const bool ok = factor::expr::parse(req.expr, feats_.lookup(), e, err);
    assert(ok && "Factors 表的规范串必可再解析");
    (void)ok;
  }
  const factor::Dag d = factor::build(e);
  const int N = static_cast<int>(d.nodes.size()), root = d.root();

  const auto publish_message = [&](std::string msg) {
    std::lock_guard<std::mutex> lock(mutex);
    message = std::move(msg);
  };

  // ---- 装载 (缓存命中只补缺的特征) ----
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty()) {
    publish_message("特征库在该区间无数据 (先算特征)");
    return true;
  }
  std::string ckey = req.scope.features_dir + "|" + std::to_string(req.scope.uni.hash);
  for (const std::string &m : req.scope.months)
    ckey += "|" + m;
  const int days = static_cast<int>(dates.size()), A = static_cast<int>(req.scope.uni.size());
  const bool fresh = req.reload || cache.key != ckey || cache.dates != dates;
  total_.store(days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  const int threads = static_cast<int>(hw_threads());
  if (fresh) {
    cache = Cache{};
    init_loaded(feats_, days, A, cache.L);
    cache.L.feat_codes = d.feats;
    if (!load_planes(feats_, reader, dates, cache.L, cancel_, done_, /*with_labels=*/true, /*with_costs=*/true)) {
      cache = Cache{};
      if (reader.stale())
        publish_message("特征库判废 (子轴/字段表与当前不符), 需重算特征");
      return false;
    }
    cache.key = ckey;
    cache.dates = dates;
    const size_t H = static_cast<size_t>(cache.L.hd.n), n = cache.L.n();
    cache.ry.assign(H, std::vector<uint16_t>(n));
    for (size_t h = 0; h < H; ++h)
      factor::cpu::stat::prep_label(cache.L.labels[h].lv.data(), cache.L.labels[h].m.data(), cache.L.T, cache.L.A, cache.ry[h].data(), threads);
  } else {
    std::vector<std::string> missing;
    for (const std::string &c : d.feats)
      if (std::find(cache.L.feat_codes.begin(), cache.L.feat_codes.end(), c) == cache.L.feat_codes.end())
        missing.push_back(c);
    if (!missing.empty()) {
      Loaded tmp;
      init_loaded(feats_, days, A, tmp);
      tmp.feat_codes = missing;
      if (!load_planes(feats_, reader, dates, tmp, cancel_, done_, /*with_labels=*/false)) {
        if (reader.stale()) {
          cache = Cache{}; // 库换了, 缓存的标签也不可信
          publish_message("特征库判废 (子轴/字段表与当前不符), 需重算特征");
        }
        return false;
      }
      for (size_t i = 0; i < missing.size(); ++i) {
        cache.L.feat_codes.push_back(missing[i]);
        cache.L.planes.push_back(std::move(tmp.planes[i]));
      }
    } else {
      done_.store(days, std::memory_order_relaxed);
    }
  }
  // 按 Dag::feats 序重排 (平面下标 = feats 下标), 不再用的释放
  {
    std::vector<factor::check::Plane> np(d.feats.size());
    for (size_t i = 0; i < d.feats.size(); ++i) {
      const auto it = std::find(cache.L.feat_codes.begin(), cache.L.feat_codes.end(), d.feats[i]);
      assert(it != cache.L.feat_codes.end());
      np[i] = std::move(cache.L.planes[static_cast<size_t>(it - cache.L.feat_codes.begin())]);
    }
    cache.L.planes = std::move(np);
    cache.L.feat_codes = d.feats;
  }
  Loaded &L = cache.L;
  const size_t n = L.n(), H = static_cast<size_t>(L.hd.n);

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = req.key();
  res.file = req.file;
  res.expr = req.expr;
  res.frame = req.frame;
  res.scope.universe = req.universe;
  res.scope.start_date = req.start_date;
  res.scope.end_date = req.end_date;
  res.scope.days = L.days, res.scope.T = L.T, res.scope.A = L.A;
  res.scope.backend = "cpu";
  res.dates = dates;
  res.n_hold = L.n_hold;
  res.impact_amt = req.impact_amt;
  res.hd = L.hd;

  // ---- 数据检查 (与 FactorsService 同一套) ----
  for (int i = 0; i < N; ++i) {
    check_leaf_domains(d, i, L, res.error);
    if (!res.error.empty()) {
      res.scope.time = now_string();
      std::lock_guard<std::mutex> lock(mutex);
      result = std::move(res);
      return true;
    }
  }

  // ---- 评估: 一棵 Dag 后序顺序走, 每节点全核 ----
  status_.store(InspectStatus::Running, std::memory_order_release);
  total_.store(d.n_ops, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  // 标签口径: 毛 (缓存的平面 + rank) / 扣冲击 (即兴算净平面 + rank, 不缓存)
  std::vector<LabelPlane> net;
  std::vector<std::vector<uint16_t>> net_ry;
  const std::vector<LabelPlane> *labels = &L.labels;
  const std::vector<std::vector<uint16_t>> *ry = &cache.ry;
  if (req.impact_amt > 0) {
    const CostPlane *cost = nullptr;
    for (size_t c = 0; c < feats_.costs.size(); ++c)
      if (feats_.costs[c].amt == req.impact_amt)
        cost = &L.costs[c];
    assert(cost && "请求的冲击金额档不在字段表 (UI 只应给 FeatureTable::costs 里的档)");
    net_labels(L, *cost, static_cast<float>(req.sell_impact), net);
    net_ry.assign(H, std::vector<uint16_t>(n));
    for (size_t h = 0; h < H; ++h)
      factor::cpu::stat::prep_label(net[h].lv.data(), net[h].m.data(), L.T, L.A, net_ry[h].data(), threads);
    labels = &net, ry = &net_ry;
    if (cancel_.load(std::memory_order_relaxed))
      return false;
  }
  std::vector<factor::cpu::stat::Label> lab(H);
  for (size_t h = 0; h < H; ++h)
    lab[h] = {(*labels)[h].lv.data(), (*labels)[h].sv.data(), (*labels)[h].m.data(), (*ry)[h].data()};
  factor::cpu::Pool pool;
  pool.prepare(d.n_slots, n);
  std::vector<factor::cpu::ParScratch> sc(static_cast<size_t>(threads));
  const auto plane_of = [&](int i) -> const factor::check::Plane * {
    const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
    return nd.op < 0 ? &L.planes[static_cast<size_t>(nd.feat)] : &pool.slots[static_cast<size_t>(nd.slot)];
  };
  for (int i = 0; i < N; ++i) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
    if (nd.op < 0)
      continue;
    const factor::check::Plane *in[3] = {};
    for (int a = 0; a < factor::expr::kOps[nd.op].arity; ++a)
      in[a] = plane_of(nd.in[a]);
    const Clock::time_point t0 = Clock::now();
    factor::cpu::run_node_par(nd, in, pool.slots[static_cast<size_t>(nd.slot)], L.T, L.A, threads, sc, L.cs.m.data());
    res.eval_ms += ms_since(t0);
    done_.fetch_add(1, std::memory_order_relaxed);
  }
  if (cancel_.load(std::memory_order_relaxed))
    return false;

  // ---- Stat: 一级 Row[H][T] 留下, 二级 summarize ----
  const factor::check::Plane *rp = plane_of(root); // Stat 只看池内 (g 在算子内部)
  res.valid_pct = valid_pct_of(rp->m.data(), L.cs.m.data(), n);
  res.rows.resize(H * static_cast<size_t>(L.T));
  {
    std::vector<uint16_t> ws(n);
    factor::cpu::stat::eval(rp->v.data(), rp->m.data(), L.cs.m.data(), req.frame, L.T, L.A, L.hd, lab.data(), ws.data(), res.rows.data(),
                            threads);
  }
  for (int hi = 0; hi < L.n_hold; ++hi)
    res.hold[hi] = factor::stat::summarize(res.rows.data() + static_cast<size_t>(hi) * L.T, L.T, L.hd.h[hi]);
  res.scope.time = now_string();
  {
    std::lock_guard<std::mutex> lock(mutex);
    result = std::move(res);
  }
  return true;
}

} // namespace GUI::Factors
