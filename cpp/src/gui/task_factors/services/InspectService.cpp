// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/EvalCpu.hpp"
#include "factor/Exec.hpp"
#include "factor/Stat/Cpu.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <thread>
#include <vector>

namespace GUI::Factors {

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

} // namespace

// 计算 flow 的状态 (worker 私有, 跨请求存活; 分级见头文件)
struct InspectService::Flow {
  factor::exec::ForkJoin ex;
  // S 作用域
  std::string scope_key; // features_dir | 子轴 hash | 月份表; 空 = 未装载
  std::vector<std::string> dates;
  Loaded L; // planes / feat_codes = 作用域内读过的特征 (只增; 每次评估前把当前 Dag::feats 换到前排); labels (毛) / costs / cs 常驻
  // Y 毛标签 [H]: ry = prep_label 常驻, lab 指向 L.labels + ry
  std::vector<std::vector<uint16_t>> ry;
  std::vector<factor::cpu::stat::Label> lab;
  // X / R / C / L 常驻缓冲
  factor::cpu::Pool pool;
  std::vector<factor::cpu::ParScratch> nsc;
  factor::cpu::stat::Workspace w;
  std::vector<factor::stat::Row> rows; // 毛口径一级 [H][T], 发布时与 result.rows 交换
  std::vector<uint16_t> rx, rx_all;    // w.ws 的拷贝 [T][A] (池内 / 不看池), 发布时与 result.rx / rx_all 交换 (ws 自身下次评估还要用)
  std::vector<uint8_t> ones;           // 全 1 门控 [T][A], rx_all 那遍 rank_x 的 g

  Flow() : ex(static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))) {}
  void reset_scope() {
    scope_key.clear();
    dates.clear();
    L = Loaded{};
    ry.clear(), lab.clear();
  }
};

bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.file = row.file;
  req.expr = row.expr;
  req.frame = row.frame;
  req.reload = reload;
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
  Flow F;
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
      std::lock_guard<std::mutex> lock(mutex); // 旧 result 不清: 页面继续整页画旧因子, 新结果发布时一次性覆盖 (不闪)
      message.clear();
    }
    status_.store(InspectStatus::Loading, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
    const bool ok = evaluate(req, F);
    status_.store(ok ? InspectStatus::Done : InspectStatus::Cancelled, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
}

bool InspectService::evaluate(const InspectRequest &req, Flow &F) {
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
  const int nt = F.ex.threads();

  const auto publish_message = [&](std::string msg) {
    std::lock_guard<std::mutex> lock(mutex);
    message = std::move(msg);
  };
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };

  // ---- S: 作用域 (标签 / 成本 / 门控 + 特征缓存) ----
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty()) {
    publish_message("特征库在该区间无数据 (先算特征)");
    return true;
  }
  std::string ckey = req.scope.features_dir + "|" + std::to_string(req.scope.uni.hash) + "|cap" + std::to_string(req.max_impact);
  for (const std::string &m : req.scope.months)
    ckey += "|" + m;
  const int days = static_cast<int>(dates.size()), A = static_cast<int>(req.scope.uni.size());
  total_.store(days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  const auto stale_out = [&] {
    if (reader.stale()) { // 库换了, 缓存的一切不可信
      F.reset_scope();
      publish_message("特征库判废 (子轴/字段表与当前不符), 需重算特征");
    }
    return false;
  };
  if (req.reload || F.scope_key != ckey || F.dates != dates) {
    F.reset_scope();
    init_loaded(feats_, days, A, F.L);
    F.L.max_impact = static_cast<float>(req.max_impact);
    F.L.feat_codes = d.feats;
    if (!load_planes(feats_, reader, dates, F.L, F.ex, cancel_, done_, /*with_labels=*/true, /*with_costs=*/true, /*with_index=*/true))
      return stale_out();
    F.scope_key = ckey;
    F.dates = dates;
  } else {
    std::vector<std::string> missing;
    for (const std::string &c : d.feats)
      if (std::find(F.L.feat_codes.begin(), F.L.feat_codes.end(), c) == F.L.feat_codes.end())
        missing.push_back(c);
    if (missing.empty()) {
      done_.store(days, std::memory_order_relaxed);
    } else {
      Loaded tmp;
      init_loaded(feats_, days, A, tmp);
      tmp.feat_codes = missing;
      if (!load_planes(feats_, reader, dates, tmp, F.ex, cancel_, done_, /*with_labels=*/false))
        return stale_out();
      for (size_t i = 0; i < missing.size(); ++i) {
        F.L.feat_codes.push_back(missing[i]);
        F.L.planes.push_back(std::move(tmp.planes[i]));
      }
    }
  }
  Loaded &L = F.L;
  // 当前 Dag::feats 换到前排 (平面下标 = feats 下标, check_leaf_domains / plane_of 都按它), 其余缓存特征跟在后面
  for (size_t i = 0; i < d.feats.size(); ++i) {
    const auto it = std::find(L.feat_codes.begin() + static_cast<long>(i), L.feat_codes.end(), d.feats[i]);
    assert(it != L.feat_codes.end());
    const size_t j = static_cast<size_t>(it - L.feat_codes.begin());
    std::swap(L.feat_codes[i], L.feat_codes[j]);
    std::swap(L.planes[i], L.planes[j]);
  }
  const size_t n = L.n();
  const int H = L.hd.n, T = L.T;
  F.w.ensure(T, A, H, nt);

  // ---- Y: 毛标签 [H] (作用域内一次) ----
  if (F.lab.empty()) {
    F.ry.resize(static_cast<size_t>(H));
    F.lab.resize(static_cast<size_t>(H));
    for (int h = 0; h < H; ++h) {
      const LabelPlane &p = L.labels[static_cast<size_t>(h)];
      std::vector<uint16_t> &ry = F.ry[static_cast<size_t>(h)];
      ry.resize(n);
      factor::cpu::stat::prep_label(F.ex, p.lv.data(), p.m.data(), T, A, ry.data(), F.w);
      F.lab[static_cast<size_t>(h)] = {p.lv.data(), p.sv.data(), p.m.data(), ry.data()};
    }
    if (cancelled())
      return false;
  }

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = req.key();
  res.file = req.file;
  res.expr = req.expr;
  res.frame = req.frame;
  res.scope.universe = req.universe;
  res.scope.start_date = req.start_date;
  res.scope.end_date = req.end_date;
  res.scope.days = L.days, res.scope.T = T, res.scope.A = A;
  res.scope.backend = "cpu";
  res.dates = dates;
  res.read_scope = req.scope;
  res.uni_index = L.index;
  res.n_hold = H;
  res.hd = L.hd;
  const auto publish = [&] {
    res.scope.time = now_string();
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<factor::stat::Row> spare = std::move(result.rows); // 旧 Row / rx 缓冲回收给 Flow, 下次原地写
    std::vector<uint16_t> spare_rx = std::move(result.rx), spare_rx_all = std::move(result.rx_all);
    result = std::move(res);
    result.rows.swap(F.rows);
    result.rx.swap(F.rx);
    result.rx_all.swap(F.rx_all);
    F.rows = std::move(spare);
    F.rx = std::move(spare_rx);
    F.rx_all = std::move(spare_rx_all);
  };

  // ---- 数据检查 (与 FactorsService 同一套) ----
  for (int i = 0; i < N; ++i) {
    check_leaf_domains(d, i, L, res.error);
    if (!res.error.empty()) {
      F.rows.clear(); // 无一级 / 无 rx
      F.rx.clear();
      F.rx_all.clear();
      publish();
      return true;
    }
  }

  // ---- X: Dag 后序顺序走, 每节点全核 ----
  status_.store(InspectStatus::Running, std::memory_order_release);
  total_.store(d.n_ops + 2, std::memory_order_relaxed); // 节点 + 成本池化 + 评估
  done_.store(0, std::memory_order_relaxed);
  F.pool.prepare(d.n_slots, n);
  if (F.nsc.size() < static_cast<size_t>(nt))
    F.nsc.resize(static_cast<size_t>(nt));
  const auto plane_of = [&](int i) -> const factor::check::Plane * {
    const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
    return nd.op < 0 ? &L.planes[static_cast<size_t>(nd.feat)] : &F.pool.slots[static_cast<size_t>(nd.slot)];
  };
  for (int i = 0; i < N; ++i) {
    if (cancelled())
      return false;
    const factor::DagNode &nd = d.nodes[static_cast<size_t>(i)];
    if (nd.op < 0)
      continue;
    const factor::check::Plane *in[3] = {};
    for (int a = 0; a < factor::expr::kOps[nd.op].arity; ++a)
      in[a] = plane_of(nd.in[a]);
    const Clock::time_point t0 = Clock::now();
    factor::cpu::run_node(F.ex, nd, in, F.pool.slots[static_cast<size_t>(nd.slot)], T, A, F.nsc, L.cs.m.data());
    res.eval_ms += ms_since(t0);
    done_.fetch_add(1, std::memory_order_relaxed);
  }

  // ---- R: rank_x 一次 ----
  const factor::check::Plane *rp = plane_of(root); // Stat 只看池内 (g 在算子内部)
  res.valid_pct = valid_pct_of(rp->m.data(), L.cs.m.data(), n);
  const size_t nrow = static_cast<size_t>(H) * T;
  if (F.rows.size() != nrow)
    F.rows.resize(nrow);
  res.hold.resize(static_cast<size_t>(H));
  // 先多跑一遍不看池的秩 (g ≡ 1) → rx_all (只给 K 线显示池外段), 再跑评估用的池内秩 (ws 留给 eval_rows)
  if (F.ones.size() != n)
    F.ones.assign(n, uint8_t{1});
  factor::cpu::stat::rank_x(F.ex, rp->v.data(), rp->m.data(), F.ones.data(), req.frame, T, A, F.w);
  F.rx_all.assign(F.w.ws.begin(), F.w.ws.begin() + static_cast<std::ptrdiff_t>(n));
  factor::cpu::stat::rank_x(F.ex, rp->v.data(), rp->m.data(), L.cs.m.data(), req.frame, T, A, F.w);
  F.rx.assign(F.w.ws.begin(), F.w.ws.begin() + static_cast<std::ptrdiff_t>(n)); // ws 随结果发布 (右上标的轴 / K 线着色)
  if (cancelled())
    return false;

  // ---- C: 换手成本 (FactorsLoad cost_tiers, 与 Run 同一份; 契约【换手成本】) ----
  cost_tiers(F.ex, F.rx.data(), L, feats_, res.costs);
  done_.fetch_add(1, std::memory_order_relaxed);
  if (cancelled())
    return false;

  // ---- L: 评估 (只毛): 一级 Row[H][T] + 二级 HoldStat[H] ----
  factor::cpu::stat::eval_rows(F.ex, req.frame, T, A, L.hd, F.lab.data(), F.w, F.rows.data(), res.hold.data());
  done_.fetch_add(1, std::memory_order_relaxed);
  publish();
  return true;
}

} // namespace GUI::Factors
