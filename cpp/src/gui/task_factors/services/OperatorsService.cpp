// OperatorsService — 见头文件. 本 TU include 三后端头 (Stream / Naive; GPU 经 GpuRun.hpp),
// 依赖受控浮点: CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 op_check 同待遇.
#include "gui/task_factors/services/OperatorsService.hpp"

#include "factor/TS/Naive.hpp"  // IWYU pragma: keep
#include "factor/TS/Stream.hpp" // IWYU pragma: keep

#include "factor/CS/Naive.hpp"  // IWYU pragma: keep
#include "factor/CS/Stream.hpp" // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"
#include "misc/profiler.hpp"

#include <cassert>
#include <chrono>
#include <cstring>
#include <random>
#include <string>

namespace GUI::Factors {

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// 本轮实际参数: 参数列含 d 才吃请求的 d (否则与 op_check 同给 1); k/k2 按 Check.hpp kKParams
factor::Param param_of(const OperatorRow &row, const OperatorsRequest &rq) {
  factor::Param p;
  p.d = std::strchr(row.params, 'd') != nullptr ? rq.d : 1;
  factor::check::set_k(row.name, p);
  return p;
}

// 一个算子: 造数 (PLAIN, 配方按算子, 与 op_check 同 seed 同张量) → naive / stream / gpu 各计时 → 对拍
template <class S, class N, int AR, factor::Win W, bool IS_CS>
void run_op(const OperatorsRequest &rq, OperatorRow &row) {
  using namespace factor::check;
  const std::string nm = row.name;
  const Recipe rc = recipe_of(nm);
  std::mt19937 rng(rq.seed);
  Data d;
  d.T = rq.T(), d.A = rq.A;
  fill(d.x, rc.x, Profile::PLAIN, d.T, d.A, rng);
  fill(d.y, rc.y, Profile::PLAIN, d.T, d.A, rng);
  fill(d.z, rc.z, Profile::PLAIN, d.T, d.A, rng);

  const factor::Param &p = row.param; // 复位阶段已按请求填好 (见 param_of)

  Plane ref, got;
  Clock::time_point t0 = Clock::now();
  run_naive<N>(d, p, AR, ref);
  row.naive_ms = ms_since(t0);

  t0 = Clock::now();
  if constexpr (IS_CS)
    run_stream_cs<S>(d, p, AR, got);
  else
    run_stream_ts<S, AR, W>(d, p, got);
  row.stream_ms = ms_since(t0);
  row.stream = compare(ref, got, tol_of(nm, false, Profile::PLAIN));

  if (factor::gpu::available()) {
    Plane g;
    g.resize(static_cast<size_t>(d.T) * d.A);
    auto call = IS_CS ? factor::gpu::run_cs : factor::gpu::run_ts;
    t0 = Clock::now();
    call(row.name, pv(d.x, AR >= 1), pm(d.x, AR >= 1), pv(d.y, AR >= 2), pm(d.y, AR >= 2),
         pv(d.z, AR >= 3), pm(d.z, AR >= 3), g.v.data(), g.m.data(), d.T, d.A, p);
    row.gpu_ms = ms_since(t0);
    row.gpu = compare(ref, g, tol_of(nm, true, Profile::PLAIN));
  } else {
    row.gpu_ms = -1;
    row.gpu = {};
  }
}

} // namespace

OperatorsService::OperatorsService() {
  // 表序即行序; 静态列直接抄 OpTable, 跑手按同一行实例化 (缺任一后端同名 struct → 此处编译错)
#define ROW_TS(Name, ar, win, prm, gpu, tex, note)                                          \
  rows.push_back({#Name, false, ar, factor::Win::win, factor::Strat::gpu, prm, tex, note}); \
  runners_.push_back(&run_op<factor::ts::Name, factor::naive::ts::Name, ar, factor::Win::win, false>);
#define ROW_CS(Name, ar, prm, gpu, tex, note)                                                \
  rows.push_back({#Name, true, ar, factor::Win::POINT, factor::Strat::gpu, prm, tex, note}); \
  runners_.push_back(&run_op<factor::cs::Name, factor::naive::cs::Name, ar, factor::Win::POINT, true>);
  OP_TS(ROW_TS)
  OP_CS(ROW_CS)
#undef ROW_TS
#undef ROW_CS
  assert(rows.size() == runners_.size());
  // 未跑之前也显示默认请求下的参数 (d 默认值), 表一打开就有 Args 列
  for (OperatorRow &r : rows)
    r.param = param_of(r, OperatorsRequest{});
}

void OperatorsService::Request(const OperatorsRequest &req) {
  assert(req.days >= 1 && req.A >= 1 && req.d >= 1);
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = req;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&OperatorsService::worker_loop, this);
  }
}

void OperatorsService::Stop() {
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

void OperatorsService::worker_loop() {
  TraceThread("OperatorsWorker");
  while (true) {
    OperatorsRequest req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = *pending_;
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed); // 与消费同临界区, 免竞争
    }

    // GPU 探测 + 首次热身 (CUDA 上下文初始化几百 ms, 不能算进首个算子的 gpu_ms)
    gpu_.store(factor::gpu::available(), std::memory_order_relaxed);
    if (gpu_.load(std::memory_order_relaxed) && !gpu_warmed_) {
      factor::check::Plane x, o;
      x.resize(factor::kSegLen), o.resize(factor::kSegLen);
      factor::Param p;
      factor::gpu::run_ts("TsAbs", x.v.data(), x.m.data(), nullptr, nullptr, nullptr, nullptr, o.v.data(),
                          o.m.data(), factor::kSegLen, 1, p);
      gpu_warmed_ = true;
    }

    // 全行复位 → Running
    {
      std::lock_guard<std::mutex> lock(mutex);
      current = req;
      for (OperatorRow &r : rows) {
        r.param = param_of(r, req);
        r.status = RowStatus::Pending;
        r.stream = {}, r.gpu = {};
        r.naive_ms = 0, r.stream_ms = 0, r.gpu_ms = -1;
      }
    }
    done_.store(0, std::memory_order_relaxed);
    failed_.store(0, std::memory_order_relaxed);
    status_.store(OperatorsStatus::Running, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);

    bool cancelled = false;
    for (size_t i = 0; i < rows.size(); ++i) {
      if (cancel_.load(std::memory_order_relaxed)) {
        cancelled = true;
        break;
      }
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[i].status = RowStatus::Running;
      }
      epoch_.fetch_add(1, std::memory_order_relaxed);

      OperatorRow r = rows[i]; // 静态列 + Running; 只有本线程写 rows, 无锁读安全
      runners_[i](req, r);
      r.status = RowStatus::Done;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[i] = r;
      }
      done_.fetch_add(1, std::memory_order_relaxed);
      if (!r.stream.ok() || (r.gpu_ms >= 0 && !r.gpu.ok()))
        failed_.fetch_add(1, std::memory_order_relaxed);
      epoch_.fetch_add(1, std::memory_order_relaxed);
    }
    status_.store(cancelled ? OperatorsStatus::Cancelled : OperatorsStatus::Done, std::memory_order_release);
  }
}

} // namespace GUI::Factors
