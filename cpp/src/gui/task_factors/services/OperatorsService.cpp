// OperatorsService — 见头文件. 本 TU include 三后端头 (Stream / Cpu; GPU 经 GpuRun.hpp),
// 依赖受控浮点: CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 op_check 同待遇.
#include "gui/task_factors/services/OperatorsService.hpp"

#include "factor/TS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/TS/Stream.hpp" // IWYU pragma: keep

#include "factor/CS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/CS/Stream.hpp" // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"
#include "factor/Stat/Check.hpp"
#include "misc/profiler.hpp"

#include <cassert>
#include <chrono>
#include <cstring>
#include <random>
#include <string>

namespace GUI::Factors {

// 一轮的常驻件 (worker 栈上, 轮末析构释放):
//   输入  按 (槽位 x/y/z, Gen) 懒造一次 (并行 fill, 主 rng 派生) + 上传显存一次, 全表复用; 最多 8 张 ≈ 960MB 宿主
//   输出  cpu 参考 / stream / gpu 三块, 开轮 resize 预触页, 跨算子只 ensure 不清 (契约: 后端写满每格)
//   GPU   常驻 Session (输出平面 / 工作区在里面), gpu 输出宿主缓冲 pin 住走 DMA
// 算子之间除一张输出的 D2H 与 compare 外不留 overhead, cpu_ms / stream_ms 里也不再混首次触页
struct RoundCtx {
  static constexpr int kSlots = 3, kGens = 4; // Gen: NORM / POS / SMALL / GROUP
  const OperatorsRequest &rq;
  std::mt19937 rng;
  size_t n;
  factor::check::Plane host[kSlots][kGens];
  factor::gpu::DevPlane *dev[kSlots][kGens] = {};
  factor::gpu::Session *gpu = nullptr;
  factor::check::Plane ref, got, g;

  RoundCtx(const OperatorsRequest &r, bool use_gpu) : rq(r), rng(r.seed), n(static_cast<size_t>(r.times) * r.A) {
    ref.resize(n), got.resize(n), g.resize(n);
    if (use_gpu) {
      gpu = factor::gpu::session_open(n);
      factor::gpu::pin(g.v.data(), n * sizeof(float));
      factor::gpu::pin(g.m.data(), n);
    }
  }
  ~RoundCtx() {
    if (gpu) {
      factor::gpu::unpin(g.v.data());
      factor::gpu::unpin(g.m.data());
      factor::gpu::session_close(gpu); // 上传过的输入平面随会话释放
    }
  }
  RoundCtx(const RoundCtx &) = delete;
  RoundCtx &operator=(const RoundCtx &) = delete;

  // 槽位 slot 的 Gen 平面: 首次用到才造 (+ 上传), 之后整轮复用. PLAIN profile (与 op_check 的 GUI 口径同)
  const factor::check::Plane &plane(int slot, factor::check::Gen gen) {
    assert(slot >= 0 && slot < kSlots);
    factor::check::Plane &p = host[slot][static_cast<int>(gen)];
    if (p.v.empty()) {
      factor::check::fill(p, gen, factor::check::Profile::PLAIN, rq.times, rq.A, rng);
      if (gpu)
        dev[slot][static_cast<int>(gen)] = factor::gpu::upload(gpu, p.v.data(), p.m.data());
    }
    return p;
  }
  const factor::gpu::DevPlane *dplane(int slot, factor::check::Gen gen) {
    plane(slot, gen);
    return dev[slot][static_cast<int>(gen)];
  }
};

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// 本轮实际参数: 参数列含 d 才吃默认窗长表 (否则与 op_check 同给 1); k/k2 按 Check.hpp kKParams
factor::Param param_of(const OperatorRow &row) {
  factor::Param p;
  p.d = std::strchr(row.params, 'd') != nullptr ? factor::check::default_d(row.e_name) : 1;
  factor::check::set_k(row.e_name, p);
  return p;
}

// 一个算子: 从 RoundCtx 取输入 (配方按算子, 元数不到的槽位空) → cpu / stream / gpu 各计时 → 对拍
template <class S, class C, int AR, factor::T W, bool IS_CS>
void run_op(RoundCtx &R, OperatorRow &row) {
  using namespace factor::check;
  const std::string nm = row.e_name;
  const Recipe rc = recipe_of(nm);
  const Gen gens[3] = {rc.x, rc.y, rc.z};
  const Plane *pl[3] = {};
  const factor::gpu::DevPlane *dp[3] = {};
  for (int s = 0; s < AR; ++s) {
    pl[s] = &R.plane(s, gens[s]);
    dp[s] = R.gpu ? R.dplane(s, gens[s]) : nullptr;
  }
  Data d;
  d.T = R.rq.times, d.A = R.rq.A;
  d.x = pl[0], d.y = pl[1], d.z = pl[2];

  const factor::Param &p = row.param; // 复位阶段已按请求填好 (见 param_of)

  Clock::time_point t0 = Clock::now();
  run_cpu<C>(d, p, AR, R.ref);
  row.cpu_ms = ms_since(t0);

  t0 = Clock::now();
  if constexpr (IS_CS)
    run_stream_cs<S>(d, p, AR, R.got);
  else
    run_stream_ts<S, AR, W>(d, p, R.got);
  row.stream_ms = ms_since(t0);
  row.stream = compare(R.ref, R.got, tol_of(nm, false, Profile::PLAIN));

  if (R.gpu) {
    double kms = -1; // 纯 kernel (cudaEvent); 输入已常驻, 这里只剩 kernel + 一张输出 D2H
    if constexpr (IS_CS)
      factor::gpu::run_cs(R.gpu, row.e_name, dp[0], dp[1], dp[2], R.g.v.data(), R.g.m.data(), d.T, d.A, p, &kms);
    else
      factor::gpu::run_ts(R.gpu, row.e_name, dp[0], dp[1], dp[2], R.g.v.data(), R.g.m.data(), d.T, d.A, p, &kms);
    row.gpu_ms = kms;
    row.gpu = compare(R.ref, R.g, tol_of(nm, true, Profile::PLAIN));
  } else {
    row.gpu_ms = -1;
    row.gpu = {};
  }
}

// Stat 评估算子: 造数 (PLAIN, 与 op_check 同口径) → cpu (全核) / gpu 各计时 → 一级 + 二级对拍.
// 表列: cpu_ms / gpu_ms = eval (每因子一次), stream 列 n/a, gpu Diff = 一级 + 二级合并; prep 与二级汇总进 extra
void run_stat(const OperatorsRequest &rq, OperatorRow &row, StatExtra &extra) {
  using namespace factor::stat::check;
  std::mt19937 rng(rq.seed);
  Data d;
  make(d, Profile::PLAIN, rq.times, rq.A, rng);
  Result cpu;
  run_cpu(d, cpu, cpu_threads());
  row.cpu_ms = cpu.eval_ms;
  row.stream_ms = -1;
  row.stream = {};
  extra.cpu_prep_ms = cpu.prep_ms;
  extra.n_hold = d.hd.n;
  for (int i = 0; i < d.hd.n; ++i)
    extra.hold[i] = cpu.stat[static_cast<size_t>(i)];
  if (!factor::gpu::available()) {
    row.gpu_ms = -1;
    row.gpu = {};
    extra.gpu_prep_ms = -1;
    return;
  }
  Result gpu;
  gpu.rows.assign(cpu.rows.size(), factor::stat::Row{});
  std::vector<factor::gpu::StatLabelHost> lab(static_cast<size_t>(d.hd.n));
  for (int i = 0; i < d.hd.n; ++i)
    lab[static_cast<size_t>(i)] = {d.lab[static_cast<size_t>(i)].lv.data(), d.lab[static_cast<size_t>(i)].sv.data(),
                                   d.lab[static_cast<size_t>(i)].m.data()};
  factor::gpu::run_stat(d.x.v.data(), d.x.m.data(), d.T, d.A, d.hd, lab.data(), gpu.rows.data(), &gpu.prep_ms, &gpu.eval_ms);
  summarize_all(gpu, d);
  const Tol tol = tol_of(Profile::PLAIN);
  row.gpu_ms = gpu.eval_ms;
  extra.gpu_prep_ms = gpu.prep_ms;
  const Diff dr = compare_rows(cpu.rows.data(), gpu.rows.data(), cpu.rows.size(), tol);
  const Diff ds = compare_stat(cpu.stat.data(), gpu.stat.data(), d.hd.n, tol);
  row.gpu = dr; // 两级合并成一个 Diff (表只有一格)
  row.gpu.mask_bad += ds.mask_bad;
  row.gpu.val_bad += ds.val_bad;
  row.gpu.compared += ds.compared;
  if (ds.worst > row.gpu.worst)
    row.gpu.worst = ds.worst;
  if (row.gpu.worst_at < 0)
    row.gpu.worst_at = ds.worst_at;
}

} // namespace

OperatorsService::OperatorsService() {
  // 行序 = 全局 idx (UI 默认序 / operators.json 的 idx 都按它): 首行 Stat, 之后 = OpTable 表序.
  // 首行: Stat 评估算子 (不在 OpTable; T / A / Kernel 空; 二元 = 因子 x + 每持有期一组标签 y_h)
  {
    OperatorRow s;
    s.e_name = "Stat";
    s.c_name = "评估";
    s.arity = 2;
    s.params = "";
    s.operand = R"tex(x, y_h \in \mathbb{R};\; h \in \{5,10,30\})tex";
    s.op = R"tex(\mathrm{rIC},\,\mathrm{IR},\,t,\,G_{20},\,\mathrm{LS},\,\mathrm{SR},\,\beta,\,\mathrm{mono},\,\mathrm{rAC})tex";
    s.note = "因子评估: 每 (h, t) 的 IC / 20 组均值 / 多空 / rank-AC, 再沿 t 汇总; 挖掘的第一道闸, 每轮先跑";
    s.classified = false;
    rows.push_back(s);
    runners_.push_back(nullptr); // 另走 run_stat
  }
  assert(stat_index() == 0);
  // 静态列直接抄 OpTable 三维分类列, 跑手按 A 域 token 粘贴选后端命名空间
  // (SELF → ts, ALL/GROUP → cs; 缺任一后端同名 struct → 此处编译错)
#define RUN_SELF(Name, ar, t) (&run_op<factor::ts::Name, factor::cpu::ts::Name, ar, factor::T::t, false>)
#define RUN_ALL(Name, ar, t) (&run_op<factor::cs::Name, factor::cpu::cs::Name, ar, factor::T::t, true>)
#define RUN_GROUP RUN_ALL
#define ROW(Name, c_name, ar, t, a, kern, prm, operand, op, note)                                              \
  rows.push_back({#Name, c_name, ar, factor::T::t, factor::A::a, factor::Kern::kern, prm, operand, op, note}); \
  runners_.push_back(RUN_##a(Name, ar, t));
  OP_ALL(ROW)
#undef ROW
#undef RUN_GROUP
#undef RUN_ALL
#undef RUN_SELF
  assert(rows.size() == runners_.size());
  // 未跑之前也显示每算子默认参数, 表一打开 Operands 列就有实际值
  for (OperatorRow &r : rows)
    r.param = param_of(r);
}

void OperatorsService::Request(const OperatorsRequest &req) {
  assert(req.times >= factor::kSegLen && req.times % factor::kSegLen == 0 && req.A >= 2);
  OperatorsRequest r = req;
  // 时间戳做种子: 不做复现, 同一轮内所有算子共享同一张量 (run_op 都从 r.seed 造数)
  r.seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = r;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&OperatorsService::worker_loop, this);
  }
}

void OperatorsService::AdoptSnapshot(const OperatorsRequest &req, std::vector<OperatorRow> &&snap, const StatExtra &extra,
                                     int failed) {
  assert(!thread_.joinable() && status_.load() == OperatorsStatus::Idle && "载入只在 worker 未起时做");
  assert(snap.size() == rows.size());
  {
    std::lock_guard<std::mutex> lock(mutex);
    rows = std::move(snap);
    stat = extra;
    current = req;
    from_json = true;
  }
  done_.store(total(), std::memory_order_relaxed);
  failed_.store(failed, std::memory_order_relaxed);
  status_.store(OperatorsStatus::Done, std::memory_order_release);
  epoch_.fetch_add(1, std::memory_order_relaxed);
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
    if (factor::gpu::available() && !gpu_warmed_) {
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
      from_json = false; // 这一轮是本进程算的
      for (OperatorRow &r : rows) {
        r.param = param_of(r);
        r.status = RowStatus::Pending;
        r.stream = {}, r.gpu = {};
        r.cpu_ms = 0, r.stream_ms = 0, r.gpu_ms = -1;
      }
      stat = StatExtra{};
    }
    done_.store(0, std::memory_order_relaxed);
    failed_.store(0, std::memory_order_relaxed);
    status_.store(OperatorsStatus::Running, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);

    // 执行序 = 行序: 首行 Stat 先跑 (挖掘的第一道闸, 先验它), 然后按 OpTable 表序逐算子.
    // 一轮的输入缓存 / 输出缓冲 / GPU 会话都在 R 里, 轮末 (含取消) 析构释放
    bool cancelled = false;
    RoundCtx R(req, factor::gpu::available());
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
      StatExtra extra;
      if (i == stat_index())
        run_stat(req, r, extra);
      else
        runners_[i](R, r);
      r.status = RowStatus::Done;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[i] = r;
        if (i == stat_index())
          stat = extra;
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
