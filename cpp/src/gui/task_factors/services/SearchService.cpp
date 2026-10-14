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
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace GUI::Factors {

namespace {

namespace grid = factor::search::grid;
namespace fs = factor::stat;
using factor::search::Result;
using factor::search::Score;
using Clock = std::chrono::steady_clock;

constexpr int kCarryDays = 5;      // 块首多带的交易日 (ROLL 预热 + rAC lag): 行数 = kMaxD = TS 归一窗 ≥ max_lag
constexpr int kCpuForest = 64;     // CPU 一张共享 DAG 的候选数 (只为公共前缀, 根即算即放)
constexpr int kGpuMinBatch = 8;    // 定块大小时按至少这么多候选同在显存算 (核内 8 候选共用标签行)
constexpr double kHostFrac = 0.6;  // 宿主可用内存的使用比例
constexpr double kVramFrac = 0.85; // 显存空闲量的使用比例
static_assert(kCarryDays * factor::kSegLen == factor::expr::kMaxD, "carry 行数须等于窗长上限 kMaxD");

__attribute__((format(printf, 1, 2))) std::string strf(const char *f, ...) {
  char buf[4096];
  va_list ap;
  va_start(ap, f);
  const int n = std::vsnprintf(buf, sizeof(buf), f, ap);
  va_end(ap);
  assert(n >= 0 && n < static_cast<int>(sizeof(buf)) && "格式化超长");
  (void)n;
  return buf;
}

// 过程日志 <factor_dir>/log/<因子名>.log: 每次 Optimize 覆盖; 逐行即刷 (断言崩了也留得下); 行首 = 距开跑秒数.
// 只记流程中间量 (分块 / 尺度 / 网格 / 分支演化 / 高原 / 相关), 终选结果看因子文件 weights
struct RunLog {
  std::ofstream f;
  Clock::time_point t0;
  __attribute__((format(printf, 2, 3))) void operator()(const char *fmt, ...) {
    char buf[4096];
    va_list ap;
    va_start(ap, fmt);
    const int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    assert(n >= 0 && n < static_cast<int>(sizeof(buf)) && "日志行超长");
    (void)n;
    f << strf("[%8.1fs] ", std::chrono::duration<double>(Clock::now() - t0).count()) << buf << '\n';
    f.flush();
  }
};

// 删 log/ 里因子文件已不存在的 log (删 / 改名后的残留)
void prune_logs(const std::filesystem::path &factor_dir, const std::filesystem::path &log_dir) {
  for (const auto &ent : std::filesystem::directory_iterator(log_dir))
    if (ent.is_regular_file() && ent.path().extension() == ".log" && !std::filesystem::exists(factor_dir / (ent.path().stem().string() + ".json")))
      std::filesystem::remove(ent.path());
}

// 槽在坐标 u 处的参数值 (整数槽按整数写)
std::string slot_val(const grid::Slot &s, double u) {
  const double v = grid::to_v(s, u);
  return s.integer ? strf("%d", static_cast<int>(v)) : strf("%.4g", v);
}
// 坐标 u[P] → "Op.d=16, Op.k=0.0042"
std::string params_of(const std::vector<grid::Slot> &slots, const std::vector<double> &u) {
  std::string out;
  for (size_t j = 0; j < slots.size(); ++j)
    out += (j ? ", " : "") + slots[j].name + "=" + slot_val(slots[j], u[j]);
  return out;
}
// 坐标变换名 (Grid.hpp to_u)
const char *coord_name(const grid::Slot &s) {
  if (s.key == grid::Key::D)
    return "log2";
  switch (s.kdom) {
  case factor::KDom::OPEN01:
    return "logit";
  case factor::KDom::GE0:
  case factor::KDom::ANY:
    return "/σ";
  case factor::KDom::TOD:
    return "/32分";
  default:
    return "log2";
  }
}

// 一块的装载数据 (+ CPU 的预处理标签); 整轮缓存或单块复用
struct ChunkData {
  int idx = -1; // 块号; -1 = 空
  int t_lo = 0, t0_abs = 0;
  Loaded L;
  LabelsHost lab; // CPU; 装载后清空, 首次评估时算 (指 L 的标签, 随块同生)
  factor::stat::Span span() const { return {L.T, L.A, t0_abs, t_lo}; }
};

// 一个候选 (评过 / 待评); 分数每 h 一份
struct Entry {
  std::vector<double> u;
  std::vector<int> offs; // 粗格点偏移 (粗阶段的点才有)
  factor::expr::Expr e;
  Result r;
  Score sc[fs::kMaxHold];
  int pass = -1; // 评过的趟; -1 = 待评
};

// 分支 (粗网格后, 每 h 各自一组)
struct Branch {
  std::vector<double> center;
  int best_id = -1; // 该支当前最优候选
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
  factor::cpu::stat::Workspace w;
  std::vector<uint16_t> rx; // 候选的 r16 [T][A] (块形, 候选间复用)
  Flow() : ex(static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))) {}
};

bool MakeSearchRequest(const SharedData &data, const FactorRow &row, bool gpu, SearchRequest &req) {
  assert(!row.tmpl.empty() && "Optimize 只接模板解析过的因子");
  req = SearchRequest{};
  req.factor_dir = data.config.factor_dir + "/" + data.config.universe;
  req.file = row.file;
  req.tmpl = row.tmpl;
  req.frame = row.frame;
  req.gpu = gpu;
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
    stage_.store(SearchStage::Scale, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(mutex);
      result = SearchResult{};
      result.file = req.file;
      result.tmpl = req.tmpl;
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
  const Clock::time_point t_start = Clock::now();
  const int nt = F.ex.threads();
  const int A = static_cast<int>(req.scope.uni.size());
  // 搜索窗内的持有期 (字段表升序), 全部同评 (契约: 不跨 h 比, 各自搜)
  std::vector<int> holds;
  for (const LabelCol &lc : feats_.labels)
    if (factor::search::hold_in_window(lc.hold))
      holds.push_back(lc.hold);
  SearchResult res;
  res.file = req.file;
  res.tmpl = req.tmpl;
  res.backend = req.gpu ? "gpu" : "cpu";
  RunLog lg;
  lg.t0 = t_start;
  {
    const std::filesystem::path log_dir = std::filesystem::path(req.factor_dir) / "log";
    std::filesystem::create_directories(log_dir);
    prune_logs(req.factor_dir, log_dir);
    lg.f.open(log_dir / (std::filesystem::path(req.file).stem().string() + ".log"), std::ios::trunc);
    assert(lg.f.is_open() && "Optimize log 打不开");
    const std::time_t now = std::time(nullptr);
    char ts[32];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    lg("Optimize %s  @ %s", req.file.c_str(), ts);
    lg("模板: %s", req.tmpl.c_str());
    lg("后端 %s, 股票池 %s, 区间 %s ~ %s", res.backend.c_str(), req.universe.c_str(), req.start_date.c_str(), req.end_date.c_str());
  }
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };
  const auto abort_with = [&](std::string msg) {
    lg("中止: %s", msg.c_str());
    res.message = std::move(msg);
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    std::lock_guard<std::mutex> lock(mutex);
    result = res;
    return true;
  };
  if (holds.empty())
    return abort_with("字段表里没有搜索窗 (30m ~ T+1) 内的持有期标签");
  const int H = static_cast<int>(holds.size());
  fs::Holds hd;
  hd.n = H;
  for (int hi = 0; hi < H; ++hi) {
    hd.h[hi] = holds[static_cast<size_t>(hi)];
    SearchHold sh;
    sh.hold = hd.h[hi];
    res.holds.push_back(std::move(sh));
  }
  fs::assert_holds(hd);
  assert(factor::search::max_lag(hd) <= kCarryDays * factor::kSegLen && "carry 不够 rAC 的 lag");
  {
    std::string hs;
    for (int hi = 0; hi < H; ++hi)
      hs += (hi ? " " : "") + factor::stat::hold_name(hd.h[hi]);
    lg("持有期 %d 个: %s", H, hs.c_str());
  }
  const auto hold_of = [&](int hi) { return factor::stat::hold_name(hd.h[hi]); };
  const factor::search::Objective obj; // ric_pow 取契约默认

  // ---- 模板 + 可搜槽 ----
  factor::expr::Expr e0;
  {
    std::string err;
    const bool ok = factor::expr::parse(req.tmpl, feats_.lookup(), e0, err);
    assert(ok && "Factors 表的模板规范串必可再解析");
    (void)ok;
  }
  std::vector<grid::Slot> slots = grid::slots_of(e0);
  for (const grid::Slot &s : slots)
    res.slots.push_back(s.name);
  if (slots.empty())
    return abort_with("模板无自由参数 (裸键名 d / k / k2 才搜; 写了数字的参数固定)");
  lg("自由参数 %d 个:", static_cast<int>(slots.size()));
  for (const grid::Slot &s : slots)
    lg("  %-24s 坐标 %-5s 范围 [%s, %s] 中心 u0=%.3g%s", s.name.c_str(), coord_name(s), s.lo_bounded ? strf("%.3g", s.u_lo).c_str() : "-inf",
       s.hi_bounded ? strf("%.3g", s.u_hi).c_str() : "+inf", s.u0, s.integer ? " 整数" : "");
  const factor::expr::Expr ep = e0.placeholder(); // 结构 / 特征叶序 / 槽数与任一具体化相同 (尺度也在它上算)
  const factor::Dag d0 = factor::build(ep);
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
    for (int hi = 0; hi < H; ++hi) {
      std::vector<SearchCand> &cands = res.holds[static_cast<size_t>(hi)].cands;
      cands.clear();
      for (const Entry &en : ents) {
        if (en.pass < 0)
          continue;
        const Score &sc = en.sc[hi];
        SearchCand c;
        c.expr = en.e.canon;
        c.u = en.u;
        c.pass = en.pass;
        c.ok = sc.ok;
        c.score = sc.score;
        c.ls = sc.ls;
        c.ric = sc.ric;
        cands.push_back(std::move(c));
      }
      std::stable_sort(cands.begin(), cands.end(), [](const SearchCand &a, const SearchCand &b) { return a.score > b.score; });
    }
    std::lock_guard<std::mutex> lock(mutex);
    result = res;
  };
  // ---- 作用域 / 日期 ----
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty())
    return abort_with("特征库在该区间无数据 (先算特征)");
  const int days = static_cast<int>(dates.size());

  // ---- 分块与预算 (字节 / 格; 见头文件【分块流式】) ----
  //   L0  宿主 Loaded: 特征 (v+m) 5·nf + 截面门控 5 + 标签 (lv,sv,m) 5·H
  //   Wc  CPU 工作区: ry 2·H + ws 2 + 池 (s0+2)·5
  //   F0  设备常驻: 特征 5·nf + 门控 1 + 搜索会话标签 (lv,sv,m,ry) 7·H; 每候选 7 (根平面 5 + rx 2); 中间量 2·s0 槽
  const double per_day = static_cast<double>(factor::kSegLen) * A;
  const double L0 = 5.0 * nf + 5.0 + 5.0 * H;
  const double Wc = 2.0 * H + 2.0 + 5.0 * (s0 + 2);
  const double F0 = 5.0 * nf + 1.0 + 7.0 * H;
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
  res.n_assets = A;
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - kCarryDays);
    res.chunk_plan.push_back(SearchChunk{day1 - load0, (day1 - load0) * per_day * (L0 + (req.gpu ? 0.0 : 2.0 * H)) / (1024.0 * 1024.0)});
  }
  chunks_.store(n_chunks, std::memory_order_relaxed);
  publish(); // 装载前先发布分块计划
  constexpr double kMiB = 1024.0 * 1024.0;
  lg("数据: %d 只 × %d 天, 特征 %d 列, 标签 %d 列, 共享 DAG 槽 %d; 每格 %.0f 字节 (宿主装载) + %.0f (CPU 工作区)", A, days, nf, H, s0, L0, req.gpu ? 0.0 : Wc);
  lg("预算: 内存 %.0f MB%s; 块 ≤ %d 天 + 前带 %d 天, 共 %d 块, 全部常驻 %s (需 %.0f MB)%s", host_budget / kMiB,
     req.gpu ? strf(", 显存 %.0f MB", vram_budget / kMiB).c_str() : "", D, kCarryDays, n_chunks, cache_all ? "是" : "否 (每趟逐块重读)", cache_bytes / kMiB,
     req.gpu ? strf(", GPU 批 %d", batch).c_str() : "");
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - kCarryDays);
    lg("  块 %d: 统计 %s ~ %s (%d 天), 装载 %d 天, %.0f MB", ci + 1, dates[static_cast<size_t>(day0)].c_str(), dates[static_cast<size_t>(day1 - 1)].c_str(), day1 - day0,
       day1 - load0, res.chunk_plan[static_cast<size_t>(ci)].mb);
  }

  std::vector<ChunkData> chunks(cache_all ? static_cast<size_t>(n_chunks) : 1);
  bool domain_checked = false;
  // 装一块: 交易日 [day0, day1), 前带 carry; 首块顺带查特征叶值域
  const auto load_chunk = [&](int ci, ChunkData &cd) -> Rc {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - kCarryDays);
    const std::vector<std::string> cdates(dates.begin() + load0, dates.begin() + day1);
    const Clock::time_point t_load = Clock::now();
    cd.idx = -1;
    cd.lab.ry.clear();
    cd.L = Loaded{};
    init_loaded(feats_, day1 - load0, A, cd.L);
    select_holds(feats_, holds, cd.L);
    cd.L.feat_codes = d0.feats;
    status_.store(SearchStatus::Loading, std::memory_order_release);
    total_.store(day1 - load0, std::memory_order_relaxed);
    done_.store(0, std::memory_order_relaxed);
    if (!load_planes(feats_, reader, cdates, cd.L, F.ex, cancel_, done_, /*with_labels=*/true, /*with_costs=*/false, /*with_index=*/false)) {
      if (reader.stale()) {
        res.message = "特征库判废 (子轴/字段表与当前不符), 需重算特征";
        lg("中止: %s", res.message.c_str());
        publish();
        return Rc::Abort;
      }
      return Rc::Cancel;
    }
    lg("装载块 %d/%d: %d 天, %.1fs", ci + 1, n_chunks, day1 - load0, std::chrono::duration<double>(Clock::now() - t_load).count());
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
          lg("中止: %s", err.c_str());
          publish();
          return Rc::Abort;
        }
      }
    }
    return Rc::Ok;
  };

  // ---- 截面相关的抽样行 (corr 趟; 契约【多权重】): 绝对行 t_abs ≡ 0 (mod stride) 且 ≥ t_lo 的 r16 行回宿主, 块评完两两攒 ----
  struct RankSink {
    std::vector<int> ids;                    // 参与的候选 (下标 m)
    std::vector<std::pair<int, int>> pairs;  // 要算的 (m_a, m_b), m_a < m_b (同 h 高原集内)
    int t_first = 0, n_rows = 0;             // 本块抽样行: t_first + i·stride
    std::vector<std::vector<uint16_t>> rows; // [m][n_rows × A]
    std::vector<double> sum;                 // [pair] Σ 逐行 Pearson
    std::vector<long long> cnt;              // [pair] 有效行数
    int pos(int id) const {
      const auto it = std::find(ids.begin(), ids.end(), id);
      assert(it != ids.end());
      return static_cast<int>(it - ids.begin());
    }
  };
  const int stride = std::max(1, days * factor::kSegLen / factor::search::kCorrRows);
  RankSink *sink = nullptr; // 非空 = 本趟抽样
  const auto sink_begin = [&](ChunkData &cd) {
    if (!sink)
      return;
    int t = cd.t_lo;
    while ((cd.t0_abs + t) % stride != 0)
      ++t;
    sink->t_first = t;
    sink->n_rows = t < cd.L.T ? (cd.L.T - 1 - t) / stride + 1 : 0;
    sink->rows.assign(sink->ids.size(), std::vector<uint16_t>(static_cast<size_t>(sink->n_rows) * A));
  };
  const auto sink_rows_cpu = [&](int id, const uint16_t *rx) { // rx = 该候选的 r16 [T][A]
    if (!sink)
      return;
    uint16_t *dst = sink->rows[static_cast<size_t>(sink->pos(id))].data();
    for (int i = 0; i < sink->n_rows; ++i)
      std::copy_n(rx + static_cast<size_t>(sink->t_first + i * stride) * A, A, dst + static_cast<size_t>(i) * A);
  };
  const auto sink_end = [&] { // 本块的抽样行两两 Pearson (Stat/Host lag_row, lag 行换成另一候选) 攒进 sum / cnt
    if (!sink || sink->n_rows == 0)
      return;
    const int P = static_cast<int>(sink->pairs.size());
    F.ex.rows(P, [&](int p0, int p1, int) {
      for (int p = p0; p < p1; ++p) {
        const uint16_t *ra = sink->rows[static_cast<size_t>(sink->pairs[static_cast<size_t>(p)].first)].data();
        const uint16_t *rb = sink->rows[static_cast<size_t>(sink->pairs[static_cast<size_t>(p)].second)].data();
        for (int i = 0; i < sink->n_rows; ++i) {
          fs::Row row;
          factor::cpu::stat::lag_row(ra + static_cast<size_t>(i) * A, rb + static_cast<size_t>(i) * A, A, row);
          if (row.ok_rac)
            sink->sum[static_cast<size_t>(p)] += row.rac, sink->cnt[static_cast<size_t>(p)]++;
        }
      }
    });
    sink->rows.clear();
  };

  // ---- 一块上评一批候选 (CPU): 共享 DAG 顺序走, 根算完立刻 Search ----
  const auto cpu_chunk = [&](ChunkData &cd, const std::vector<int> &ids) -> bool {
    Loaded &L = cd.L;
    const int T = L.T;
    const size_t n = L.n();
    F.w.ensure(A, H, 0, nt);
    F.rx.resize(n);
    if (cd.lab.ry.empty())
      prep_labels(F.ex, L, F.w, cd.lab);
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
        fs::Batch B;
        B.C = 1, B.x[0] = {rp->v.data(), rp->m.data()};
        for (size_t k = b0; k < b1; ++k) {
          if (Fd.roots[k - b0] != i)
            continue;
          Result out;
          factor::cpu::search::run(F.ex, B, L.cs.m.data(), req.frame, cd.span(), cd.lab.L, F.rx.data(), F.w, &out);
          ents[static_cast<size_t>(ids[k])].r.merge(out, H);
          sink_rows_cpu(ids[k], F.rx.data());
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
    for (int hi = 0; hi < H; ++hi)
      lab[hi] = label_of(L.labels[static_cast<size_t>(hi)], nullptr);
    factor::gpu::SearchSession *ss = factor::gpu::search_open(T, A, batch, hd, n_full_of(L));
    factor::gpu::search_load(ss, lab);
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
      for (int k = 0; k < C; ++k) {
        const int id = ids[b0 + static_cast<size_t>(k)];
        ents[static_cast<size_t>(id)].r.merge(out[static_cast<size_t>(k)], H);
        if (sink && sink->n_rows > 0)
          factor::gpu::search_ranks(ss, k, sink->t_first, stride, sink->n_rows, sink->rows[static_cast<size_t>(sink->pos(id))].data());
      }
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
    const Clock::time_point t_pass = Clock::now();
    double load_s = 0.0;
    for (int ci = 0; ci < n_chunks; ++ci) {
      ChunkData &cd = chunks[cache_all ? static_cast<size_t>(ci) : 0];
      if (cd.idx != ci) {
        const Clock::time_point t_load = Clock::now();
        const Rc rc = load_chunk(ci, cd);
        if (rc != Rc::Ok)
          return rc;
        load_s += std::chrono::duration<double>(Clock::now() - t_load).count();
      }
      status_.store(SearchStatus::Running, std::memory_order_release);
      total_.store(static_cast<int>(ids.size()) * n_chunks, std::memory_order_relaxed);
      done_.store(static_cast<int>(ids.size()) * ci, std::memory_order_relaxed);
      sink_begin(cd);
      if (!(req.gpu ? gpu_chunk(cd, ids) : cpu_chunk(cd, ids)))
        return Rc::Cancel;
      sink_end();
      chunk_.store(ci + 1, std::memory_order_relaxed);
    }
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      for (int hi = 0; hi < H; ++hi)
        en.sc[hi] = factor::search::score_of(en.r, hd, hi, obj);
      if (en.pass < 0) // corr 趟重评不改首评趟号
        en.pass = pass;
    }
    std::string per;
    for (int hi = 0; hi < H; ++hi) {
      int n_pos = 0;
      double best = -std::numeric_limits<double>::infinity();
      for (int id : ids) {
        const double sc = ents[static_cast<size_t>(id)].sc[hi].score;
        n_pos += std::isfinite(sc);
        best = std::max(best, sc);
      }
      per += strf("%s%s 正分 %d 最高 %.4g", hi ? "; " : "", hold_of(hi).c_str(), n_pos, best);
    }
    const double pass_s = std::chrono::duration<double>(Clock::now() - t_pass).count();
    lg("第 %d 趟完成: %d 候选 × %d 块, %.1fs (装载 %.1fs, 评估 %.1fs, %.1f ms/候选·块) | %s", pass, static_cast<int>(ids.size()), n_chunks, pass_s, load_s,
       pass_s - load_s, 1e3 * (pass_s - load_s) / (static_cast<double>(ids.size()) * n_chunks), per.c_str());
    res.n_pass = pass + 1;
    publish();
    epoch_.fetch_add(1, std::memory_order_relaxed);
    return Rc::Ok;
  };
  const auto handle = [&](Rc rc, bool &ret) {
    if (rc == Rc::Cancel)
      lg("已取消");
    ret = rc != Rc::Cancel;
    return rc != Rc::Ok;
  };
  bool ret = false;

  // ---- 尺度: 先装块 0 (趟 0 反正要), 阈值槽 (ANY / GE0) 的坐标单位 = 占位 DAG 里该节点输入的池内有效格标准差 (CPU 走到该节点即止;
  //      上游自由参数取占位值, σ 只是坐标刻度, 粗网格 ±kSpan 单位 + 细化兜得住) ----
  if (handle(load_chunk(0, chunks[0]), ret))
    return ret;
  // ---- 满档只数 n_full (显示用): 块 0 池宽. 评估的 cap 逐块取该块的 n_full_of (CPU labels_raw / GPU search_open 同一份) ----
  res.n_full = n_full_of(chunks[0].L);
  lg("满档一腿 %.1f 只 (块 1 池内平均有效票数 / %d 组)", res.n_full, fs::kGroups);
  {
    std::vector<size_t> need; // 要尺度的槽
    std::vector<int> want;    // 对应输入节点在 d0 的下标
    for (size_t j = 0; j < slots.size(); ++j) {
      if (!grid::needs_scale(slots[j]))
        continue;
      const factor::expr::Node &nd = ep.nodes[static_cast<size_t>(slots[j].node)];
      assert(factor::expr::kOps[nd.op].arity >= 1);
      const std::string key = factor::expr::to_string(ep, nd.args[0]);
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
        if (cancelled()) {
          lg("已取消");
          return false;
        }
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
      for (size_t j = 0, q = 0; j < slots.size(); ++j) {
        if (!grid::needs_scale(slots[j]))
          continue;
        const double sd = sigma[q++];
        if (!(std::isfinite(sd) && sd > 0.0)) // 输入常量 → 阈值无意义; 自由参数没法落值, 整个搜索放弃
          return abort_with("阈值槽 " + slots[j].name + " 的输入在块 0 上是常量 (σ = 0), 搜不动");
        grid::set_scale(slots[j], sd);
        lg("尺度: %s σ = %.4g (输入 %s, 上游自由参数取占位值)", slots[j].name.c_str(), sd, d0.keys[static_cast<size_t>(want[q - 1])].c_str());
      }
    }
  }

  // ---- 趟 0: 粗网格 (围中心铺; 步长由预算反推), 一趟得全部 h 的分 ----
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
  lg("粗网格: 步长 %.3g 单位 (每趟 ≤ %d 点), 笛卡尔积 %d 点, 合法且去重 %d 候选", coarse.step, grid::kLayerMax, static_cast<int>(lattice.size()),
     static_cast<int>(ids.size()));
  for (size_t j = 0; j < slots.size(); ++j) {
    int lo = 0, hi = 0;
    for (const std::vector<int> &o : lattice)
      lo = std::min(lo, o[j]), hi = std::max(hi, o[j]);
    const grid::Slot &s = slots[j];
    std::string vals;
    for (int o = lo; o <= hi; ++o)
      vals += (o > lo ? " " : "") + slot_val(s, s.u0 + o * coarse.step);
    lg("  %-24s %d 点: %s", s.name.c_str(), hi - lo + 1, vals.c_str());
  }
  if (ids.empty())
    return abort_with("粗网格无一格点参数合法 (值域互斥?), 文件不动");
  stage_.store(SearchStage::Coarse, std::memory_order_relaxed);
  if (handle(run_pass(ids, 0), ret))
    return ret;

  // ---- 分支: 每 h 各自取粗网格的全部互不相邻局部峰 ----
  std::vector<std::vector<Branch>> branches(static_cast<size_t>(H));
  int n_branch = 0;
  for (int hi = 0; hi < H; ++hi) {
    std::vector<double> lscore(lattice.size(), -std::numeric_limits<double>::infinity());
    for (size_t li = 0; li < lattice.size(); ++li)
      if (lattice_id[li] >= 0)
        lscore[li] = ents[static_cast<size_t>(lattice_id[li])].sc[hi].score;
    for (int li : grid::pick_branches(lattice, lscore)) {
      const int id = lattice_id[static_cast<size_t>(li)];
      Branch b;
      b.center = ents[static_cast<size_t>(id)].u;
      b.best_id = id;
      b.best = ents[static_cast<size_t>(id)].sc[hi].score;
      b.step = coarse.step * 0.5;
      branches[static_cast<size_t>(hi)].push_back(std::move(b));
    }
    res.holds[static_cast<size_t>(hi)].n_branch = static_cast<int>(branches[static_cast<size_t>(hi)].size());
    n_branch += res.holds[static_cast<size_t>(hi)].n_branch;
    const std::vector<Branch> &bs = branches[static_cast<size_t>(hi)];
    lg("分支 %s: %d 支", hold_of(hi).c_str(), static_cast<int>(bs.size()));
    for (size_t bi = 0; bi < bs.size(); ++bi)
      lg("  #%d %.4g @ %s", static_cast<int>(bi), bs[bi].best, params_of(slots, bs[bi].center).c_str());
  }
  if (n_branch == 0) {
    publish();
    return abort_with("粗网格在任一持有期都无正分候选 (LS ≤ 0 或 IC ≤ 0), 文件不动");
  }

  // ---- 趟 1..: 每 h 每支围 top-1 细化 (点数 ≤ kLayerMax), 步长减半, 增益 < kMinGain 停; 全部 h 全部活支的新点拼成一趟 ----
  int pass = 1;
  for (int level = 1; level <= grid::kMaxLevels; ++level) {
    std::vector<std::vector<std::vector<int>>> pts(static_cast<size_t>(H)); // [hi][bi] 本层格点候选
    ids.clear();
    int n_alive = 0, n_pts = 0, n_bad = 0, n_old = 0; // 铺的点 / 参数不合法 / 已评过或本层重复
    for (int hi = 0; hi < H; ++hi) {
      std::vector<Branch> &bs = branches[static_cast<size_t>(hi)];
      pts[static_cast<size_t>(hi)].resize(bs.size());
      for (size_t bi = 0; bi < bs.size(); ++bi) {
        if (!bs[bi].alive)
          continue;
        ++n_alive;
        for (const std::vector<double> &u : grid::refine_lattice(slots, bs[bi].center, bs[bi].step)) {
          ++n_pts;
          const int id = add_cand(u, {});
          if (id < 0) {
            ++n_bad;
            continue;
          }
          pts[static_cast<size_t>(hi)][bi].push_back(id);
          if (ents[static_cast<size_t>(id)].pass < 0 && std::find(ids.begin(), ids.end(), id) == ids.end())
            ids.push_back(id);
          else
            ++n_old;
        }
      }
    }
    lg("细化第 %d 层: 存活 %d 支, 铺 %d 点 → 不合法 %d, 已评/重复 %d, 新点 %d%s", level, n_alive, n_pts, n_bad, n_old, static_cast<int>(ids.size()),
       ids.empty() ? " (不另占一趟)" : "");
    if (!ids.empty()) { // 新点全已评过 → 不占趟号
      level_.store(level, std::memory_order_relaxed);
      alive_.store(n_alive, std::memory_order_relaxed);
      stage_.store(SearchStage::Refine, std::memory_order_relaxed);
      if (handle(run_pass(ids, pass), ret))
        return ret;
      ++pass;
    }
    bool any = false;
    for (int hi = 0; hi < H; ++hi) {
      std::vector<Branch> &bs = branches[static_cast<size_t>(hi)];
      for (size_t bi = 0; bi < bs.size(); ++bi) {
        Branch &b = bs[bi];
        if (!b.alive)
          continue;
        int best_id = -1;
        double best = -std::numeric_limits<double>::infinity();
        for (int id : pts[static_cast<size_t>(hi)][bi]) {
          const double sc = ents[static_cast<size_t>(id)].sc[hi].score;
          if (sc > best)
            best = sc, best_id = id;
        }
        const bool gained = best_id >= 0 && best > b.best * (1.0 + grid::kMinGain);
        const double before = b.best, step = b.step;
        if (best_id >= 0 && best > b.best) {
          b.center = ents[static_cast<size_t>(best_id)].u;
          b.best_id = best_id;
          b.best = best;
        }
        if (gained) {
          b.step *= 0.5;
          any = true;
        } else {
          b.alive = false;
        }
        lg("  %s #%d 步长 %.3g: %.4g → %.4g (%+.1f%%) %s @ %s", hold_of(hi).c_str(), static_cast<int>(bi), step, before, best, 100.0 * (best / before - 1.0),
           gained ? "继续" : "停", params_of(slots, b.center).c_str());
      }
    }
    if (!any) {
      lg("细化结束: 全部分支停");
      break;
    }
    if (level == grid::kMaxLevels)
      lg("细化结束: 到层数上限 %d", grid::kMaxLevels);
  }

  // ---- 高原集: 每 h 各支最优里 score ≥ (1 − kPlateau)·max; 并集再评一趟抽样 r16, 同 h 两两截面相关 ----
  std::vector<std::vector<int>> plateau(static_cast<size_t>(H)); // [hi] 候选 id
  RankSink sk;
  for (int hi = 0; hi < H; ++hi) {
    SearchHold &sh = res.holds[static_cast<size_t>(hi)];
    for (const Branch &b : branches[static_cast<size_t>(hi)])
      sh.max_score = std::max(sh.max_score, b.best);
    for (const Branch &b : branches[static_cast<size_t>(hi)])
      if (b.best >= (1.0 - factor::search::kPlateau) * sh.max_score && std::find(plateau[static_cast<size_t>(hi)].begin(), plateau[static_cast<size_t>(hi)].end(), b.best_id) == plateau[static_cast<size_t>(hi)].end())
        plateau[static_cast<size_t>(hi)].push_back(b.best_id);
    sh.n_plateau = static_cast<int>(plateau[static_cast<size_t>(hi)].size());
    for (int id : plateau[static_cast<size_t>(hi)])
      if (std::find(sk.ids.begin(), sk.ids.end(), id) == sk.ids.end())
        sk.ids.push_back(id);
  }
  for (int hi = 0; hi < H; ++hi) {
    const std::vector<int> &pl = plateau[static_cast<size_t>(hi)];
    for (size_t a = 0; a < pl.size(); ++a)
      for (size_t b = a + 1; b < pl.size(); ++b) {
        std::pair<int, int> pr(sk.pos(pl[a]), sk.pos(pl[b]));
        if (pr.first > pr.second)
          std::swap(pr.first, pr.second);
        if (std::find(sk.pairs.begin(), sk.pairs.end(), pr) == sk.pairs.end())
          sk.pairs.push_back(pr);
      }
  }
  sk.sum.assign(sk.pairs.size(), 0.0);
  sk.cnt.assign(sk.pairs.size(), 0);
  for (int hi = 0; hi < H; ++hi) {
    const SearchHold &sh = res.holds[static_cast<size_t>(hi)];
    const std::vector<int> &pl = plateau[static_cast<size_t>(hi)];
    lg("高原 %s: 阈值 %.4g (= %.0f%% × 最高 %.4g), %d 个", hold_of(hi).c_str(), (1.0 - factor::search::kPlateau) * sh.max_score,
       100.0 * (1.0 - factor::search::kPlateau), sh.max_score, static_cast<int>(pl.size()));
    for (size_t a = 0; a < pl.size(); ++a) {
      const Entry &en = ents[static_cast<size_t>(pl[a])];
      lg("  [%d] %.4g @ %s", static_cast<int>(a), en.sc[hi].score, params_of(slots, en.u).c_str());
    }
  }
  if (sk.pairs.empty())
    lg("截面相关: 各持有期高原 ≤ 1 个, 跳过");
  else
    lg("截面相关: 并集 %d 候选重评, 抽样步长 %d 行 (约 %d 行), %d 对", static_cast<int>(sk.ids.size()), stride, days * factor::kSegLen / stride,
       static_cast<int>(sk.pairs.size()));
  if (!sk.pairs.empty()) { // 单候选的 h 不必算相关; 重评 (r 清零再攒, 分数不变)
    sink = &sk;
    stage_.store(SearchStage::Corr, std::memory_order_relaxed);
    if (handle(run_pass(sk.ids, pass), ret))
      return ret;
    sink = nullptr;
  }
  const auto pair_of = [&](int id_a, int id_b) -> size_t {
    std::pair<int, int> pr(sk.pos(id_a), sk.pos(id_b));
    if (pr.first > pr.second)
      std::swap(pr.first, pr.second);
    const auto it = std::find(sk.pairs.begin(), sk.pairs.end(), pr);
    assert(it != sk.pairs.end());
    return static_cast<size_t>(it - sk.pairs.begin());
  };
  const auto corr_of = [&](int id_a, int id_b) -> double {
    const size_t p = pair_of(id_a, id_b);
    return sk.cnt[p] > 0 ? sk.sum[p] / static_cast<double>(sk.cnt[p]) : 0.0; // 无一有效行 (两候选从不同时有秩) → 视作不相关
  };

  // ---- 终选: 每 h pick_uncorrelated → ≤ kPicks 权重; 全部 h 的 picks 整个替换文件 weights ----
  std::vector<SearchWeight> ws;
  for (int hi = 0; hi < H; ++hi) {
    const std::vector<int> &pl = plateau[static_cast<size_t>(hi)];
    if (pl.empty())
      continue;
    const int M = static_cast<int>(pl.size());
    std::vector<std::vector<double>> corr(static_cast<size_t>(M), std::vector<double>(static_cast<size_t>(M), 0.0));
    std::vector<double> score(static_cast<size_t>(M));
    for (int a = 0; a < M; ++a) {
      score[static_cast<size_t>(a)] = ents[static_cast<size_t>(pl[static_cast<size_t>(a)])].sc[hi].score;
      for (int b = a + 1; b < M; ++b)
        corr[static_cast<size_t>(a)][static_cast<size_t>(b)] = corr[static_cast<size_t>(b)][static_cast<size_t>(a)] =
            corr_of(pl[static_cast<size_t>(a)], pl[static_cast<size_t>(b)]);
    }
    for (int a = 0; a < M; ++a)
      for (int b = a + 1; b < M; ++b)
        lg("相关 %s [%d]-[%d]: %.3f (有效行 %lld)", hold_of(hi).c_str(), a, b, corr[static_cast<size_t>(a)][static_cast<size_t>(b)],
           sk.cnt[pair_of(pl[static_cast<size_t>(a)], pl[static_cast<size_t>(b)])]);
    const std::vector<int> picked = factor::search::pick_uncorrelated(corr, score);
    {
      std::string idx;
      for (int a : picked)
        idx += strf("[%d]", a);
      lg("终选 %s: %s", hold_of(hi).c_str(), idx.c_str());
    }
    SearchHold &sh = res.holds[static_cast<size_t>(hi)];
    for (int a : picked) {
      const Entry &en = ents[static_cast<size_t>(pl[static_cast<size_t>(a)])];
      SearchPick pk;
      pk.expr = en.e.canon;
      pk.score = en.sc[hi].score, pk.ls = en.sc[hi].ls, pk.ric = en.sc[hi].ric;
      for (int b : picked)
        if (b != a)
          pk.corr = std::max(pk.corr, corr[static_cast<size_t>(a)][static_cast<size_t>(b)]);
      sh.picks.push_back(std::move(pk));
      ws.push_back(SearchWeight{sh.hold, en.e.canon});
    }
  }
  assert(!ws.empty());                              // n_branch > 0 ⇒ 至少一个 h 有高原集
  WriteFactorWeights(req.factor_dir, req.file, ws); // 只写权重; 看板数据由随后的 Run 用同一份目标从 stat 算
  res.wrote = true;
  res.n_weights = static_cast<int>(ws.size());
  res.message = "weights 已写入 (" + std::to_string(ws.size()) + " 个权重, " + std::to_string(H) + " 个持有期), 自动 Run 重算 stat";
  {
    int n_eval = 0;
    for (const Entry &en : ents)
      n_eval += en.pass >= 0;
    lg("完成: 写入 %d 个权重, 共评 %d 候选, %d 趟", static_cast<int>(ws.size()), n_eval, res.n_pass);
  }
  publish();
  return true;
}

} // namespace GUI::Factors
