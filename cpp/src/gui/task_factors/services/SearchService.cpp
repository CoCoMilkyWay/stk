// SearchService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Search/Cpu.hpp → Stat/Cpu.hpp), 依赖受控浮点:
// CMake PRECISE_MATH 源.
#include "gui/task_factors/services/SearchService.hpp"

#include "factor/Dag.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Space.hpp"
#include "factor/Search/Surface.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // Engine<Cpu | Gpu>
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"
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
#include <optional>
#include <string>
#include <utility>

namespace GUI::Factors {

namespace {

namespace space = factor::search::space;
namespace surface = factor::search::surface;
namespace fs = factor::stat;
using factor::search::Score;
using Clock = std::chrono::steady_clock;

constexpr int kCpuForest = 64;     // CPU 一张共享 DAG 的候选数 (只为公共前缀, 根即算即放)
constexpr int kGpuMinBatch = 8;    // 定块大小时按至少这么多候选同在显存算 (核内 8 候选共用标签行)
constexpr double kVramFrac = 0.85; // 显存空闲量的使用比例 (宿主: FactorsLoad host_budget)

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

// 过程日志 <factor_dir>/log/<因子名>.log: 每次 Search 覆盖; 逐行即刷 (断言崩了也留得下); 行首 = 距开跑秒数.
// 只记流程中间量 (分块 / 分位表 / 采样 / 曲面 / 峰 / 细化 / 相关), 终选结果看因子文件 weights
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
std::string slot_val(const space::Slot &s, double u) {
  const double v = space::to_v(s, u);
  return s.integer ? strf("%d", static_cast<int>(v)) : strf("%.4g", v);
}
// 坐标 u[P] → "Op.d=16, Op.k=0.0042"
std::string params_of(const std::vector<space::Slot> &slots, const std::vector<double> &u) {
  std::string out;
  for (size_t j = 0; j < slots.size(); ++j)
    out += (j ? ", " : "") + slots[j].name + "=" + slot_val(slots[j], u[j]);
  return out;
}
// 坐标变换名 (Space.hpp to_v)
const char *coord_name(const space::Slot &s) {
  if (s.key == space::Key::D)
    return "log2";
  switch (s.kdom) {
  case factor::KDom::OPEN01:
    return "logit";
  case factor::KDom::GE0:
  case factor::KDom::ANY:
    return "logit分位";
  case factor::KDom::TOD:
    return "行";
  default:
    return "log2";
  }
}

// 一块的作用域块 (X 叶 + Y 门控 / 所搜持有期的标签; c.t_lo = 前带行数); 整轮缓存或单块复用. 标签秩归引擎会话 (bind 到哪块算哪块)
struct ChunkData {
  int idx = -1; // 块号; -1 = 空
  ScopeCache c;
};

// 一块的抽样行 (契约【多权重】): 统计区 [t_lo, T) 里绝对行 ≡ 0 (mod stride) 的行 = 块内行 t_first + i·stride (i < n_rows)
struct Samp {
  int t_first = 0, n_rows = 0;
};
Samp samp_of(int T, int t_lo, int t0_abs, int stride) {
  int t = t_lo;
  while ((t0_abs + t) % stride != 0)
    ++t;
  return {t, t < T ? (T - 1 - t) / stride + 1 : 0};
}
// 候选的 r16 抽样行: 按块序拼进 dst [R][A] (off = 已落行数)
struct SampleP : ProbeBase {
  static constexpr uint8_t need = 0;
  std::vector<uint16_t> *dst = nullptr;
  int stride = 1, off = 0;
  void root(const Ctx &c, const RootView &v) {
    const fs::Span s = c.s();
    const Samp sm = samp_of(s.T, s.t_lo, s.t0_abs, stride);
    if (sm.n_rows > 0)
      v.ranks(sm.t_first, stride, sm.n_rows, dst->data() + static_cast<size_t>(off) * s.A);
    off += sm.n_rows;
  }
};
using SearchProbes = Probes<HoldP, SampleP>;

// 一个候选 (评过 / 待评)
struct Entry {
  std::vector<double> u;
  factor::expr::Expr e;
  SearchProbes ps; // 跨块续攒 (每趟开头重置)
  Score sc;
  std::vector<uint16_t> r16; // 抽样行 r16 [行][A] (评时落, 截面相关用)
  int pass = -1;             // 评过的趟; -1 = 待评
};

// 曲面 + 终选 (每趟后重算)
struct Surface {
  surface::Model M;
  std::vector<int> ids;    // 观测 (M 的下标 → 候选 id)
  std::vector<double> mu;  // 观测处后验均值
  std::vector<int> peaks;  // 峰 (观测下标, μ 降序)
  std::vector<int> picks;  // 终选 = 活跃峰 (候选 id, μ 降序)
  std::vector<double> pmu; // picks 的 μ
};

} // namespace

bool MakeSearchRequest(const SharedData &data, const std::string &pool, int seg, const FactorRow &row, int hold, bool gpu, SearchRequest &req) {
  assert(!row.tmpl.empty() && "Search 只接模板解析过的因子");
  req = SearchRequest{};
  req.spec = SearchSpec{row.file, row.tmpl, row.frame, hold};
  return MakeFactorScope(data, pool, seg, gpu, req.scope);
}

SearchService::SearchService() { Start(); }
SearchService::~SearchService() { Stop(); }

bool SearchService::run(SearchRequest &req) {
  pass_.store(0, std::memory_order_relaxed);
  chunk_.store(0, std::memory_order_relaxed);
  chunks_.store(0, std::memory_order_relaxed);
  stage_.store(SearchStage::Prepare, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(mutex);
    result = SearchResult{};
    result.file = req.spec.file;
    result.tmpl = req.spec.tmpl;
    result.backend = req.scope.gpu ? "gpu" : "cpu";
  }
  publish();
  if (!flow_)
    flow_ = std::make_unique<FactorFlow>();
  return evaluate(req, *flow_);
}

bool SearchService::evaluate(const SearchRequest &req, FactorFlow &F) {
  TraceN("SearchRun");
  const Clock::time_point t_start = Clock::now();
  const FeatureTable &ft = feats();
  const FactorScope &scope = req.scope;
  const SearchSpec &sp = req.spec;
  assert(scope.seg == ft.seg && "请求层与字段表层不符");
  const int seg = ft.seg;
  // 给定的持有期 (字段表已只收本层合法 (hold_ok) 的)
  const std::vector<int> keys = keys_of(ft.y.labels);
  assert(std::find(keys.begin(), keys.end(), sp.hold) != keys.end() && "Search 的持有期须是字段表标签组之一");
  const fs::Holds hd = holds_of({sp.hold});
  fs::assert_holds(hd, seg);
  SearchResult res;
  res.file = sp.file;
  res.tmpl = sp.tmpl;
  res.hold = sp.hold;
  res.backend = scope.gpu ? "gpu" : "cpu";
  RunLog lg;
  lg.t0 = t_start;
  {
    const std::filesystem::path log_dir = std::filesystem::path(scope.factor_dir) / "log";
    std::filesystem::create_directories(log_dir);
    prune_logs(scope.factor_dir, log_dir);
    lg.f.open(log_dir / (std::filesystem::path(sp.file).stem().string() + ".log"), std::ios::trunc);
    assert(lg.f.is_open() && "Search log 打不开");
    const std::time_t now = std::time(nullptr);
    char ts[32];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    lg("Search %s  @ %s", sp.file.c_str(), ts);
    lg("模板: %s", sp.tmpl.c_str());
    lg("后端 %s, 池 %s, 持有期 %s", res.backend.c_str(), scope.pool.c_str(), fs::hold_name(sp.hold).c_str());
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

  // ---- 模板 (可搜槽的区间要 A, 装完块 0 才建) ----
  factor::expr::Expr e0;
  {
    std::string err;
    const bool ok = factor::expr::parse(sp.tmpl, ft.lookup(), seg, e0, err);
    assert(ok && "Factors 表的模板规范串必可再解析");
    (void)ok;
  }
  if (!e0.is_template())
    return abort_with("模板无自由参数 (裸键名 d / k / k2 才搜; 写了数字的参数固定)");
  std::vector<space::Slot> slots;
  // 块首前带: 统计首行的 rAC lag 行 (t_lo − max_lag) 也须整窗 ⇒ 行数 ≥ 模板回看 + max_lag
  const int look = space::lookback(e0), lag = factor::search::max_lag(hd, seg);
  const int carry = (look + lag + seg - 1) / seg;
  lg("前带: 模板回看 %d 行 + rAC lag %d 行 → %d 天", look, lag, carry);
  const factor::expr::Expr ep = e0.placeholder();                                           // 结构 / 特征叶序 / 槽数与任一具体化相同
  const factor::Dag d0 = factor::build_forest({&ep}, /*keep_roots=*/false, /*fuse=*/false); // 值域检查 (plan_forest) 按根归因, 不融合
  const int nf = static_cast<int>(d0.feats.size()), s0 = d0.n_slots;

  // 候选表: 规范串去重
  std::vector<Entry> ents;
  std::map<std::string, int> seen;
  const auto add_cand = [&](const std::vector<double> &u) -> int {
    Entry en;
    if (!space::apply(e0, slots, u, seg, en.e))
      return -1;
    if (const auto it = seen.find(en.e.canon); it != seen.end())
      return it->second;
    en.u = u;
    const int id = static_cast<int>(ents.size());
    seen.emplace(en.e.canon, id);
    ents.push_back(std::move(en));
    return id;
  };
  const auto publish_result = [&] {
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    res.cands.clear();
    for (const Entry &en : ents) {
      if (en.pass < 0)
        continue;
      SearchCand c;
      c.expr = en.e.canon;
      c.u = en.u;
      c.pass = en.pass;
      c.ok = en.sc.ok;
      c.score = en.sc.score;
      c.ls = en.sc.ls;
      c.ric = en.sc.ric;
      c.sign = en.sc.sign;
      c.se = en.sc.se;
      res.cands.push_back(std::move(c));
    }
    std::stable_sort(res.cands.begin(), res.cands.end(), [](const SearchCand &a, const SearchCand &b) { return a.score > b.score; });
    {
      std::lock_guard<std::mutex> lock(mutex);
      result = res;
    }
    publish();
  };
  // ---- 作用域 / 日期 (评估期 = 模板叶所用库落盘日交集) ----
  EvalScope es;
  if (std::string why; !open_job(F, ft, scope, d0.feats, cancel_, es, why))
    return abort_with(why);
  const std::vector<misc::Ymd> &dates = es.dates;
  const int days = static_cast<int>(dates.size()), A = static_cast<int>(es.rs.axis.size());
  lg("评估期 %s ~ %s (%d 天, 所用库落盘日交集)", misc::dash(dates.front()).c_str(), misc::dash(dates.back()).c_str(), days);
  // r16 抽样行: 绝对行 t_abs ≡ 0 (mod stride) (全作用域约 kCorrRows 行; 截面相关 + 阈值槽分位表)
  const int stride = std::max(1, days * seg / factor::search::kCorrRows);

  // ---- 分块与预算 (字节 / 格; 见头文件【分块流式】) ----
  //   L0  宿主作用域块 (ScopeCache): cell_bytes X (叶) + Y (截面门控 + 所搜持有期的标签, 不装成本)
  //   We  宿主引擎 (随绑定的块重开): CPU = Engine<Cpu>::host_bytes (活槽 s0 + 2); GPU 批核不拷回 = 0
  //   Dv  显存常驻: Engine<Gpu>::dev_bytes (中间量 2·s0 槽); 每候选 Engine<Gpu>::kDevRootBytes
  //   r16 常驻: 每候选 (抽样行 × A × 2) 字节; 预留趟 0 的 kInit 个, 细化的随评随增
  const XSpec xs{d0.feats};
  YSpec ys{hd, Amts{}, 0.f}; // cap: 多块时预扫后置全期 n_full
  const double per_day = static_cast<double>(seg) * A;
  const double L0 = cell_bytes(xs) + cell_bytes(ys);
  const double We = scope.gpu ? 0.0 : Engine<Cpu>::host_bytes(s0 + 2, hd.n);
  const double Dv = Engine<Gpu>::dev_bytes(nf, 2 * s0, hd.n);
  const double r16_bytes = (static_cast<double>(days) * seg / stride + 1.0) * A * sizeof(uint16_t);
  const double host_cap = host_budget() - space::kInit * r16_bytes;
  assert(host_cap > 0.0 && "内存预算装不下趟 0 的 r16 抽样行");
  int D = std::clamp(static_cast<int>(host_cap / (per_day * (L0 + We))) - carry, 1, days);
  double vram_budget = 0.0;
  if (scope.gpu) {
    assert(factor::gpu::available() && "GPU 后端不可用");
    size_t vfree = 0, vtotal = 0;
    factor::gpu::mem_info(&vfree, &vtotal);
    vram_budget = kVramFrac * static_cast<double>(vfree);
    const int Dg = std::clamp(static_cast<int>(vram_budget / (per_day * (Dv + Engine<Gpu>::kDevRootBytes * kGpuMinBatch))) - carry, 1, days);
    D = std::min(D, Dg);
  }
  const int n_chunks = (days + D - 1) / D;
  const int Tmax = std::min(days, D + carry) * seg; // 最长一块的行数
  int batch = 1;
  if (scope.gpu) {
    const double nmax = static_cast<double>(Tmax) * A;
    batch = std::clamp(static_cast<int>((vram_budget - nmax * Dv) / (Engine<Gpu>::kDevRootBytes * nmax)), 1, factor::gpu::search_max_batch());
  }
  // 整轮缓存全部块 (装得下) 还是一块复用
  double cache_bytes = 0.0;
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - carry);
    cache_bytes += (day1 - load0) * per_day * L0;
  }
  const bool cache_all = cache_bytes + Tmax * static_cast<double>(A) * We <= host_cap;
  res.n_chunk = n_chunks, res.chunk_days = D, res.batch = batch;
  res.n_assets = A;
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - carry);
    res.chunk_plan.push_back(SearchChunk{day1 - load0, (day1 - load0) * per_day * L0 / (1024.0 * 1024.0)});
  }
  chunks_.store(n_chunks, std::memory_order_relaxed);
  publish_result(); // 装载前先发布分块计划
  constexpr double kMiB = 1024.0 * 1024.0;
  lg("数据: %d 只 × %d 天, 特征 %d 列, 标签 %d 列, 共享 DAG 槽 %d; 每格 %.0f 字节 (宿主装载) + %.0f (CPU 工作区)", A, days, nf, hd.n, s0, L0, We);
  lg("预算: 内存 %.0f MB%s; 块 ≤ %d 天 + 前带 %d 天, 共 %d 块, 全部常驻 %s (需 %.0f MB)%s", host_cap / kMiB,
     scope.gpu ? strf(", 显存 %.0f MB", vram_budget / kMiB).c_str() : "", D, carry, n_chunks, cache_all ? "是" : "否 (每趟逐块重读)", cache_bytes / kMiB,
     scope.gpu ? strf(", GPU 批 %d", batch).c_str() : "");
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - carry);
    lg("  块 %d: 统计 %s ~ %s (%d 天), 装载 %d 天, %.0f MB", ci + 1, misc::dense(dates[static_cast<size_t>(day0)]).c_str(), misc::dense(dates[static_cast<size_t>(day1 - 1)]).c_str(), day1 - day0,
       day1 - load0, res.chunk_plan[static_cast<size_t>(ci)].mb);
  }

  // ---- 满仓只数 = 全期池宽 (与 Run 同一个 cap): 一块 = 块 0 的 YPlanes::n_full 即全期; 多块先预扫全期门控 → ys.cap, 各块装载即用它
  //      (引擎的 Search 会话取 y.n_full) ----
  if (n_chunks > 1) {
    const Clock::time_point t_scan = Clock::now();
    if (!n_full_of(ft, es, F.lr, 0, static_cast<size_t>(days), F.ex, cancel_, *this, ys.cap)) {
      if (F.lr.stale())
        return abort_with(kStaleMsg);
      lg("已取消");
      return false;
    }
    lg("预扫全期门控: %.1fs", std::chrono::duration<double>(Clock::now() - t_scan).count());
  }

  std::vector<ChunkData> chunks(cache_all ? static_cast<size_t>(n_chunks) : 1);
  bool domain_checked = false;
  // 装一块: 交易日 [day0, day1), 前带 carry; 首块顺带查特征叶值域. Ok / Cancel / Stale | Broken (res.message 已填)
  const auto load_chunk = [&](int ci, ChunkData &cd) -> Rc {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - carry);
    const Clock::time_point t_load = Clock::now();
    cd.idx = -1;
    cd.c.reset();
    const Rc rc = ensure_xy(cd.c, ft, es, F.lr, static_cast<size_t>(load0), static_cast<size_t>(day1), ys, xs.codes, /*retain=*/true, F.ex, cancel_, *this);
    if (rc == Rc::Stale)
      res.message = kStaleMsg;
    if (rc != Rc::Ok)
      return rc;
    lg("装载块 %d/%d: %d 天, %.1fs", ci + 1, n_chunks, day1 - load0, std::chrono::duration<double>(Clock::now() - t_load).count());
    cd.idx = ci;
    cd.c.t_lo = (day0 - load0) * seg;
    if (!domain_checked) {
      domain_checked = true;
      std::vector<std::string> why;
      std::vector<uint8_t> need;
      if (plan_forest(F.ex, cd.c, d0, why, need) == 0) {
        res.message = why[0];
        return Rc::Broken;
      }
    }
    return Rc::Ok;
  };

  // ---- r16 抽样行 (契约【多权重】): 各块的抽样行 (samp_of) 按块序拼成每候选一张 [R][A] ----
  int R = 0;
  for (int ci = 0; ci < n_chunks; ++ci) {
    const int day0 = ci * D, day1 = std::min(days, day0 + D), load0 = std::max(0, day0 - carry);
    R += samp_of((day1 - load0) * seg, (day0 - load0) * seg, load0 * seg, stride).n_rows;
  }
  // 截面相关 (不定向): 两候选抽样行逐行 Pearson (Stat/Host lag_row, lag 行换成另一候选) 的均值; 无一有效行 → 0 (视作不相关). 按需现算并缓存
  std::map<std::pair<int, int>, double> rho_raw;
  const auto ensure_rho = [&](const std::vector<int> &ids_) {
    std::vector<std::pair<int, int>> need;
    for (size_t a = 0; a < ids_.size(); ++a)
      for (size_t b = a + 1; b < ids_.size(); ++b) {
        const std::pair<int, int> pr(std::min(ids_[a], ids_[b]), std::max(ids_[a], ids_[b]));
        if (pr.first != pr.second && !rho_raw.count(pr) && std::find(need.begin(), need.end(), pr) == need.end())
          need.push_back(pr);
      }
    std::vector<double> out(need.size(), 0.0);
    F.ex.rows(static_cast<int>(need.size()), [&](int p0, int p1, int) {
      for (int p = p0; p < p1; ++p) {
        const uint16_t *ra = ents[static_cast<size_t>(need[static_cast<size_t>(p)].first)].r16.data();
        const uint16_t *rb = ents[static_cast<size_t>(need[static_cast<size_t>(p)].second)].r16.data();
        double sum = 0.0;
        long long cnt = 0;
        for (int i = 0; i < R; ++i) {
          fs::Row row;
          factor::cpu::stat::lag_row(ra + static_cast<size_t>(i) * A, rb + static_cast<size_t>(i) * A, A, row);
          if (row.ok_rac)
            sum += row.rac, ++cnt;
        }
        out[static_cast<size_t>(p)] = cnt > 0 ? sum / static_cast<double>(cnt) : 0.0;
      }
    });
    for (size_t p = 0; p < need.size(); ++p)
      rho_raw[need[p]] = out[p];
  };
  // 定向后的持仓相关 (两者方向相反 → 原值相关取负); 先 ensure_rho
  const auto rho = [&](int a, int b) -> double {
    if (a == b)
      return 1.0;
    const auto it = rho_raw.find({std::min(a, b), std::max(a, b)});
    assert(it != rho_raw.end());
    return ents[static_cast<size_t>(a)].sc.sign * ents[static_cast<size_t>(b)].sc.sign * it->second;
  };

  // ---- 一块上评一批候选 (两后端同一份, 引擎 E 见 FactorsEngine): 每批一张共享森林 (CPU ≤ kCpuForest 根即算即放;
  //      GPU ≤ batch 按显存减半, 一批根平面同在显存一次 search_eval), 评完逐候选进探针 (HoldP 续攒 Accum + SampleP 抽样 r16) ----
  const auto eval_chunk = [&](auto &E, ChunkData &cd, const std::vector<int> &ids) -> bool {
    E.bind(cd.c);
    for (size_t b0 = 0; b0 < ids.size();) {
      std::vector<const factor::expr::Expr *> ptrs;
      for (size_t k = b0; k < ids.size(); ++k)
        ptrs.push_back(&ents[static_cast<size_t>(ids[k])].e);
      int C = 0;
      const factor::Dag Fd = E.template plant<SearchProbes>(ptrs, C);
      assert(Fd.feats == d0.feats && "同结构候选的特征叶序必与基线一致 (平面下标按 d0.feats 装)");
      for (int k = 0; k < C; ++k)
        assert(Fd.first_root(k) == k && "候选按规范串去重, 根两两不同");
      const std::vector<fs::Frame> fr(static_cast<size_t>(C), sp.frame);
      if (!E.forest(
              Fd, nullptr, fr.data(), [&](int k) -> SearchProbes & { return ents[static_cast<size_t>(ids[b0 + static_cast<size_t>(k)])].ps; },
              [&](int, double) { return !cancelled(); }, [&](int) { publish(1); }))
        return false;
      b0 += static_cast<size_t>(C);
    }
    return true;
  };

  // ---- 一趟: ids (全是待评) 沿全部块流过 (引擎 E), 收尾打分 + 发布 ----
  const auto run_pass = [&](auto &E, const std::vector<int> &ids, int pass) -> Rc {
    assert(!ids.empty());
    pass_.store(pass, std::memory_order_relaxed);
    chunk_.store(0, std::memory_order_relaxed);
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      assert(en.pass < 0 && "每个候选只评一次");
      en.ps = SearchProbes{};
      en.r16.assign(static_cast<size_t>(R) * A, fs::kRankNone);
      en.ps.at<SampleP>().dst = &en.r16; // ents 本趟不再增 → 地址稳
      en.ps.at<SampleP>().stride = stride;
    }
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
      begin(Phase::Running, ids.size() * static_cast<size_t>(n_chunks));
      publish(ids.size() * static_cast<size_t>(ci)); // 前面的块已评完
      if (!eval_chunk(E, cd, ids))
        return Rc::Cancel;
      chunk_.store(ci + 1, std::memory_order_relaxed);
    }
    const ScopeCache &last = chunks[cache_all ? static_cast<size_t>(n_chunks - 1) : 0].c;
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      assert(en.ps.at<SampleP>().off == R && "各块抽样行须拼满");
      en.ps.finish(F.ex, last);
      const HoldP &hp = en.ps.at<HoldP>();
      assert(hp.n_hold == 1);
      en.sc = factor::search::score_signed(hp.hold[0]);
      en.pass = pass;
    }
    int n_pos = 0;
    double best = -std::numeric_limits<double>::infinity();
    for (int id : ids) {
      const double sc = ents[static_cast<size_t>(id)].sc.score;
      n_pos += std::isfinite(sc);
      best = std::max(best, sc);
    }
    const double pass_s = std::chrono::duration<double>(Clock::now() - t_pass).count();
    lg("第 %d 趟完成: %d 候选 × %d 块, %.1fs (装载 %.1fs, 评估 %.1fs, %.1f ms/候选·块) | 正分 %d 最高 %.4g", pass, static_cast<int>(ids.size()), n_chunks, pass_s,
       load_s, pass_s - load_s, 1e3 * (pass_s - load_s) / (static_cast<double>(ids.size()) * n_chunks), n_pos, best);
    res.n_pass = pass + 1;
    publish_result();
    return Rc::Ok;
  };
  // 结局 → run 的返回: Cancel → false; Stale / Broken (正常输入问题, message 已填) → 发布 + true
  const auto handle = [&](Rc rc, bool &ret) {
    if (rc == Rc::Cancel)
      lg("已取消");
    if (rc == Rc::Stale || rc == Rc::Broken) {
      lg("中止: %s", res.message.c_str());
      publish_result();
    }
    ret = rc != Rc::Cancel;
    return rc != Rc::Ok;
  };
  bool ret = false;

  // ---- 准备: 先装块 0 (趟 0 反正要) → A = n_full · kGroups 定槽区间 → 阈值槽分位表 ----
  if (handle(load_chunk(0, chunks[0]), ret))
    return ret;
  res.n_full = chunks[0].c.y.n_full;
  lg("满仓一腿 %.1f 只 (全期池内平均有效资产数 / %d 组, 各块共用)", res.n_full, fs::kGroups);
  const double a_eff = res.n_full * fs::kGroups;
  slots = space::slots_of(e0, seg, a_eff);
  assert(!slots.empty());
  for (const space::Slot &s : slots)
    res.slots.push_back(s.name);
  lg("自由参数 %d 个 (区间按池内平均有效资产数 %.0f 定):", static_cast<int>(slots.size()), a_eff);
  for (const space::Slot &s : slots)
    lg("  %-24s 坐标 %-8s 区间 [%.3g, %.3g] 中性 u0=%.3g%s", s.name.c_str(), coord_name(s), s.u_lo, s.u_hi, s.u0, s.integer ? " 整数" : "");
  // 阈值槽 (ANY / GE0) 由深到浅 (前序逆序: 子树先于祖先): 中性具体式在 CPU 上走到该槽的输入节点即止,
  // 取块 0 抽样行的池内有效值 (GE0: |x|) 升序作分位表
  std::optional<Engine<Cpu>> qe; // 分位表恒走 CPU (要宿主平面); 用完即放 (活槽不与评估引擎同在)
  qe.emplace(F.ex, cancel_, 1);
  for (size_t jj = slots.size(); jj-- > 0;) {
    space::Slot &sl = slots[jj];
    if (!space::needs_qtab(sl))
      continue;
    const factor::expr::Expr ec = space::neutral(e0, slots, seg);
    const factor::Dag dc = factor::build(ec, /*fuse=*/false);
    const factor::expr::Node &nd = ec.nodes[static_cast<size_t>(sl.node)];
    assert(factor::expr::kOps[nd.op].arity >= 1);
    const std::string key = factor::expr::to_string(ec, nd.args[0]);
    const auto it = std::find(dc.keys.begin(), dc.keys.end(), key);
    assert(it != dc.keys.end() && "Expr 子树必在同一 Expr 的 Dag 里");
    const int want = static_cast<int>(it - dc.keys.begin());
    std::vector<uint8_t> prefix(dc.nodes.size(), 0); // 走到该输入节点即止
    std::fill_n(prefix.begin(), want + 1, static_cast<uint8_t>(1));
    const ScopeCache &c = chunks[0].c;
    const fs::Span s = c.span();
    const Samp sm = samp_of(s.T, s.t_lo, s.t0_abs, stride);
    std::vector<float> tab;
    qe->bind(c);
    if (!qe->walk(dc, prefix.data(), [&](int i, double) {
          if (i == want) {
            const factor::check::Plane *p = qe->plane(i);
            const uint8_t *gate = c.y.gate.data();
            for (int r = 0; r < sm.n_rows; ++r)
              for (int a = 0; a < A; ++a) {
                const size_t k = static_cast<size_t>(sm.t_first + r * stride) * A + a;
                if (p->m[k] && gate[k])
                  tab.push_back(sl.kdom == factor::KDom::GE0 ? std::fabs(p->v[k]) : p->v[k]);
              }
          }
          return !cancelled();
        })) {
      lg("已取消");
      return false;
    }
    std::sort(tab.begin(), tab.end());
    if (tab.size() < 2 || !(tab.front() < tab.back())) // 输入常量 → 阈值无意义; 自由参数没法落值, 整个搜索放弃
      return abort_with("阈值槽 " + sl.name + " 的输入在块 0 抽样行上是常量, 搜不动");
    const size_t n_tab = tab.size();
    space::set_qtab(sl, std::move(tab));
    lg("分位表: %s ← %zu 个值 (输入 %s), 区间 [%s, %s], 中性 %s", sl.name.c_str(), n_tab, key.c_str(), slot_val(sl, sl.u_lo).c_str(), slot_val(sl, sl.u_hi).c_str(),
       slot_val(sl, sl.u0).c_str());
  }
  qe.reset();

  // ---- 趟 0: R_d 采样 (Space design), 一趟得分 ----
  std::vector<int> ids;
  {
    int n_bad = 0;
    for (const std::vector<double> &u : space::design(slots, space::kInit)) {
      const int id = add_cand(u);
      if (id < 0)
        ++n_bad;
      else if (std::find(ids.begin(), ids.end(), id) == ids.end())
        ids.push_back(id);
    }
    lg("采样: R_d %d 点 → 不合法 %d, 取整去重后 %d 候选; r16 抽样步长 %d 行 (每候选 %d 行)", space::kInit, n_bad, static_cast<int>(ids.size()), stride, R);
  }
  if (ids.empty())
    return abort_with("采样点无一参数合法 (值域互斥?), 文件不动");

  // 候选 id 的方向 + 参数 (日志)
  const auto at = [&](int id) {
    const Entry &en = ents[static_cast<size_t>(id)];
    return (en.sc.sign < 0 ? std::string("取反 ") : std::string()) + params_of(slots, en.u);
  };

  // ---- 曲面: 已评且 ok 的点 (x = 单位立方坐标, y = g, 噪声 = SE) 拟合 GP → 峰 → pick_diverse 得活跃峰 (= 当前终选) ----
  Surface S;
  const auto fit = [&] {
    S = Surface{};
    std::vector<std::vector<double>> x;
    std::vector<double> y, se;
    for (int id = 0; id < static_cast<int>(ents.size()); ++id) {
      const Entry &en = ents[static_cast<size_t>(id)];
      if (en.pass < 0 || !en.sc.ok)
        continue;
      S.ids.push_back(id);
      x.push_back(space::unit(slots, en.u));
      y.push_back(en.sc.g);
      se.push_back(en.sc.se);
    }
    if (!S.ids.empty()) {
      S.M = surface::fit(x, y, se);
      std::vector<uint8_t> usable;
      for (size_t i = 0; i < x.size(); ++i) {
        S.mu.push_back(surface::mean(S.M, x[i]));
        usable.push_back(std::isfinite(ents[static_cast<size_t>(S.ids[i])].sc.score));
      }
      S.peaks = surface::peaks(S.M, S.mu, usable);
    }
    std::vector<int> pk;
    for (const int i : S.peaks)
      pk.push_back(S.ids[static_cast<size_t>(i)]);
    ensure_rho(pk);
    const size_t M = pk.size();
    std::vector<double> mu(M), se_pk(M);
    std::vector<std::vector<double>> corr(M, std::vector<double>(M, 0.0));
    for (size_t a = 0; a < M; ++a) {
      mu[a] = S.mu[static_cast<size_t>(S.peaks[a])], se_pk[a] = ents[static_cast<size_t>(pk[a])].sc.se;
      for (size_t b = 0; b < M; ++b)
        corr[a][b] = rho(pk[a], pk[b]);
    }
    for (const int a : factor::search::pick_diverse(mu, se_pk, corr))
      S.picks.push_back(pk[static_cast<size_t>(a)]), S.pmu.push_back(mu[static_cast<size_t>(a)]);
    res.n_peak = static_cast<int>(M);
    std::string ell;
    for (const double l : S.M.ell)
      ell += strf("%s%.3g", ell.empty() ? "" : " ", l);
    lg("曲面: %d 点, ℓ = [%s], σf = %.3g; 峰 %d, 活跃 %d", static_cast<int>(S.ids.size()), ell.c_str(), S.ids.empty() ? 0.0 : std::sqrt(S.M.sf2) * S.M.ys,
       static_cast<int>(M), static_cast<int>(S.picks.size()));
    for (size_t a = 0; a < M; ++a) {
      const bool on = std::find(S.picks.begin(), S.picks.end(), pk[a]) != S.picks.end();
      lg("  %s μ %.4g  SE %.4g  观测 %.4g @ %s", on ? "*" : " ", mu[a], se_pk[a], ents[static_cast<size_t>(pk[a])].sc.g, at(pk[a]).c_str());
    }
  };

  // ---- 趟 0 采样 → 趟 1..: 每个活跃峰 c, 每槽 ± 方向: 步长从 ℓ_j/2 起 (夹到区间端), 落到已评点就减半再试; 新点 μ + sd − μ_c > SE_c 才评;
  //      回到 c / 不合法 / 本趟已提 / 不值得 → 该方向止. 新点去重拼成一趟; 没有新点 → 停 (fin). 评估引擎按后端只构造一次 ----
  stage_.store(SearchStage::Design, std::memory_order_relaxed);
  const bool fin = with_engine(scope.gpu, F.ex, cancel_, scope.gpu ? batch : kCpuForest, [&](auto &E) {
    if (handle(run_pass(E, ids, 0), ret))
      return false;
    for (int pass = 1;; ++pass) {
      fit();
      ids.clear();
      for (size_t q = 0; q < S.picks.size(); ++q) {
        const int c = S.picks[q];
        const std::vector<double> uc = ents[static_cast<size_t>(c)].u;
        const double se_c = ents[static_cast<size_t>(c)].sc.se;
        for (size_t j = 0; j < slots.size(); ++j)
          for (const int dir : {-1, 1}) {
            double step = 0.5 * S.M.ell[j] * (slots[j].u_hi - slots[j].u_lo);
            for (;;) {
              std::vector<double> u = uc;
              u[j] = std::clamp(uc[j] + dir * step, slots[j].u_lo, slots[j].u_hi);
              step = std::fabs(u[j] - uc[j]);
              if (!(step > 0.0))
                break;
              const int id = add_cand(u);
              if (id < 0 || id == c)
                break;
              if (ents[static_cast<size_t>(id)].pass >= 0) {
                step *= 0.5;
                continue;
              }
              if (std::find(ids.begin(), ids.end(), id) != ids.end())
                break;
              double mu = 0.0, sd = 0.0;
              surface::predict(S.M, space::unit(slots, ents[static_cast<size_t>(id)].u), mu, sd);
              if (mu + sd - S.pmu[q] > se_c)
                ids.push_back(id);
              break;
            }
          }
      }
      alive_.store(static_cast<int>(S.picks.size()), std::memory_order_relaxed);
      lg("细化第 %d 趟: 活跃峰 %d, 新点 %d", pass, static_cast<int>(S.picks.size()), static_cast<int>(ids.size()));
      if (ids.empty())
        return true;
      stage_.store(SearchStage::Refine, std::memory_order_relaxed);
      if (handle(run_pass(E, ids, pass), ret))
        return false;
    }
  });
  if (!fin)
    return ret;

  // ---- 终选: 最后一次曲面的活跃峰即 weights; picks 替换文件里同 h 的 weights ----
  std::vector<SearchWeight> ws;
  for (const int c : S.picks) {
    const Entry &en = ents[static_cast<size_t>(c)];
    SearchPick pk;
    pk.expr = en.e.canon;
    pk.score = en.sc.score, pk.ls = en.sc.ls, pk.ric = en.sc.ric, pk.sign = en.sc.sign;
    for (const int o : S.picks)
      if (o != c)
        pk.corr = std::max(pk.corr, rho(c, o));
    res.picks.push_back(std::move(pk));
    ws.push_back(SearchWeight{sp.hold, en.sc.sign, en.e.canon});
  }
  if (ws.empty()) {
    publish_result();
    return abort_with("没有统计上显著的候选 (μ ≤ SE), 文件不动");
  }
  WriteFactorWeights(scope.factor_dir, sp.file, sp.hold, ws); // 只写权重; 因子表数据由随后的 Run 用同一份目标从 stat 算
  res.wrote = true;
  res.n_weights = static_cast<int>(ws.size());
  res.message = "weights 已写入 (" + std::to_string(ws.size()) + " 个权重), 自动 Run 重算 stat";
  {
    int n_eval = 0;
    for (const Entry &en : ents)
      n_eval += en.pass >= 0;
    lg("完成: 写入 %d 个权重, 共评 %d 候选, %d 趟", static_cast<int>(ws.size()), n_eval, res.n_pass);
  }
  publish_result();
  return true;
}

} // namespace GUI::Factors
