// SearchService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Search/Cpu.hpp → Stat/Cpu.hpp), 依赖受控浮点:
// CMake PRECISE_MATH 源.
#include "gui/task_factors/services/SearchService.hpp"

#include "factor/Dag.hpp"
#include "factor/Search/Space.hpp"
#include "factor/Search/Surface.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // FactorFlow / Engine<Cpu | Gpu> / Job 管线
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
#include <limits>
#include <map>
#include <string>
#include <utility>

namespace GUI::Factors {

namespace {

namespace space = factor::search::space;
namespace surface = factor::search::surface;
namespace fs = factor::stat;
using factor::search::Score;
using Clock = std::chrono::steady_clock;

constexpr double kMiB = 1024.0 * 1024.0;

std::string vstrf(const char *f, va_list ap) {
  char buf[4096];
  const int n = std::vsnprintf(buf, sizeof(buf), f, ap);
  assert(n >= 0 && n < static_cast<int>(sizeof(buf)) && "格式化超长");
  (void)n;
  return buf;
}
__attribute__((format(printf, 1, 2))) std::string strf(const char *f, ...) {
  va_list ap;
  va_start(ap, f);
  std::string s = vstrf(f, ap);
  va_end(ap);
  return s;
}

// 过程日志 <factor_dir>/log/<因子名>.log: 每次 Search 覆盖; 逐行即刷 (断言崩了也留得下); 行首 = 距开跑秒数.
// 只记流程中间量 (分块 / 分位表 / 采样 / 曲面 / 峰 / 细化 / 相关), 终选结果看因子文件 weights
struct RunLog {
  std::ofstream f;
  Clock::time_point t0;
  __attribute__((format(printf, 2, 3))) void operator()(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    const std::string s = vstrf(fmt, ap);
    va_end(ap);
    f << strf("[%8.1fs] ", std::chrono::duration<double>(Clock::now() - t0).count()) << s << '\n';
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

// 一块的抽样行 (契约【多权重】): 统计区 [t_lo, t_hi) 里绝对行 ≡ 0 (mod stride) 的行 = 块内行 t_first + i·stride (i < n_rows)
struct Samp {
  int t_first = 0, n_rows = 0;
};
Samp samp_of(int t_hi, int t_lo, int t0_abs, int stride) {
  int t = t_lo;
  while ((t0_abs + t) % stride != 0)
    ++t;
  return {t, t < t_hi ? (t_hi - 1 - t) / stride + 1 : 0};
}
// 候选的 r16 抽样行: 按块序拼进 dst [R][A] (off = 已落行数)
struct SampleP : ProbeBase {
  static constexpr uint8_t need = kNeedRanks;
  std::vector<uint16_t> *dst = nullptr;
  int stride = 1, off = 0;
  void root(const Ctx &c, const RootView &v) {
    const fs::Span s = c.s();
    const Samp sm = samp_of(s.t_hi(), s.t_lo, s.t0_abs, stride);
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

// 一轮搜索 (eval 内建; 流程见头文件【搜索流程】): plan → prepare → design → 趟 0 → refine (趟 1..) → finish.
// 步骤返回 false = 停, eval 返回 ret (true = 正常输入问题, 已进 message; false = 取消 / 判废)
struct SearchService::Searcher {
  SearchService &W;
  FactorFlow &F;
  const FactorScope &scope;
  const FeatureTable &ft;
  const SearchSpec &sp;
  const int seg;
  const fs::Holds hd; // 给定的持有期 (字段表已只收本层合法 (hold_ok) 的)
  const Clock::time_point t_start = Clock::now();
  SearchResult res;
  RunLog lg;
  factor::expr::Expr e0;          // 模板
  factor::Dag d0;                 // 占位具体化的森林: 结构 / 特征叶序 / 槽数与任一候选同 (值域检查 plan_forest 按根归因, 不融合)
  std::vector<space::Slot> slots; // 可搜槽 (区间要 A, 装完块 0 才建)
  Job job;                        // 块归作业: P.cache_all ? 整轮缓存全部块 : 一块复用 (ensure_block 按日段 + 块规格判, 键同则不重读)
  std::vector<uint64_t> gens;     // [块位] 上次记过装载的块代 (loaded)
  int A = 0, stride = 1, R = 0;   // 资产数 / r16 抽样步长 (绝对行 ≡ 0 mod stride, 全作用域约 kCorrRows 行) / 每候选抽样行数 (各块拼)
  int n_chunks = 0, batch = 1;    // 块数 / 一张森林的候选数 (引擎 batch_of)
  std::vector<Entry> ents;        // 候选表: 规范串去重
  std::map<std::string, int> seen;
  std::map<std::pair<int, int>, double> rho_raw; // 截面相关 (不定向), 按需现算
  Surface S;
  bool ret = false;

  Searcher(SearchService &w, FactorFlow &f, const SearchRequest &req)
      : W(w), F(f), scope(req.scope), ft(*req.scope.ft), sp(req.spec), seg(ft.seg), hd(keys_of<fs::Holds>(std::vector<int>{sp.hold})) {
    assert(find_leg(ft.y.labels, sp.hold) && "Search 的持有期须是字段表标签组之一");
    assert(factor::search::search_hold(sp.hold) && "Search 只搜 kSearchHolds 档");
    fs::assert_holds(hd, seg);
    res.file = sp.file;
    res.tmpl = sp.tmpl;
    res.hold = sp.hold;
    res.backend = scope.gpu ? Engine<Gpu>::kName : Engine<Cpu>::kName;
    {
      std::lock_guard<std::mutex> lock(W.mutex);
      W.result = res;
      W.message.clear();
    }
    W.publish();
    lg.t0 = t_start;
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

  // 全流程 (引擎 E 按后端只构造一次); 停 → false
  template <class E>
  bool run(E &eng) {
    std::vector<int> ids;
    if (!plan<E>() || !prepare() || !design(ids))
      return false;
    W.stage_.store(SearchStage::Design, std::memory_order_relaxed);
    return go(run_pass(eng, ids, 0)) && refine(eng, ids);
  }

  // ---- 结局 ----
  bool cancelled() const { return W.cancel_.load(std::memory_order_relaxed); }
  // Ok → 续; 其余 → settle: Empty / Broken (正常输入问题, job.why 已填) → give_up; Cancel / Stale → abort_out (判废)
  bool go(Rc rc) {
    if (rc == Rc::Ok)
      return true;
    if (rc == Rc::Cancel)
      lg("已取消");
    else if (rc == Rc::Stale)
      lg("中止: %s", kStaleMsg);
    ret = W.settle(rc, job.why, [&](const std::string &why) {
      publish_result();
      give_up(why);
    });
    return false;
  }
  // 正常输入问题 (作用域 / 模板 / 值域 / 无显著候选): 发布当前 res + message, 停
  bool give_up(std::string msg) {
    lg("中止: %s", msg.c_str());
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    std::lock_guard<std::mutex> lock(W.mutex);
    W.result = res;
    W.message = std::move(msg);
    ret = true;
    return false;
  }
  void publish_result() {
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    res.cands.clear();
    for (const Entry &en : ents) {
      if (en.pass < 0)
        continue;
      res.cands.push_back(SearchCand{en.e.canon, en.u, en.pass, en.sc});
    }
    std::stable_sort(res.cands.begin(), res.cands.end(), [](const SearchCand &a, const SearchCand &b) { return a.sc.score > b.sc.score; });
    {
      std::lock_guard<std::mutex> lock(W.mutex);
      W.result = res;
    }
    W.publish();
  }

  // ---- 模板 → 作用域 (评估期 = 模板叶所用库落盘日交集) → 预算 → 分块 (FactorsEngine plan_job; 见头文件【分块流式】) ----
  //   load   宿主作用域块: 叶 + Y (截面门控 + 所搜持有期的标签, 不装成本)
  //   work   宿主引擎 (随绑定的块重开) / dev 显存: 引擎 E::budget 的批核口径 (d0 = 一个候选)
  //   fixed  r16 常驻: 每候选 (抽样行 × A × 2) 字节; 预留趟 0 的 kInit 个, 细化的随评随增
  // 多块 → plan_job 先预扫全期门控定满仓只数 job.spec.y.cap (与 Run 同一个 cap; 各块装载即用它, 引擎的 Search 会话取 y.n_full)
  template <class E>
  bool plan() {
    e0 = ft.reparse(sp.tmpl);
    // 前后带 (bands_of): 统计首行的 lag 行 / 交接链所及的行也须整窗, 末统计行的续持要看后一批
    const int look = space::lookback(e0);
    job.bands = bands_of(look, hd, seg);
    lg("前带: 模板回看 %d 行 + 持有期 %d 行 + 交接链 %d 天 → %d 天; 后带 %d 天", look, factor::search::hold_reach(hd, seg), fs::kChainDays,
       job.bands.front, job.bands.back);
    const factor::expr::Expr ep = e0.placeholder();
    d0 = factor::build_forest({&ep}, /*keep_roots=*/false, /*fuse=*/false);
    if (!open_job(F, scope, d0.feats, job))
      return go(Rc::Empty);
    const std::vector<misc::Ymd> &dates = job.dm.dates;
    const int days = static_cast<int>(dates.size());
    A = static_cast<int>(job.dm.rs.axis.size());
    lg("评估期 %s ~ %s (%d 天, 所用库落盘日交集)", misc::dash(dates.front()).c_str(), misc::dash(dates.back()).c_str(), days);
    stride = std::max(1, days * seg / factor::search::kCorrRows);

    job.spec.y = YSpec{hd, Amts{}, 0.f};
    job.own = !sp.keep;
    job.retain = !sp.keep;
    job.who = "Search";
    const double per_day = static_cast<double>(seg) * A;
    const double r16_bytes = (static_cast<double>(days) * seg / stride + 1.0) * A * sizeof(uint16_t);
    const size_t nx = job.own ? job.spec.x.size() : resident_leaves(F.c, block_key(ft, job.dm, job.whole(), job.spec), job.spec.x, job.retain);
    Budget bud = budget_of<E, SearchProbes>(job, d0, nx, hd.n);
    bud.fixed += factor::search::kInit * r16_bytes;
    const Clock::time_point t_plan = Clock::now();
    if (!go(plan_job(F, job, bud)))
      return false;
    const Plan &P = job.P;
    n_chunks = static_cast<int>(P.chunks.size());
    batch = E::batch_of(P, bud, per_day);
    if (n_chunks > 1)
      lg("预扫全期门控: %.1fs", std::chrono::duration<double>(Clock::now() - t_plan).count());
    res.n_chunk = n_chunks, res.chunk_days = P.D, res.batch = batch;
    res.n_assets = A;
    for (const Chunk &k : P.chunks)
      res.chunk_plan.push_back(SearchChunk{k.days(), k.days() * per_day * bud.load / kMiB});
    W.chunks_.store(n_chunks, std::memory_order_relaxed);
    publish_result(); // 装载前先发布分块计划
    lg("数据: %d 只 × %d 天, 特征 %d 列, 标签 %d 列, 共享 DAG 槽 %d; 每格 %.0f 字节 (宿主装载) + %.0f (CPU 工作区)", A, days, static_cast<int>(d0.feats.size()),
       hd.n, d0.n_slots, bud.load, bud.work);
    lg("预算: 内存 %.0f MB%s; 块 ≤ %d 天 + 前带 %d 天 + 后带 %d 天, 共 %d 块, 全部常驻 %s (需 %.0f MB)%s", P.host / kMiB,
       scope.gpu ? strf(", 显存 %.0f MB", P.vram / kMiB).c_str() : "", P.D, job.bands.front, job.bands.back, n_chunks, P.cache_all ? "是" : "否 (每趟逐块重读)", P.cache / kMiB,
       scope.gpu ? strf(", GPU 批 %d", batch).c_str() : "");
    for (int ci = 0; ci < n_chunks; ++ci) {
      const Chunk &k = P.chunks[static_cast<size_t>(ci)];
      lg("  块 %d: 统计 %s ~ %s (%d 天), 装载 %d 天, %.0f MB", ci + 1, misc::dense(dates[k.s0]).c_str(), misc::dense(dates[k.s1 - 1]).c_str(),
         static_cast<int>(k.s1 - k.s0), k.days(), res.chunk_plan[static_cast<size_t>(ci)].mb);
    }
    // r16 抽样行 (契约【多权重】): 各块的抽样行 (samp_of) 按块序拼成每候选一张 [R][A]
    for (const Chunk &k : P.chunks)
      R += samp_of(static_cast<int>(k.s1 - k.d0) * seg, static_cast<int>(k.s0 - k.d0) * seg, static_cast<int>(k.d0) * seg, stride).n_rows;
    gens.assign(std::max<size_t>(job.blocks.own.size(), 1), 0);
    return true;
  }
  // 块 ci 刚就位 (pass 的块钩子 / prepare): 真读了 (块的代变了) 才记
  void loaded(const Block &c, size_t ci, double load_s) {
    uint64_t &g = gens[job.P.cache_all ? ci : 0];
    if (c.gen == g)
      return;
    g = c.gen;
    lg("装载块 %d/%d: %d 天, %.1fs", static_cast<int>(ci) + 1, n_chunks, job.P.chunks[ci].days(), load_s);
  }

  // ---- 准备: 先装块 0 (趟 0 反正要) → 叶值域 → A = n_full · kGroups 定槽区间 → 阈值槽分位表 ----
  bool prepare() {
    const Clock::time_point t_load = Clock::now();
    if (!go(load_block(F, job, 0)))
      return false;
    loaded(job.blocks.of(0), 0, std::chrono::duration<double>(Clock::now() - t_load).count());
    { // 分位表要在块 0 上走模板前缀: 先查叶值域 (各块的候选森林由 pass 各查一次)
      std::vector<std::string> why;
      std::vector<uint8_t> need;
      if (plan_forest(F.ex, job.blocks.of(0), d0, why, need) == 0) {
        job.why = why[0];
        return go(Rc::Broken);
      }
    }
    res.n_full = job.blocks.of(0).y.n_full;
    lg("满仓一腿 %.1f 只 (全期池内平均有效资产数 / %d 组, 各块共用)", res.n_full, fs::kGroups);
    const double a_eff = res.n_full * fs::kGroups;
    slots = space::slots_of(e0, seg, a_eff);
    assert(slots.empty() == !e0.is_template());
    for (const space::Slot &s : slots)
      res.slots.push_back(s.name);
    if (slots.empty())
      lg("无自由参数: 只评模板本身 (定方向 + 判显著)");
    else
      lg("自由参数 %d 个 (区间按池内平均有效资产数 %.0f 定):", static_cast<int>(slots.size()), a_eff);
    for (const space::Slot &s : slots)
      lg("  %-24s 坐标 %-8s 区间 [%.3g, %.3g] 中性 u0=%.3g%s", s.name.c_str(), coord_name(s), s.u_lo, s.u_hi, s.u0, s.integer ? " 整数" : "");
    // 阈值槽 (ANY / GE0) 由深到浅 (前序逆序: 子树先于祖先)
    for (size_t jj = slots.size(); jj-- > 0;)
      if (space::needs_qtab(slots[jj]) && !qtab(slots[jj]))
        return false;
    return true;
  }
  // 阈值槽 sl 的分位表: 中性具体式在 CPU 上走到该槽的输入节点即止, 取块 0 抽样行的池内有效值 (GE0: |x|) 升序.
  // 恒走 CPU (要宿主平面), 引擎用完即放 (活槽不与评估引擎同在); 输入是常量 → 整个搜索放弃
  bool qtab(space::Slot &sl) {
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
    const Block &c = job.blocks.of(0);
    const fs::Span s = c.span();
    const Samp sm = samp_of(s.t_hi(), s.t_lo, s.t0_abs, stride);
    std::vector<float> tab;
    Engine<Cpu> qe(F.ex, W.cancel_);
    qe.bind(c);
    if (!qe.walk(dc, prefix.data(), [&](int i, double) {
          if (i == want) {
            const factor::check::Plane *p = qe.plane(i);
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
    if (tab.size() < 2 || !(tab.front() < tab.back()))
      return give_up("阈值槽 " + sl.name + " 的输入在块 0 抽样行上是常量, 搜不动");
    const size_t n_tab = tab.size();
    space::set_qtab(sl, std::move(tab));
    lg("分位表: %s ← %zu 个值 (输入 %s), 区间 [%s, %s], 中性 %s", sl.name.c_str(), n_tab, key.c_str(), slot_val(sl, sl.u_lo).c_str(), slot_val(sl, sl.u_hi).c_str(),
       slot_val(sl, sl.u0).c_str());
    return true;
  }

  // ---- 候选 ----
  // 坐标 u → 候选 id (规范串去重; -1 = 参数不合法)
  int add_cand(const std::vector<double> &u) {
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
  }
  // 候选 id 的方向 + 参数 (日志)
  std::string params_at(int id) const {
    const Entry &en = ents[static_cast<size_t>(id)];
    return (en.sc.sign < 0 ? std::string("取反 ") : std::string()) + params_of(slots, en.u);
  }
  // 趟 0 的点: R_d 低差异采样 (Space design), 不合法的丢, 取整去重; 无自由参数 → 模板本身一个点
  bool design(std::vector<int> &ids) {
    if (slots.empty()) {
      ids.push_back(add_cand({}));
      return true;
    }
    int n_bad = 0;
    for (const std::vector<double> &u : space::design(slots, factor::search::kInit)) {
      const int id = add_cand(u);
      if (id < 0)
        ++n_bad;
      else if (std::find(ids.begin(), ids.end(), id) == ids.end())
        ids.push_back(id);
    }
    lg("采样: R_d %d 点 → 不合法 %d, 取整去重后 %d 候选; r16 抽样步长 %d 行 (每候选 %d 行)", factor::search::kInit, n_bad, static_cast<int>(ids.size()), stride, R);
    if (ids.empty())
      return give_up("采样点无一参数合法 (值域互斥?), 文件不动");
    return true;
  }

  // ---- 截面相关 (不定向): 两候选抽样行逐行 Pearson (Stat/Host lag_row, lag 行换成另一候选) 的均值; 无一有效行 → 0 (视作不相关). 按需现算并缓存 ----
  void ensure_rho(const std::vector<int> &ids) {
    std::vector<std::pair<int, int>> need;
    for (size_t a = 0; a < ids.size(); ++a)
      for (size_t b = a + 1; b < ids.size(); ++b) {
        const std::pair<int, int> pr(std::min(ids[a], ids[b]), std::max(ids[a], ids[b]));
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
  }
  // 定向后的持仓相关 (两者方向相反 → 原值相关取负); 先 ensure_rho
  double rho(int a, int b) const {
    if (a == b)
      return 1.0;
    const auto it = rho_raw.find({std::min(a, b), std::max(a, b)});
    assert(it != rho_raw.end());
    return ents[static_cast<size_t>(a)].sc.sign * ents[static_cast<size_t>(b)].sc.sign * it->second;
  }

  // ---- 一趟: ids (全是待评) 沿全部块流过 (FactorsEngine pass, 引擎 E: 每批一张共享森林, plant 收前 ≤ batch 个, GPU 批核再按显存减半;
  //      逐候选进探针 HoldP 续攒 Accum + SampleP 抽样 r16, 末块即 finish), 收尾打分 + 发布. 坏根 (某块叶值域越界) 不打分 (score −∞) ----
  template <class E>
  Rc run_pass(E &eng, const std::vector<int> &ids, int pass) {
    assert(!ids.empty());
    W.pass_.store(pass, std::memory_order_relaxed);
    W.chunk_.store(0, std::memory_order_relaxed);
    std::vector<Root> roots;
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      assert(en.pass < 0 && "每个候选只评一次");
      en.ps = SearchProbes{};
      en.ps.hd = hd;
      en.r16.assign(static_cast<size_t>(R) * A, fs::kRankNone);
      en.ps.at<SampleP>().dst = &en.r16; // ents 本趟不再增 → 地址稳
      en.ps.at<SampleP>().stride = stride;
      roots.push_back({&en.e, sp.frame});
    }
    const Clock::time_point t_pass = Clock::now();
    // 进度: Running 期 = 候选 × 块 (每块重开, 前面的块已评完)
    struct Hooks : HookBase {
      Searcher &S;
      size_t n;
      double load_s = 0.0;
      void block(const Block &c, size_t ci, double s) {
        S.loaded(c, ci, s);
        load_s += s;
        S.W.chunk_.store(static_cast<int>(ci), std::memory_order_relaxed);
        S.W.begin(Phase::Running, n * static_cast<size_t>(S.n_chunks));
        S.W.publish(n * ci);
      }
      void done(int) { S.W.publish(1); }
    } hooks{{}, *this, ids.size()};
    const Rc rc = GUI::Factors::pass(eng, F, job, roots, batch, [&](int k) -> SearchProbes & { return ents[static_cast<size_t>(ids[static_cast<size_t>(k)])].ps; }, hooks);
    const double load_s = hooks.load_s;
    if (rc != Rc::Ok)
      return rc;
    W.chunk_.store(n_chunks, std::memory_order_relaxed);
    for (int id : ids) {
      Entry &en = ents[static_cast<size_t>(id)];
      const HoldP &hp = en.ps.at<HoldP>();
      en.pass = pass;
      if (hp.n_hold == 0) // 坏根: 没 finish
        continue;
      assert(en.ps.at<SampleP>().off == R && "各块抽样行须拼满");
      assert(hp.n_hold == 1);
      en.sc = factor::search::score_signed(hp.acc[0], hp.hold[0].hold, hp.hold[0].seg);
    }
    int n_pos = 0, top = -1; // top = 定向目标 g 最高的候选 (不看门槛)
    double best = -std::numeric_limits<double>::infinity();
    for (int id : ids) {
      const Score &sc = ents[static_cast<size_t>(id)].sc;
      n_pos += std::isfinite(sc.score);
      best = std::max(best, sc.score);
      if (sc.ok && (top < 0 || sc.g > ents[static_cast<size_t>(top)].sc.g))
        top = id;
    }
    const double pass_s = std::chrono::duration<double>(Clock::now() - t_pass).count();
    lg("第 %d 趟完成: %d 候选 × %d 块, %.1fs (装载 %.1fs, 评估 %.1fs, %.1f ms/候选·块) | 正分 %d 最高 %.4g", pass, static_cast<int>(ids.size()), n_chunks, pass_s,
       load_s, pass_s - load_s, 1e3 * (pass_s - load_s) / (static_cast<double>(ids.size()) * n_chunks), n_pos, best);
    if (top >= 0) {
      const Score &sc = ents[static_cast<size_t>(top)].sc;
      lg("  g 最高 %.3g ± %.3g: 每期毛多空 %.2fbp rIC %.4f mono %+.3f @ %s", sc.g, sc.se, 1e4 * sc.ls, sc.ric, sc.mono, params_at(top).c_str());
    }
    res.n_pass = pass + 1;
    publish_result();
    return Rc::Ok;
  }

  // ---- 曲面: 已评且 ok 的点 (x = 单位立方坐标, y = g, 噪声 = SE) 拟合 GP → 峰 → pick_diverse 得活跃峰 (= 当前终选).
  //      无自由参数: 唯一的点即峰, μ = 观测 g ----
  void fit() {
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
    if (!S.ids.empty() && slots.empty()) {
      S.mu = y;
      if (std::isfinite(ents[static_cast<size_t>(S.ids[0])].sc.score))
        S.peaks = {0};
    } else if (!S.ids.empty()) {
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
    if (slots.empty())
      lg("单点: 峰 %d, 活跃 %d", static_cast<int>(M), static_cast<int>(S.picks.size()));
    else
      lg("曲面: %d 点, ℓ = [%s], σf = %.3g; 峰 %d, 活跃 %d", static_cast<int>(S.ids.size()), ell.c_str(), S.ids.empty() ? 0.0 : std::sqrt(S.M.sf2) * S.M.ys,
         static_cast<int>(M), static_cast<int>(S.picks.size()));
    for (size_t a = 0; a < M; ++a) {
      const bool on = std::find(S.picks.begin(), S.picks.end(), pk[a]) != S.picks.end();
      const Score &sc = ents[static_cast<size_t>(pk[a])].sc;
      lg("  %s μ %.4g  SE %.4g  观测 %.4g  mono %+.3f @ %s", on ? "*" : " ", mu[a], se_pk[a], sc.g, sc.mono, params_at(pk[a]).c_str());
    }
  }
  // ---- 趟 1..: 每趟前拟合曲面; 每个活跃峰 c, 每槽 ± 方向: 步长从 ℓ_j/2 起 (夹到区间端), 落到已评点就减半再试; 新点 μ + sd − μ_c > SE_c 才评;
  //      回到 c / 不合法 / 本趟已提 / 不值得 → 该方向止. 新点去重拼成一趟; 没有新点 → 停 (true, 末次曲面即终选) ----
  template <class E>
  bool refine(E &eng, std::vector<int> &ids) {
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
      W.alive_.store(static_cast<int>(S.picks.size()), std::memory_order_relaxed);
      lg("细化第 %d 趟: 活跃峰 %d, 新点 %d", pass, static_cast<int>(S.picks.size()), static_cast<int>(ids.size()));
      if (ids.empty())
        return true;
      W.stage_.store(SearchStage::Refine, std::memory_order_relaxed);
      if (!go(run_pass(eng, ids, pass)))
        return false;
    }
  }

  // 无显著时的代表点: 过门槛的峰里 μ 最高的 (差在显著); 没有峰 → 可用点里 mono 最高的 (差在分层); 一个可用点都没有 → 无
  std::optional<SearchTop> top_of() const {
    int id = -1;
    double mu = 0.0;
    if (!S.peaks.empty()) {
      id = S.ids[static_cast<size_t>(S.peaks[0])], mu = S.mu[static_cast<size_t>(S.peaks[0])];
    } else {
      for (int i = 0; i < static_cast<int>(ents.size()); ++i) {
        const Score &sc = ents[static_cast<size_t>(i)].sc;
        if (ents[static_cast<size_t>(i)].pass >= 0 && sc.ok &&
            (id < 0 || sc.mono > ents[static_cast<size_t>(id)].sc.mono || (sc.mono == ents[static_cast<size_t>(id)].sc.mono && sc.g > ents[static_cast<size_t>(id)].sc.g)))
          id = i;
      }
    }
    if (id < 0)
      return std::nullopt;
    const Entry &en = ents[static_cast<size_t>(id)];
    return SearchTop{en.e.canon, en.sc.sign, en.sc.mono, en.sc.ls, en.sc.ric, en.sc.g, en.sc.se, mu, !S.peaks.empty()};
  }

  // ---- 终选: 最后一次曲面的活跃峰即 weights; picks 替换文件里同 h 的 weights (sp.fresh: 先清空全部); 空 picks 也写 (= 清掉该 h).
  //      结局 (评估期 + 无显著时的代表点) 同落 search[] ----
  void finish() {
    std::vector<SearchWeight> ws;
    for (const int c : S.picks) {
      const Entry &en = ents[static_cast<size_t>(c)];
      SearchPick pk;
      pk.expr = en.e.canon;
      pk.sc = en.sc;
      for (const int o : S.picks)
        if (o != c)
          pk.corr = std::max(pk.corr, rho(c, o));
      res.picks.push_back(std::move(pk));
      ws.push_back(SearchWeight{sp.hold, en.sc.sign, en.e.canon});
    }
    const std::vector<misc::Ymd> &dates = job.dm.dates;
    SearchMark mk{sp.hold, dates.front(), dates.back(), static_cast<int>(dates.size()), ws.empty() ? top_of() : std::nullopt};
    WriteFactorSearch(scope.factor_dir, sp.file, sp.fresh, mk, ws); // 不写 stat; 因子表数据由随后的 Run 用同一份目标从 stat 算
    res.wrote = true;
    res.n_weights = static_cast<int>(ws.size());
    if (mk.top)
      lg("无显著, 代表点 (%s): g %.3g ± %.3g μ %.3g mono %+.3f @ %s", mk.top->peak ? "峰" : "mono 最高", mk.top->g, mk.top->se, mk.top->mu, mk.top->mono,
         mk.top->expr.c_str());
    {
      std::lock_guard<std::mutex> lock(W.mutex);
      W.message = ws.empty() ? strf("没有显著 (μ > SE) 且 mono ≥ %.2f 的候选, 已清掉该 Hold 的权重", factor::search::kMinMono)
                             : "weights 已写入 (" + std::to_string(ws.size()) + " 个权重), 自动 Run 重算 stat";
    }
    int n_eval = 0;
    for (const Entry &en : ents)
      n_eval += en.pass >= 0;
    lg("完成: 写入 %d 个权重, 共评 %d 候选, %d 趟", static_cast<int>(ws.size()), n_eval, res.n_pass);
    publish_result();
  }
};

bool MakeSearchRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, int hold, bool fresh,
                       bool gpu, SearchRequest &req) {
  assert(!row.tmpl.empty() && "Search 只接模板解析过的因子");
  req = SearchRequest{};
  req.spec = SearchSpec{row.file, row.tmpl, row.frame, hold, fresh};
  return MakeFactorScope(data, std::move(ft), pool, gpu, req.scope);
}

SearchService::SearchService() { Start(); }
SearchService::~SearchService() { Stop(); }

bool SearchService::run(SearchRequest &req) {
  pass_.store(0, std::memory_order_relaxed);
  chunk_.store(0, std::memory_order_relaxed);
  chunks_.store(0, std::memory_order_relaxed);
  stage_.store(SearchStage::Prepare, std::memory_order_relaxed);
  const bool ok = eval(req, flow());
  if (req.spec.keep)
    flow().drop_engines(); // 块留给会话的下一个请求, 引擎照弃
  else
    flow().drop(); // 块 / 引擎只活一轮 (宿主 / 显存不跨请求占)
  return ok;
}

bool SearchService::eval(const SearchRequest &req, FactorFlow &F) {
  TraceN("SearchRun");
  Searcher s(*this, F, req);
  if (!F.with_engine<SearchProbes>(req.scope.gpu, [&](auto &E) { return s.run(E); }))
    return s.ret;
  s.finish();
  return true;
}

} // namespace GUI::Factors
