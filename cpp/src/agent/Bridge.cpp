// Bridge — 见头文件. agent/ 里唯一 include 项目因子 / 特征 / 服务头的 TU (CMake PRECISE: 因子契约头的消费者)
#include "agent/Bridge.hpp"
#include "agent/Config.hpp" // kMaxOps / kGoodOps / kWorkDir

#include "factor/Expr.hpp"            // kOps / operand_tex
#include "factor/GpuRun.hpp"          // available
#include "factor/Search/Contract.hpp" // search_hold / Score
#include "gui/task_factors/services/FactorsFile.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/SearchService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // hold_desc
#include "shared/FeatureLib.hpp"              // feature_pools
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <iterator>
#include <map>
#include <mutex>

namespace agent {

namespace F = GUI::Factors;
namespace fs = std::filesystem;

namespace {

F::LevelId level_id(int level) {
  assert(level >= 0 && static_cast<size_t>(level) < F::kLevelCount);
  return static_cast<F::LevelId>(level);
}

// 字段表视图: 每层建一次 (GUI 线程独占)
std::shared_ptr<const F::FeatureTable> table(const SharedData &data, int level) {
  static F::FeatureTables tables;
  return F::table_of(tables, data.feature, level_id(level));
}

std::string num(double v) {
  char b[32];
  std::snprintf(b, sizeof(b), "%.3g", v);
  return b;
}

// 表达式的算子节点数 (解析不了 = 0, 由落文件报原因)
int ops_of(const F::FeatureTable &ft, const std::string &src) {
  factor::expr::Expr e;
  std::string err;
  return factor::expr::parse(src, ft.lookup(), ft.seg, e, err) ? e.n_ops : 0;
}

bool searchable(const F::FactorRow &r) { return r.error.empty() && !r.tmpl.empty() && r.tmpl != r.expr; }

using Tops = std::map<std::string, Top>; // 文件 → 最近一趟 Search 无显著时的 top

// 一个因子文件 → 本持有期下的结果 (权重按 score 降序, 结局随之定)
Result result_of(const F::FeatureTable &ft, const fs::path &p, int hold, const Tops &tops) {
  std::vector<F::FactorRow> rows;
  F::ScanFactorFile(p, ft, rows);
  assert(!rows.empty());
  const F::FactorRow &r0 = rows.front();
  Result r;
  r.file = r0.file, r.name = r0.name_en, r.name_cn = r0.name_cn, r.tmpl = r0.tmpl, r.note = r0.note;
  if (const auto it = tops.find(r.file); it != tops.end())
    r.top = it->second;
  bool run = true; // 权重都 Run 过
  for (const F::FactorRow &w : rows) {
    if (!w.error.empty()) {
      if (r.error.empty())
        r.error = w.error;
      continue;
    }
    if (w.expr.empty())
      continue;
    run = run && w.has_stat;
    Weight x;
    x.expr = w.expr, x.hold_src = w.hold_src;
    const factor::search::Score s = F::RowScore(w, hold);
    x.has = s.ok;
    if (s.ok) {
      x.st = {s.mono, s.ls, s.ric};
      x.score = s.score, x.valid = w.valid_pct / 100.0;
    }
    r.ws.push_back(std::move(x));
  }
  std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
  const bool searched = fs::exists(p.parent_path() / "log" / (r0.name_en + ".log"));
  const bool null = r.ws.empty() ? searched || r.top.has_value() : run; // 搜过无显著 / 权重都 Run 过却无本持有期统计
  r.verdict = !r.error.empty() ? Verdict::Broken : r.best() ? Verdict::Weights
                                               : null       ? Verdict::Null
                                                            : Verdict::Pending;
  return r;
}

// 结果序: 有统计的在前, 最佳权重 score 降序
void sort_results(std::vector<Result> &v) {
  std::stable_sort(v.begin(), v.end(), [](const Result &a, const Result &b) {
    const Weight *x = a.best(), *y = b.best();
    return (x != nullptr) != (y != nullptr) ? x != nullptr : x && x->score > y->score;
  });
}

// 一趟 Search 的结局 → 无显著时的 top (有终选或无可用候选 → 假)
bool top_of(const F::SearchResult &sr, Top &t) {
  if (!sr.picks.empty())
    return false;
  const F::SearchCand *b = nullptr;
  for (const F::SearchCand &c : sr.cands)
    if (c.sc.ok && (!b || c.sc.g > b->sc.g))
      b = &c;
  if (!b)
    return false;
  const factor::search::Score &s = b->sc;
  t.expr = b->expr, t.sign = s.sign;
  t.st = {s.mono, s.ls, s.ric};
  t.g = s.g, t.se = s.se;
  if (!(s.ls > 0.0))
    t.miss = "每期毛多空 ≤ 0";
  else if (!(s.ric > 0.0))
    t.miss = "rIC = 0";
  else if (s.mono < factor::search::kMinMono)
    t.miss = "mono " + num(s.mono) + " < " + num(factor::search::kMinMono);
  else
    t.miss = "不显著 (曲面峰值不超过标准误)";
  return true;
}

std::vector<Result> scan(const F::FeatureTable &ft, const std::string &dir, int hold, const Tops &tops) {
  std::vector<Result> out;
  for (const fs::path &p : F::list_factor_files(dir))
    out.push_back(result_of(ft, p, hold, tops));
  sort_results(out);
  return out;
}

// 达标 → 空; 否则 = 差在哪 (取 score 最高的权重说)
std::string gate_miss(const Gate &g, const Result &r) {
  if (r.verdict == Verdict::Broken)
    return "BROKEN: " + r.error;
  for (const Weight &w : r.ws)
    if (w.has && w.st.mono >= g.mono && w.st.ric >= g.ric)
      return "";
  const Weight *w = r.best();
  if (!w)
    return "本持有期无显著权重";
  std::string s = "最佳权重未达门槛:";
  if (w->st.mono < g.mono)
    s += " mono " + num(w->st.mono) + " < " + num(g.mono);
  if (w->st.ric < g.ric)
    s += " rIC " + num(w->st.ric) + " < " + num(g.ric);
  return s;
}

// 一道评估 (Search / Run 同构): 服务 + 发请求时的 epoch
template <class Svc>
struct Lane {
  std::unique_ptr<Svc> svc;
  uint64_t e0 = 0;

  void reset() { svc.reset(), svc = std::make_unique<Svc>(); } // 先析构旧的: Stop() join worker, 常驻块随之释放
  template <class Req>
  void send(Req req) {
    e0 = svc->epoch.load(std::memory_order_acquire);
    svc->Request(std::move(req));
  }
  // 请求发出后服务跑完一轮: epoch 越过发请求时的值 (已 begin) 且阶段落到终态 (已 end); 先读 epoch 再读阶段
  bool finished() const {
    if (svc->epoch.load(std::memory_order_acquire) <= e0)
      return false;
    const Phase ph = svc->phase.load(std::memory_order_acquire);
    return ph == Phase::Done || ph == Phase::Cancelled;
  }
  bool cancelled() const { return svc->phase.load(std::memory_order_acquire) == Phase::Cancelled; }
  void cancel() const { svc->RequestCancel(); }
  void read(EvalState &s) const {
    const size_t n = svc->total.load(std::memory_order_relaxed);
    s.frac = n > 0 ? static_cast<double>(svc->done.load(std::memory_order_relaxed)) / static_cast<double>(n) : 0.0;
    std::lock_guard<std::mutex> lock(svc->mutex);
    s.message = svc->message;
  }
};

} // namespace

// ---- catalog ----
namespace catalog {

std::vector<Choice> pools() {
  std::vector<Choice> out;
  for (size_t i = 0; i < feature_pools().size(); ++i)
    out.push_back({static_cast<int>(i), feature_pools()[i].name});
  return out;
}
std::string pool_cn(const std::string &pool) { return pool_def(pool).name_cn; }
std::vector<Choice> levels(const std::string &pool) {
  std::vector<Choice> out;
  const TierId tier = pool_def(pool).tier;
  for (const F::FactorLevel &l : F::kFactorLevels)
    if (F::level_for_pool(l, tier))
      out.push_back({l.id, l.name});
  return out;
}
std::vector<Choice> holds(const SharedData &data, int level) {
  std::vector<Choice> out;
  for (const F::LegCol &lg : table(data, level)->y.labels)
    if (factor::search::search_hold(lg.key))
      out.push_back({lg.key, factor::stat::hold_name(lg.key)});
  return out;
}
std::string hold_desc(int hold, int level) { return F::hold_desc(hold, level_id(level)); }
bool gpu() { return factor::gpu::available(); }

bool ready(const SharedData &data, const Scope &sc, bool gpu) {
  F::FactorScope probe;
  return F::MakeFactorScope(data, table(data, sc.level), sc.pool, gpu, probe);
}

Context context(const SharedData &data, const Scope &sc) {
  const std::shared_ptr<const F::FeatureTable> ft = table(data, sc.level);
  Context c;
  c.pool = sc.pool;
  c.pool_cn = pool_cn(sc.pool);
  c.level = F::factor_level(level_id(sc.level)).name;
  c.hold = factor::stat::hold_name(sc.hold);
  c.hold_desc = hold_desc(sc.hold, sc.level);
  c.seg = ft->seg;
  c.ts_root_d = factor::stat::ts_norm_d(ft->seg);
  c.max_d = factor::kMaxD;
  for (const F::FeatCol &l : ft->x.leaves) {
    if (!l.allowed)
      continue;
    const FeatureMetadata &m = ft->feature->meta(l.tier).features[l.level][l.col];
    c.leaves.push_back({l.code, m.name_cn, m.formula, m.description});
  }
  for (const factor::expr::OpInfo &o : factor::expr::kOps)
    c.ops.push_back({o.name, o.c_name, factor::expr::operand_tex(o), factor::dom_name(o.out), o.op, o.note});
  c.library = scan(*ft, F::factor_dir_of(data, sc.pool, level_id(sc.level)), sc.hold, {});
  return c;
}

std::string dir(const Scope &sc) {
  return (fs::path(kWorkDir) / sc.pool / F::factor_level(level_id(sc.level)).name / factor::stat::hold_name(sc.hold)).string();
}

std::vector<Result> tried(const SharedData &data, const Scope &sc) { return scan(*table(data, sc.level), dir(sc) + "/factors", sc.hold, {}); }

} // namespace catalog

// ---- Lab ----
struct Lab::Impl {
  const SharedData *data = nullptr;
  Scope sc;
  std::string dir;
  bool gpu = false;
  Lane<F::SearchService> search;
  Lane<F::FactorsService> run;
  // 队列
  Step step = Step::Idle;
  std::deque<std::string> queue;
  int total = 0, done = 0;
  std::string file;
  bool halt = false;      // 已 cancel: 在跑的那道收尾后不再续下一步
  std::deque<Tick> ticks; // 未取走的步
  std::vector<Result> results;
  Tops tops; // 同作用域跨会话留着

  std::shared_ptr<const F::FeatureTable> ft() const { return table(*data, sc.level); }

  // 在跑的那道
  template <class Fn>
  decltype(auto) lane(Fn &&fn) const {
    assert(step != Step::Idle);
    return step == Step::Search ? fn(search) : fn(run);
  }

  void send_search(const F::FactorRow &row) {
    F::SearchRequest req;
    [[maybe_unused]] const bool ok = F::MakeSearchRequest(*data, ft(), sc.pool, row, sc.hold, /*fresh=*/true, gpu, req);
    assert(ok && "bind 时已验过 ready");
    req.scope.factor_dir = dir;
    req.spec.keep = true;
    search.send(std::move(req));
    step = Step::Search;
    ticks.push_back({step, file, {}});
  }
  void send_run() {
    F::FactorsRequest req;
    [[maybe_unused]] const bool ok = F::MakeFactorsRequest(*data, ft(), sc.pool, /*evaluate=*/true, gpu, req);
    assert(ok && "bind 时已验过 ready");
    req.scope.factor_dir = dir;
    req.spec.only_file = file;
    req.spec.keep = true;
    run.send(std::move(req));
    step = Step::Run;
    ticks.push_back({step, file, {}});
  }
  // Search 跑完: 记下 top (无显著才有), 返回是否已落盘
  bool search_done() {
    std::lock_guard<std::mutex> lock(search.svc->mutex);
    const F::SearchResult &sr = search.svc->result;
    Top t;
    if (top_of(sr, t))
      tops[file] = std::move(t);
    else
      tops.erase(file);
    return sr.wrote;
  }

  // 重扫一个文件, 结果表保序
  const Result &refresh(const std::string &f) {
    const auto at = [&] { return std::find_if(results.begin(), results.end(), [&](const Result &x) { return x.file == f; }); };
    Result r = result_of(*ft(), fs::path(dir) / f, sc.hold, tops);
    if (const auto it = at(); it == results.end())
      results.push_back(std::move(r));
    else
      *it = std::move(r);
    sort_results(results);
    return *at();
  }
  // 出队下一个: 有自由参数 → Search (随后 Run); 具体式 → 直接 Run; BROKEN → 跳过
  void next() {
    step = Step::Idle;
    while (!queue.empty()) {
      file = std::move(queue.front());
      queue.pop_front();
      std::vector<F::FactorRow> rows;
      F::ScanFactorFile(fs::path(dir) / file, *ft(), rows);
      assert(!rows.empty());
      if (searchable(rows.front()))
        return send_search(rows.front());
      if (rows.front().error.empty() && !rows.front().expr.empty())
        return send_run();
      settle();
    }
    file.clear();
  }
  // 在评的文件评完
  void settle() {
    ++done;
    ticks.push_back({Step::Idle, file, refresh(file)});
  }
  // 在跑的那道跑完一轮 → 下一步
  void pump() {
    if (step == Step::Idle || !lane([](const auto &l) { return l.finished(); }))
      return;
    if (halt || lane([](const auto &l) { return l.cancelled(); })) {
      queue.clear();
      step = Step::Idle;
      return;
    }
    if (step == Step::Search && search_done())
      return send_run();
    settle();
    next();
  }
};

Lab::Lab() : p_(std::make_unique<Impl>()) {}
Lab::~Lab() = default;

void Lab::bind(const SharedData &data, const Scope &sc, std::string dir, bool gpu) {
  Impl &I = *p_;
  assert(I.step == Step::Idle && "评估队列忙");
  assert(catalog::ready(data, sc, gpu));
  if (!I.search.svc || sc != I.sc || dir != I.dir || gpu != I.gpu) {
    I.search.reset(), I.run.reset();
    I.sc = sc, I.dir = std::move(dir), I.gpu = gpu;
    I.tops.clear();
  }
  I.data = &data;
  I.ticks.clear();
  I.results = scan(*I.ft(), I.dir, I.sc.hold, I.tops);
}

void Lab::stage(Staged &s) {
  Impl &I = *p_;
  const Cand &c = s.cand;
  if (!F::ValidFactorName(c.name))
    s.err = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
  else if (!F::ValidFactorNameCn(c.name_cn))
    s.err = "中文名须 1..10 个汉字";
  else if (const int n = ops_of(*I.ft(), c.expr); n > kMaxOps)
    s.err = "算子 " + std::to_string(n) + " 个, 超过上限 " + std::to_string(kMaxOps) + " (建议 " + std::to_string(kGoodOps) + " 个左右)";
  else
    s.file = F::AddFactorFile(I.dir, *I.ft(), c.name, c.name_cn, c.expr, c.note, s.err);
  if (s.file.empty())
    s.res.verdict = Verdict::Unstaged;
  else
    s.res = I.refresh(s.file);
}

void Lab::adopt(Submitted &s, const Gate &g) {
  const Impl &I = *p_;
  const auto it = std::find_if(I.results.begin(), I.results.end(), [&](const Result &r) { return r.name == s.name; });
  if (it == I.results.end()) {
    s.err = "会话目录里没有这个候选";
    return;
  }
  s.err = gate_miss(g, *it);
  if (!s.err.empty())
    return;
  const fs::path to = fs::path(F::factor_dir_of(*I.data, I.sc.pool, level_id(I.sc.level))) / it->file;
  if (fs::exists(to)) {
    s.err = "正式目录已有 " + it->file;
    return;
  }
  fs::create_directories(to.parent_path());
  fs::copy_file(fs::path(I.dir) / it->file, to);
}

void Lab::eval(std::vector<std::string> files) {
  Impl &I = *p_;
  assert(I.step == Step::Idle && I.search.svc && "评估队列忙或未 bind");
  I.queue.assign(std::make_move_iterator(files.begin()), std::make_move_iterator(files.end()));
  I.total = static_cast<int>(I.queue.size()), I.done = 0;
  I.halt = false;
  I.next();
}

void Lab::cancel() {
  Impl &I = *p_;
  I.halt = true;
  I.queue.clear();
  I.ticks.clear();
  if (I.step != Step::Idle)
    I.lane([](const auto &l) { l.cancel(); });
}

bool Lab::busy() const { return p_->step != Step::Idle; }

std::optional<Tick> Lab::poll() {
  Impl &I = *p_;
  if (I.ticks.empty())
    I.pump();
  if (I.ticks.empty())
    return std::nullopt;
  Tick t = std::move(I.ticks.front());
  I.ticks.pop_front();
  return t;
}

EvalState Lab::live() const {
  const Impl &I = *p_;
  EvalState s{.step = I.step, .done = I.done, .total = I.total, .file = I.file};
  if (I.step != Step::Idle)
    I.lane([&](const auto &l) { l.read(s); });
  return s;
}

const std::vector<Result> &Lab::results() const { return p_->results; }

} // namespace agent
