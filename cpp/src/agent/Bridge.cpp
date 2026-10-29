// Bridge — 见头文件. agent/ 里唯一 include 项目因子 / 特征 / 服务头的 TU (CMake PRECISE: 因子契约头的消费者)
#include "agent/Bridge.hpp"

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

bool searchable(const F::FactorRow &r) { return r.error.empty() && !r.tmpl.empty() && r.tmpl != r.expr; }

// 一个因子文件 → 本持有期下的结果 (权重按 score 降序)
Result result_of(const F::FeatureTable &ft, const fs::path &p, int hold) {
  std::vector<F::FactorRow> rows;
  F::ScanFactorFile(p, ft, rows);
  assert(!rows.empty());
  const F::FactorRow &r0 = rows.front();
  Result r;
  r.file = r0.file, r.name = r0.name_en, r.name_cn = r0.name_cn, r.tmpl = r0.tmpl, r.note = r0.note;
  r.searched = fs::exists(p.parent_path() / "log" / (r0.name_en + ".log"));
  for (const F::FactorRow &w : rows) {
    if (!w.error.empty()) {
      if (r.error.empty())
        r.error = w.error;
      continue;
    }
    if (w.expr.empty())
      continue;
    Weight x;
    x.expr = w.expr, x.hold_src = w.hold_src;
    const factor::search::Score s = F::RowScore(w, hold);
    x.has = s.ok;
    if (s.ok) {
      x.score = s.score, x.ls = s.ls, x.ric = s.ric, x.mono = s.mono;
      x.valid = w.valid_pct / 100.0;
    }
    r.ws.push_back(std::move(x));
  }
  std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
  return r;
}

// 结果序: 有统计的在前, 最佳权重 score 降序
void sort_results(std::vector<Result> &v) {
  std::stable_sort(v.begin(), v.end(), [](const Result &a, const Result &b) {
    const Weight *x = a.best(), *y = b.best();
    return (x != nullptr) != (y != nullptr) ? x != nullptr : x && x->score > y->score;
  });
}

std::vector<Result> scan(const F::FeatureTable &ft, const std::string &dir, int hold) {
  std::vector<Result> out;
  for (const fs::path &p : F::list_factor_files(dir))
    out.push_back(result_of(ft, p, hold));
  sort_results(out);
  return out;
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

std::string GateMiss(const Gate &g, const Result &r) {
  if (!r.error.empty())
    return "BROKEN: " + r.error;
  for (const Weight &w : r.ws)
    if (w.has && w.mono >= g.mono && w.ric >= g.ric)
      return "";
  const Weight *w = r.best();
  if (!w)
    return "本持有期无显著权重";
  std::string s = "最佳权重未达门槛:";
  if (w->mono < g.mono)
    s += " mono " + num(w->mono) + " < " + num(g.mono);
  if (w->ric < g.ric)
    s += " rIC " + num(w->ric) + " < " + num(g.ric);
  return s;
}

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
  c.library = scan(*ft, F::factor_dir_of(data, sc.pool, level_id(sc.level)), sc.hold);
  return c;
}

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
  std::vector<Result> results;

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
  }
  bool search_wrote() const {
    std::lock_guard<std::mutex> lock(search.svc->mutex);
    return search.svc->result.wrote;
  }

  // 重扫一个文件, 结果表保序
  void refresh(const std::string &f) {
    Result r = result_of(*ft(), fs::path(dir) / f, sc.hold);
    const auto it = std::find_if(results.begin(), results.end(), [&](const Result &x) { return x.file == f; });
    if (it == results.end())
      results.push_back(std::move(r));
    else
      *it = std::move(r);
    sort_results(results);
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
    refresh(file);
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
  }
  I.data = &data;
  I.results = scan(*I.ft(), I.dir, I.sc.hold);
}

void Lab::stage(Staged &s) {
  Impl &I = *p_;
  const Cand &c = s.cand;
  if (!F::ValidFactorName(c.name))
    s.err = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
  else if (!F::ValidFactorNameCn(c.name_cn))
    s.err = "中文名须 1..10 个汉字";
  else
    s.file = F::AddFactorFile(I.dir, *I.ft(), c.name, c.name_cn, c.expr, c.note, s.err);
  if (!s.file.empty())
    I.refresh(s.file);
}

void Lab::adopt(Submitted &s, const Gate &g) {
  const Impl &I = *p_;
  const auto it = std::find_if(I.results.begin(), I.results.end(), [&](const Result &r) { return r.name == s.name; });
  if (it == I.results.end()) {
    s.err = "会话目录里没有这个候选";
    return;
  }
  s.err = GateMiss(g, *it);
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
  I.next();
}

void Lab::update() {
  Impl &I = *p_;
  if (I.step == Step::Idle || !I.lane([](const auto &l) { return l.finished(); }))
    return;
  if (I.lane([](const auto &l) { return l.cancelled(); })) {
    I.queue.clear();
    I.step = Step::Idle;
    return;
  }
  if (I.step == Step::Search && I.search_wrote())
    return I.send_run();
  I.settle();
  I.next();
}

void Lab::cancel() {
  Impl &I = *p_;
  I.queue.clear();
  if (I.step != Step::Idle)
    I.lane([](const auto &l) { l.cancel(); });
}

bool Lab::busy() const { return p_->step != Step::Idle; }

EvalState Lab::state() const {
  const Impl &I = *p_;
  EvalState s{.step = I.step, .done = I.done, .total = I.total, .file = I.file};
  if (I.step != Step::Idle)
    I.lane([&](const auto &l) { l.read(s); });
  return s;
}

const std::vector<Result> &Lab::results() const { return p_->results; }

const Result &Lab::result(const std::string &file) const {
  const std::vector<Result> &v = p_->results;
  const auto it = std::find_if(v.begin(), v.end(), [&](const Result &r) { return r.file == file; });
  assert(it != v.end() && "已落的候选应在结果里");
  return *it;
}

} // namespace agent
