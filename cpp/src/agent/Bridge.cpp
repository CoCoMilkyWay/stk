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
#include <filesystem>
#include <mutex>

namespace agent {

namespace F = GUI::Factors;
namespace fs = std::filesystem;

namespace {

F::LevelId level_id(int level) {
  assert(level >= 0 && static_cast<size_t>(level) < F::kLevelCount);
  return static_cast<F::LevelId>(level);
}

// 请求发出后服务跑完一轮: epoch 越过发请求时的值 (已 begin) 且阶段落到终态 (已 end); 先读 epoch 再读阶段
bool finished(const Progress &p, uint64_t e0) {
  if (p.epoch.load(std::memory_order_acquire) <= e0)
    return false;
  const Phase ph = p.phase.load(std::memory_order_acquire);
  return ph == Phase::Done || ph == Phase::Cancelled;
}

bool searchable(const F::FactorRow &r) { return r.error.empty() && !r.tmpl.empty() && r.tmpl != r.expr; }

} // namespace

struct Bridge::Impl {
  F::FeatureTables tables;
  // 会话 (作用域 + 目录 + 后端): 两个 worker 的常驻块随它, 换了就重建
  Scope cur;
  std::string dir;
  bool gpu = false;
  std::unique_ptr<F::SearchService> searcher;
  std::unique_ptr<F::FactorsService> runner;
  // 队列
  enum class Step : uint8_t { Idle,
                              Search,
                              Run };
  Step step = Step::Idle;
  std::vector<std::string> queue;
  int total = 0, done = 0;
  std::string file;
  uint64_t e0 = 0;
  uint64_t evals = 0;

  std::shared_ptr<const F::FeatureTable> ft(const SharedData &data, int level) { return F::table_of(tables, data.feature, level_id(level)); }

  void search(const SharedData &data, const F::FactorRow &row) {
    F::SearchRequest req;
    const bool ok = F::MakeSearchRequest(data, ft(data, cur.level), cur.pool, row, cur.hold, /*fresh=*/true, gpu, req);
    assert(ok && "作用域 eval 时已验过");
    req.scope.factor_dir = dir;
    req.spec.keep = true;
    e0 = searcher->epoch.load(std::memory_order_acquire);
    step = Step::Search;
    searcher->Request(std::move(req));
  }
  void run(const SharedData &data) {
    F::FactorsRequest req;
    const bool ok = F::MakeFactorsRequest(data, ft(data, cur.level), cur.pool, /*evaluate=*/true, gpu, req);
    assert(ok && "作用域 eval 时已验过");
    req.scope.factor_dir = dir;
    req.spec.only_file = file;
    req.spec.keep = true;
    e0 = runner->epoch.load(std::memory_order_acquire);
    step = Step::Run;
    runner->Request(std::move(req));
  }
  // 出队下一个: 有自由参数 → Search (随后 Run); 具体式 → 直接 Run; BROKEN → 跳过
  void next(const SharedData &data) {
    step = Step::Idle;
    while (!queue.empty()) {
      file = std::move(queue.front());
      queue.erase(queue.begin());
      std::vector<F::FactorRow> rows;
      F::ScanFactorFile(fs::path(dir) / file, *ft(data, cur.level), rows);
      assert(!rows.empty());
      if (searchable(rows.front()))
        return search(data, rows.front());
      if (rows.front().error.empty() && !rows.front().expr.empty())
        return run(data);
      ++done, ++evals;
    }
    file.clear();
  }
};

Bridge::Bridge() : p_(std::make_unique<Impl>()) {}
Bridge::~Bridge() = default;

// ---- ① 作用域 ----
std::vector<Choice> Bridge::pools() {
  std::vector<Choice> out;
  for (size_t i = 0; i < feature_pools().size(); ++i)
    out.push_back({static_cast<int>(i), feature_pools()[i].name});
  return out;
}
std::string Bridge::pool_cn(const std::string &pool) { return pool_def(pool).name_cn; }
std::vector<Choice> Bridge::levels(const std::string &pool) {
  std::vector<Choice> out;
  const TierId tier = pool_def(pool).tier;
  for (const F::FactorLevel &l : F::kFactorLevels)
    if (F::level_for_pool(l, tier))
      out.push_back({l.id, l.name});
  return out;
}
std::vector<Choice> Bridge::holds(const SharedData &data, int level) {
  std::vector<Choice> out;
  for (const F::LegCol &lg : p_->ft(data, level)->y.labels)
    if (factor::search::search_hold(lg.key))
      out.push_back({lg.key, factor::stat::hold_name(lg.key)});
  return out;
}
std::string Bridge::hold_desc(int hold, int level) { return F::hold_desc(hold, level_id(level)); }

// ---- ② 上下文 ----
Context Bridge::context(const SharedData &data, const Scope &sc) {
  const std::shared_ptr<const F::FeatureTable> ft = p_->ft(data, sc.level);
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
  c.library = results(data, sc, F::factor_dir_of(data, sc.pool, level_id(sc.level)));
  return c;
}

// ---- ③ 落地 ----
std::string Bridge::stage(const SharedData &data, const Scope &sc, const std::string &dir, const Cand &c, std::string &err) {
  if (!F::ValidFactorName(c.name)) {
    err = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
    return "";
  }
  if (!F::ValidFactorNameCn(c.name_cn)) {
    err = "中文名须 1..10 个汉字";
    return "";
  }
  return F::AddFactorFile(dir, *p_->ft(data, sc.level), c.name, c.name_cn, c.expr, c.note, err);
}

// ---- ④ 评估 ----
bool Bridge::ready(const SharedData &data, const Scope &sc, bool gpu) const {
  F::FactorScope probe;
  return F::MakeFactorScope(data, p_->ft(data, sc.level), sc.pool, gpu, probe);
}

void Bridge::eval(const SharedData &data, const Scope &sc, const std::string &dir, const std::vector<std::string> &files, bool gpu) {
  Impl &I = *p_;
  assert(I.step == Impl::Step::Idle && "评估队列忙");
  assert(ready(data, sc, gpu));
  if (!I.searcher || sc != I.cur || dir != I.dir || gpu != I.gpu) {
    I.searcher.reset(), I.runner.reset(); // 析构 Stop() join worker, 常驻块随之释放
    I.searcher = std::make_unique<F::SearchService>();
    I.runner = std::make_unique<F::FactorsService>();
    I.cur = sc, I.dir = dir, I.gpu = gpu;
  }
  I.queue = files;
  I.total = static_cast<int>(files.size()), I.done = 0;
  I.next(data);
}

void Bridge::update(const SharedData &data) {
  Impl &I = *p_;
  if (I.step == Impl::Step::Search && finished(*I.searcher, I.e0)) {
    if (I.searcher->phase.load(std::memory_order_acquire) == Phase::Cancelled) {
      I.queue.clear();
      I.step = Impl::Step::Idle;
      return;
    }
    bool wrote = false;
    {
      std::lock_guard<std::mutex> lock(I.searcher->mutex);
      wrote = I.searcher->result.wrote;
    }
    if (wrote)
      return I.run(data);
    ++I.done, ++I.evals;
    I.next(data);
  } else if (I.step == Impl::Step::Run && finished(*I.runner, I.e0)) {
    ++I.done, ++I.evals;
    if (I.runner->phase.load(std::memory_order_acquire) == Phase::Cancelled) {
      I.queue.clear();
      I.step = Impl::Step::Idle;
      return;
    }
    I.next(data);
  }
}

void Bridge::cancel() {
  Impl &I = *p_;
  I.queue.clear();
  if (I.step == Impl::Step::Search)
    I.searcher->RequestCancel();
  else if (I.step == Impl::Step::Run)
    I.runner->RequestCancel();
}

EvalState Bridge::state() const {
  const Impl &I = *p_;
  EvalState s;
  s.busy = I.step != Impl::Step::Idle;
  s.done = I.done, s.total = I.total;
  s.file = I.file;
  const auto read = [&](auto &svc) {
    const size_t n = svc.total.load(std::memory_order_relaxed);
    s.frac = n > 0 ? static_cast<double>(svc.done.load(std::memory_order_relaxed)) / static_cast<double>(n) : 0.0;
    std::lock_guard<std::mutex> lock(svc.mutex);
    s.message = svc.message;
  };
  if (I.step == Impl::Step::Search) {
    s.stage = "search";
    read(*I.searcher);
  } else if (I.step == Impl::Step::Run) {
    s.stage = "run";
    read(*I.runner);
  }
  return s;
}

uint64_t Bridge::evals() const { return p_->evals; }

std::vector<Result> Bridge::results(const SharedData &data, const Scope &sc, const std::string &dir) {
  const std::shared_ptr<const F::FeatureTable> ft = p_->ft(data, sc.level);
  std::vector<Result> out;
  for (const fs::path &p : F::list_factor_files(dir)) {
    std::vector<F::FactorRow> rows;
    F::ScanFactorFile(p, *ft, rows);
    assert(!rows.empty());
    const F::FactorRow &r0 = rows.front();
    Result r;
    r.file = r0.file, r.name = r0.name_en, r.name_cn = r0.name_cn, r.tmpl = r0.tmpl, r.note = r0.note;
    r.searched = fs::exists(fs::path(dir) / "log" / (r0.name_en + ".log"));
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
      const factor::search::Score s = F::RowScore(w, sc.hold);
      x.has = s.ok;
      if (s.ok) {
        x.score = s.score, x.ls = s.ls, x.ric = s.ric, x.mono = s.mono;
        x.valid = w.valid_pct / 100.0;
      }
      r.ws.push_back(std::move(x));
    }
    std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
    out.push_back(std::move(r));
  }
  std::stable_sort(out.begin(), out.end(), [](const Result &a, const Result &b) {
    const Weight *x = a.best(), *y = b.best();
    return (x != nullptr) != (y != nullptr) ? x != nullptr : x && x->score > y->score;
  });
  return out;
}

bool Bridge::gpu_available() { return factor::gpu::available(); }

// ---- ⑤ 采纳 ----
bool Bridge::adopt(const SharedData &data, const Scope &sc, const std::string &dir, const std::string &file, std::string &err) {
  const fs::path from = fs::path(dir) / file;
  const fs::path to_dir = F::factor_dir_of(data, sc.pool, level_id(sc.level));
  assert(fs::exists(from));
  if (fs::exists(to_dir / file)) {
    err = "正式目录已有 " + file;
    return false;
  }
  fs::create_directories(to_dir);
  fs::copy_file(from, to_dir / file);
  return true;
}

} // namespace agent
