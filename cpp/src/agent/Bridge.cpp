// Bridge — 见头文件. agent/ 里唯一 include 项目因子 / 特征 / 服务头的 TU (CMake PRECISE: 因子契约头的消费者)
#include "agent/Bridge.hpp"
#include "agent/Config.hpp" // kMaxOps / kGoodOps / kWorkDir

#include "factor/Dag.hpp"             // build (叶 → 评估期)
#include "factor/Expr.hpp"            // kOps / operand_tex
#include "factor/GpuRun.hpp"          // available
#include "factor/Search/Contract.hpp" // search_hold / Score / kMinMono
#include "gui/task_factors/services/FactorsFile.hpp"
#include "gui/task_factors/services/FactorsScope.hpp" // span_of
#include "gui/task_factors/services/RunService.hpp"
#include "gui/task_factors/services/SearchService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // hold_desc
#include "misc/date.hpp"                      // dash
#include "shared/FeatureLib.hpp"              // feature_pools
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <iterator>
#include <map>
#include <mutex>
#include <tuple>

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

bool searchable(const F::FactorRow &r) { return r.error.empty() && !r.tmpl.empty(); }

using Fails = std::map<std::string, std::string>; // 文件 → Search 中途放弃的原因 (本次运行里, 同作用域跨会话留着)

// 作用域此刻的数据: 未就绪 → 空 (此时一切统计都不可见)
std::optional<F::FactorScope> fscope_of(const SharedData &data, const std::shared_ptr<const F::FeatureTable> &ft, const std::string &pool) {
  F::FactorScope fsc;
  if (!F::MakeFactorScope(data, ft, pool, /*gpu=*/false, fsc))
    return std::nullopt;
  return fsc;
}

// 评估期 (同池 + 首末 + 天数); days = 0 → 不成 (与任何统计都不同窗)
struct Period {
  std::string pool;
  misc::Ymd start = 0, end = 0;
  int days = 0;
  bool same(const std::string &p, misc::Ymd s, misc::Ymd e, int n) const { return days > 0 && p == pool && s == start && e == end && n == days; }
  std::string text() const { return misc::dash(start) + " ~ " + misc::dash(end) + ", " + std::to_string(days) + " 天"; }
};
// 叶为模板 tmpl 的叶 (空 = 只有标签库与因子池库) 时此刻的评估期
Period period_of(const std::optional<F::FactorScope> &fsc, const F::FeatureTable &ft, const std::string &tmpl) {
  Period s;
  if (!fsc)
    return s;
  std::vector<F::LeafId> leaves;
  if (!tmpl.empty())
    leaves = ft.x.ids(factor::build(ft.reparse(tmpl).placeholder()).feats);
  F::Domain dm;
  std::string why;
  if (!F::span_of(ft, *fsc, leaves, SIZE_MAX, dm, why))
    return s;
  s.pool = fsc->pool, s.start = dm.dates.front(), s.end = dm.dates.back(), s.days = static_cast<int>(dm.dates.size());
  return s;
}

// 因子文件 search[] 的代表点 → Top (落选原因按统计定)
Top top_of(const F::SearchTop &t) {
  Top x;
  x.expr = t.expr, x.sign = t.sign;
  x.st = {t.mono, t.ls, t.ric};
  x.g = t.g, x.se = t.se, x.mu = t.mu, x.peak = t.peak;
  if (t.peak)
    x.miss = "mono 过门槛, 但曲面估计 " + num(t.mu) + " ≤ 标准误 " + num(t.se) + " (不显著)";
  else if (t.mono < factor::search::kMinMono)
    x.miss = "没有点 mono ≥ " + num(factor::search::kMinMono) + " (最高 " + num(t.mono) + ")";
  else
    x.miss = "mono 过门槛的点每期毛多空都 ≤ 0 (或 rIC = 0)";
  return x;
}

// 一个因子文件 → 本持有期下的结果 (权重按 score 降序, 结局随之定; 可见性按此刻评估期)
Result result_of(const F::FeatureTable &ft, const std::optional<F::FactorScope> &fsc, const fs::path &p, int hold, const Fails &fails) {
  std::vector<F::FactorRow> rows;
  F::ScanFactorFile(p, ft, rows);
  assert(!rows.empty());
  const F::FactorRow &r0 = rows.front();
  Result r;
  r.file = r0.file, r.name = r0.name_en, r.name_cn = r0.name_cn, r.tmpl = r0.tmpl, r.note = r0.note;
  r.ops = ops_of(ft, r0.tmpl);
  const Period now = r0.tmpl.empty() ? Period{} : period_of(fsc, ft, r0.tmpl);
  bool fresh = true; // 证据都在此刻评估期上
  const auto mk = std::find_if(r0.searched.begin(), r0.searched.end(), [&](const F::SearchMark &m) { return m.hold == hold; });
  if (mk != r0.searched.end()) {
    fresh = now.same(now.pool, mk->start_date, mk->end_date, mk->days);
    if (mk->top)
      r.top = top_of(*mk->top);
  }
  if (const auto it = fails.find(r.file); it != fails.end())
    r.error = "Search 放弃: " + it->second;
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
    if (w.has_stat)
      fresh = fresh && now.same(w.scope.pool, w.scope.start_date, w.scope.end_date, w.scope.days);
    Weight x;
    x.expr = w.expr, x.sign = w.sign, x.hold_src = w.hold_src;
    const factor::search::Score s = F::RowScore(w, hold);
    x.has = s.ok;
    if (s.ok) {
      x.st = {s.mono, s.ls, s.ric};
      x.score = s.score, x.se = s.se, x.valid = w.valid_pct / 100.0;
    }
    r.ws.push_back(std::move(x));
  }
  std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
  r.visible = fresh && r.ops <= kMaxOps;
  const bool null = r.ws.empty() ? mk != r0.searched.end() : run; // 搜过无显著 / 权重都 Run 过却无本持有期统计
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

std::vector<Result> scan(const F::FeatureTable &ft, const std::optional<F::FactorScope> &fsc, const std::string &dir, int hold, const Fails &fails) {
  std::vector<Result> out;
  for (const fs::path &p : F::list_factor_files(dir))
    out.push_back(result_of(ft, fsc, p, hold, fails));
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
  void drop() { svc.reset(); }
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
  void read(Lab::Live &l) const {
    const size_t n = svc->total.load(std::memory_order_relaxed);
    l.frac = n > 0 ? static_cast<double>(svc->done.load(std::memory_order_relaxed)) / static_cast<double>(n) : 0.0;
    std::lock_guard<std::mutex> lock(svc->mutex);
    l.message = svc->message;
  }
};

Context context_of(const SharedData &data, const Scope &sc) {
  const std::shared_ptr<const F::FeatureTable> ft = table(data, sc.level);
  Context c;
  c.pool = sc.pool;
  c.pool_cn = catalog::pool_cn(sc.pool);
  c.level = F::factor_level(level_id(sc.level)).name;
  c.hold = factor::stat::hold_name(sc.hold);
  c.hold_desc = catalog::hold_desc(sc.hold, sc.level);
  const std::optional<F::FactorScope> fsc = fscope_of(data, ft, sc.pool);
  const Period base = period_of(fsc, *ft, "");
  c.window = base.days > 0 ? base.text() : "未就绪";
  c.seg = ft->seg;
  c.ts_root_d = factor::stat::ts_norm_d(ft->seg);
  c.max_d = factor::kMaxD;
  c.min_mono = factor::search::kMinMono;
  for (const F::FeatCol &l : ft->x.leaves) {
    if (!l.allowed)
      continue;
    const FeatureMetadata &m = ft->feature->meta(l.tier).features[l.level][l.col];
    c.leaves.push_back({l.code, m.name_cn, m.formula, m.description});
  }
  for (const factor::expr::OpInfo &o : factor::expr::kOps)
    c.ops.push_back({o.name, o.c_name, factor::expr::operand_tex(o), factor::dom_name(o.out), o.op, o.note});
  c.library = scan(*ft, fsc, F::factor_dir_of(data, sc.pool, level_id(sc.level)), sc.hold, {});
  return c;
}

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

std::string label(const Scope &sc) {
  return sc.pool + " / " + F::factor_level(level_id(sc.level)).name + " / " + factor::stat::hold_name(sc.hold);
}
std::string dir(const Scope &sc) {
  return (fs::path(kWorkDir) / sc.pool / F::factor_level(level_id(sc.level)).name / factor::stat::hold_name(sc.hold)).string();
}

} // namespace catalog

// ---- Lab ----
struct Lab::Impl {
  // 作用域
  const SharedData *data = nullptr;
  Scope sc;
  std::string dir; // 候选目录
  Context ctx;
  std::vector<Result> results;
  Fails fails; // 同作用域跨会话留着
  // 两道 (下标 = Step), 首次 eval 才建
  static_assert(static_cast<size_t>(Step::Search) == 0 && static_cast<size_t>(Step::Run) == 1);
  bool gpu = false;
  std::tuple<Lane<F::SearchService>, Lane<F::RunService>> lanes;
  // 队列
  bool running = false; // eval 起, 到 End 入队
  bool halt = false;    // 已 cancel: 在评那步收尾即 End
  Step at = Step::End;  // 在评那步的道; End = 无
  std::deque<std::string> queue;
  std::string file;       // 在评的文件
  F::FactorRow row;       // 其首行 (Search 的模板)
  std::deque<Tick> ticks; // 未取走的步

  std::shared_ptr<const F::FeatureTable> ft() const { return table(*data, sc.level); }
  std::optional<F::FactorScope> fsc() const { return fscope_of(*data, ft(), sc.pool); }
  bool built() const { return std::get<0>(lanes).svc != nullptr; }
  template <class Fn>
  void each_lane(Fn &&fn) {
    std::apply([&](auto &...l) { (fn(l), ...); }, lanes);
  }
  // 在评那步的道
  template <class Fn>
  decltype(auto) on_lane(Fn &&fn) const {
    assert(at == Step::Search || at == Step::Run);
    return at == Step::Search ? fn(std::get<0>(lanes)) : fn(std::get<1>(lanes));
  }

  // 一道的请求 (按道的服务分派)
  F::SearchRequest request(const Lane<F::SearchService> &) const {
    F::SearchRequest req;
    [[maybe_unused]] const bool ok = F::MakeSearchRequest(*data, ft(), sc.pool, row, sc.hold, /*fresh=*/true, gpu, req);
    assert(ok && "作用域须 ready");
    req.scope.factor_dir = dir;
    req.spec.keep = true;
    return req;
  }
  F::RunRequest request(const Lane<F::RunService> &) const {
    F::RunRequest req;
    [[maybe_unused]] const bool ok = F::MakeRunRequest(*data, ft(), sc.pool, /*evaluate=*/true, gpu, req);
    assert(ok && "作用域须 ready");
    req.scope.factor_dir = dir;
    req.spec.only_file = file;
    req.spec.keep = true;
    return req;
  }
  template <Step S>
  void send() {
    auto &l = std::get<static_cast<size_t>(S)>(lanes);
    l.send(request(l));
    at = S;
    ticks.push_back({S, file, {}});
  }
  // Search 跑完: 落了盘 → true (随后 Run); 中途放弃 → 记下原因 (该文件 BROKEN)
  bool searched() {
    F::SearchService &svc = *std::get<0>(lanes).svc;
    std::lock_guard<std::mutex> lock(svc.mutex);
    if (svc.result.wrote)
      fails.erase(file);
    else
      fails[file] = svc.message;
    return svc.result.wrote;
  }

  // 重扫一个文件, 结果表保序
  const Result &refresh(const std::string &f) {
    const auto find = [&] { return std::find_if(results.begin(), results.end(), [&](const Result &x) { return x.file == f; }); };
    Result r = result_of(*ft(), fsc(), fs::path(dir) / f, sc.hold, fails);
    if (const auto it = find(); it == results.end())
      results.push_back(std::move(r));
    else
      *it = std::move(r);
    sort_results(results);
    return *find();
  }
  // 出队下一个: 模板合法 → Search (定方向 + 搜参, 落了盘随后 Run); BROKEN → 直接评完; 队空 → End
  void next() {
    while (!queue.empty()) {
      file = std::move(queue.front());
      queue.pop_front();
      std::vector<F::FactorRow> rows;
      F::ScanFactorFile(fs::path(dir) / file, *ft(), rows);
      assert(!rows.empty());
      row = std::move(rows.front());
      if (searchable(row))
        return send<Step::Search>();
      settle();
    }
    close();
  }
  void settle() { ticks.push_back({Step::Settle, file, refresh(file)}); }
  void close() {
    queue.clear(), file.clear();
    at = Step::End;
    running = false;
    ticks.push_back({Step::End, {}, {}});
  }
  // 在评那步的道跑完一轮 → 下一步
  void pump() {
    if (at == Step::End || !on_lane([](const auto &l) { return l.finished(); }))
      return;
    if (halt || on_lane([](const auto &l) { return l.cancelled(); }))
      return close();
    if (at == Step::Search && searched())
      return send<Step::Run>();
    settle();
    next();
  }
};

Lab::Lab() : p_(std::make_unique<Impl>()) {}
Lab::~Lab() = default;

void Lab::bind(const SharedData &data, const Scope &sc) {
  Impl &I = *p_;
  assert(!I.running && "评估队列忙");
  if (sc != I.sc) {
    I.each_lane([](auto &l) { l.drop(); });
    I.fails.clear();
  }
  I.data = &data;
  I.sc = sc;
  I.dir = (fs::path(catalog::dir(sc)) / "factors").string();
  I.ctx = context_of(data, sc);
  I.results = scan(*I.ft(), I.fsc(), I.dir, sc.hold, I.fails);
  I.ticks.clear();
}

const Scope &Lab::scope() const { return p_->sc; }
const Context &Lab::ctx() const { return p_->ctx; }
const std::vector<Result> &Lab::results() const { return p_->results; }

void Lab::stage(Staged &s) {
  Impl &I = *p_;
  const Cand &c = s.cand;
  std::string err;
  if (!F::ValidFactorName(c.name))
    err = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
  else if (!F::ValidFactorNameCn(c.name_cn))
    err = "中文名须 1..10 个汉字";
  else if (const int n = ops_of(*I.ft(), c.expr); n > kMaxOps)
    err = "算子 " + std::to_string(n) + " 个, 超过上限 " + std::to_string(kMaxOps) + " (建议 " + std::to_string(kGoodOps) + " 个左右)";
  else
    s.file = F::AddFactorFile(I.dir, *I.ft(), c.name, c.name_cn, c.expr, c.note, err);
  if (s.file.empty())
    s.res.error = std::move(err), s.res.verdict = Verdict::Unstaged;
  else
    s.res = I.refresh(s.file);
}

void Lab::adopt(Submitted &s, const Gate &g) {
  const Impl &I = *p_;
  const auto it = std::find_if(I.results.begin(), I.results.end(), [&](const Result &r) { return r.name == s.name; });
  if (it == I.results.end() || !it->visible) {
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

void Lab::eval(std::vector<std::string> files, bool gpu) {
  Impl &I = *p_;
  assert(!I.running && I.data && "评估队列忙或未 bind");
  if (!I.built() || gpu != I.gpu) {
    I.each_lane([](auto &l) { l.reset(); });
    I.gpu = gpu;
  }
  I.queue.assign(std::make_move_iterator(files.begin()), std::make_move_iterator(files.end()));
  I.running = true, I.halt = false;
  I.next();
}

void Lab::cancel() {
  Impl &I = *p_;
  if (!I.running)
    return;
  I.halt = true;
  I.queue.clear();
  I.on_lane([](const auto &l) { l.cancel(); });
}

bool Lab::busy() const { return p_->running; }

std::optional<Lab::Tick> Lab::poll() {
  Impl &I = *p_;
  if (I.ticks.empty())
    I.pump();
  if (I.ticks.empty())
    return std::nullopt;
  Tick t = std::move(I.ticks.front());
  I.ticks.pop_front();
  return t;
}

void Lab::live(Live &l) const {
  const Impl &I = *p_;
  if (I.at == Step::End)
    l = {};
  else
    I.on_lane([&](const auto &x) { x.read(l); });
}

} // namespace agent
