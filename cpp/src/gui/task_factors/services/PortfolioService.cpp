// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / Probes (只 CPU)
#include "gui/task_factors/services/FactorsLoad.hpp"   // fold_exit
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp" // feature_pools
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <tuple>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;

constexpr int kDaySeg = 1; // 池指数的层 (日层)

std::filesystem::path portfolio_path(const std::string &dir, const std::string &name) {
  assert(ValidFactorName(name) && "组合名: 仅 [A-Za-z0-9_]");
  return std::filesystem::path(dir) / (name + ".json");
}

int seg_of_level(const std::string &name) {
  for (const FactorLevel &l : kFactorLevels)
    if (name == l.name)
      return l.seg;
  assert(false && "组合文件: level 不是因子层名");
  __builtin_unreachable();
}

} // namespace

// ============================================================================
// 组合文件
// ============================================================================

std::vector<std::string> ListPortfolios(const std::string &dir) {
  std::vector<std::string> names;
  if (std::filesystem::is_directory(dir))
    for (const auto &ent : std::filesystem::directory_iterator(dir))
      if (ent.is_regular_file() && ent.path().extension() == ".json" && ValidFactorName(ent.path().stem().string()))
        names.push_back(ent.path().stem().string());
  std::sort(names.begin(), names.end());
  return names;
}

std::vector<PortfolioEntry> LoadPortfolio(const std::string &dir, const std::string &name) {
  std::ifstream f(portfolio_path(dir, name));
  assert(f.is_open() && "组合文件打不开");
  const json j = json::parse(f, nullptr, /*allow_exceptions=*/false);
  assert(!j.is_discarded() && j.is_object() && j.contains("factors") && j["factors"].is_array() && "组合文件: 顶层须是 {factors: [...]}");
  std::vector<PortfolioEntry> es;
  for (const json &o : j["factors"]) {
    assert(o.is_object() && o.contains("pool") && o.contains("level") && o.contains("file") && o.contains("expr") && o.contains("hold") &&
           "组合文件: 条目缺键");
    PortfolioEntry e;
    e.pool = o["pool"].get<std::string>();
    e.seg = seg_of_level(o["level"].get<std::string>());
    e.file = o["file"].get<std::string>();
    e.expr = o["expr"].get<std::string>();
    e.hold = o["hold"].get<int>();
    es.push_back(std::move(e));
  }
  return es;
}

// 一条目一行 (人能直接读 / diff)
void SavePortfolio(const std::string &dir, const std::string &name, const std::vector<PortfolioEntry> &es) {
  const std::filesystem::path path = portfolio_path(dir, name);
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream f(tmp);
    assert(f.is_open() && "组合文件: 临时文件打不开");
    f << "{\n  \"factors\": [";
    for (size_t i = 0; i < es.size(); ++i) {
      const PortfolioEntry &e = es[i];
      ojson o;
      o["pool"] = e.pool;
      o["level"] = factor_level(e.seg).name;
      o["file"] = e.file;
      o["hold"] = e.hold;
      o["expr"] = e.expr;
      f << (i ? ",\n    " : "\n    ") << o.dump();
    }
    f << (es.empty() ? "]\n}\n" : "\n  ]\n}\n");
    assert(f.good() && "组合文件: 写入失败");
  }
  std::filesystem::rename(tmp, path);
}

bool RenamePortfolio(const std::string &dir, const std::string &from, const std::string &to) {
  const std::filesystem::path a = portfolio_path(dir, from), b = portfolio_path(dir, to);
  if (std::filesystem::exists(b))
    return false;
  std::filesystem::rename(a, b);
  return true;
}

void DeletePortfolio(const std::string &dir, const std::string &name) { std::filesystem::remove(portfolio_path(dir, name)); }

// ============================================================================
// 解析 / 请求
// ============================================================================

std::shared_ptr<const FeatureTable> table_of(FeatureTables &tables, const Feature &feature, int seg) {
  std::shared_ptr<const FeatureTable> &t = tables[seg];
  if (!t)
    t = std::make_shared<FeatureTable>(BuildFeatureTable(feature, seg));
  return t;
}

void ResolvePortfolio(const SharedData &data, FeatureTables &tables, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out) {
  out.clear();
  std::map<std::string, std::vector<FactorRow>> scanned; // 因子文件路径 → 行 (同文件只扫一次)
  for (const PortfolioEntry &e : es) {
    PortfolioItem it;
    it.e = e;
    it.name_en = std::filesystem::path(e.file).stem().string();
    if (!level_for_pool(factor_level(e.seg), pool_def(e.pool).tier)) {
      it.missing = "该池没有这个因子层";
      out.push_back(std::move(it));
      continue;
    }
    const std::shared_ptr<const FeatureTable> ft = table_of(tables, data.feature, e.seg);
    const std::filesystem::path p = std::filesystem::path(factor_dir_of(data, e.pool, e.seg)) / e.file;
    const bool exists = std::filesystem::is_regular_file(p);
    auto [sit, fresh] = scanned.try_emplace(p.string());
    if (fresh && exists)
      ScanFactorFile(p, *ft, sit->second);
    const auto rit = std::find_if(sit->second.begin(), sit->second.end(), [&](const FactorRow &r) { return r.expr == e.expr; });
    if (!exists) {
      it.missing = "因子文件不存在: " + p.string();
    } else if (rit == sit->second.end()) {
      it.missing = "因子文件里没有这个权重 (规范串已变)";
    } else {
      assert(rit->error.empty() && "有具体规范串的行必是有效权重");
      it.name_cn = rit->name_cn;
      it.frame = rit->frame;
      const std::vector<int> hk = keys_of(ft->y.labels);
      if (std::find(hk.begin(), hk.end(), e.hold) == hk.end())
        it.missing = "持有期 " + factor::stat::hold_name(e.hold) + " 不在该层标签里";
    }
    out.push_back(std::move(it));
  }
}

void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, PortfolioRequest &req) {
  assert(!data.asset.items.empty() && "资产轴未就绪");
  req = PortfolioRequest{};
  req.reload = reload;
  for (PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    HoldJob j;
    if (!MakeFactorScope(data, it.e.pool, it.e.seg, /*gpu=*/false, j.f.scope)) {
      it.missing = "标签库未建";
      continue;
    }
    j.key = it.e.key();
    j.f.ft = table_of(tables, data.feature, it.e.seg);
    j.f.expr = it.e.expr;
    j.f.frame = it.frame;
    j.hold = it.e.hold;
    req.factors.push_back(std::move(j));
  }
  if (req.factors.empty())
    return;
  for (const PoolDef &d : feature_pools()) {
    PoolJob p;
    if (!MakeFactorScope(data, d.name, kDaySeg, /*gpu=*/false, p.scope))
      continue;
    p.key = d.name;
    p.ft = table_of(tables, data.feature, kDaySeg);
    assert(!p.ft->y.labels.empty() && "日层须有标签 (load_y 至少一组)");
    req.pools.push_back(std::move(p));
  }
}

std::string PortfolioRequest::sig() const {
  std::string s;
  for (const HoldJob &j : factors)
    s += j.sig() + "\n";
  for (const PoolJob &p : pools)
    s += p.sig() + "\n";
  return s;
}

// ============================================================================
// Service
// ============================================================================

// 计算 flow 的状态 (worker 私有, 跨请求存活). 作用域块 c: 同作用域的因子共用 Y, X 只留当前因子的叶
struct PortfolioService::Flow : FactorFlow {
  Engine<Cpu> E;                                 // 毛标签秩 (Stat 会话, 作用域内一次) + 活槽 (HostPool 常驻)
  std::vector<factor::stat::Row> rows;           // [T] 本 hold 的一级 Row
  std::vector<factor::stat::CostAcc> cost;       // [T] 本 hold 的逐行换手
  std::map<std::string, std::vector<int>> holds; // 本轮请求: group_of → 该组全部因子的持有期 (升序去重; Y 一组一次装全)
  explicit Flow(const std::atomic<bool> &cancel) : E(ex, cancel, 1) {}
};

namespace {

// 本 hold 的逐行换手落地 [rows_all] (最小金额段; 无段 C = 0 只出量): cost_rows 的 land
struct CostRowsP : ProbeBase {
  static constexpr uint8_t need = kNeedLc;
  std::vector<factor::stat::CostAcc> *dst = nullptr;
  static double bytes(const Shape &sh, int) { return sizeof(factor::stat::CostAcc) * static_cast<double>(sh.T); }
  void open(const Ctx &c) {
    assert(c.L.hd.n == 1 && c.c.y.ad.n <= 1 && "Portfolio 一个持有期 × 至多一个金额段");
    land_open(*dst, static_cast<size_t>(c.c.rows_all()), c.s());
  }
  void root(const Ctx &c, const RootView &v) {
    const YPlanes &y = c.c.y;
    const factor::stat::Span s = c.s();
    const factor::stat::Cost cost = y.ad.n ? cost_of(y.costs[0]) : factor::stat::Cost{};
    factor::stat::CostAcc tot;
    factor::cpu::stat::cost_rows(c.ex, v.lc, s, c.L, y.ad.n ? &cost : nullptr, y.ad.n, *c.w, &tot, dst->data() + s.t0_abs);
  }
};
// G20 多头出参线 (Stat 契约【净值】/【换手成本】, 每批资金 1/h̄) → nav = [kCurveLines][rows_all]:
//   pnl = G20 绝对收益 fold_exit 到平仓行; imp = 建仓冲击 / h̄, fix = (平仓量 (+ 无段时增仓量)) / h̄, 记在建仓行 (不交接批的平仓量一并)
struct CurveP : ProbeBase {
  static constexpr uint8_t need = 0;
  const std::vector<factor::stat::Row> *rows = nullptr;
  const std::vector<factor::stat::CostAcc> *cost = nullptr;
  int amt = 0;
  BookLines nav;
  static double bytes(const Shape &sh, int) { return (sizeof(float) * static_cast<double>(kCurveLines) + sizeof(double)) * sh.T; }
  void finish(const Ctx &c) {
    using factor::stat::kLong;
    const YPlanes &y = c.c.y;
    const int T = c.c.rows_all(), hold = c.L.hd.h[0], G = factor::stat::leg_group(kLong);
    const double h = factor::stat::hold_span(hold, factor::geo_of(y.sh.seg));
    assert(rows->size() == static_cast<size_t>(T) && cost->size() == static_cast<size_t>(T));
    amt = y.ad.n ? y.ad.a[0] : 0;
    BookLines pnl;
    fold_exit(c.ex, hold, y.sh.seg, T, 1, 1, [&](int, int t) {
                const factor::stat::Row &r = (*rows)[static_cast<size_t>(t)];
                return r.ok ? factor::stat::book_abs(r.grp[G], r.cnt[G], r.mkt[kLong], c.L.cap) : 0.0; }, pnl);
    nav.n = kCurveLines, nav.P = T;
    nav.v.assign(static_cast<size_t>(kCurveLines) * T, 0.f);
    std::copy(pnl.v.begin(), pnl.v.end(), nav.v.begin() + static_cast<long>(kCurvePnl) * T);
    float *imp = nav.v.data() + static_cast<size_t>(kCurveImp) * T, *fix = nav.v.data() + static_cast<size_t>(kCurveFix) * T;
    for (int t = 0; t < T; ++t) {
      const factor::stat::CostLeg &cl = (*cost)[static_cast<size_t>(t)].leg[kLong];
      imp[t] = static_cast<float>(cl.imp / h);
      fix[t] = static_cast<float>((cl.sell + (y.ad.n ? 0.0 : cl.buy)) / h);
    }
  }
};
using PortfolioProbes = Probes<RowsP, CostRowsP, CurveP>;

// 缓存跟请求走 (因子 / 池同一条): reload → 全丢; 请求里没有的 key 丢掉
template <class Job, class Curve>
void prune(std::map<std::string, std::shared_ptr<const Curve>> &m, const std::vector<Job> &jobs, bool reload) {
  if (reload)
    m.clear();
  std::erase_if(m, [&](const auto &kv) { return std::none_of(jobs.begin(), jobs.end(), [&](const Job &j) { return j.key == kv.first; }); });
}
// 要算的: 缺 / 签名变了
template <class Job, class Curve>
std::vector<const Job *> todo_of(const std::map<std::string, std::shared_ptr<const Curve>> &m, const std::vector<Job> &jobs) {
  std::vector<const Job *> todo;
  for (const Job &j : jobs)
    if (const auto it = m.find(j.key); it == m.end() || it->second->sig != j.sig())
      todo.push_back(&j);
  return todo;
}

// Y 共用组: 同 (层, 池) 的因子装同一份 Y (作用域相同时 ensure_y 命中)
std::string group_of(const HoldJob &j) { return std::to_string(j.f.scope.seg) + "|" + j.f.scope.pool; }

} // namespace

PortfolioService::PortfolioService() { Start(); }
PortfolioService::~PortfolioService() { Stop(); }

template <class Job, class Curve>
bool PortfolioService::run_jobs(const std::vector<const Job *> &todo, std::map<std::string, std::shared_ptr<const Curve>> &out, Flow &F) {
  for (const Job *j : todo) {
    auto c = std::make_shared<Curve>();
    c->sig = j->sig();
    if (!eval(*j, F, *c))
      return false;
    {
      std::lock_guard<std::mutex> lock(mutex);
      out[j->key] = std::move(c);
      ++result_gen;
    }
    jobs_done.fetch_add(1, std::memory_order_relaxed);
    publish();
  }
  return true;
}

bool PortfolioService::run(PortfolioRequest &req) {
  if (!flow_)
    flow_ = std::make_unique<Flow>(cancel_);
  Flow &F = *flow_;
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
    prune(result.factors, req.factors, req.reload);
    prune(result.pools, req.pools, req.reload);
    ++result_gen;
  }
  if (req.reload)
    F.c.reset();
  publish();

  // 只有 worker 写 result: 这里读不加锁
  std::vector<const HoldJob *> todo_f = todo_of(result.factors, req.factors);
  std::stable_sort(todo_f.begin(), todo_f.end(), [](const HoldJob *a, const HoldJob *b) {
    return std::tie(a->f.scope.seg, a->f.scope.pool) < std::tie(b->f.scope.seg, b->f.scope.pool);
  });
  F.holds.clear();
  for (const HoldJob &j : req.factors)
    F.holds[group_of(j)].push_back(j.hold);
  for (auto &[g, hk] : F.holds) {
    std::sort(hk.begin(), hk.end());
    hk.erase(std::unique(hk.begin(), hk.end()), hk.end());
  }
  const std::vector<const PoolJob *> todo_p = todo_of(result.pools, req.pools);
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size() + todo_p.size()), std::memory_order_relaxed);
  return run_jobs(todo_f, result.factors, F) && run_jobs(todo_p, result.pools, F);
}

bool PortfolioService::stale_out(Flow &F) {
  F.c.reset(); // 库换了, 缓存的一切不可信
  std::lock_guard<std::mutex> lock(mutex);
  message = kStaleMsg;
  return false;
}

// 与 Run / Inspect 同一条管线 (eval_job), 探针只评该 hold: 一级 Row + 逐行换手 → 净值引擎 fold_exit (G20 多头)
bool PortfolioService::eval(const HoldJob &j, Flow &F, FactorCurve &out) {
  TraceN("PortfolioFactor");
  const FeatureTable &ft = *j.f.ft;
  const FactorScope &sc = j.f.scope;
  out.seg = sc.seg;
  out.hold = j.hold;

  // ---- 管线 eval_job: Y = 同组全部 hold + 最小金额段 (组内因子共用); X 只留本因子的叶; 探针 hd = {本 hold} ----
  const std::vector<int> ck = keys_of(ft.y.costs);
  assert((ck.empty() || ck.front() > 0) && "金额段须 > 0");
  const YSpec ys{holds_of(F.holds.at(group_of(j))), amts_of(ck.empty() ? std::vector<int>{} : std::vector<int>{ck.front()}), static_cast<float>(sc.max_impact),
                 IndexKind::None};
  const factor::expr::Expr e = parse_job(j.f);
  PortfolioProbes ps;
  ps.hd = holds_of({j.hold});
  ps.at<RowsP>().dst = &F.rows;
  ps.at<CostRowsP>().dst = &F.cost;
  ps.at<CurveP>().rows = &F.rows;
  ps.at<CurveP>().cost = &F.cost;
  JobOut o;
  const Rc rc = eval_job(F.E, F, ft, sc, {&e}, &j.f.frame, ys, /*retain=*/true, "Portfolio", [&](int) -> PortfolioProbes & { return ps; }, cancel_, *this, o, [] {}, [](int) {});
  if (rc == Rc::Empty || rc == Rc::Broken) {
    out.error = rc == Rc::Empty ? o.empty : o.why[0];
    return true;
  }
  if (rc == Rc::Cancel)
    return false;
  if (rc == Rc::Stale)
    return stale_out(F);
  out.dates = o.es.dates;
  out.amt = ps.at<CurveP>().amt;
  out.nav = std::move(ps.at<CurveP>().nav);
  return true;
}

// 日层池篮子: 无叶作用域 (标签库 + 池成员) → load_y 顺带算篮子指数 / 估值 / 换手, 整块搬进 PoolCurve; 块用完即弃
bool PortfolioService::eval(const PoolJob &j, Flow &F, PoolCurve &out) {
  TraceN("PortfolioPool");
  const FeatureTable &ft = *j.ft;
  assert(j.scope.seg == ft.seg && ft.seg == kDaySeg);
  EvalScope es;
  if (std::string why; !eval_scope(ft, j.scope, {}, es, why)) {
    out.error = why;
    return true;
  }
  open_libs(j.scope, F.lr, &cancel_);
  const int days = static_cast<int>(es.dates.size()), A = static_cast<int>(es.rs.axis.size());
  const std::vector<int> hk = keys_of(ft.y.labels);
  assert(std::find(hk.begin(), hk.end(), factor::stat::hold_close(1)) != hk.end() && "日层须有 T+1C 标签 (池指数 C(D) → C(D+1))");
  const YSpec ys{holds_of({factor::stat::hold_close(1)}), {}, 0.f, IndexKind::Full};
  assert_fits("Portfolio", static_cast<double>(days) * A * cell_bytes(ys), days, A, ft.seg);
  ScopeCache c;
  const Rc rc = ensure_xy(c, ft, es, F.lr, 0, es.days.size(), ys, {}, /*retain=*/true, F.ex, cancel_, *this);
  if (rc == Rc::Cancel)
    return false;
  if (rc == Rc::Stale)
    return stale_out(F);
  const size_t n = YPlanes::kBaskets * es.dates.size();
  out.dates = es.dates;
  assert(c.y.index.n == static_cast<int>(YPlanes::kBaskets) * kWeights && c.y.index.P == days);
  out.index = std::move(c.y.index);
  for (int w = 0; w < kWeights; ++w)
    out.turn[w] = c.y.turn[w];
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m) {
      assert(c.y.val[w][m].size() == n);
      out.val[w][m] = std::move(c.y.val[w][m]);
    }
  return true;
}

} // namespace GUI::Factors
