// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / eval_basket (只 CPU) / Probes / LinesP / group_abs
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

std::filesystem::path portfolio_path(const std::string &dir, const std::string &name) {
  assert(ValidFactorName(name) && "组合名: 仅 [A-Za-z0-9_]");
  return std::filesystem::path(dir) / (name + ".json");
}

// Y 共用组: 同 (层, 池) 的因子装同一份 Y (作用域相同时 ensure_xy 的 Y 键命中)
std::string group_of(const HoldJob &j) { return std::to_string(j.scope.seg) + "|" + j.scope.pool; }

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
    e.seg = factor_level(o["level"].get<std::string>()).seg;
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
      if (!find_leg(ft->y.labels, e.hold))
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
    if (!MakeFactorScope(data, table_of(tables, data.feature, it.e.seg), it.e.pool, /*gpu=*/false, j.scope)) {
      it.missing = "标签库未建";
      continue;
    }
    j.key = it.e.key();
    j.f = FactorJob{it.e.expr, it.frame};
    j.hold = it.e.hold;
    req.factors.push_back(std::move(j));
  }
  std::map<std::string, std::vector<int>> holds; // group_of → 组内全部持有期
  for (const HoldJob &j : req.factors)
    holds[group_of(j)].push_back(j.hold);
  for (auto &[g, hk] : holds) {
    std::sort(hk.begin(), hk.end());
    hk.erase(std::unique(hk.begin(), hk.end()), hk.end());
  }
  for (HoldJob &j : req.factors)
    j.group = holds.at(group_of(j));
  if (req.factors.empty())
    return;
  for (const PoolDef &d : feature_pools()) {
    PoolJob p;
    if (!MakeFactorScope(data, table_of(tables, data.feature, kBasketSeg), d.name, /*gpu=*/false, p.scope))
      continue;
    p.key = d.name;
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

namespace {

// G20 多头出参线 [kCurveLines][rows_all] (Stat 契约【净值】/【换手成本】, 每批资金 1/h̄; fold_book stride 1 = 逐行):
//   pnl = 行的 G20 绝对收益, 平仓行实现; imp = 建仓冲击, fix = 平仓量 (+ 无段时增仓量), 建仓行即付 (不交接批的平仓量一并)
struct CurveP : LinesP<kCurveLines> {
  static constexpr uint8_t need = kNeedRows | kNeedLc;
  double cap = 0.0;
  std::vector<CostTier> tiers;                            // cost_tiers 的分段攒 (出参线不用)
  factor::stat::CostAcc qty[factor::stat::kMaxHold] = {}; // cost_tiers 的量攒 (出参线不用)
  std::vector<factor::stat::CostAcc> land;                // 本块逐行换手 [T]
  static double bytes(const Shape &sh, int H) { return LinesP::bytes(sh, H) + sizeof(factor::stat::CostAcc) * static_cast<double>(sh.T); }
  void open(const Ctx &c) {
    assert(c.L.hd.n == 1 && c.c.y.ad.n <= 1 && "出参线: 一个持有期, 至多一个金额段");
    LinesP::open(c);
    x.at_exit[kCurveImp] = x.at_exit[kCurveFix] = 0;
    cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    at(hi, kCurvePnl, t) = static_cast<float>(group_abs(r, factor::stat::leg_group(factor::stat::kLong), cap));
  }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    land.assign(static_cast<size_t>(s.T), factor::stat::CostAcc{});
    cost_tiers(c.ex, v.lc, s, c.L, c.c.y, *c.w, tiers, qty, land.data());
    for (int t = s.t_lo; t < s.T; ++t) {
      const factor::stat::CostLeg &cl = land[static_cast<size_t>(t)].leg[factor::stat::kLong];
      at(0, kCurveImp, t) = static_cast<float>(cl.imp);
      at(0, kCurveFix, t) = static_cast<float>(cl.sell + (c.c.y.ad.n ? 0.0 : cl.buy));
    }
  }
};
using PortfolioProbes = Probes<CurveP>;

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

} // namespace

PortfolioService::PortfolioService() { Start(); }
PortfolioService::~PortfolioService() { Stop(); }

template <class Job, class Curve>
bool PortfolioService::run_jobs(const std::vector<const Job *> &todo, std::map<std::string, std::shared_ptr<const Curve>> &out, FactorFlow &F) {
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
  FactorFlow &F = flow(); // 作用域块: 同作用域的因子共用 Y, X 只留当前因子的叶
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
    return std::tie(a->scope.seg, a->scope.pool) < std::tie(b->scope.seg, b->scope.pool);
  });
  const std::vector<const PoolJob *> todo_p = todo_of(result.pools, req.pools);
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size() + todo_p.size()), std::memory_order_relaxed);
  return run_jobs(todo_f, result.factors, F) && run_jobs(todo_p, result.pools, F);
}

// 与 Run / Inspect 同一条管线 (eval_job), 探针只评该 hold: 落线 CurveP (G20 多头)
bool PortfolioService::eval(const HoldJob &j, FactorFlow &F, FactorCurve &out) {
  TraceN("PortfolioFactor");
  const FactorScope &sc = j.scope;
  out.seg = sc.seg;
  out.hold = j.hold;

  // ---- 管线 eval_job: Y = 同组全部 hold + 最小金额段 (组内因子共用); X 只留本因子的叶; 探针 hd = {本 hold} ----
  Amts ad = all_amts(*sc.ft);
  ad.n = std::min(ad.n, 1);
  assert((ad.n == 0 || ad.a[0] > 0) && "金额段须 > 0");
  const YSpec ys{holds_of(j.group), ad, static_cast<float>(sc.max_impact)};
  const factor::expr::Expr e = sc.ft->reparse(j.f.expr);
  PortfolioProbes ps;
  ps.hd = holds_of({j.hold});
  JobOut o;
  const Rc rc = eval_job(F.engine<Cpu>(), F, sc, {&e}, &j.f.frame, ys, /*retain=*/true, "Portfolio", [&](int) -> PortfolioProbes & { return ps; }, cancel_, *this, o);
  if (aborted(rc))
    return abort_out(rc);
  if (rc != Rc::Ok) {
    out.error = o.why;
    return true;
  }
  out.dates = o.es.dates;
  out.amt = ad.n ? ad.a[0] : 0;
  out.nav = std::move(ps.at<CurveP>().out);
  return true;
}

// 日层池篮子 (eval_basket, 与 eval_job 对仗): 篮子指数 / 估值 / 换手, 整份搬进 PoolCurve
bool PortfolioService::eval(const PoolJob &j, FactorFlow &F, PoolCurve &out) {
  TraceN("PortfolioPool");
  Job o;
  Baskets B;
  const Rc rc = eval_basket(F, j.scope, cancel_, *this, o, B);
  if (aborted(rc))
    return abort_out(rc);
  if (rc != Rc::Ok) {
    out.error = o.why;
    return true;
  }
  const int days = static_cast<int>(o.es.dates.size());
  const size_t n = Baskets::kBaskets * o.es.dates.size();
  out.dates = o.es.dates;
  assert(B.index.n == static_cast<int>(Baskets::kBaskets) * kWeights && B.index.P == days);
  out.index = std::move(B.index);
  for (int w = 0; w < kWeights; ++w)
    out.turn[w] = B.turn[w];
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m) {
      assert(B.val[w][m].size() == n);
      out.val[w][m] = std::move(B.val[w][m]);
    }
  return true;
}

} // namespace GUI::Factors
