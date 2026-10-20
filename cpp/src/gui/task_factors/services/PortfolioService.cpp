// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // Engine<Cpu> (只 CPU)
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp" // feature_pools
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;
using factor::stat::kLegs;
using factor::stat::kLong;

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
    PortfolioJob j;
    if (!MakeFactorScope(data, it.e.pool, it.e.seg, /*gpu=*/false, j.scope)) {
      it.missing = "标签库未建";
      continue;
    }
    j.key = it.e.key();
    j.ft = table_of(tables, data.feature, it.e.seg);
    j.expr = it.e.expr;
    j.frame = it.frame;
    j.hold = it.e.hold;
    req.factors.push_back(std::move(j));
  }
  if (req.factors.empty())
    return;
  for (const PoolDef &d : feature_pools()) {
    PoolJob p;
    if (!MakeFactorScope(data, d.name, kDaySeg, /*gpu=*/false, p.scope))
      continue;
    p.ft = table_of(tables, data.feature, kDaySeg);
    assert(!p.ft->y.labels.empty() && "日层须有标签 (load_y 至少一组)");
    req.pools.push_back(std::move(p));
  }
}

std::string PortfolioRequest::sig() const {
  std::string s;
  for (const PortfolioJob &j : factors)
    s += j.key + "|" + std::to_string(j.scope.max_impact) + "\n";
  for (const PoolJob &p : pools)
    s += p.scope.pool + "\n";
  return s;
}

// ============================================================================
// Service
// ============================================================================

// 计算 flow 的状态 (worker 私有, 跨请求存活)
struct PortfolioService::Flow {
  misc::exec::ForkJoin ex;
  LibReaders lr; // 已建的库 (清单没变的复用)
  ScopeCache c;  // 因子的作用域块: 同作用域的因子共用 Y, X 只留当前因子的叶
  Engine<Cpu> E;
  std::vector<uint16_t> rx;            // [T][A]
  std::vector<uint8_t> lc;             // [T][A]
  std::vector<factor::stat::Row> rows; // [T] 一级 (单 hold)

  explicit Flow(const std::atomic<bool> &cancel) : ex(misc::exec::hw()), E(ex, cancel, 1) {}
};

PortfolioService::PortfolioService() { Start(); }
PortfolioService::~PortfolioService() { Stop(); }

bool PortfolioService::run(PortfolioRequest &req) {
  if (!flow_)
    flow_ = std::make_unique<Flow>(cancel_);
  Flow &F = *flow_;
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
    if (req.reload) {
      result.factors.clear();
      result.pools.clear();
    }
    std::erase_if(result.factors, [&](const auto &kv) {
      return std::none_of(req.factors.begin(), req.factors.end(), [&](const PortfolioJob &j) { return j.key == kv.first; });
    });
    ++result_gen;
  }
  if (req.reload)
    F.c.reset();
  publish();

  // 只有 worker 写 result: 这里读不加锁
  std::vector<const PortfolioJob *> todo_f;
  for (const PortfolioJob &j : req.factors)
    if (const auto it = result.factors.find(j.key); it == result.factors.end() || it->second->max_impact != j.scope.max_impact)
      todo_f.push_back(&j);
  std::vector<const PoolJob *> todo_p;
  for (const PoolJob &p : req.pools)
    if (!result.pools.contains(p.scope.pool))
      todo_p.push_back(&p);
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size() + todo_p.size()), std::memory_order_relaxed);

  for (const PortfolioJob *j : todo_f) {
    auto c = std::make_shared<FactorCurve>();
    if (!eval_factor(*j, F, *c))
      return false;
    {
      std::lock_guard<std::mutex> lock(mutex);
      result.factors[j->key] = std::move(c);
      ++result_gen;
    }
    jobs_done.fetch_add(1, std::memory_order_relaxed);
    publish();
  }
  for (const PoolJob *p : todo_p) {
    auto c = std::make_shared<PoolCurve>();
    if (!eval_pool(*p, F, *c))
      return false;
    {
      std::lock_guard<std::mutex> lock(mutex);
      result.pools[p->scope.pool] = std::move(c);
      ++result_gen;
    }
    jobs_done.fetch_add(1, std::memory_order_relaxed);
    publish();
  }
  return true;
}

// 与 InspectService::evaluate 同一条 flow, 只留该 hold 的逐行序列
bool PortfolioService::eval_factor(const PortfolioJob &j, Flow &F, FactorCurve &out) {
  TraceN("PortfolioFactor");
  const FeatureTable &ft = *j.ft;
  const FactorScope &sc = j.scope;
  assert(sc.seg == ft.seg && "条目层与字段表层不符");
  factor::expr::Expr e;
  {
    std::string err;
    const bool ok = factor::expr::parse(j.expr, ft.lookup(), ft.seg, e, err);
    assert(ok && "解析时找回的规范串必可再解析");
    (void)ok;
  }
  const factor::Dag d = factor::build(e);
  const int N = static_cast<int>(d.nodes.size()), root = d.root();
  out.key = j.key;
  out.seg = sc.seg;
  out.hold = j.hold;
  out.max_impact = sc.max_impact;
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };

  // ---- 作用域 + 装载 (Y = 该 hold + 全部金额段; X 只留本因子的叶) ----
  EvalScope es;
  if (std::string why; !eval_scope(ft, sc, d.feats, es, why)) {
    out.error = why;
    return true;
  }
  open_libs(sc, F.lr, &cancel_);
  const int days = static_cast<int>(es.dates.size()), A = static_cast<int>(es.rs.axis.size());
  const YSpec ys{holds_of({j.hold}), amts_of(keys_of(ft.y.costs)), static_cast<float>(sc.max_impact), /*index=*/false};
  { // 宿主峰值: Y + X (本因子) + 活槽 + rx / lc + 标签秩 + Row [T]
    const double cells = static_cast<double>(days) * ft.seg * A;
    const double per = cell_bytes(ys) + kPlaneBytes * static_cast<double>(d.feats.size() + static_cast<size_t>(d.n_slots)) + 3.0 + 2.0;
    assert_fits("Portfolio", cells * per + static_cast<double>(days) * ft.seg * sizeof(factor::stat::Row), days, A, ft.seg);
  }
  if (!ensure_y(F.c, ft, es, F.lr, 0, es.days.size(), ys, F.ex, cancel_, *this) ||
      !ensure_x(F.c, ft, es, F.lr, d.feats, /*retain=*/true, F.ex, cancel_, *this)) {
    if (F.lr.stale()) { // 库换了, 缓存的一切不可信
      F.c.reset();
      std::lock_guard<std::mutex> lock(mutex);
      message = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
    }
    return false;
  }
  const YPlanes &y = F.c.y;
  const int T = y.sh.T;
  const size_t n = y.sh.n();
  F.E.bind(F.c);
  factor::cpu::stat::StatSession &ss = F.E.stat_session();
  if (cancelled())
    return false;

  // ---- 数据检查 (与 Inspect / Run 同一套) ----
  {
    const std::vector<const factor::check::Plane *> in = leaves(F.c, d.feats);
    for (int i = 0; i < N; ++i) {
      check_leaf_domains(d, i, in, out.error);
      if (!out.error.empty())
        return true;
    }
  }

  // ---- 因子: Dag 后序顺序走 ----
  begin(Phase::Running, static_cast<size_t>(d.n_ops + 2)); // 节点 + 评估 + 成本
  if (!F.E.walk(d, nullptr, [&](int i, double) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0)
          publish(1);
        return !cancelled();
      }))
    return false;
  const factor::check::Plane *rp = F.E.plane(root);

  // ---- 秩 + 腿码 + 一级 Row ----
  F.rx.resize(n);
  F.lc.resize(n);
  F.rows.assign(static_cast<size_t>(T), factor::stat::Row{});
  const factor::stat::Span &s = ss.s;
  factor::cpu::stat::rank_x(F.ex, factor::stat::Plane{rp->v.data(), rp->m.data()}, y.gate.data(), j.frame, s, F.rx.data(), ss.w);
  factor::cpu::stat::leg_codes(F.ex, F.rx.data(), s, F.lc.data());
  factor::stat::Accum acc[factor::stat::kMaxHold];
  factor::cpu::stat::accum_rows(F.ex, s, F.rx.data(), ss.L, ss.w, acc, factor::cpu::stat::LandRows{F.rows.data(), T});
  publish(1);
  if (cancelled())
    return false;

  const double cap = y.n_full;
  assert(cap > 0.0);
  constexpr int gl = factor::stat::leg_group(kLong);
  out.dates = es.dates;
  out.lng.resize(static_cast<size_t>(T));
  out.ls.resize(static_cast<size_t>(T));
  for (int t = 0; t < T; ++t) {
    const factor::stat::Row &r = F.rows[static_cast<size_t>(t)];
    out.lng[static_cast<size_t>(t)] = static_cast<float>(r.grp[gl] + r.mkt[kLong] * r.cnt[gl] / factor::stat::book_den(r.cnt[gl], cap));
    out.ls[static_cast<size_t>(t)] = r.ls;
  }

  // ---- 逐行换手成本 (cost_rows 的逐行版: 每行一份 CostAcc, t < h 无 lag 不攒) ----
  const int C = y.ad.n, h = factor::stat::hold_rows(j.hold, factor::geo_of(sc.seg));
  out.amts.assign(y.ad.a, y.ad.a + C);
  out.wimp.assign(static_cast<size_t>(C) * kLegs * T, 0.f);
  out.we.assign(static_cast<size_t>(C) * kLegs * T, 0.f);
  if (C > 0) {
    factor::stat::Cost cs[factor::stat::kMaxCost];
    for (int k = 0; k < C; ++k)
      cs[k] = cost_of(y.costs[static_cast<size_t>(k)]);
    F.ex.rows(T, [&](int t0, int t1, int) {
      for (int t = std::max(t0, h); t < t1; ++t) {
        const size_t base = static_cast<size_t>(t) * A;
        const uint8_t *lag = F.lc.data() + base - static_cast<size_t>(h) * A;
        factor::stat::Cost cr[factor::stat::kMaxCost];
        for (int k = 0; k < C; ++k)
          cr[k] = cs[k].at(base);
        factor::stat::CostAcc ca[factor::stat::kMaxCost] = {};
        long long cov[1 + factor::stat::kMaxCost] = {};
        factor::cpu::stat::cost_row(F.lc.data() + base, &lag, 1, cr, C, A, cap, ca, cov);
        for (int k = 0; k < C; ++k)
          for (int l = 0; l < kLegs; ++l) {
            const size_t o = (static_cast<size_t>(k) * kLegs + l) * T + t;
            out.wimp[o] = static_cast<float>(ca[k].leg[l].wimp);
            out.we[o] = static_cast<float>(ca[k].leg[l].we);
          }
      }
    });
  }
  publish(1);
  return !cancelled();
}

// 日层池等权指数: 无叶作用域 (标签库 + 池成员) → load_y 顺带算 index; 块用完即弃
bool PortfolioService::eval_pool(const PoolJob &j, Flow &F, PoolCurve &out) {
  TraceN("PortfolioPool");
  const FeatureTable &ft = *j.ft;
  assert(j.scope.seg == ft.seg && ft.seg == kDaySeg);
  out.pool = j.scope.pool;
  EvalScope es;
  if (std::string why; !eval_scope(ft, j.scope, {}, es, why)) {
    out.error = why;
    return true;
  }
  open_libs(j.scope, F.lr, &cancel_);
  const int days = static_cast<int>(es.dates.size()), A = static_cast<int>(es.rs.axis.size());
  const YSpec ys{holds_of({ft.y.labels.front().key}), {}, 0.f, /*index=*/true};
  assert_fits("Portfolio", static_cast<double>(days) * A * cell_bytes(ys), days, A, ft.seg);
  ScopeCache c;
  if (!ensure_y(c, ft, es, F.lr, 0, es.days.size(), ys, F.ex, cancel_, *this)) {
    if (F.lr.stale()) {
      F.c.reset();
      std::lock_guard<std::mutex> lock(mutex);
      message = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
    }
    return false;
  }
  assert(c.y.index.size() == es.dates.size());
  out.dates = es.dates;
  out.index = std::move(c.y.index);
  return true;
}

} // namespace GUI::Factors
