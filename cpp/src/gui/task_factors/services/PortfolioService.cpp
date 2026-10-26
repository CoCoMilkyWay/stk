// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsBook.hpp"   // run_book (G20 多头净值)
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
#include <tuple>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;
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
    FactorJob j;
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
    p.key = d.name;
    p.ft = table_of(tables, data.feature, kDaySeg);
    assert(!p.ft->y.labels.empty() && "日层须有标签 (load_y 至少一组)");
    req.pools.push_back(std::move(p));
  }
}

std::string PortfolioRequest::sig() const {
  std::string s;
  for (const FactorJob &j : factors)
    s += j.sig() + "\n";
  for (const PoolJob &p : pools)
    s += p.sig() + "\n";
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
  std::vector<uint16_t> rx;                      // [T][A]
  std::map<std::string, std::vector<int>> holds; // 本轮请求: group_of → 该组全部因子的持有期 (升序去重; Y 一组一次装全)

  explicit Flow(const std::atomic<bool> &cancel) : ex(misc::exec::hw()), E(ex, cancel, 1) {}
};

namespace {

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
std::string group_of(const FactorJob &j) { return std::to_string(j.scope.seg) + "|" + j.scope.pool; }

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
  std::vector<const FactorJob *> todo_f = todo_of(result.factors, req.factors);
  std::stable_sort(todo_f.begin(), todo_f.end(), [](const FactorJob *a, const FactorJob *b) {
    return std::tie(a->scope.seg, a->scope.pool) < std::tie(b->scope.seg, b->scope.pool);
  });
  F.holds.clear();
  for (const FactorJob &j : req.factors)
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

bool PortfolioService::load_failed(Flow &F) {
  if (F.lr.stale()) { // 库换了, 缓存的一切不可信
    F.c.reset();
    std::lock_guard<std::mutex> lock(mutex);
    message = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
  }
  return false;
}

// 与 InspectService::evaluate 同一条 flow, 只留该 hold 的逐行序列
bool PortfolioService::eval(const FactorJob &j, Flow &F, FactorCurve &out) {
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
  out.seg = sc.seg;
  out.hold = j.hold;
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };

  // ---- 作用域 + 装载 (Y = 同组全部 hold + 最小金额段, 组内因子共用; X 只留本因子的叶) ----
  EvalScope es;
  if (std::string why; !eval_scope(ft, sc, d.feats, es, why)) {
    out.error = why;
    return true;
  }
  open_libs(sc, F.lr, &cancel_);
  const int days = static_cast<int>(es.dates.size()), A = static_cast<int>(es.rs.axis.size());
  const std::vector<int> ck = keys_of(ft.y.costs);
  assert((ck.empty() || ck.front() > 0) && "金额段须 > 0");
  const YSpec ys{holds_of(F.holds.at(group_of(j))), amts_of(ck.empty() ? std::vector<int>{} : std::vector<int>{ck.front()}), static_cast<float>(sc.max_impact),
                 /*index=*/false, /*book=*/true};
  { // 宿主峰值: Y + X (本因子) + 活槽 + rx + 标签秩 + 权 / 出参 [T] (wt / pnl / imp / fix) + 引擎出参 [T] (double × 4)
    const double cells = static_cast<double>(days) * ft.seg * A;
    const double per = cell_bytes(ys) + kPlaneBytes * static_cast<double>(d.feats.size() + static_cast<size_t>(d.n_slots)) + 2.0 + 2.0;
    assert_fits("Portfolio", cells * per + static_cast<double>(days) * ft.seg * (sizeof(float) * 4 + sizeof(double) * 4), days, A, ft.seg);
  }
  if (!ensure_y(F.c, ft, es, F.lr, 0, es.days.size(), ys, F.ex, cancel_, *this) ||
      !ensure_x(F.c, ft, es, F.lr, d.feats, /*retain=*/true, F.ex, cancel_, *this))
    return load_failed(F);
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
      check_leaf_domains(F.ex, d, i, in, out.error);
      if (!out.error.empty())
        return true;
    }
  }

  // ---- 因子: Dag 后序顺序走 ----
  begin(Phase::Running, static_cast<size_t>(d.n_ops + 2)); // 节点 + 秩 + 净值引擎
  if (!F.E.walk(d, nullptr, [&](int i, double) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0)
          publish(1);
        return !cancelled();
      }))
    return false;
  const factor::check::Plane *rp = F.E.plane(root);

  // ---- 秩 ----
  const double cap = y.n_full;
  assert(cap > 0.0);
  F.rx.resize(n);
  out.dates = es.dates;
  const factor::stat::Span &s = ss.s;
  factor::cpu::stat::rank_x(F.ex, factor::stat::Plane{rp->v.data(), rp->m.data()}, y.gate.data(), j.frame, s, F.rx.data(), ss.w);
  int hi = 0;
  while (hi < y.hd.n && y.hd.h[hi] != j.hold)
    ++hi;
  assert(hi < y.hd.n && "组 Y 须含本因子的持有期");
  publish(1);
  if (cancelled())
    return false;

  // ---- G20 多头净值引擎 (FactorsBook; 一个篮子 = rx 落 G20, 秩平面 RankSrc) ----
  BookSpec bs = book_spec(y, j.hold);
  assert(y.ad.n <= 1);
  out.amt = y.ad.n && bs.kind != BookKind::Day ? y.ad.a[0] : 0; // Day 型集合竞价成交, 无盘口冲击
  const auto g20 = [](uint16_t r16) { return uint64_t{factor::stat::leg_code(r16) == factor::stat::kLcLong}; };
  const std::vector<float> wt = intent_weights(F.ex, F.rx.data(), T, A, 1, g20, [cap](int) { return cap; });
  bs.nb = 1, bs.side[0] = 1, bs.fields = bk_bit(kBkImp) | bk_bit(kBkFix) | bk_bit(kBkExpo);
  bs.unit_in = out.amt > 0 ? 0.0 : 1.0;
  const LegPlane &lab = y.labels[static_cast<size_t>(hi)];
  BookOut bo;
  run_book(F.ex, bs,
           RankSrc<decltype(g20)>{F.rx.data(), g20, wt.data(), 1, lab.v[kLong].data(), lab.m.data(), out.amt > 0 ? y.costs[0].v[kLong].data() : nullptr,
                                  static_cast<size_t>(A)},
           bo);
  const auto take = [&](BookField k, std::vector<float> &v) {
    const double *p = bo.at(k, 0);
    v.assign(p, p + T);
  };
  take(kBkPnl, out.pnl);
  take(kBkImp, out.imp);
  take(kBkFix, out.fix);
  const size_t S = static_cast<size_t>(y.sh.seg);
  out.expo.assign(static_cast<size_t>(y.sh.days), 0.f);
  for (size_t dd = 0; dd < out.expo.size(); ++dd) {
    double e = 0.0;
    for (size_t t = dd * S; t < (dd + 1) * S; ++t)
      e += bo.at(kBkExpo, 0)[t];
    out.expo[dd] = static_cast<float>(e / static_cast<double>(S));
  }
  publish(1);
  return !cancelled();
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
  const YSpec ys{holds_of({ft.y.labels.front().key}), {}, 0.f, /*index=*/true};
  assert_fits("Portfolio", static_cast<double>(days) * A * cell_bytes(ys), days, A, ft.seg);
  ScopeCache c;
  if (!ensure_y(c, ft, es, F.lr, 0, es.days.size(), ys, F.ex, cancel_, *this))
    return load_failed(F);
  const size_t n = YPlanes::kBaskets * es.dates.size();
  out.dates = es.dates;
  for (int w = 0; w < kWeights; ++w) {
    assert(c.y.index[w].size() == n);
    out.index[w] = std::move(c.y.index[w]);
    out.turn[w] = c.y.turn[w];
  }
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m) {
      assert(c.y.val[w][m].size() == n);
      out.val[w][m] = std::move(c.y.val[w][m]);
    }
  return true;
}

} // namespace GUI::Factors
