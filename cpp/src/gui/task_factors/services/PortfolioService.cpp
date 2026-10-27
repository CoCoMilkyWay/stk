// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // with_engine / eval_job / Probes / LinesP / group_abs
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

// Y 共用组: 同 (层, 池) 的因子装同一份 Y, 一张森林 (PortfolioService::eval 按组)
std::string group_of(const FactorTask &t) { return std::string(factor_level(t.scope.lv).name) + "|" + t.scope.pool; }

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
    e.lv = level_of(o["level"].get<std::string>());
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
      o["level"] = factor_level(e.lv).name;
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

void ResolvePortfolio(const SharedData &data, FeatureTables &tables, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out) {
  out.clear();
  std::map<std::string, std::vector<FactorRow>> scanned; // 因子文件路径 → 行 (同文件只扫一次)
  for (const PortfolioEntry &e : es) {
    PortfolioItem it;
    it.e = e;
    it.name_en = std::filesystem::path(e.file).stem().string();
    if (!level_for_pool(factor_level(e.lv), pool_def(e.pool).tier)) {
      it.missing = "该池没有这个因子层";
      out.push_back(std::move(it));
      continue;
    }
    const std::shared_ptr<const FeatureTable> ft = table_of(tables, data.feature, e.lv);
    const std::filesystem::path p = std::filesystem::path(factor_dir_of(data, e.pool, e.lv)) / e.file;
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

void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, bool gpu, PortfolioRequest &req) {
  assert(!data.asset.items.empty() && "资产轴未就绪");
  req = PortfolioRequest{};
  req.reload = reload;
  for (PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    FactorTask t;
    if (!MakeFactorScope(data, table_of(tables, data.feature, it.e.lv), it.e.pool, gpu, t.scope)) {
      it.missing = "标签库未建";
      continue;
    }
    t.key = it.e.key();
    t.f = FactorRef{it.e.expr, it.frame};
    t.hold = it.e.hold;
    req.factors.push_back(std::move(t));
  }
  std::map<std::string, std::vector<int>> holds; // group_of → 组内全部持有期
  for (const FactorTask &t : req.factors)
    holds[group_of(t)].push_back(t.hold);
  for (auto &[g, hk] : holds) {
    std::sort(hk.begin(), hk.end());
    hk.erase(std::unique(hk.begin(), hk.end()), hk.end());
  }
  for (FactorTask &t : req.factors)
    t.group = holds.at(group_of(t));
  if (req.factors.empty())
    return;
  for (const PoolDef &d : feature_pools()) {
    PoolTask p;
    if (!MakeFactorScope(data, table_of(tables, data.feature, kBasketLevel), d.name, /*gpu=*/false, p.scope))
      continue;
    p.key = d.name;
    req.pools.push_back(std::move(p));
  }
}

std::string PortfolioRequest::sig() const {
  std::string s;
  for (const FactorTask &t : factors)
    s += t.sig() + "\n";
  for (const PoolTask &p : pools)
    s += p.sig() + "\n";
  return s;
}

// ============================================================================
// Service
// ============================================================================

namespace {

// G20 多头出参线 [kCurveLines][rows_all] (Stat 契约【净值】/【换手成本】, 每批资金 1/h̄; fold_book stride 1 = 逐行):
//   pnl = 行的 G20 绝对收益, 平仓行实现; imp = 建仓冲击, fix = 固定量 (CostLeg::fix), 建仓行即付 (不交接批的平仓量一并)
struct CurveP : LinesP<kCurveLines> {
  static constexpr uint8_t need = kNeedRows | kNeedLc;
  double cap = 0.0;
  std::vector<factor::stat::CostAcc> land; // 本块逐行换手 [hi][T] (cost_rows 的 land, 一个金额段)
  static double bytes(const Shape &sh, int H) { return LinesP::bytes(sh, H) + sizeof(factor::stat::CostAcc) * static_cast<double>(H) * sh.T; }
  void open(const Ctx &c) {
    assert(c.c.y.ad.n == 1 && "出参线: 一个金额段");
    LinesP::open(c);
    for (int hi = 0; hi < c.L.hd.n; ++hi)
      x.at_exit[static_cast<size_t>(hi * kCurveLines + kCurveImp)] = x.at_exit[static_cast<size_t>(hi * kCurveLines + kCurveFix)] = 0;
    cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    at(hi, kCurvePnl, t) = static_cast<float>(group_abs(r, factor::stat::leg_group(factor::stat::kLong), cap));
  }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const int H = c.L.hd.n;
    land.assign(static_cast<size_t>(H) * s.T, factor::stat::CostAcc{});
    const std::vector<factor::stat::Cost> cs = costs_of(c.c.y);
    factor::stat::CostAcc acc[factor::stat::kMaxHold];
    factor::cpu::stat::cost_rows(c.ex, v.lc, s, c.L, cs.data(), 1, *c.w, acc, land.data());
    for (int hi = 0; hi < H; ++hi)
      for (int t = s.t_lo; t < s.T; ++t) {
        const factor::stat::CostLeg &cl = land[static_cast<size_t>(hi) * s.T + t].leg[factor::stat::kLong];
        at(hi, kCurveImp, t) = static_cast<float>(cl.imp);
        at(hi, kCurveFix, t) = static_cast<float>(cl.fix);
      }
  }
};
using PortfolioProbes = Probes<CurveP>;

// 缓存跟请求走 (因子 / 池同一条): reload → 全丢; 请求里没有的 key 丢掉
template <class Task, class Curve>
void prune(std::map<std::string, std::shared_ptr<const Curve>> &m, const std::vector<Task> &ts, bool reload) {
  if (reload)
    m.clear();
  std::erase_if(m, [&](const auto &kv) { return std::none_of(ts.begin(), ts.end(), [&](const Task &t) { return t.key == kv.first; }); });
}
// 要算的: 缺 / 签名变了
template <class Task, class Curve>
std::vector<const Task *> todo_of(const std::map<std::string, std::shared_ptr<const Curve>> &m, const std::vector<Task> &ts) {
  std::vector<const Task *> todo;
  for (const Task &t : ts)
    if (const auto it = m.find(t.key); it == m.end() || it->second->sig != t.sig())
      todo.push_back(&t);
  return todo;
}

} // namespace

PortfolioService::PortfolioService() { Start(); }
PortfolioService::~PortfolioService() { Stop(); }

template <class Curve>
void PortfolioService::put(std::map<std::string, std::shared_ptr<const Curve>> &m, const std::string &key, std::shared_ptr<Curve> c) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    m[key] = std::move(c);
    ++result_gen;
  }
  jobs_done.fetch_add(1, std::memory_order_relaxed);
  publish();
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

  // 只有 worker 写 result: 这里读不加锁. 因子按 (层, 池, 串, 口径) 排: 一张森林 = 同 (层, 池) 的一段
  std::vector<const FactorTask *> todo_f = todo_of(result.factors, req.factors);
  std::sort(todo_f.begin(), todo_f.end(), [](const FactorTask *a, const FactorTask *b) {
    return std::tie(a->scope.lv, a->scope.pool, a->f.expr, a->f.frame) < std::tie(b->scope.lv, b->scope.pool, b->f.expr, b->f.frame);
  });
  const std::vector<const PoolTask *> todo_p = todo_of(result.pools, req.pools);
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size() + todo_p.size()), std::memory_order_relaxed);
  const bool ok = [&] {
    for (size_t i0 = 0, i1 = 0; i0 < todo_f.size(); i0 = i1) {
      i1 = i0 + 1;
      while (i1 < todo_f.size() && group_of(*todo_f[i1]) == group_of(*todo_f[i0]))
        ++i1;
      if (!eval(std::vector<const FactorTask *>(todo_f.begin() + static_cast<ptrdiff_t>(i0), todo_f.begin() + static_cast<ptrdiff_t>(i1)), F))
        return false;
    }
    for (const PoolTask *p : todo_p)
      if (!eval(*p, F))
        return false;
    return true;
  }();
  F.drop_engines();
  return ok;
}

// 一张森林 (与 Run / Inspect 同一条管线 eval_job): 根 = 去重的 (规范串, 口径), 探针 hd = 该根的全部 hold; 落线 CurveP (G20 多头), 根评完即逐条发布
bool PortfolioService::eval(const std::vector<const FactorTask *> &ts, FactorFlow &F) {
  TraceN("PortfolioForest");
  const FactorScope &sc = ts.front()->scope;
  RootSet rs;                       // 条目 i ↔ ts[i]
  std::vector<std::vector<int>> hk; // 根 → 持有期
  for (const FactorTask *t : ts) {
    rs.add(*sc.ft, t->f.expr, t->f.frame);
    hk.resize(rs.size());
    hk[static_cast<size_t>(rs.of.back())].push_back(t->hold);
  }
  std::vector<PortfolioProbes> ps(rs.size());
  for (size_t k = 0; k < rs.size(); ++k) {
    std::sort(hk[k].begin(), hk[k].end());
    hk[k].erase(std::unique(hk[k].begin(), hk[k].end()), hk[k].end());
    ps[k].hd = keys_of<Holds>(hk[k]);
  }

  // ---- 管线 eval_job: Y = 组内全部 hold + 首个金额段 (组内共用); X 只留本森林的叶 ----
  Amts ad = keys_of<Amts>(sc.ft->y.costs);
  ad.n = 1;
  JobOut o;
  o.job.spec.y = YSpec{keys_of<Holds>(ts.front()->group), ad, static_cast<float>(sc.max_impact)};
  o.job.who = "Portfolio";
  struct Hooks : HookBase {
    PortfolioService &W;
    const std::vector<const FactorTask *> &ts;
    const RootSet &rs;
    const std::vector<PortfolioProbes> &ps;
    const JobOut &o;
    std::vector<uint8_t> posted = {}; // 条目已发布
    void post(size_t i, const std::string &why, const PortfolioProbes *p) {
      if (posted[i])
        return;
      const FactorTask &t = *ts[i];
      auto c = std::make_shared<FactorCurve>();
      c->sig = t.sig();
      c->lv = t.scope.lv;
      c->hold = t.hold;
      c->error = why;
      if (p) {
        const int hi = factor::stat::hold_index(p->hd, t.hold);
        assert(hi >= 0);
        c->dates = o.job.dm.dates;
        c->amt = o.job.spec.y.ad.k[0];
        c->nav = p->at<CurveP>().out.slice(hi * kCurveLines, kCurveLines);
      }
      W.put(W.result.factors, t.key, std::move(c));
      posted[i] = 1;
    }
    // 值域已查: 坏根的条目发错误线
    void plan(size_t b0, const Forest &, const std::vector<std::string> &why, const std::vector<uint8_t> &) {
      assert(b0 == 0 && "eval_job 根全进一张森林");
      for (size_t i = 0; i < ts.size(); ++i)
        if (const std::string &w = why[static_cast<size_t>(rs.of[i])]; !w.empty())
          post(i, w, nullptr);
    }
    // 发布根 k 的全部条目: 线 hi · kCurveLines 起的一段
    void done(int k) {
      for (size_t i = 0; i < ts.size(); ++i)
        if (rs.of[i] == k)
          post(i, "", &ps[static_cast<size_t>(k)]);
    }
  } hooks{{}, *this, ts, rs, ps, o, std::vector<uint8_t>(ts.size(), 0)};
  const Rc rc = F.with_engine<PortfolioProbes>(sc.gpu, [&](auto &eng) {
    return eval_job(eng, F, sc, rs.roots(), [&](int k) -> PortfolioProbes & { return ps[static_cast<size_t>(k)]; }, o, hooks);
  });
  return settle(rc, o.job.why, [&](const std::string &why) { // 评估域不成 / 全部根坏 → 没发布的条目发错误线
    for (size_t i = 0; i < ts.size(); ++i)
      hooks.post(i, why, nullptr);
  });
}

// 池篮子 (同一条管线 eval_job, 无根; 块 = 门控 + T+1C 标签 + B, 归作业不占 F.c): 块钩子 baskets → 指数 / 估值 / 换手, 整份搬进 PoolCurve
bool PortfolioService::eval(const PoolTask &t, FactorFlow &F) {
  TraceN("PortfolioPool");
  const FeatureTable &ft = *t.scope.ft;
  assert(ft.lv == kBasketLevel && "PoolCurve 按日");
  assert(find_leg(ft.y.labels, factor::stat::hold_close(1)) && "篮子须有 T+1C 标签 (池指数 C(行) → C(D+1))");
  auto c = std::make_shared<PoolCurve>();
  c->sig = t.sig();
  JobOut o;
  o.job.spec.y = YSpec{keys_of<Holds>(std::vector<int>{factor::stat::hold_close(1)}), {}, 0.f};
  o.job.spec.b = true;
  o.job.own = true;
  o.job.who = "Portfolio";
  Probes<> none;
  struct Hooks : HookBase {
    FactorFlow &F;
    Baskets B = {};
    void block(const Block &blk, size_t, double) { B = baskets(F.ex, blk.y, blk.b); }
  } hooks{{}, F};
  const Rc rc = F.with_engine<Probes<>>(false, [&](auto &eng) {
    return eval_job(eng, F, t.scope, {}, [&](int) -> Probes<> & { return none; }, o, hooks);
  });
  if (!settle(rc, o.job.why, [&](const std::string &why) {
        c->error = why;
        put(result.pools, t.key, std::move(c));
      }))
    return false;
  if (rc != Rc::Ok)
    return true;
  Baskets &B = hooks.B;
  const std::vector<misc::Ymd> &dates = o.job.dm.dates;
  const int days = static_cast<int>(dates.size());
  const size_t n = Baskets::kBaskets * dates.size();
  c->dates = dates;
  assert(B.index.n == static_cast<int>(Baskets::kBaskets) * kWeights && B.index.P == days);
  c->index = std::move(B.index);
  assert(B.index_px.n == c->index.n && B.index_px.P == days);
  c->index_px = std::move(B.index_px);
  for (int w = 0; w < kWeights; ++w)
    c->turn[w] = B.turn[w] * factor::stat::kDaysPerYear * ft.seg;
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m) {
      assert(B.val[w][m].size() == n);
      c->val[w][m] = std::move(B.val[w][m]);
    }
  put(result.pools, t.key, std::move(c));
  return true;
}

} // namespace GUI::Factors
