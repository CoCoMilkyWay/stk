// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/PortfolioService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // with_engine / eval_job / eval_baskets / Probes / LinesP / group_abs
#include "gui/task_factors/services/FactorsFile.hpp"   // ValidFactorName / FactorRow / ScanFactorFile
#include "misc/profiler.hpp"
#include "shared/Basket.hpp"     // basket_count
#include "shared/FeatureLib.hpp" // pool_def
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;

std::filesystem::path portfolio_path(const std::string &dir, const std::string &name) {
  assert(ValidFactorName(name) && "组合名: 仅 [A-Za-z0-9_]");
  return std::filesystem::path(dir) / (name + ".json");
}

// 一张森林的组 = 同评估域: (层, 池, 所用库). 评估期 = 所用库落盘日交集, 所用库不同的因子不同组 (只用 bar 叶的不被 Lob 库截短)
std::tuple<LevelId, std::string, LibMask> group_of(const FactorTask &t) {
  const FeatureTable &ft = *t.scope.ft;
  std::vector<std::string> feats;
  for (const factor::expr::Node &n : ft.reparse(t.f.expr).nodes)
    if (n.op < 0)
      feats.push_back(n.feat);
  return {t.scope.lv, t.scope.pool, used_libs(ft, pool_id(t.scope.pool), ft.x.ids(feats))};
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
    e.lv = level_of(o["level"].get<std::string>());
    e.file = o["file"].get<std::string>();
    e.expr = o["expr"].get<std::string>();
    e.hold = o["hold"].get<int>();
    e.sign = o.value("sign", 1);
    assert((e.sign == 1 || e.sign == -1) && "组合文件: sign 须是 ±1");
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
      o["sign"] = e.sign;
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
    const std::shared_ptr<const FeatureTable> ft = table_of(tables, data.feature, e.lv);
    const std::filesystem::path p = std::filesystem::path(factor_dir_of(data, e.pool, e.lv)) / e.file;
    const bool exists = std::filesystem::is_regular_file(p);
    auto [sit, fresh] = scanned.try_emplace(p.string());
    if (fresh && exists)
      ScanFactorFile(p, *ft, sit->second);
    const auto rit = std::find_if(sit->second.begin(), sit->second.end(), [&](const FactorRow &r) { return r.expr == e.expr && r.sign == e.sign; });
    if (!exists) {
      it.missing = "因子文件不存在: " + p.string();
    } else if (rit == sit->second.end()) {
      it.missing = "因子文件里没有这个权重 (规范串 / 方向已变)";
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
    t.f = FactorRef{it.e.expr, it.frame, factor::stat::dir_of(it.e.sign)};
    t.hold = it.e.hold;
    req.factors.push_back(std::move(t));
  }
  if (req.factors.empty())
    return;
  BasketTask b;
  if (MakeFactorScope(data, table_of(tables, data.feature, kBasketLevel), pool_def(kAllPool).name, /*gpu=*/false, b.scope))
    req.baskets = std::move(b);
}

std::string PortfolioRequest::sig() const {
  std::string s;
  for (const FactorTask &t : factors)
    s += t.sig() + "\n";
  if (baskets)
    s += baskets->sig() + "\n";
  return s;
}

// ============================================================================
// Service
// ============================================================================

namespace {

// G20 多头的账 [hold][kBookCols][rows_all] (Stat 契约【净值】/【换手成本】, 每批资金 1/h̄; fold_book stride 1 = 逐行):
//   ret = 行的 G20 (所评方向的高端组; 行已定向) 绝对收益, 平仓行实现; imp = 买入冲击, fix = 固定量 (CostBook::fix), 建仓行即付 (不交接批的平仓量一并); buy = 增仓量, 记在建仓行
struct BookP : LinesP<kBookCols> {
  static constexpr uint8_t need = kNeedRows | kNeedEc;
  static constexpr Segs segs = Segs::First;
  double cap = 0.0;
  void open(const Ctx &c) {
    assert(c.c.y.ad.n == 1 && "账: 一个金额段");
    LinesP::open(c);
    for (int hi = 0; hi < c.L.hd.n; ++hi)
      for (const int k : {kBookImp, kBookFix, kBookBuy})
        x.at_exit[static_cast<size_t>(hi * kBookCols + k)] = 0;
    cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    at(hi, kBookRet, t) = static_cast<float>(group_abs(r, factor::stat::end_group(factor::stat::kTop), cap));
  }
  void root(const Ctx &c, const RootView &v) {
    const std::vector<factor::stat::Cost> cs = costs_of(c.c.y);
    factor::stat::CostAcc acc[factor::stat::kMaxHold];
    // 一个金额段: cost_rows 的逐行下标 = hi
    factor::cpu::stat::cost_rows(c.ex, v.ec, c.s(), c.L, cap, cs.data(), 1, *c.w, acc, [&](size_t hi, int t, const factor::stat::CostAcc &r) {
      const factor::stat::CostBook &cl = r.book[factor::stat::kTop];
      at(static_cast<int>(hi), kBookImp, t) = static_cast<float>(cl.imp);
      at(static_cast<int>(hi), kBookFix, t) = static_cast<float>(cl.fix);
      at(static_cast<int>(hi), kBookBuy, t) = static_cast<float>(cl.buy);
    });
  }
};
using PortfolioProbes = Probes<BookP>;

// 因子缓存: reload → 全丢; 本请求的 (used = now) 都留, 其余按上次被请求的先后 (旧的先丢) 丢到总字节 ≤ kKeepBytes
constexpr size_t kKeepBytes = size_t{1} << 30;
size_t bytes_of(const PortfolioBook &b) {
  size_t n = b.dates.size() * sizeof(misc::Ymd);
  for (const BookLines &c : b.col)
    n += c.v.size() * sizeof(float);
  return n;
}
void prune(std::map<std::string, std::shared_ptr<const PortfolioBook>> &m, std::map<std::string, uint64_t> &used, uint64_t now, bool reload) {
  if (reload)
    m.clear();
  std::erase_if(used, [&](const auto &kv) { return kv.second != now && !m.contains(kv.first); });
  size_t bytes = 0;
  std::vector<std::pair<uint64_t, std::string>> old;
  for (const auto &[k, c] : m) {
    bytes += bytes_of(*c);
    const auto u = used.find(k);
    assert(u != used.end() && "缓存的因子必被请求过");
    if (u->second != now)
      old.emplace_back(u->second, k);
  }
  std::sort(old.begin(), old.end());
  for (const auto &[u, k] : old) {
    if (bytes <= kKeepBytes)
      break;
    bytes -= bytes_of(*m.at(k));
    m.erase(k), used.erase(k);
  }
}
// 要算的: 缺 / 签名变了
std::vector<const FactorTask *> todo_of(const std::map<std::string, std::shared_ptr<const PortfolioBook>> &m, const std::vector<FactorTask> &ts) {
  std::vector<const FactorTask *> todo;
  for (const FactorTask &t : ts)
    if (const auto it = m.find(t.key); it == m.end() || it->second->sig != t.sig())
      todo.push_back(&t);
  return todo;
}

} // namespace

PortfolioService::PortfolioService() : FactorWorker(Prio::Interactive) { Start(); }
PortfolioService::~PortfolioService() { Stop(); }

template <class Set>
void PortfolioService::put(Set &&set) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    set(result);
    ++result_gen;
  }
  jobs_done.fetch_add(1, std::memory_order_relaxed);
  publish();
}

bool PortfolioService::run(PortfolioRequest &req) {
  FactorFlow &F = flow();
  ++tick_;
  for (const FactorTask &t : req.factors)
    used_[t.key] = tick_;
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
    prune(result.factors, used_, tick_, req.reload);
    if (req.reload || !req.baskets)
      result.baskets.reset(), result.stocks.reset();
    ++result_gen;
  }
  publish();

  // 只有 worker 写 result: 这里读不加锁. 一张森林 = 一组 (group_of), 组内按 (串, 口径, 方向) 排
  const std::vector<const FactorTask *> todo_f = todo_of(result.factors, req.factors);
  std::map<std::tuple<LevelId, std::string, LibMask>, std::vector<const FactorTask *>> groups;
  for (const FactorTask *t : todo_f)
    groups[group_of(*t)].push_back(t);
  for (auto &[g, ts] : groups)
    std::sort(ts.begin(), ts.end(), [](const FactorTask *a, const FactorTask *b) { return std::tie(a->f.expr, a->f.frame, a->f.dir) < std::tie(b->f.expr, b->f.frame, b->f.dir); });
  const bool todo_b = req.baskets && !result.baskets;
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size()) + todo_b, std::memory_order_relaxed);
  const bool ok = [&] {
    Lease l(F);
    if (!l)
      return false;
    if (req.reload)
      F.S.clear();
    for (const auto &[g, ts] : groups)
      if (!eval(ts, F))
        return false;
    return !todo_b || eval(*req.baskets, F);
  }();
  F.end();
  return ok;
}

// 一张森林 (与 Run / Inspect 同一条管线 eval_job): 根 = 去重的 (规范串, 口径, 方向), 根的 hd = 其条目的 hold 之并; 落账 BookP (G20 多头), 根评完即逐条发布
bool PortfolioService::eval(const std::vector<const FactorTask *> &ts, FactorFlow &F) {
  TraceN("PortfolioForest");
  const FactorScope &sc = ts.front()->scope;
  RootSet rs; // 条目 i ↔ ts[i]
  for (const FactorTask *t : ts)
    rs.add(*sc.ft, t->f.expr, t->f.frame, t->f.dir, keys_of<Holds>(std::vector<int>{t->hold}));
  std::vector<PortfolioProbes> ps(rs.size());

  // ---- 管线 eval_job: Y = 各根 hold 之并 + 首个金额段 (BookP) ----
  JobOut o;
  o.job.who = "Portfolio";
  struct Hooks : HookBase {
    PortfolioService &W;
    const std::vector<const FactorTask *> &ts;
    const std::vector<PortfolioProbes> &ps;
    const JobOut &o;
    std::shared_ptr<PortfolioBook> book(int e, const std::string &why) const {
      const FactorTask &t = *ts[static_cast<size_t>(e)];
      auto b = std::make_shared<PortfolioBook>();
      b->sig = t.sig();
      b->lv = t.scope.lv;
      b->error = why;
      return b;
    }
    void land(int e, std::shared_ptr<const PortfolioBook> b) {
      W.put([&](PortfolioResult &r) { r.factors[ts[static_cast<size_t>(e)]->key] = std::move(b); });
    }
    void fail(int e, const std::string &why) { land(e, book(e, why)); }
    // 条目 e 的根 k 评完: 列 hi · kBookCols 起的一段, 一列一线
    void post(int e, int k) {
      const PortfolioProbes &p = ps[static_cast<size_t>(k)];
      auto b = book(e, "");
      const int hi = factor::stat::hold_index(o.fr.hd[static_cast<size_t>(k)], ts[static_cast<size_t>(e)]->hold);
      assert(hi >= 0);
      b->dates = o.job.dm.dates;
      b->amt = o.job.spec.y.ad.k[0];
      for (int c = 0; c < kBookCols; ++c)
        b->col[c] = p.at<BookP>().out.slice(hi * kBookCols + c, 1);
      land(e, std::move(b));
    }
  };
  const Rc rc = F.with_engine<PortfolioProbes>(sc.gpu, [&](auto &eng) {
    return eval_job(eng, F, sc, rs, [&](int k) -> PortfolioProbes & { return ps[static_cast<size_t>(k)]; }, o, Hooks{{}, *this, ts, ps, o});
  });
  return settle(rc);
}

// 篮子 (管线 eval_baskets: all 池评估域, 整期装不下截最近的日, 一趟扫完) → 两本账同发布: 篮子 basket_count 线 (不扣成本: 无 imp / fix 列)
// + 个股 all 池轴每只一线 (只毛收益; 线 → 全局 id = 评估域池轴)
bool PortfolioService::eval(const BasketTask &t, FactorFlow &F) {
  TraceN("PortfolioBaskets");
  assert(t.scope.ft->lv == kBasketLevel && "篮子账按日");
  Job j;
  j.who = "Portfolio";
  Baskets bs;
  const Rc rc = eval_baskets(F, t.scope, j, bs);
  if (!settle(rc))
    return false;
  auto b = std::make_shared<PortfolioBook>(), s = std::make_shared<PortfolioBook>();
  for (PortfolioBook *x : {b.get(), s.get()})
    x->sig = t.sig(), x->lv = kBasketLevel;
  if (rc != Rc::Ok) {
    b->error = s->error = j.why;
  } else {
    b->dates = s->dates = j.dm.dates;
    assert(bs.index.n == static_cast<int>(basket_count()) && static_cast<size_t>(bs.index.P) == b->dates.size());
    assert(bs.buy.n == bs.index.n && bs.buy.P == bs.index.P);
    assert(bs.stock.n == static_cast<int>(j.dm.rs.axis.size()) && bs.stock.P == bs.index.P);
    b->col[kBookRet] = std::move(bs.index);
    b->col[kBookBuy] = std::move(bs.buy);
    s->col[kBookRet] = std::move(bs.stock);
    s->ids = j.dm.rs.axis.ids;
    assert(std::ranges::is_sorted(s->ids) && "池轴 sub → 全局 id 升序 (line_of 二分)");
    for (int m = 0; m < kPoolVals; ++m)
      b->val[m] = std::move(bs.val[m]), s->val[m] = std::move(bs.stock_val[m]);
  }
  put([&](PortfolioResult &r) { r.baskets = std::move(b), r.stocks = std::move(s); });
  return true;
}

} // namespace GUI::Factors
