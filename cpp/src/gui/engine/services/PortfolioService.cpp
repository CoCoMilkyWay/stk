// PortfolioService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/services/PortfolioService.hpp"

#include "disk/Disk.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "gui/engine/Engine.hpp"
#include "gui/engine/File.hpp"
#include "misc/profiler.hpp"
#include "shared/Basket.hpp"
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <map>
#include <numeric>
#include <sstream>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Engine {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;

disk::Key portfolio_key(const std::string &name) {
  assert(ValidFactorName(name) && "组合名: 仅 [A-Za-z0-9_]");
  return {.name = name};
}

std::tuple<FactorLevelId, std::string, LibMask> group_of(const FactorTask &t) {
  const FeatureTable &ft = *t.scope.ft;
  std::vector<std::string> feats;
  for (const factor::expr::Node &n : ft.reparse(t.f.expr).nodes)
    if (n.op < 0)
      feats.push_back(n.feat);
  return {t.scope.lv, t.scope.pool, used_libs(ft, pool_id(t.scope.pool), ft.x.ids(feats))};
}

} // namespace

// ============================================================================
// 组合文件与请求解析
// ============================================================================

std::vector<std::string> ListPortfolios() {
  std::vector<std::string> names;
  for (const disk::Key &k : disk::list(disk::PortfolioFile))
    names.push_back(k.name);
  return names;
}

std::vector<PortfolioEntry> LoadPortfolio(const std::string &name) {
  const json j = json::parse(disk::read(disk::PortfolioFile, portfolio_key(name)), nullptr, /*allow_exceptions=*/false);
  assert(!j.is_discarded() && j.is_object() && j.contains("factors") && j["factors"].is_array() && "组合文件: 顶层须是 {factors: [...]}");
  std::vector<PortfolioEntry> es;
  for (const json &o : j["factors"]) {
    assert(o.is_object() && o.contains("pool") && o.contains("level") && o.contains("file") && o.contains("expr") && o.contains("hold") &&
           "组合文件: 条目缺键");
    PortfolioEntry e;
    e.pool = o["pool"].get<std::string>();
    e.lv = factor_level_of(o["level"].get<std::string>());
    e.file = o["file"].get<std::string>();
    e.expr = o["expr"].get<std::string>();
    e.hold = o["hold"].get<int>();
    e.sign = o.value("sign", 1);
    assert((e.sign == 1 || e.sign == -1) && "组合文件: sign 须是 ±1");
    es.push_back(std::move(e));
  }
  return es;
}

void SavePortfolio(const std::string &name, const std::vector<PortfolioEntry> &es) {
  std::ostringstream f;
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
  disk::write(disk::PortfolioFile, portfolio_key(name), f.str());
}

bool RenamePortfolio(const std::string &from, const std::string &to) {
  if (disk::exists(disk::PortfolioFile, portfolio_key(to)))
    return false;
  disk::rename(disk::PortfolioFile, portfolio_key(from), portfolio_key(to));
  return true;
}

void DeletePortfolio(const std::string &name) { disk::remove(disk::PortfolioFile, portfolio_key(name)); }

void ResolvePortfolio(const SharedData &data, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out) {
  out.clear();
  std::map<std::string, std::vector<FactorRow>> scanned;
  for (const PortfolioEntry &e : es) {
    PortfolioItem it;
    it.e = e;
    it.name_en = std::filesystem::path(e.file).stem().string();
    const std::shared_ptr<const FeatureTable> ft = table_of(data.feature, e.lv);
    const FactorDir dir = factor_dir_of(e.pool, e.lv);
    const std::filesystem::path p = dir.path(e.file);
    const bool exists = std::filesystem::is_regular_file(p);
    auto [sit, fresh] = scanned.try_emplace(p.string());
    if (fresh && exists)
      ScanFactorFile(dir, e.file, *ft, sit->second);
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

void MakePortfolioRequest(const SharedData &data, std::vector<PortfolioItem> &items, bool reload, bool gpu, PortfolioRequest &req) {
  assert(!data.asset.items.empty() && "资产轴未就绪");
  req = PortfolioRequest{};
  req.reload = reload;
  for (PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    FactorTask t;
    if (!MakeFactorScope(data, table_of(data.feature, it.e.lv), it.e.pool, /*years=*/0, gpu, t.scope)) {
      it.missing = "标签库未建";
      continue;
    }
    t.key = it.e.key();
    t.f = FactorRef{it.e.expr, it.frame, factor::stat::dir_of(it.e.sign)};
    t.hold = it.e.hold;
    req.factors.push_back(std::move(t));
  }
  BasketTask b;
  if (MakeFactorScope(data, table_of(data.feature, kBasketLevel), pool_def(kAllPool).name, /*years=*/0, /*gpu=*/false, b.scope))
    req.baskets = std::move(b);
}

std::string BasketTask::sig() const {
  const FeatureLib &L = scope.libs[kAllBar];
  return L.dir + "#" + std::to_string(L.axis.hash) + "#" + std::to_string(L.dates.size()) + "#" + std::to_string(basket_table().hash());
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
// 纯计算核与薄 Service 壳
// ============================================================================

namespace {

struct BookP : LinesP<kBookCols> {
  static constexpr uint8_t need = kNeedRows | kNeedEc;
  static constexpr Segs segs = Segs::First;
  double cap = 0.0;
  void init(const PassCtx &p) {
    assert(p.ad.n == 1 && "账: 一个金额段");
    LinesP::init(p);
    for (int hi = 0; hi < p.hd.n; ++hi)
      for (const int k : {kBookImp, kBookFix, kBookBuy})
        x.at_exit[static_cast<size_t>(hi * kBookCols + k)] = 0;
  }
  void open(const Ctx &c) {
    LinesP::open(c);
    cap = c.L.cap;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    at(hi, kBookRet, t) = static_cast<float>(group_abs(r, factor::stat::end_group(factor::stat::kTop), cap));
  }
  void close(const Ctx &c, const RootView &v) {
    factor::stat::Cost cs[factor::stat::kMaxCost];
    [[maybe_unused]] const int C = fill_costs(c.c.y, cs);
    assert(C == 1);
    factor::stat::CostAcc acc[factor::stat::kMaxHold];
    factor::cpu::stat::cost_rows(c.ex, v.ec, c.s(), c.L, cap, cs, 1, *c.w, acc, [&](size_t hi, int t, const factor::stat::CostAcc &r) {
      const factor::stat::CostBook &cl = r.book[factor::stat::kTop];
      at(static_cast<int>(hi), kBookImp, t) = static_cast<float>(cl.imp);
      at(static_cast<int>(hi), kBookFix, t) = static_cast<float>(cl.fix);
      at(static_cast<int>(hi), kBookBuy, t) = static_cast<float>(cl.buy);
    });
  }
};
using PortfolioProbes = Probes<BookP>;

constexpr double kKeepBytes = static_cast<double>(size_t{1} << 30);

} // namespace

Rc EvalPortfolioGroup(std::span<const FactorTask *const> ts, FactorFlow &F,
                      const std::function<void(size_t, std::shared_ptr<const PortfolioBook>)> &on_book) {
  TraceN("PortfolioForest");
  assert(!ts.empty());
  const FactorScope &sc = ts.front()->scope;
  RootSet rs;
  for (const FactorTask *t : ts) {
    rs.hd = keys_union(rs.hd, keys_of<Holds>(std::vector<int>{t->hold}));
    rs.add(*sc.ft, t->f.expr, t->f.frame, t->f.dir);
  }
  std::vector<PortfolioProbes> ps(rs.size());
  ForestOut o;
  o.job.who = "Portfolio";
  return with_backend<PortfolioProbes>(F, sc.gpu, [&](auto &eng) {
    return eval_forest(eng, F, sc, rs, std::span(ps), o, /*whole=*/false, [&](int e, int k, const std::string &why) {
      const FactorTask &t = *ts[static_cast<size_t>(e)];
      auto b = std::make_shared<PortfolioBook>();
      b->lv = t.scope.lv;
      if (!why.empty()) {
        b->error = why;
      } else {
        const int hi = factor::stat::hold_index(o.job.spec.y.hd, t.hold);
        assert(hi >= 0);
        b->dates = o.job.dm.stat_dates();
        b->amt = o.job.spec.y.ad.k[0];
        b->keys = {0};
        for (int c = 0; c < kBookCols; ++c)
          b->col[c] = ps[static_cast<size_t>(k)].at<BookP>().out.slice(hi * kBookCols + c, 1);
      }
      if (on_book)
        on_book(static_cast<size_t>(e), std::move(b));
    });
  });
}

Rc EvalBasketBooks(const BasketTask &t, FactorFlow &F, Quote::PerMarket<std::shared_ptr<const PortfolioBook>> &out) {
  TraceN("PortfolioBaskets");
  assert(t.scope.ft->lv == kBasketLevel && "篮子账按日");
  Job j;
  j.who = "Portfolio";
  Baskets bs;
  const Rc rc = eval_baskets(F, t.scope, j, bs);
  if (aborted(rc))
    return rc;

  Quote::PerMarket<std::shared_ptr<PortfolioBook>> q;
  for (std::shared_ptr<PortfolioBook> &x : q) {
    x = std::make_shared<PortfolioBook>();
    x->lv = kBasketLevel;
    if (rc != Rc::Ok)
      x->error = j.why;
    else
      x->dates = j.dm.stat_dates();
  }
  if (rc == Rc::Ok) {
    q[Quote::kBasket]->keys.resize(basket_table().size());
    std::iota(q[Quote::kBasket]->keys.begin(), q[Quote::kBasket]->keys.end(), 0u);
    q[Quote::kStock]->keys = j.dm.rs.axis.ids;
    assert(std::ranges::is_sorted(q[Quote::kStock]->keys) && "池轴 sub → 全局 id 升序 (line_of 二分)");
    for (int k = 0; k < Quote::kMarkets; ++k)
      static_cast<Book &>(*q[k]) = std::move(bs[k]);
  }
  for (int k = 0; k < Quote::kMarkets; ++k)
    out[k] = std::move(q[k]);
  return rc;
}

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
  if (req.reload)
    cache_.clear(), quotes_sig_.clear();
  std::map<std::string, std::shared_ptr<const PortfolioBook>> hit;
  std::vector<const FactorTask *> todo_f;
  for (const FactorTask &t : req.factors)
    if (const auto *p = cache_.find(t.sig()))
      hit.emplace(t.key, *p);
    else
      todo_f.push_back(&t);
  const std::string bsig = req.baskets ? req.baskets->sig() : std::string{};
  const bool keep_b = req.baskets && bsig == quotes_sig_;
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
    result.factors = std::move(hit);
    if (!keep_b)
      result.quotes = {}, quotes_sig_.clear();
    ++result_gen;
  }
  cache_.trim([](double c) { return c > kKeepBytes; });
  publish();

  std::map<std::tuple<FactorLevelId, std::string, LibMask>, std::vector<const FactorTask *>> groups;
  for (const FactorTask *t : todo_f)
    groups[group_of(*t)].push_back(t);
  for (auto &[g, ts] : groups)
    std::sort(ts.begin(), ts.end(), [](const FactorTask *a, const FactorTask *b) {
      return std::tie(a->f.expr, a->f.frame, a->f.dir) < std::tie(b->f.expr, b->f.frame, b->f.dir);
    });
  const bool todo_b = req.baskets && !keep_b;
  jobs_done.store(0, std::memory_order_relaxed);
  jobs_total.store(static_cast<int>(todo_f.size()) + todo_b, std::memory_order_relaxed);

  FactorFlow F = flow();
  Lease l(F);
  if (!l)
    return false;
  if (req.reload)
    F.S.clear();
  for (const auto &[g, ts] : groups) {
    const Rc rc = EvalPortfolioGroup(ts, F, [&](size_t e, std::shared_ptr<const PortfolioBook> b) {
      const double bytes = static_cast<double>(b->bytes());
      cache_.put(ts[e]->sig(), b, bytes);
      put([&](PortfolioResult &r) { r.factors[ts[e]->key] = b; });
      cache_.trim([](double c) { return c > kKeepBytes; });
    });
    if (!settle(rc))
      return false;
  }
  if (!todo_b)
    return true;
  Quote::PerMarket<std::shared_ptr<const PortfolioBook>> q;
  const Rc rc = EvalBasketBooks(*req.baskets, F, q);
  if (!settle(rc))
    return false;
  quotes_sig_ = bsig;
  put([&](PortfolioResult &r) { r.quotes = std::move(q); });
  return true;
}

} // namespace GUI::Engine
