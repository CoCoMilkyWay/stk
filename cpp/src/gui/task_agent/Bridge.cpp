// Bridge — 见头文件. 评估路径调 SearchFactor / EvalFactorFile, CMake 列入 MATH_PRECISE 源.
#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Async.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Bay.hpp"

#include "factor/Expr.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Search/Contract.hpp"
#include "features/Storage/FeatureLevels.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/TimeIndex.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/engine/File.hpp"
#include "gui/engine/Load.hpp"
#include "gui/util/PoolTip.hpp"
#include "misc/date.hpp"
#include "orders/OrdersDay.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace GUI::Agent {

static_assert(kEvalYears == Engine::kEvalYears);

namespace {

namespace fx = factor::expr;
namespace fs = factor::stat;
namespace fsearch = factor::search;
using text::num;

Engine::FactorLevelId level_id(int level) {
  assert(level >= 0 && static_cast<size_t>(level) < Engine::kFactorLevelCount);
  return static_cast<Engine::FactorLevelId>(level);
}

std::shared_ptr<const Engine::FeatureTable> table(const SharedData &data, int level) {
  return Engine::table_of(data.feature, level_id(level));
}

Engine::FactorDir candidates(const Scope &sc, bool test) {
  return {.file = disk::AgentFactorFile, .log = disk::RunAgentSearchLog, .key = catalog::key(sc, test)};
}

std::optional<Engine::FactorScope> fscope_of(const SharedData &data, const std::shared_ptr<const Engine::FeatureTable> &ft, const Scope &sc) {
  Engine::FactorScope fsc;
  if (!Engine::MakeFactorScope(data, ft, sc.pool, sc.years, /*gpu=*/false, fsc))
    return std::nullopt;
  return fsc;
}

const FeatureMetadata &meta_of(const Engine::FeatureTable &ft, const Engine::FeatCol &c) { return ft.feature->meta(c.tier).features[c.level][c.col]; }

std::string family_of(const FeatureMetadata &m) {
  const std::string_view f = m.file, pre = "features/Node/", ext = ".hpp";
  assert(f.starts_with(pre) && f.ends_with(ext) && "节点文件在 features/Node/ 下");
  return std::string(f.substr(pre.size(), f.size() - pre.size() - ext.size()));
}

Src src_of(const Engine::FeatCol &c) { return c.tier == TierId::Lob ? Src::Lob : Src::Bar; }

constexpr const char *kTfDef[] = {"", "sign(x)·ln(1+|x|)", "asinh(x)", "tanh(x)", "sign(x)·√|x|"};
static_assert(std::size(kTfDef) == static_cast<size_t>(ts::TfId::kCount));

bool measure_word(const std::string &w) {
  return w == "amt" || w == "vol" || w == "n" || w == "all" || std::any_of(w.begin(), w.end(), [](char ch) { return ch >= '0' && ch <= '9'; });
}

Opened opened_of(const Engine::FeatureTable &ft, const Scope &sc, bool has_lob) {
  Opened o{.lob_open = sc.lob, .has_lob = has_lob};
  const Open &pick = sc.leaves_of(Engine::factor_level(ft.lv).name);
  o.leaves.assign(ft.x.leaves.size(), 0);
  for (size_t i = 0; i < ft.x.leaves.size(); ++i) {
    const Engine::FeatCol &l = ft.x.leaves[i];
    if (l.allowed && sc.has(src_of(l)) && pick.has(l.code))
      o.leaves[i] = 1;
  }
  o.ops.assign(std::size(fx::kOps), 0);
  for (size_t i = 0; i < std::size(fx::kOps); ++i)
    o.ops[i] = sc.ops.has(fx::kOps[i].name);
  return o;
}

// 行的收盘时刻 HH:MM 与竞价段行数
std::vector<std::string> clocks_of(const Engine::FactorLevel &l) {
  std::vector<std::string> out;
  for (int k = 0; k < l.seg; ++k) {
    ClockTime c{15, 0, 0};
    if (l.lvl == L_1min) {
      const ClockTime m = L_1min_to_Clock(static_cast<size_t>(k));
      const int total = m.hour * 60 + m.minute + 1;
      c = {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
    } else if (l.seg > 1) {
      c = bin_to_Clock(static_cast<size_t>(240 / (l.seg - 1)), static_cast<size_t>(k));
    }
    char b[16];
    format_time_hm(b, sizeof(b), c);
    out.emplace_back(b);
  }
  return out;
}

int open_of(const std::vector<std::string> &clock) {
  if (clock.size() == 1)
    return 0;
  const int n = static_cast<int>(std::count_if(clock.begin(), clock.end(), [](const std::string &c) { return c <= "09:30"; }));
  assert(n >= 1 && clock[static_cast<size_t>(n - 1)] == "09:30");
  return n;
}

// 同式键: 截面根 CsRank / CsNormRank / CsZ 互换不改截面排序与分组
std::string twin_of_expr(const fx::Expr &e) {
  const fx::Node &r = e.root();
  const std::string_view nm = r.op >= 0 ? std::string_view(fx::kOps[r.op].name) : std::string_view();
  return nm == "CsRank" || nm == "CsNormRank" || nm == "CsZ" ? "Cs:" + fx::to_string(e, r.args[0]) : e.canon;
}

// 评估期两档缓存 (仅 Bar 库 / 含 Lob 库): bind 时各算至多一次, 扫上百个因子文件零重复 span_of
struct Period {
  std::string pool;
  misc::Ymd start = 0, end = 0;
  int days = 0;
  bool same(const std::string &p, misc::Ymd s, misc::Ymd e, int n) const { return days > 0 && p == pool && s == start && e == end && n == days; }
  std::string text() const { return misc::dash(start) + " ~ " + misc::dash(end) + ", " + std::to_string(days) + " 天"; }
};

// out 非空: 另交回评估域 (Lab::bind 取评估期交易日与池轴代码)
Period period_of(const std::optional<Engine::FactorScope> &fsc, const Engine::FeatureTable &ft, const std::vector<Engine::LeafId> &leaves, Engine::Domain *out = nullptr) {
  Period s;
  if (!fsc)
    return s;
  Engine::Domain dm;
  std::string why;
  if (!Engine::span_of(ft, *fsc, leaves, SIZE_MAX, dm, why))
    return s;
  s.pool = fsc->pool, s.start = dm.first(), s.end = dm.dates.back(), s.days = static_cast<int>(dm.stat_days());
  if (out)
    *out = std::move(dm);
  return s;
}

struct SpanCache {
  Period bar_only, with_lob;
  bool has_lob = false;

  SpanCache(const std::optional<Engine::FactorScope> &fsc, const Engine::FeatureTable &ft) {
    bar_only = period_of(fsc, ft, {});
    for (size_t i = 0; i < ft.x.leaves.size(); ++i)
      if (ft.x.leaves[i].allowed && ft.x.leaves[i].tier == TierId::Lob) {
        with_lob = period_of(fsc, ft, {static_cast<Engine::LeafId>(i)});
        has_lob = true;
        break;
      }
  }
  const Period &for_expr(const Engine::FeatureTable &ft, const fx::Expr &e) const {
    if (has_lob)
      for (const fx::Node &n : e.nodes)
        if (n.op < 0 && !n.target && ft.x.find(n.feat)->tier == TierId::Lob)
          return with_lob;
    return bar_only;
  }
};

using Fails = std::map<std::string, std::string>;

// fixed = Config::fixed_impact (净多空扣固定段换手成本)
Result result_of(const Engine::FeatureTable &ft, const SpanCache &spans, const Opened &open, const Engine::FactorDir &dir, const std::string &file, int hold,
                 double fixed, const Fails &fails) {
  std::vector<Engine::FactorRow> rows;
  Engine::ScanFactorFile(dir, file, ft, rows);
  assert(!rows.empty());
  const Engine::FactorRow &r0 = rows.front();
  Result r{.file = r0.file, .name = r0.name_en, .name_cn = r0.name_cn, .tmpl = r0.tmpl, .note = r0.note};
  bool opened = true;
  Period now;
  if (!r0.tmpl.empty()) {
    const fx::Expr te = ft.reparse(r0.tmpl);
    r.ops = te.n_ops;
    r.twin = twin_of_expr(te);
    now = spans.for_expr(ft, te);
    opened = open.all_open(ft, te);
  }
  bool fresh = true;
  const auto mk = std::find_if(r0.searched.begin(), r0.searched.end(), [&](const Engine::SearchMark &m) { return m.hold == hold; });
  if (mk != r0.searched.end())
    fresh = now.same(now.pool, mk->start_date, mk->end_date, mk->days);
  if (const auto it = fails.find(r.file); it != fails.end())
    r.error = "Search 放弃: " + it->second;
  bool run = true;
  for (const Engine::FactorRow &w : rows) {
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
    Weight x{.expr = w.expr, .sign = w.sign, .hold_src = w.hold_src};
    const fsearch::Roll *rl = Engine::RowRoll(w, hold);
    x.has = rl != nullptr;
    if (rl) {
      const fs::HoldStat *h = w.find_hold(hold);
      assert(h && "RowRoll 非空须有该持有期的 HoldStat");
      assert(w.costs.front().amt == Engine::kAmtFixed && "成本段首个是固定段");
      const fs::CostAcc &c = w.turn_of(*h);
      x.score = fsearch::score_of(*rl);
      x.ric = h->ric.mean;
      x.lng = fs::end_period(*h, fs::kTop), x.sht = -fs::end_period(*h, fs::kBot);
      x.gross = fs::ls_period(*h), x.net = fs::net_ls_period(*h, c, fixed), x.cost = x.gross - x.net;
      x.turn = fs::turn_annual(*h, c);
      assert(h->n_all > 0 && "有滚动窗须有统计行");
      const double n = static_cast<double>(h->n_all);
      x.buy = 0.5 * (c.book[fs::kTop].buy + c.book[fs::kBot].buy) / n, x.sell = 0.5 * (c.book[fs::kTop].sell + c.book[fs::kBot].sell) / n;
      x.impact = fixed;
      double mid = 0.0;
      for (const float g : h->grp)
        mid += g / static_cast<double>(fs::kGroups);
      for (const float g : h->grp)
        x.grp.push_back(g - mid);
      for (const Engine::YearStat &y : *w.find_years(hold))
        x.years.push_back({y.year, y.ric, y.wins ? y.ric_med : std::numeric_limits<double>::quiet_NaN(), y.turn});
    }
    r.ws.push_back(std::move(x));
  }
  std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
  r.visible = fresh && r.ops <= kMaxOps && opened;
  const bool null = r.ws.empty() ? mk != r0.searched.end() : run;
  r.verdict = !r.error.empty() ? Verdict::Broken : r.best() ? Verdict::Weights
                                               : null       ? Verdict::Null
                                                            : Verdict::Pending;
  return r;
}

void sort_results(std::vector<Result> &v) {
  std::stable_sort(v.begin(), v.end(), [](const Result &a, const Result &b) {
    const Weight *x = a.best(), *y = b.best();
    return (x != nullptr) != (y != nullptr) ? x != nullptr : x && x->score > y->score;
  });
}

std::vector<Result> scan(const Engine::FeatureTable &ft, const SpanCache &spans, const Opened &open, const Engine::FactorDir &dir, int hold, double fixed,
                         const Fails &fails) {
  std::vector<Result> out;
  for (const std::string &file : dir.files())
    out.push_back(result_of(ft, spans, open, dir, file, hold, fixed, fails));
  sort_results(out);
  return out;
}

} // namespace

std::string Opened::closed_err(const Engine::FeatureTable &ft, const fx::Expr &e) const {
  std::vector<std::string_view> seen;
  std::string ls, os;
  for (const fx::Node &x : e.nodes) {
    if (has(ft, x))
      continue;
    const std::string_view name = x.op >= 0 ? std::string_view(fx::kOps[x.op].name) : std::string_view(x.feat);
    if (std::ranges::contains(seen, name))
      continue;
    seen.push_back(name);
    if (x.op >= 0) {
      os += (os.empty() ? "" : ", ") + std::string(name);
    } else {
      const Src s = src_of(*ft.x.find(name));
      const char *why = s == Src::Lob && !lob_open ? (!has_lob ? " (lob 在本层无落盘)" : " (lob 来源未开放)") : "";
      ls += (ls.empty() ? "" : ", ") + std::string(name) + why;
    }
  }
  if (ls.empty() && os.empty())
    return {};
  return "用了本会话未开放的" + (ls.empty() ? "" : " 叶: " + ls + ";") + (os.empty() ? "" : " 算子: " + os + ";") + " 只能用系统提示特征表与算子表里列的";
}

// =============================================================================
// 叶词、门槛与作用域目录 catalog
// =============================================================================
std::pair<std::string, std::vector<std::string>> LeafWords(const std::string &code) {
  const size_t dot = code.find('.', code.find('.') + 1);
  assert(dot != std::string::npos && "叶名 = 档.层.列");
  std::vector<std::string> ws;
  for (size_t a = dot + 1, b; a <= code.size(); a = b + 1) {
    b = std::min(code.find('_', a), code.size());
    ws.push_back(code.substr(a, b - a));
  }
  return {code.substr(0, dot), ws};
}

std::string LeafKin(const std::string &code) {
  const auto [g, ws] = LeafWords(code);
  std::string s = g + ".";
  for (size_t k = 0; k < ws.size(); ++k)
    s += (k ? "_" : "") + (measure_word(ws[k]) ? std::string("*") : ws[k]);
  return s;
}

std::string LeafCol(const std::string &code) { return code.substr(code.find('.', code.find('.') + 1) + 1); }

std::vector<std::vector<const Leaf *>> KinGroups(const std::vector<const Leaf *> &ls) {
  std::vector<std::vector<const Leaf *>> gs;
  std::unordered_map<std::string_view, size_t> at;
  for (const Leaf *l : ls)
    if (const auto [it, fresh] = at.try_emplace(l->kin, gs.size()); fresh)
      gs.push_back({l});
    else
      gs[it->second].push_back(l);
  return gs;
}

std::string KinName(const std::vector<const Leaf *> &g) {
  const size_t W = LeafWords(g.front()->code).second.size();
  std::vector<std::vector<std::string>> at(W);
  for (const Leaf *l : g) {
    const std::vector<std::string> ws = LeafWords(l->code).second;
    assert(ws.size() == W && "同组叶词数相同");
    for (size_t k = 0; k < W; ++k)
      if (std::find(at[k].begin(), at[k].end(), ws[k]) == at[k].end())
        at[k].push_back(ws[k]);
  }
  size_t all = 1;
  for (const std::vector<std::string> &v : at)
    all *= v.size();
  std::string s;
  if (all != g.size()) {
    for (const Leaf *l : g)
      s += (s.empty() ? "" : ", ") + LeafCol(l->code);
    return s;
  }
  for (size_t k = 0; k < W; ++k) {
    std::string w;
    for (const std::string &x : at[k])
      w += (w.empty() ? "" : ",") + x;
    s += (k ? "_" : "") + (at[k].size() > 1 ? "{" + w + "}" : w);
  }
  return s;
}

const Weight *Result::best() const {
  const auto it = std::find_if(ws.begin(), ws.end(), [](const Weight &w) { return w.has && std::isfinite(w.score); });
  return it != ws.end() ? &*it : ws.empty() || !ws.front().has ? nullptr
                                                               : &ws.front();
}

std::string GateMiss(const Gate &g, const Result &r) {
  if (r.verdict == Verdict::Broken)
    return "BROKEN: " + r.error;
  for (const Weight &w : r.ws)
    if (w.has && w.score >= g.ric)
      return "";
  const Weight *w = r.best();
  if (!w)
    return "本持有期无权重";
  return "最佳权重未达入库门槛: 中位 rIC " + num(w->score) + " < " + num(g.ric);
}

namespace catalog {

std::vector<Choice> pools() {
  std::vector<Choice> out;
  const std::vector<PoolId> &po = GUI::pool_order();
  for (size_t i = 0; i < po.size(); ++i)
    out.push_back({static_cast<int>(i), pool_def(po[i]).name});
  return out;
}
std::vector<PoolRow> pool_rows(int level) {
  std::vector<PoolRow> out;
  const std::vector<PoolId> &po = GUI::pool_order();
  for (size_t i = 0; i < po.size(); ++i) {
    const PoolDef &d = pool_def(po[i]);
    PoolRow r{.key = static_cast<int>(i), .name = d.name, .name_cn = d.name_cn};
    for (const Src s : kVals<Src>)
      r.built[s] = load_lib(lib_id(po[i], s == Src::Lob ? TierId::Lob : TierId::Bar)).ready();
    r.factors = Engine::factor_dir_of(d.name, level_id(level)).files().size();
    out.push_back(std::move(r));
  }
  return out;
}
bool pool_movable(int at) { return at >= kPinnedPools; }
void move_pool(SharedData &data, int from, int to) { GUI::move_pool(data, {from, to}); }
std::string pool_cn(const std::string &pool) { return pool_def(pool).name_cn; }
std::vector<Choice> levels() {
  std::vector<Choice> out;
  for (const Engine::FactorLevel &l : Engine::kFactorLevels)
    out.push_back({l.id, l.name});
  return out;
}
std::vector<Choice> holds(const SharedData &data, int level) {
  std::vector<Choice> out;
  for (const Engine::LegCol &lg : table(data, level)->y.labels)
    if (fsearch::search_hold(lg.key))
      out.push_back({lg.key, fs::hold_name(lg.key)});
  return out;
}
std::string hold_desc(int hold, int level) { return Engine::hold_desc(hold, level_id(level)); }
bool gpu() { return factor::gpu::available(); }

bool ready(const SharedData &data, const Scope &sc, bool gpu) {
  Engine::FactorScope probe;
  return Engine::MakeFactorScope(data, table(data, sc.level), sc.pool, sc.years, gpu, probe);
}
std::string label(const Scope &sc) {
  return sc.pool + " / " + Engine::factor_level(level_id(sc.level)).name + " / " + fs::hold_name(sc.hold);
}
disk::Key key(const Scope &sc, bool test) {
  return {.pool = sc.pool, .level = Engine::factor_level(level_id(sc.level)).name, .hold = fs::hold_token(sc.hold), .test = test};
}

} // namespace catalog

// =============================================================================
// Lab 实现: 单线程 Async worker (与左翼 Chat 同构, 零共享可变态)
// =============================================================================
namespace {

struct Job {
  Tool tool = Tool::ViewFeature;
  std::shared_ptr<const ViewEnv> env;
  LabTask task;
};

LabStep first_step(Cat c) { return c == Cat::Look ? LabStep::Look : c == Cat::Query ? LabStep::Docs
                                                                                    : LabStep::Search; }
Engine::Prio prio_of(Cat c) { return c == Cat::Trial ? Engine::Prio::Batch : Engine::Prio::Interactive; }

} // namespace

struct Lab::Impl {
  const SharedData *data = nullptr;
  Scope sc;
  bool is_test = false;
  Engine::FactorDir dir;
  Opened open;
  Context context;
  std::optional<SpanCache> spans;
  std::vector<Result> results;
  Fails fails;
  std::map<std::string, Sweep> sweeps;
  std::map<std::string, std::vector<Daily>> days;
  std::shared_ptr<const ViewEnv> env_cache;

  Progress prog;
  Async<Job, LabStep, LabOut> worker;

  Impl()
      : worker([this](Job &j, const auto &pipe) -> LabOut {
          prog.begin(Phase::Loading, 0);
          const Cat cat = def(j.tool).cat;
          pipe.step(first_step(cat));
          Engine::FactorFlow flow(pipe.cancel, prog, prio_of(cat));
          const std::function<void(LabStep)> step = [&](LabStep s) { pipe.step(s); };
          static const ViewEnv kNoEnv;
          Bay bay{j.env ? *j.env : kNoEnv, flow, pipe.cancel, step};
          ToolOut out = j.task(bay);
          if (bay.cancelled())
            return {.cancelled = true};
          return {.cancelled = false, .out = std::move(out)};
        }) {}

  std::shared_ptr<const Engine::FeatureTable> ft() const { return table(*data, sc.level); }
  double fixed() const { return data->config.fixed_impact; }

  Engine::FactorScope factor_scope(bool gpu) const {
    Engine::FactorScope fsc;
    [[maybe_unused]] const bool ok = Engine::MakeFactorScope(*data, ft(), sc.pool, sc.years, gpu, fsc);
    assert(ok && "会话内作用域已就绪");
    fsc.dir = dir;
    return fsc;
  }

  std::shared_ptr<const ViewEnv> view_env() {
    if (env_cache)
      return env_cache;
    auto p = std::make_shared<ViewEnv>();
    ViewEnv &env = *p;
    env.fsc = factor_scope(false);
    env.hold = sc.hold;
    env.first = context.first, env.last = context.last;
    env.open = context.open;
    env.level = context.level;
    env.clock = context.clock;
    env.opened = open;
    env.cands.reserve(results.size());
    for (const Result &r : results) {
      CandRef cr;
      if (!r.visible) {
        cr.error = "候选 `" + r.name + "` 不在当前可见范围 (" + (r.ops > kMaxOps ? "算子超上限" : "评估期或开放集不同") + ")";
      } else if (const Weight *w = r.best()) {
        cr.expr = w->expr, cr.sign = w->sign;
        cr.label = r.name + (w->sign < 0 ? " (反向: " : " (") + w->expr + ")";
      } else {
        cr.error = "候选 `" + r.name + "` 没有可用的权重 (" + def(r.verdict).cn + ")";
      }
      env.cands.emplace(r.name, std::move(cr));
    }
    env.orders = data->asset.catalog();
    env.exchanges.resize(data->asset.items.size());
    for (size_t i = 0; i < data->asset.items.size(); ++i)
      env.exchanges[i] = data->asset.items[i].exchange_type;
    env_cache = std::move(p);
    return env_cache;
  }

  void attach_extras(Result &r) const {
    if (const auto it = sweeps.find(r.file); it != sweeps.end())
      r.sweep = it->second;
    const Weight *w = r.best();
    if (const auto it = days.find(r.file); it != days.end() && w)
      for (const Daily &d : it->second)
        if (d.expr == w->expr && d.sign == w->sign)
          r.daily = d;
  }

  void rescan_all() {
    results = scan(*ft(), *spans, open, dir, sc.hold, fixed(), fails);
    for (Result &r : results)
      attach_extras(r);
    env_cache.reset();
  }

  void rescan_one(const std::string &file) {
    Result got = result_of(*ft(), *spans, open, dir, file, sc.hold, fixed(), fails);
    attach_extras(got);
    const auto it = std::find_if(results.begin(), results.end(), [&](const Result &x) { return x.file == file; });
    if (it != results.end())
      *it = std::move(got);
    else
      results.push_back(std::move(got));
    sort_results(results);
    env_cache.reset();
  }

  // 落文件前的全部查验 (空 = 可落); e = 解析好的式
  std::string vet(const Cand &c, fx::Expr &e) const {
    if (!Engine::ValidFactorName(c.name))
      return "英文名不合规则: 仅 [A-Za-z0-9_], ≤ 64";
    if (!Engine::ValidFactorNameCn(c.name_cn))
      return "中文名不合规则: 1..10 个汉字, 不含字母数字标点";
    for (const Result &r : results)
      if (r.name == c.name)
        return "候选英文名 `" + c.name + "` 已用过, 换个名字";
    for (const Result &r : context.library)
      if (r.name == c.name)
        return "名字 `" + c.name + "` 与正式因子库重名, 换个名字";

    const std::shared_ptr<const Engine::FeatureTable> t = ft();
    fs::Frame frame = fs::Frame::CS;
    std::string err;
    if (!fx::parse(c.expr, t->lookup(), t->seg, e, err) || !fx::root_frame(e, t->seg, frame, err) || !t->check_grid(e, err))
      return err;
    if (std::string cl = open.closed_err(*t, e); !cl.empty())
      return cl;
    if (e.n_ops > kMaxOps)
      return "算子节点 " + std::to_string(e.n_ops) + " 个 > 上限 " + std::to_string(kMaxOps) + " (精简结构, 建议 " + std::to_string(kGoodOps) + " 个以内)";

    const std::string tw = twin_of_expr(e);
    for (const Result &r : results)
      if (r.visible && r.twin == tw)
        return "与已试候选 `" + r.name + "` 同式" + (r.tmpl == e.canon ? "" : " (只换了截面根 CsRank / CsNormRank / CsZ: 分组与 rIC 完全一样)");
    for (const Result &r : context.library)
      if (r.visible && r.twin == tw)
        return "与正式库因子 `" + r.name + "` 同式" + (r.tmpl == e.canon ? "" : " (只换了截面根 CsRank / CsNormRank / CsZ)");
    return {};
  }
};

Engine::FactorScope FactorScopeOf(const Lab &lab, bool gpu) { return lab.impl().factor_scope(gpu); }

Lab::Lab() : impl_(std::make_unique<Impl>()) {}
Lab::~Lab() = default;

void Lab::bind(const SharedData &data, const Scope &sc, bool test) {
  assert(!busy() && "worker 正忙, 不能换作用域");
  const bool same_cand = impl_->data && catalog::key(impl_->sc, impl_->is_test) == catalog::key(sc, test);
  impl_->data = &data, impl_->sc = sc, impl_->is_test = test;
  impl_->dir = candidates(sc, test);
  if (!same_cand)
    impl_->fails.clear(), impl_->sweeps.clear(), impl_->days.clear();

  const std::shared_ptr<const Engine::FeatureTable> ft = impl_->ft();
  const std::optional<Engine::FactorScope> fsc = fscope_of(data, ft, sc);
  impl_->spans.emplace(fsc, *ft);
  impl_->open = opened_of(*ft, sc, impl_->spans->has_lob);

  Context &c = impl_->context;
  c = Context{};
  c.pool = sc.pool, c.pool_cn = catalog::pool_cn(sc.pool);
  c.level = Engine::factor_level(level_id(sc.level)).name;
  c.hold = fs::hold_name(sc.hold), c.hold_key = fs::hold_token(sc.hold), c.hold_desc = catalog::hold_desc(sc.hold, sc.level);
  Per<Src, bool> has{}, on{};
  std::vector<Engine::LeafId> opened_ids;
  for (size_t i = 0; i < ft->x.leaves.size(); ++i) {
    const Engine::FeatCol &l = ft->x.leaves[i];
    if (!l.allowed)
      continue;
    const Src src = src_of(l);
    has[src] = true;
    const FeatureMetadata &m = meta_of(*ft, l);
    const size_t tf = static_cast<size_t>(m.ts_tf);
    Leaf x{l.code, m.name_cn, m.formula, m.description, m.ts_tf == ts::TfId::None ? "" : ts::TF_NAMES[tf], kTfDef[tf], family_of(m), src};
    x.kin = LeafKin(x.code) + "|" + x.tf;
    x.hay = text::lower(x.code + "\n" + x.name_cn + "\n" + x.formula + "\n" + x.desc + "\n" + x.tf + "\n" + x.family);
    if (impl_->open.leaves[i]) {
      on[src] = true;
      opened_ids.push_back(static_cast<Engine::LeafId>(i));
      c.leaves.push_back(x);
    }
    c.all_leaves.push_back(std::move(x));
  }
  for (const Src s : kVals<Src>)
    if (on[s])
      c.srcs += (c.srcs.empty() ? "" : " / ") + std::string(def(s).id);
  c.has_lob = has[Src::Lob];
  Engine::Domain dm;
  const Period base = period_of(fsc, *ft, opened_ids, &dm);
  if (base.days > 0) {
    const std::vector<misc::Ymd> ds = dm.stat_dates();
    for (const misc::Ymd d : ds)
      c.days.push_back(static_cast<int>(d));
    const AssetAxis &ax = asset_axis();
    const size_t n = dm.rs.axis.size();
    for (size_t i = 0; i < n; ++i)
      c.codes.push_back(ax.code(dm.rs.axis.global(i)));
    c.traded.assign(ds.size() * n, 1);
    if (const std::shared_ptr<const OrdersCatalog> oc = data.asset.catalog())
      for (size_t d = 0; d < ds.size(); ++d)
        if (const size_t di = oc->day_idx(ds[d]); di != OrdersCatalog::kNoDay)
          for (size_t i = 0; i < n; ++i)
            c.traded[d * n + i] = !oc->at(di, dm.rs.axis.global(i)).empty();
  }
  c.grid = kLevelToken[Engine::factor_level(ft->lv).lvl];
  c.years = sc.years;
  c.window = base.days > 0 ? base.text() : "未就绪";
  c.first = static_cast<int>(base.start), c.last = static_cast<int>(base.end);
  c.seg = ft->seg;
  c.clock = clocks_of(Engine::factor_level(ft->lv));
  c.open = open_of(c.clock);
  c.ts_root_d = fs::ts_norm_d(ft->seg);
  c.max_d = factor::kMaxD;
  c.roll_parts = fsearch::kRollParts;
  for (size_t i = 0; i < std::size(fx::kOps); ++i) {
    const fx::OpInfo &o = fx::kOps[i];
    Op x{o.name, o.c_name, fx::operand_tex(o), factor::dom_name(o.out), o.op, o.note};
    if (impl_->open.ops[i])
      c.ops.push_back(x);
    c.all_ops.push_back(std::move(x));
  }
  c.library = scan(*ft, *impl_->spans, impl_->open, Engine::factor_dir_of(sc.pool, level_id(sc.level)), sc.hold, impl_->fixed(), {});
  impl_->rescan_all();
}

const Scope &Lab::scope() const { return impl_->sc; }
bool Lab::test() const { return impl_->is_test; }
const Context &Lab::ctx() const { return impl_->context; }
const std::vector<Result> &Lab::results() const { return impl_->results; }

const Result *Lab::tried(std::string_view name) const {
  for (const Result &r : impl_->results)
    if (r.visible && r.name == name)
      return &r;
  return nullptr;
}

const Result &Lab::result(const std::string &file) const {
  const auto it = std::find_if(impl_->results.begin(), impl_->results.end(), [&](const Result &r) { return r.file == file; });
  assert(it != impl_->results.end() && "候选文件不在候选目录里");
  return *it;
}

std::string Lab::search_log(const std::string &file) const {
  const std::filesystem::path p = impl_->dir.log_path(file);
  if (!std::filesystem::exists(p))
    return {};
  std::ifstream f(p, std::ios::binary);
  return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

std::string Lab::vet(const Cand &c) const {
  assert(impl_->data);
  fx::Expr e;
  return impl_->vet(c, e);
}

Staged Lab::stage(Cand c, std::string_view hyp_title) {
  assert(impl_->data && !busy());
  const auto bad = [&](std::string why) {
    Staged s{.cand = std::move(c)};
    s.res = {.name = s.cand.name, .name_cn = s.cand.name_cn, .tmpl = s.cand.expr, .note = s.cand.note, .error = std::move(why), .verdict = Verdict::Unstaged, .visible = false};
    return s;
  };
  fx::Expr e;
  if (std::string why = impl_->vet(c, e); !why.empty())
    return bad(std::move(why));
  const std::string full_note = c.hyp + (hyp_title.empty() ? "" : " " + std::string(hyp_title)) + " · " + c.note;
  std::string err;
  const std::string file = Engine::AddFactorFile(impl_->dir, *impl_->ft(), c.name, c.name_cn, e.canon, full_note, err);
  if (file.empty())
    return bad(std::move(err));
  impl_->fails.erase(file);
  impl_->rescan_one(file);
  return {std::move(c), file, result(file)};
}

const Result &Lab::absorb(const std::string &file, bool ok, const std::string &err, Sweep sweep, std::vector<Daily> days) {
  assert(impl_->data && !busy() && !file.empty());
  if (ok)
    impl_->fails.erase(file);
  else
    impl_->fails[file] = err;
  impl_->sweeps[file] = std::move(sweep);
  impl_->days[file] = std::move(days);
  impl_->rescan_one(file);
  return result(file);
}

Adopted Lab::adopt(std::string name, const Gate &g, bool dry) {
  assert(impl_->data && !busy());
  const Result *r = tried(name);
  if (!r) {
    const auto any = std::find_if(impl_->results.begin(), impl_->results.end(), [&](const Result &x) { return x.name == name; });
    return {std::move(name), any == impl_->results.end() ? "候选目录里没有 `" + name + "`" : "候选 `" + name + "` 不可见 (不在当前评估期或开放集上)"};
  }
  if (const std::string why = GateMiss(g, *r); !why.empty())
    return {std::move(name), why};
  if (impl_->is_test || dry)
    return {std::move(name), {}};
  const Engine::FactorDir formal = Engine::factor_dir_of(impl_->sc.pool, level_id(impl_->sc.level));
  const std::filesystem::path dst = formal.path(r->file);
  if (std::filesystem::exists(dst))
    return {std::move(name), "正式因子库已有同名文件 `" + r->file + "`"};
  disk::write(formal.file, formal.of(r->file), disk::read(impl_->dir.file, impl_->dir.of(r->file)));
  impl_->context.library = scan(*impl_->ft(), *impl_->spans, impl_->open, formal, impl_->sc.hold, impl_->fixed(), {});
  return {std::move(name), {}};
}

void Lab::discard(const std::string &file) {
  assert(impl_->data && !busy() && !file.empty());
  Engine::DeleteFactorFile(impl_->dir, file);
  disk::remove(impl_->dir.log, impl_->dir.of(file));
  impl_->fails.erase(file), impl_->sweeps.erase(file), impl_->days.erase(file);
  std::erase_if(impl_->results, [&](const Result &r) { return r.file == file; });
  impl_->env_cache.reset();
}

void Lab::send(Tool tool, LabTask task) {
  assert(impl_->data && !busy() && task);
  std::shared_ptr<const ViewEnv> env;
  if (def(tool).cat != Cat::Query)
    env = impl_->view_env();
  impl_->worker.send({tool, std::move(env), std::move(task)});
}

void Lab::cancel() { impl_->worker.cancel(); }
bool Lab::busy() const { return impl_->worker.busy(); }

std::optional<Lab::Tick> Lab::poll() {
  auto t = impl_->worker.poll();
  return t ? std::optional<Tick>(Tick{t->step, std::move(t->out)}) : std::nullopt;
}

void Lab::live(LabLive &dst) const {
  const size_t tot = impl_->prog.total.load(std::memory_order_relaxed);
  const size_t dn = impl_->prog.done.load(std::memory_order_relaxed);
  dst.frac = tot > 0 ? std::clamp(static_cast<double>(dn) / static_cast<double>(tot), 0.0, 1.0) : 0.0;
}

} // namespace GUI::Agent
