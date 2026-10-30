// Bridge — 见头文件. agent/ 里唯一 include 项目因子 / 特征 / 服务头的 TU (CMake PRECISE: 因子契约头的消费者)
#include "agent/Bridge.hpp"
#include "agent/Config.hpp" // kMaxOps / kGoodOps / kWorkDir
#include "agent/Text.hpp"

#include "factor/Dag.hpp"             // build (叶 → 评估期)
#include "factor/Expr.hpp"            // kOps / operand_tex
#include "factor/GpuRun.hpp"          // available
#include "factor/Search/Contract.hpp" // search_hold / Score / kMinMono
#include "factor/Search/Space.hpp"    // slots_of (曲面的槽)
#include "features/TimeIndex.hpp"     // bin_to_Clock (行的收盘时刻)
#include "gui/task_factors/services/FactorsFile.hpp"
#include "gui/task_factors/services/FactorsScope.hpp" // span_of
#include "gui/task_factors/services/LensService.hpp"
#include "gui/task_factors/services/RunService.hpp"
#include "gui/task_factors/services/SearchService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // hold_desc
#include "misc/date.hpp"                      // dash
#include "shared/AssetAxis.hpp"               // 码 ↔ 全局资产下标
#include "shared/FeatureLib.hpp"              // feature_pools
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <tuple>

namespace agent {

namespace F = GUI::Factors;
namespace fs = std::filesystem;

static_assert(kGroups == factor::stat::kGroups && kRankNone == factor::stat::kRankNone && kRankMax == factor::stat::kRankMax);

double Mono(const double *g, int n) { return factor::stat::mono_of(g, n); }

namespace {

using text::num;

F::LevelId level_id(int level) {
  assert(level >= 0 && static_cast<size_t>(level) < F::kLevelCount);
  return static_cast<F::LevelId>(level);
}

// 字段表视图: 每层建一次 (GUI 线程独占)
std::shared_ptr<const F::FeatureTable> table(const SharedData &data, int level) {
  static F::FeatureTables tables;
  return F::table_of(tables, data.feature, level_id(level));
}

// 表达式的算子节点数 (解析不了 = 0, 由落文件报原因)
int ops_of(const F::FeatureTable &ft, const std::string &src) {
  factor::expr::Expr e;
  std::string err;
  return factor::expr::parse(src, ft.lookup(), ft.seg, e, err) ? e.n_ops : 0;
}

// 具体式 (无自由参数) 的解析; 不成 → false + err
bool concrete(const F::FeatureTable &ft, const std::string &src, factor::expr::Expr &e, std::string &err) {
  if (!factor::expr::parse(src, ft.lookup(), ft.seg, e, err))
    return false;
  if (e.is_template()) {
    err = "含自由参数 (裸 d / k / k2): 观察只接具体式, 参数写成数字";
    return false;
  }
  return true;
}

// 具体特征式 (任意子式) → CsRank 包装的规范串; 不成 → false + err
bool feature_root(const F::FeatureTable &ft, const std::string &src, std::string &root, std::string &err) {
  factor::expr::Expr e;
  if (!concrete(ft, src, e, err))
    return false;
  [[maybe_unused]] const bool ok = concrete(ft, "CsRank(" + e.canon + ")", e, err);
  assert(ok && "具体式包一层 CsRank 必可解析");
  root = e.canon;
  return true;
}

// 层 lvl 的叶 档.层.col (字段表里有且可作输入; 没有 → 空)
std::string leaf(const F::FeatureTable &ft, size_t lvl, const std::string &col) {
  for (const F::FeatCol &c : ft.x.leaves)
    if (c.allowed && c.level == lvl && c.code.size() > col.size() && c.code.ends_with("." + col))
      return c.code;
  return {};
}
PerBar<std::string> kline_of(const F::FeatureTable &ft) {
  const F::FactorLevel &l = F::factor_level(ft.lv);
  const auto bar = [&](const char *f) {
    char b[64];
    std::snprintf(b, sizeof(b), l.kline, f);
    return leaf(ft, l.lvl, b);
  };
  // 量额: K 线列名式里没有的层 (1min 的 K 线在盘口档) 取本层别档的同名列
  const auto flow = [&](const char *f) {
    const std::string s = bar(f);
    return s.empty() ? leaf(ft, l.lvl, f) : s;
  };
  PerBar<std::string> k;
  k[Bar::Close] = bar("close"), k[Bar::High] = bar("high"), k[Bar::Low] = bar("low"), k[Bar::Volume] = flow("volume"), k[Bar::Amount] = flow("amount");
  k[Bar::PreClose] = leaf(ft, L_1day, "pre_close"), k[Bar::LimUp] = leaf(ft, L_1day, "lim_up"), k[Bar::LimDn] = leaf(ft, L_1day, "lim_dn");
  return k;
}

// 层的每行收盘时刻 HH:MM: 日层 = 收盘; 分钟层 = 该分钟末; 分钟箱层 = 箱收盘
std::vector<std::string> clocks_of(const F::FactorLevel &l) {
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
// 每日开头属集合竞价段的行数: 日内层收盘时刻不晚于 09:30 的行 (分钟箱 1 行, 1min 15 行); 日层 0
int open_of(const std::vector<std::string> &clock) {
  if (clock.size() == 1)
    return 0;
  const int n = static_cast<int>(std::count_if(clock.begin(), clock.end(), [](const std::string &c) { return c <= "09:30"; }));
  assert(n >= 1 && clock[static_cast<size_t>(n - 1)] == "09:30");
  return n;
}

// Lens 的落地 → Look (r16 原样, 行 / 格换成 agent 的形); 因子根与 labels 对齐; 特征根前 n_feats 个 = LookSpec::feats, 其后按 kBars 序 = bars 里取了的 K 线叶
Look look_of(F::LensResult &&r, const F::FeatureTable &ft, const std::vector<std::string> &labels, size_t n_feats, const PerBar<bool> &bars) {
  Look o;
  o.error = std::move(r.error);
  if (!o.error.empty())
    return o;
  o.dates.assign(r.dates.begin(), r.dates.end());
  o.seg = ft.seg;
  o.clock = clocks_of(F::factor_level(ft.lv));
  o.open = open_of(o.clock);
  for (const uint32_t id : r.ids)
    o.codes.push_back(asset_axis().code(id));
  o.gate = std::move(r.gate), o.label = std::move(r.label), o.mkt = std::move(r.mkt);
  assert(r.roots.size() == labels.size());
  for (size_t j = 0; j < r.roots.size(); ++j) {
    F::LensRoot &x = r.roots[j];
    LookRoot y;
    y.label = labels[j];
    y.error = std::move(x.error), y.rx = std::move(x.rx);
    for (const factor::stat::Row &w : x.rows) {
      LookRow z;
      z.ok = w.ok, z.ric = w.ric, z.mkt = w.mkt;
      std::copy_n(w.grp, kGroups, z.grp);
      std::copy_n(w.cnt, kGroups, z.cnt);
      y.rows.push_back(z);
    }
    for (const F::LensCell &c : x.cells)
      y.cells.push_back({c.t, c.a, c.e == factor::stat::kTop, c.ret});
    o.factors.push_back(std::move(y));
  }
  size_t i = 0;
  for (; i < n_feats; ++i)
    o.feats.push_back({std::move(r.feats[i].error), std::move(r.feats[i].v)});
  for (const Bar b : kBars)
    if (bars[b]) {
      o.kline[b] = {std::move(r.feats[i].error), std::move(r.feats[i].v)};
      ++i;
    }
  assert(i == r.feats.size());
  return o;
}

bool searchable(const F::FactorRow &r) { return r.error.empty() && !r.tmpl.empty(); }

// Search 评过的点 → 曲面: 各点的具体式按模板的槽 (前序节点) 读回参数值
Surface surface_of(const F::FeatureTable &ft, const F::SearchResult &r) {
  namespace sp = factor::search::space;
  Surface s;
  if (r.slots.empty())
    return s;
  const factor::expr::Expr t = ft.reparse(r.tmpl);
  const std::vector<sp::Slot> slots = sp::slots_of(t, ft.seg, r.n_full * kGroups);
  assert(slots.size() == r.slots.size());
  s.slots = r.slots;
  for (const F::SearchCand &c : r.cands) {
    if (!c.sc.ok)
      continue;
    const factor::expr::Expr e = ft.reparse(c.expr);
    assert(e.nodes.size() == t.nodes.size() && "具体式与模板同构");
    Probe p;
    p.st = {c.sc.mono, c.sc.ls, c.sc.ric};
    for (const sp::Slot &x : slots) {
      const auto &q = e.nodes[static_cast<size_t>(x.node)].p;
      p.v.push_back(x.key == sp::Key::D ? static_cast<double>(q.d) : x.key == sp::Key::K ? q.k
                                                                                         : q.k2);
    }
    s.pts.push_back(std::move(p));
  }
  return s;
}

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
      const factor::stat::HoldStat *h = w.find_hold(hold);
      assert(h && "RowScore ok 须有该持有期的 HoldStat");
      double mid = 0.0;
      for (const float g : h->grp)
        mid += g / static_cast<double>(kGroups);
      for (const float g : h->grp)
        x.grp.push_back(g - mid);
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
  c.first = static_cast<int>(base.start), c.last = static_cast<int>(base.end);
  c.seg = ft->seg;
  c.clock = clocks_of(F::factor_level(ft->lv));
  c.open = open_of(c.clock);
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
  c.kline = kline_of(*ft);
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
std::string dir(const Scope &sc, bool test) {
  const fs::path d = fs::path(kWorkDir) / sc.pool / F::factor_level(level_id(sc.level)).name / factor::stat::hold_name(sc.hold);
  return (test ? d / "test" : d).string();
}

} // namespace catalog

// ---- Lab ----
struct Lab::Impl {
  // 作用域
  const SharedData *data = nullptr;
  Scope sc;
  bool test = false;
  std::string dir; // 候选目录
  Context ctx;
  std::vector<Result> results;
  Fails fails; // 同作用域跨会话留着
  // 三道 (下标 = Step), 首次用到才建; Search / Run 随评估后端重建
  static_assert(static_cast<size_t>(Step::Search) == 0 && static_cast<size_t>(Step::Run) == 1 && static_cast<size_t>(Step::Look) == 2);
  bool gpu = false;
  std::tuple<Lane<F::SearchService>, Lane<F::RunService>, Lane<F::LensService>> lanes;
  // 在做的那件事
  bool running = false;            // eval / look 起, 到 End 入队
  bool halt = false;               // 已 cancel: 在跑那步收尾即 End
  Step at = Step::End;             // 在跑那步的道; End = 无
  std::string file;                // eval: 在评的文件
  F::FactorRow row;                // eval: 其首行 (Search 的模板)
  Surface surf;                    // eval: Search 落了盘时的曲面 (随定论交出)
  std::vector<std::string> labels; // look: 因子的名字
  size_t n_feats = 0;              // look: 特征根数 (其后是 K 线叶)
  PerBar<bool> bars;               // look: 取了的 K 线叶
  std::deque<Tick> ticks;          // 未取走的步

  std::shared_ptr<const F::FeatureTable> ft() const { return table(*data, sc.level); }
  std::optional<F::FactorScope> fsc() const { return fscope_of(*data, ft(), sc.pool); }
  template <class Fn>
  void each_lane(Fn &&fn) {
    std::apply([&](auto &...l) { (fn(l), ...); }, lanes);
  }
  // 在跑那步的道
  template <class Fn>
  decltype(auto) on_lane(Fn &&fn) const {
    switch (at) {
    case Step::Search:
      return fn(std::get<0>(lanes));
    case Step::Run:
      return fn(std::get<1>(lanes));
    case Step::Look:
      return fn(std::get<2>(lanes));
    default:
      assert(false && "无在跑的道");
      __builtin_unreachable();
    }
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
    ticks.push_back({S, {}});
  }
  // Search 跑完: 落了盘 → 取曲面, true (随后 Run); 中途放弃 → 记下原因 (该文件 BROKEN)
  bool searched() {
    F::SearchService &svc = *std::get<0>(lanes).svc;
    std::lock_guard<std::mutex> lock(svc.mutex);
    if (svc.result.wrote)
      fails.erase(file), surf = surface_of(*ft(), svc.result);
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
  // 开评 file: 模板合法 → Search (定方向 + 搜参, 落了盘随后 Run); BROKEN → 直接评完
  void start() {
    std::vector<F::FactorRow> rows;
    F::ScanFactorFile(fs::path(dir) / file, *ft(), rows);
    assert(!rows.empty());
    row = std::move(rows.front());
    surf = {};
    if (searchable(row))
      return send<Step::Search>();
    if (!settle())
      close();
  }
  // 评完: 定论 (带曲面) 入队; 无显著而有代表点 → 接着观察它, true
  bool settle() {
    Result r = refresh(file);
    r.surf = std::move(surf), surf = {};
    const bool rep = r.verdict == Verdict::Null && r.top && r.visible;
    const std::string name = r.name;
    ticks.push_back({Step::Settle, std::move(r)});
    if (!rep)
      return false;
    LookFactor f;
    std::string err;
    [[maybe_unused]] const bool ok = factor_of(name, f, err);
    assert(ok && "代表点是 Search 评过的具体式");
    look({.factors = {std::move(f)}});
    return true;
  }
  void close() {
    file.clear();
    at = Step::End;
    running = false;
    ticks.push_back({Step::End, {}});
  }
  // 观察跑完: 取走结果
  void looked() {
    F::LensService &svc = *std::get<2>(lanes).svc;
    F::LensResult r;
    {
      std::lock_guard<std::mutex> lock(svc.mutex);
      r = std::move(svc.result);
    }
    ticks.push_back({Step::Look, look_of(std::move(r), *ft(), labels, n_feats, bars)});
  }
  // 在跑那步的道跑完一轮 → 下一步
  void pump() {
    if (at == Step::End || !on_lane([](const auto &l) { return l.finished(); }))
      return;
    if (halt || on_lane([](const auto &l) { return l.cancelled(); }))
      return close();
    if (at == Step::Look) {
      looked();
      return close();
    }
    if (at == Step::Search && searched())
      return send<Step::Run>();
    if (!settle())
      close();
  }

  bool factor_of(const std::string &arg, LookFactor &out, std::string &err) const {
    const auto it = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.name == arg; });
    std::string src = arg, label = arg;
    int sign = 1;
    if (it != results.end()) {
      if (!it->visible) {
        err = "候选 " + arg + " 的统计不在当前评估期上; 可直接给具体根式";
        return false;
      }
      if (const Weight *w = it->best()) {
        src = w->expr, sign = w->sign;
      } else if (it->top) {
        src = it->top->expr, sign = it->top->sign, label = arg + " (无显著, 用代表点)";
      } else {
        err = "候选 " + arg + " 没有本持有期的权重或代表点 (" + def(it->verdict).cn + "); 可直接给具体根式";
        return false;
      }
    }
    const std::shared_ptr<const F::FeatureTable> t = ft();
    factor::expr::Expr e;
    factor::stat::Frame f;
    if (!concrete(*t, src, e, err) || !factor::expr::root_frame(e, t->seg, f, err) || !t->check_grid(e, err)) {
      err = (it != results.end() ? "" : "不是候选英文名, 按根式解析: ") + err;
      return false;
    }
    out = {e.canon, it != results.end() ? label : e.canon, f == factor::stat::Frame::TS, sign};
    return true;
  }

  // 发一次观察 (参数已校验)
  void look(LookSpec s) {
    assert(!s.factors.empty() || !s.feats.empty() || s.kline);
    Lane<F::LensService> &l = std::get<2>(lanes);
    if (!l.svc)
      l.reset();
    F::LensRequest req;
    [[maybe_unused]] const bool ok = F::MakeFactorScope(*data, ft(), sc.pool, /*gpu=*/false, req.scope);
    assert(ok && "作用域须 ready");
    req.spec.hold = sc.hold;
    labels.clear();
    for (const LookFactor &f : s.factors) {
      req.spec.roots.push_back({f.expr, f.ts ? factor::stat::Frame::TS : factor::stat::Frame::CS, factor::stat::dir_of(f.sign)});
      labels.push_back(f.label);
    }
    for (const std::string &x : s.feats)
      req.spec.feats.push_back({x, factor::stat::Frame::CS, factor::stat::kFwd});
    n_feats = s.feats.size(), bars = {};
    if (s.kline)
      for (const Bar b : kBars)
        if (const std::string &src = ctx.kline[b]; !src.empty()) {
          std::string root, err;
          [[maybe_unused]] const bool ok_leaf = feature_root(*ft(), src, root, err);
          assert(ok_leaf && "K 线叶必可作特征");
          req.spec.feats.push_back({std::move(root), factor::stat::Frame::CS, factor::stat::kFwd});
          bars[b] = true;
        }
    if (!s.code.empty()) {
      const size_t id = asset_axis().find(s.code);
      assert(id < asset_axis().size() && "码须经 code_of");
      req.spec.asset = static_cast<uint32_t>(id);
    }
    l.send(std::move(req));
    at = Step::Look;
    running = true, halt = false;
  }
};

Lab::Lab() : p_(std::make_unique<Impl>()) {}
Lab::~Lab() = default;

void Lab::bind(const SharedData &data, const Scope &sc, bool test) {
  Impl &I = *p_;
  assert(!I.running && "评估队列忙");
  if (sc != I.sc)
    I.each_lane([](auto &l) { l.drop(); });
  if (sc != I.sc || test != I.test)
    I.fails.clear();
  I.data = &data;
  I.sc = sc;
  I.test = test;
  I.dir = (fs::path(catalog::dir(sc, test)) / "factors").string();
  I.ctx = context_of(data, sc);
  I.results = scan(*I.ft(), I.fsc(), I.dir, sc.hold, I.fails);
  I.ticks.clear();
}

const Scope &Lab::scope() const { return p_->sc; }
bool Lab::test() const { return p_->test; }
const Context &Lab::ctx() const { return p_->ctx; }
const std::vector<Result> &Lab::results() const { return p_->results; }

Staged Lab::stage(Cand c) {
  Impl &I = *p_;
  Staged s{.cand = std::move(c)};
  const Cand &x = s.cand;
  std::string err;
  if (!F::ValidFactorName(x.name))
    err = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
  else if (!F::ValidFactorNameCn(x.name_cn))
    err = "中文名须 1..10 个汉字";
  else if (const int n = ops_of(*I.ft(), x.expr); n > kMaxOps)
    err = "算子 " + std::to_string(n) + " 个, 超过上限 " + std::to_string(kMaxOps) + " (建议 " + std::to_string(kGoodOps) + " 个左右)";
  else
    s.file = F::AddFactorFile(I.dir, *I.ft(), x.name, x.name_cn, x.expr, x.note, err);
  if (s.ok())
    s.res = I.refresh(s.file);
  else
    s.res.error = std::move(err), s.res.verdict = Verdict::Unstaged;
  return s;
}

Adopted Lab::adopt(std::string name, const Gate &g) {
  const Impl &I = *p_;
  Adopted s{.name = std::move(name)};
  const auto it = std::find_if(I.results.begin(), I.results.end(), [&](const Result &r) { return r.name == s.name; });
  if (it == I.results.end() || !it->visible) {
    s.err = "会话目录里没有这个候选";
    return s;
  }
  s.err = gate_miss(g, *it);
  if (!s.ok())
    return s;
  const fs::path to = fs::path(F::factor_dir_of(*I.data, I.sc.pool, level_id(I.sc.level))) / it->file;
  if (fs::exists(to)) {
    s.err = "正式目录已有 " + it->file;
    return s;
  }
  if (!I.test) {
    fs::create_directories(to.parent_path());
    fs::copy_file(fs::path(I.dir) / it->file, to);
  }
  return s;
}

bool Lab::factor_of(const std::string &arg, LookFactor &out, std::string &err) const { return p_->factor_of(arg, out, err); }

bool Lab::feature_of(const std::string &src, std::string &root, std::string &err) const { return feature_root(*p_->ft(), src, root, err); }

// 池轴 = 标签库的资产轴 (子池的成员都在其内): 不在轴上的必观察不成, 先拦; 在轴上而评估期内没进过子池的由观察结果的在池行数说
bool Lab::code_of(const std::string &arg, std::string &code, std::string &err) const {
  const Impl &I = *p_;
  const AssetAxis &ax = asset_axis();
  for (const char *ex : {"", ".SZ", ".SH", ".BJ"})
    if (const std::string c = arg + ex; ax.find(c) < ax.size()) {
      const std::optional<F::FactorScope> fsc = I.fsc();
      assert(fsc && "作用域须 ready");
      const PoolAxis &pa = fsc->libs[F::factor_level(level_id(I.sc.level)).lab(pool_id(I.sc.pool))].axis;
      if (pa.sub_of(static_cast<uint32_t>(ax.find(c))) == pa.size()) {
        err = c + " 不在本池 (" + I.sc.pool + ") 特征库的资产轴上, 从没进过池; 挑池内的票 (如 view_trades / view_holders 结果里的代码)";
        return false;
      }
      code = c;
      return true;
    }
  err = "码表里没有 " + arg + " (写 6 位代码或 代码.交易所, 如 000001.SZ)";
  return false;
}

void Lab::eval(std::string file, bool gpu) {
  Impl &I = *p_;
  assert(!I.running && I.data && "评估忙或未 bind");
  if (!std::get<0>(I.lanes).svc || gpu != I.gpu) {
    std::get<0>(I.lanes).reset(), std::get<1>(I.lanes).reset();
    I.gpu = gpu;
  }
  I.file = std::move(file);
  I.running = true, I.halt = false;
  I.start();
}

void Lab::look(LookSpec s) {
  Impl &I = *p_;
  assert(!I.running && I.data && "评估忙或未 bind");
  I.look(std::move(s));
}

void Lab::cancel() {
  Impl &I = *p_;
  if (!I.running)
    return;
  I.halt = true;
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
