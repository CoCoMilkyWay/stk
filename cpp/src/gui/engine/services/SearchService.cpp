// SearchService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/services/SearchService.hpp"

#include "factor/Dag.hpp"
#include "factor/Search/Config.hpp"
#include "factor/Search/Space.hpp"
#include "gui/engine/Engine.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <utility>

namespace GUI::Engine {

namespace {

namespace space = factor::search::space;
namespace fs = factor::stat;
namespace fsearch = factor::search;
using Clock = std::chrono::steady_clock;

constexpr double kMiB = 1024.0 * 1024.0;
constexpr size_t kLogTop = 10; // 日志里列的最好点数

std::string vstrf(const char *f, va_list ap) {
  char buf[4096];
  const int n = std::vsnprintf(buf, sizeof(buf), f, ap);
  assert(n >= 0 && n < static_cast<int>(sizeof(buf)) && "格式化超长");
  (void)n;
  return buf;
}
__attribute__((format(printf, 1, 2))) std::string strf(const char *f, ...) {
  va_list ap;
  va_start(ap, f);
  std::string s = vstrf(f, ap);
  va_end(ap);
  return s;
}

struct RunLog {
  std::ofstream f;
  Clock::time_point t0;
  __attribute__((format(printf, 2, 3))) void operator()(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    const std::string s = vstrf(fmt, ap);
    va_end(ap);
    f << strf("[%8.1fs] ", std::chrono::duration<double>(Clock::now() - t0).count()) << s << '\n';
    f.flush();
  }
};

void prune_logs(const FactorDir &dir) {
  for (const disk::Key &k : disk::list(dir.log, dir.key))
    if (!disk::exists(dir.file, k))
      disk::remove(dir.log, k);
}

std::string slot_val(const space::Slot &s, double u) {
  const double v = space::to_v(s, u);
  return s.integer ? strf("%d", static_cast<int>(v)) : strf("%.4g", v);
}
std::string params_of(const std::vector<space::Slot> &slots, const std::vector<double> &u) {
  std::string out;
  for (size_t j = 0; j < slots.size(); ++j)
    out += (j ? ", " : "") + slots[j].name + "=" + slot_val(slots[j], u[j]);
  return out;
}
const char *coord_name(const space::Slot &s) {
  if (s.key == space::Key::D)
    return "log2";
  if (s.key == space::Key::W)
    return s.top ? "tan" : "线性";
  switch (s.kdom) {
  case factor::KDom::OPEN01:
    return "logit";
  case factor::KDom::GE0:
  case factor::KDom::ANY:
    return "logit分位";
  case factor::KDom::TOD:
    return "行";
  default:
    return "log2";
  }
}

int stride_of(int T) { return std::max(1, T / factor::search::kQtabRows); }
struct Samp {
  int t_first = 0, n_rows = 0;
};
// 块 0 (首块, 块首 = 评估域首行) 的统计行上每 stride 行取一行
Samp samp_of(int t_hi, int t_lo, int stride) {
  int t = t_lo;
  while (t % stride != 0)
    ++t;
  return {t, t < t_hi ? (t_hi - 1 - t) / stride + 1 : 0};
}

using SearchProbes = Probes<HoldP>; // 三级 + 逐日 (HoldP::day, 按块拼回评估期; Search 恒一个持有期)

struct Entry {
  std::vector<double> u;
  factor::expr::Expr e;
  SearchCand c;
  bool evaled = false;
};

struct Searcher {
  FactorFlow &F;
  const FactorScope &scope;
  const FeatureTable &ft;
  const SearchSpec &sp;
  SearchLive *live;
  const std::function<void(const SearchResult &)> &on_pass;
  const int seg;
  const Holds hd;
  const Clock::time_point t_start = Clock::now();
  SearchResult &res;
  std::string &why_out;
  RunLog lg;
  factor::expr::Expr e0;
  Forest f0;
  std::vector<space::Slot> slots;
  Job job;
  int A = 0, stride = 1;
  int n_chunks = 0, batch = 1;
  int n_pts = 1; // 采样点数 (fsearch::n_init; 具体式 = 1)
  std::vector<Entry> ents;
  std::map<std::string, int> seen;
  Rc rc_end = Rc::Ok;

  Searcher(const SearchRequest &req, FactorFlow &f, SearchResult &out, std::string &why, SearchLive *lv,
           const std::function<void(const SearchResult &)> &cb)
      : F(f), scope(req.scope), ft(*req.scope.ft), sp(req.spec), live(lv), on_pass(cb), seg(ft.seg),
        hd(Holds::of({sp.hold})), res(out), why_out(why) {
    assert(ft.y.has(sp.hold) && "Search 的持有期须是字段表标签组之一");
    assert(fsearch::search_hold(sp.hold) && "Search 只搜 kSearchHolds 档");
    fs::assert_holds(hd, seg);
    if (live)
      live->reset();
    res = SearchResult{};
    why_out.clear();
    res.file = sp.file;
    res.tmpl = sp.tmpl;
    res.hold = sp.hold;
    res.backend = scope.gpu ? Backend<Gpu>::kName : Backend<Cpu>::kName;
    emit_pass();
    lg.t0 = t_start;
    prune_logs(scope.dir);
    const std::filesystem::path log_path = scope.dir.log_path(sp.file);
    std::filesystem::create_directories(log_path.parent_path());
    lg.f.open(log_path, std::ios::trunc);
    assert(lg.f.is_open() && "Search log 打不开");
    const std::time_t now = std::time(nullptr);
    char ts[32];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    lg("Search %s  @ %s", sp.file.c_str(), ts);
    lg("模板: %s", sp.tmpl.c_str());
    lg("后端 %s, 池 %s, 持有期 %s, 收益口径 %s", res.backend.c_str(), scope.pool.c_str(), hold_name(sp.hold).c_str(), fs::ret_name(sp.ret));
  }

  Rc run() {
    {
      Lease l(F);
      if (!l)
        return fail(Rc::Cancel);
      const bool ok = with_backend(F, scope.gpu, [&](auto &eng) {
        std::vector<int> ids;
        if (!plan(eng) || !prepare_qtab() || !design(ids))
          return false;
        if (live)
          live->stage.store(SearchStage::Eval, std::memory_order_relaxed);
        return check(evaluate(eng, ids));
      });
      if (!ok)
        return rc_end;
    }
    finish();
    return Rc::Ok;
  }

  Rc fail(Rc rc) {
    if (rc == Rc::Cancel)
      lg("已取消");
    else if (rc == Rc::Stale)
      lg("中止: %s", kStaleMsg);
    rc_end = rc;
    return rc;
  }
  bool check(Rc rc) {
    if (rc == Rc::Ok)
      return true;
    fail(rc);
    if (!aborted(rc)) {
      emit_pass();
      give_up(rc, job.why);
    }
    return false;
  }
  bool give_up(Rc rc, std::string msg) {
    lg("中止: %s", msg.c_str());
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    why_out = std::move(msg);
    rc_end = rc;
    return false;
  }
  void emit_pass() {
    res.seconds = std::chrono::duration<double>(Clock::now() - t_start).count();
    res.cands.clear();
    for (const Entry &en : ents)
      if (en.evaled)
        res.cands.push_back(en.c);
    std::stable_sort(res.cands.begin(), res.cands.end(),
                     [](const SearchCand &a, const SearchCand &b) { return fs::score_of(a.roll) > fs::score_of(b.roll); });
    if (on_pass)
      on_pass(res);
  }

  template <class E>
  bool plan(E &) {
    e0 = ft.reparse_at(sp.tmpl, sp.hold);
    const int look = space::lookback(e0, seg);
    const factor::expr::Expr ep = e0.placeholder();
    const Root r0{&ep, mode()};
    f0 = plant_forest(ft, std::span<const Root>(&r0, 1), /*keep_roots=*/false, {}, /*fuse=*/false);
    job.who = "Search";
    const Clock::time_point t_plan = Clock::now();
    const int n_slots = space::slot_count(e0);
    n_pts = n_slots > 0 ? fsearch::n_init(n_slots) : 1;
    if (!check(open_job(F, scope, forest_need<E, SearchProbes>(ft, f0, n_pts, look, hd), job)))
      return false;
    lg("前带: 模板回看 %d 行 + 持有期 %d 行 + 交接链 %d 天 → %d 天; 后带 %d 天", look, fs::hold_reach(hd, seg), kChainDays, job.bands.front,
       job.bands.back);
    const std::vector<misc::Ymd> &dates = job.dm.dates;
    const int days = static_cast<int>(job.dm.stat_days());
    A = static_cast<int>(job.dm.rs.axis.size());
    lg("评估期 %s ~ %s (%d 天, 所用库落盘日交集的最近 %d 年) + 预热 %zu 天; 滚动窗 %d 天 (评估期 / %d)", misc::dash(job.dm.first()).c_str(),
       misc::dash(dates.back()).c_str(), days, scope.years, job.dm.warm, fs::roll_win(days), fs::kRollParts);
    res.dates = job.dm.stat_dates();
    stride = stride_of(days * seg);
    const Plan &P = job.P;
    const Budget &bud = job.b;
    const double per_day = P.per_day;
    n_chunks = static_cast<int>(P.chunks.size());
    batch = E::batch_of(P, bud);
    lg("开库 + 分块: %.1fs", std::chrono::duration<double>(Clock::now() - t_plan).count());
    res.n_chunk = n_chunks, res.chunk_days = P.D, res.batch = batch;
    res.n_assets = A;
    for (const Chunk &k : P.chunks)
      res.chunk_plan.push_back(SearchChunk{k.days(), k.days() * per_day * bud.load / kMiB});
    if (live)
      live->chunks.store(n_chunks, std::memory_order_relaxed);
    emit_pass();
    lg("数据: %d 只 × %d 天, 特征 %d 列, 标签 %d 列, 共享 DAG 槽 %d; 每格 %.0f 字节 (宿主装载) + %.0f (CPU 工作区)", A, days, static_cast<int>(f0.d.feats.size()),
       hd.n, f0.d.n_slots, bud.load, bud.work);
    lg("预算: 内存 %.0f MB%s; 块 ≤ %d 天 + 前带 %d 天 + 后带 %d 天, 共 %d 块, 全部常驻 %s (需 %.0f MB)%s", P.host / kMiB,
       scope.gpu ? strf(", 显存 %.0f MB", P.vram / kMiB).c_str() : "", P.D, job.bands.front, job.bands.back, n_chunks,
       P.cache + P.max_cells() * bud.work <= P.host ? "是" : "否 (逐块重读)", P.cache / kMiB, scope.gpu ? strf(", GPU 批 %d", batch).c_str() : "");
    for (int ci = 0; ci < n_chunks; ++ci) {
      const Chunk &k = P.chunks[static_cast<size_t>(ci)];
      lg("  块 %d: 统计 %s ~ %s (%d 天), 装载 %d 天, %.0f MB", ci + 1, misc::dense(dates[k.s0]).c_str(), misc::dense(dates[k.s1 - 1]).c_str(),
         static_cast<int>(k.s1 - k.s0), k.days(), res.chunk_plan[static_cast<size_t>(ci)].mb);
    }
    return true;
  }

  void loaded(const Block &c, size_t ci, double load_s) {
    if (c.fresh)
      lg("装载块 %d/%d: %d 天, %.1fs", static_cast<int>(ci) + 1, n_chunks, job.P.chunks[ci].days(), load_s);
  }

  bool prepare_qtab() {
    const Clock::time_point t_load = Clock::now();
    Block c;
    if (!check(load_block(F, job, 0, c)))
      return false;
    loaded(c, 0, std::chrono::duration<double>(Clock::now() - t_load).count());
    {
      std::vector<std::string> why(f0.d.roots.size());
      std::vector<uint8_t> need;
      if (plan_forest(F.S.ex, c, f0, why, need) == 0) {
        job.why = why[0];
        return check(Rc::Broken);
      }
    }
    {
      const factor::stat::Span s0 = c.span();
      const std::vector<float> &cap = c.y.labels[0]->cap;
      double sum = 0.0;
      for (int t = s0.t_lo; t < s0.t_hi(); ++t)
        sum += cap[static_cast<size_t>(t)];
      res.n_full = sum / (s0.t_hi() - s0.t_lo);
    }
    lg("满仓一组 %.1f 只 (首块统计行逐行满仓 = 池内标签有效格数 / %d 组的均值; 定参数区间用)", res.n_full, fs::kGroups);
    const double a_eff = res.n_full * fs::kGroups;
    slots = space::slots_of(e0, seg, a_eff);
    assert(slots.empty() == !e0.is_template());
    for (const space::Slot &s : slots)
      res.slots.push_back(s.name);
    if (slots.empty())
      lg("无自由参数: 只评模板本身 (定方向)");
    else
      lg("自由参数 %d 个 (区间按池内平均有效资产数 %.0f 定):", static_cast<int>(slots.size()), a_eff);
    for (const space::Slot &s : slots)
      lg("  %-24s 坐标 %-8s 区间 [%.3g, %.3g] 中性 u0=%.3g%s", s.name.c_str(), coord_name(s), s.u_lo, s.u_hi, s.u0, s.integer ? " 整数" : "");
    for (size_t jj = slots.size(); jj-- > 0;)
      if (space::needs_qtab(slots[jj]) && !qtab(slots[jj], c))
        return false;
    return true;
  }

  bool qtab(space::Slot &sl, const Block &c) {
    const factor::expr::Expr ec = space::neutral(e0, slots, seg);
    const factor::Dag dc = factor::build(ec, /*fuse=*/false);
    const factor::expr::Node &nd = ec.nodes[static_cast<size_t>(sl.node)];
    assert(factor::expr::kOps[nd.op].arity >= 1);
    const std::string key = factor::expr::to_string(ec, nd.args[0]);
    const auto it = std::find(dc.keys.begin(), dc.keys.end(), key);
    assert(it != dc.keys.end() && "Expr 子树必在同一 Expr 的 Dag 里");
    const int want = static_cast<int>(it - dc.keys.begin());
    std::vector<uint8_t> prefix(dc.nodes.size(), 0);
    std::fill_n(prefix.begin(), want + 1, static_cast<uint8_t>(1));
    const fs::Span s = c.span();
    assert(c.ck.d0 == 0 && "分位表只取首块");
    const Samp sm = samp_of(s.t_hi(), s.t_lo, stride);
    std::vector<float> tab;
    Backend<Cpu> &qe = engine_space(F.S).cpu;
    qe.bind(c);
    if (!qe.walk_dag(F.cancel, dc, ft.x.ids(dc.feats), prefix.data(), [&](int i, double) {
          if (i == want) {
            const factor::check::Plane *p = qe.plane(i);
            const uint8_t *gate = c.y.gate;
            for (int r = 0; r < sm.n_rows; ++r)
              for (int a = 0; a < A; ++a) {
                const size_t k = static_cast<size_t>(sm.t_first + r * stride) * A + a;
                if (p->m[k] && gate[k])
                  tab.push_back(sl.kdom == factor::KDom::GE0 ? std::fabs(p->v[k]) : p->v[k]);
              }
          }
          return true;
        })) {
      fail(Rc::Cancel);
      return false;
    }
    std::sort(tab.begin(), tab.end());
    if (tab.size() < 2 || !(tab.front() < tab.back()))
      return give_up(Rc::Empty, "阈值槽 " + sl.name + " 的输入在块 0 抽样行上是常量, 搜不动");
    const size_t n_tab = tab.size();
    space::set_qtab(sl, std::move(tab));
    lg("分位表: %s ← %zu 个值 (输入 %s), 区间 [%s, %s], 中性 %s", sl.name.c_str(), n_tab, key.c_str(), slot_val(sl, sl.u_lo).c_str(), slot_val(sl, sl.u_hi).c_str(),
       slot_val(sl, sl.u0).c_str());
    return true;
  }

  int add_cand(const std::vector<double> &u) {
    Entry en;
    if (!space::apply(e0, slots, u, seg, en.e))
      return -1;
    if (const auto it = seen.find(en.e.canon); it != seen.end())
      return it->second;
    en.u = u;
    en.c.expr = en.e.canon;
    for (const space::Slot &s : slots) {
      const size_t j = en.c.v.size();
      const double raw = space::to_v(s, u[j]);
      en.c.v.push_back(s.key == space::Key::D ? raw : static_cast<double>(factor::expr::kv_of(raw)));
    }
    const int id = static_cast<int>(ents.size());
    seen.emplace(en.e.canon, id);
    ents.push_back(std::move(en));
    return id;
  }
  std::string params_at(const Entry &en) const { return (en.c.sign < 0 ? std::string("取反 ") : std::string()) + params_of(slots, en.u); }
  bool design(std::vector<int> &ids) {
    if (slots.empty()) {
      ids.push_back(add_cand({}));
      return true;
    }
    assert(static_cast<int>(slots.size()) == space::slot_count(e0));
    int n_bad = 0;
    for (const std::vector<double> &u : space::design(slots, n_pts)) {
      const int id = add_cand(u);
      if (id < 0)
        ++n_bad;
      else if (std::find(ids.begin(), ids.end(), id) == ids.end())
        ids.push_back(id);
    }
    lg("采样: %zu 槽 R_d %d 点 → 不合法 %d, 取整去重后 %d 候选", slots.size(), n_pts, n_bad, static_cast<int>(ids.size()));
    if (ids.empty())
      return give_up(Rc::Empty, "采样点无一参数合法 (值域互斥?), 文件不动");
    return true;
  }

  // 正向评 (定向在 roll_of / oriented 改读); 收益口径 = 因子文件 type
  fs::Mode mode() const { return {sp.frame, fs::kFwd, sp.ret}; }

  template <class E>
  Rc evaluate(E &eng, const std::vector<int> &ids) {
    assert(!ids.empty());
    if (live)
      live->chunk.store(0, std::memory_order_relaxed);
    std::vector<Root> roots;
    roots.reserve(ids.size());
    for (const int id : ids)
      roots.push_back({&ents[static_cast<size_t>(id)].e, mode()});
    std::vector<SearchProbes> ps(ids.size());
    const Clock::time_point t_pass = Clock::now();
    double load_s = 0.0;
    Forest fr;
    Fate pass;
    const Plant plant = [&](size_t b0, int &C) -> const Forest & {
      fr = eng.template plant<SearchProbes>(ft, std::span<const Root>(roots).subspan(b0), batch, C);
      return fr;
    };
    const Hooks hk{.before = [&](const Block &c, size_t ci, double s) {
      loaded(c, ci, s);
      load_s += s;
      if (live)
        live->chunk.store(static_cast<int>(ci), std::memory_order_relaxed);
    }};
    const Rc rc = run_job(F, job, ForestPass<E, SearchProbes>(eng, F, roots, plant, std::span(ps), pass, hk));
    if (rc != Rc::Ok)
      return rc;
    if (live)
      live->chunk.store(n_chunks, std::memory_order_relaxed);
    int n_ok = 0;
    for (size_t k = 0; k < ids.size(); ++k) {
      Entry &en = ents[static_cast<size_t>(ids[k])];
      en.evaled = true;
      if (!pass.dead[k].empty())
        continue;
      SearchCand &c = en.c;
      const HoldP &hp = ps[k].at<HoldP>();
      assert(hp.n_hold == 1);
      const std::vector<fs::Sum> &day = hp.day[0];
      std::vector<double> wins;
      const fs::Roll fwd = fs::roll_of(day, sp.hold, seg, 1, &wins);
      c.sign = fs::sign_of(fwd);
      c.roll = c.sign > 0 ? fwd : fs::roll_of(day, sp.hold, seg, -1, &wins);
      c.line.assign(wins.begin(), wins.end());
      const fs::HoldStat h = fs::stat_of(fs::oriented(day, c.sign), sp.hold, seg);
      c.mono = h.mono, c.ls = fs::ls_period(h), c.ric = h.ric.mean;
      n_ok += c.roll.ok();
    }
    const double pass_s = std::chrono::duration<double>(Clock::now() - t_pass).count();
    lg("评完: %d 候选 × %d 块, %.1fs (装载 %.1fs, 评估 %.1fs, %.1f ms/候选·块) | 有滚动窗 %d", static_cast<int>(ids.size()), n_chunks, pass_s, load_s, pass_s - load_s,
       1e3 * (pass_s - load_s) / (static_cast<double>(ids.size()) * n_chunks), n_ok);
    emit_pass();
    return Rc::Ok;
  }

  void finish() {
    std::vector<const Entry *> order;
    for (const Entry &en : ents)
      if (en.evaled && en.c.roll.ok())
        order.push_back(&en);
    std::stable_sort(order.begin(), order.end(), [](const Entry *a, const Entry *b) { return a->c.roll.med > b->c.roll.med; });
    for (size_t i = 0; i < std::min(order.size(), kLogTop); ++i) {
      const SearchCand &c = order[i]->c;
      lg("  %s 中位 %s %+.4f (平均 %+.4f, %d 窗 × %d 天) · 年化多空 中位 %+.1f%% · 全期 mono %+.3f rIC %+.4f 每期毛多空 %.2fbp @ %s", i == 0 ? "*" : " ",
         "mono", c.roll.med, c.roll.mean, c.roll.n, c.roll.win, 100.0 * c.roll.ann_med, c.mono, c.ric, 1e4 * c.ls, params_at(*order[i]).c_str());
    }
    std::vector<SearchWeight> ws;
    if (!order.empty())
      ws.push_back(SearchWeight{sp.hold, order.front()->c.sign, order.front()->c.expr});
    const SearchMark mk{sp.hold, job.dm.first(), job.dm.dates.back(), static_cast<int>(job.dm.stat_days())};
    WriteFactorSearch(scope.dir, sp.file, sp.fresh, mk, ws);
    res.wrote = true;
    res.n_weights = static_cast<int>(ws.size());
    why_out = ws.empty() ? strf("没有一个参数点算得出滚动窗 (每窗 ok 行 ≥ %d), 已清掉该 Hold 的权重", fs::kRollMinRows)
                         : std::string("weights 已写入 (中位窗 mono 最高的 1 个), 自动 Run 重算 stat");
    lg("完成: 写入 %d 个权重, 共评 %d 候选", static_cast<int>(ws.size()), static_cast<int>(res.cands.size()));
    emit_pass();
  }
};

} // namespace

bool MakeSearchRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, const FactorRow &row, HoldId hold,
                       bool fresh, bool gpu, SearchRequest &req) {
  assert(!row.tmpl.empty() && "Search 只接模板解析过的因子");
  req = SearchRequest{};
  req.spec = SearchSpec{row.file, row.tmpl, row.frame, row.ret, hold, fresh};
  return MakeFactorScope(data, std::move(ft), pool, years, gpu, req.scope);
}

Rc SearchFactor(const SearchRequest &req, FactorFlow &F, SearchResult &out, std::string &why, SearchLive *live,
                const std::function<void(const SearchResult &)> &on_pass) {
  TraceN("SearchRun");
  Searcher s(req, F, out, why, live, on_pass);
  return s.run();
}

bool SearchService::run(SearchRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
  }
  FactorFlow F = flow();
  SearchResult out;
  std::string why;
  const Rc rc = SearchFactor(req, F, out, why, &live_, [&](const SearchResult &r) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      result = r;
    }
    publish();
  });
  if (!settle(rc))
    return false;
  {
    std::lock_guard<std::mutex> lock(mutex);
    result = std::move(out);
    message = std::move(why);
  }
  publish();
  return true;
}

} // namespace GUI::Engine
