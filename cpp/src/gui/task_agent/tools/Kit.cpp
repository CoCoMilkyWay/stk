// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/All.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <numeric>
#include <utility>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

// =============================================================================
// 账本: 时间线的纯折叠
// =============================================================================
int Tally::n(Cat c) const {
  int s = 0;
  for (const Tool t : kVals<Tool>)
    s += def(t).cat == c ? n(t) : 0;
  return s;
}

int Tally::failed(Cat c) const {
  int s = 0;
  for (const Tool t : kVals<Tool>)
    s += def(t).cat == c ? failed(t) : 0;
  return s;
}

int Tally::all() const { return std::accumulate(calls.begin(), calls.end(), 0); }
int Tally::failed() const { return std::accumulate(fails.begin(), fails.end(), 0); }

Tally TallyOf(Steps ss, double now) {
  Tally t;
  for (const Step &sp : ss) {
    t.llm_s += sp.span.len(now), t.usage += sp.reply.usage;
    if (!sp.act || !sp.act->span.began())
      continue;
    const Act &a = *sp.act;
    ++t.calls[a.call.tool], t.act_s += a.span.len(now);
    if (!a.span.ended())
      continue;
    t.fails[a.call.tool] += !a.out.ok;
    if (a.out.fx.verdict)
      ++t.verdicts[*a.out.fx.verdict];
    t.adopted += !a.out.fx.adopted.empty();
  }
  return t;
}

bool Finished(Steps ss) {
  for (size_t i = ss.size(); i-- > 0;) {
    if (const std::optional<Act> &a = ss[i].act; a && a->span.ended() && a->out.fx.finish)
      return true;
    if (ss[i].span.began())
      return false;
  }
  return false;
}

namespace {

// 开拍了的调用的账本作用里取一项 (非空的; 作用开拍即有, 在跑的也算)
template <class F>
std::set<std::string> effects(Steps ss, F &&f) {
  std::set<std::string> out;
  for (const Step &sp : ss)
    if (const std::optional<Act> &a = sp.act; a && a->span.began())
      if (const std::string &x = f(a->out.fx); !x.empty())
        out.insert(x);
  return out;
}

} // namespace

std::set<std::string> AdoptedOf(Steps ss) {
  return effects(ss, [](const Effect &e) -> const std::string & { return e.adopted; });
}
std::set<std::string> StagedOf(Steps ss) {
  return effects(ss, [](const Effect &e) -> const std::string & { return e.staged; });
}

// =============================================================================
// 工具实现表 (AGENT_TOOLS 唯一展开): 四形一律包成 开拍 (Call, Seat) → Prep; 存档复原按结果类型探 R::load
// =============================================================================
namespace {

using OpenFn = Prep (*)(const Call &, Seat &);
using RestoreFn = ResPtr (*)(const Call &, const json &, Seat &);

// Sync / Human: 当场交出 (Human 交的是问卷)
template <SyncSig *F>
Prep now(const Call &c, Seat &s) {
  return {F(c, s), {}};
}

// Pure: 备料 = 带上调用, 收尾 = 搬进结果
template <PureSig *F>
Prep pure(const Call &c, Seat &) {
  return {{}, [c](Bay &bay) -> Commit { return [out = F(c, bay)](ToolOut &o, Desk &) mutable { o = std::move(out); }; }};
}

template <Form K, auto F>
constexpr OpenFn open_of() {
  if constexpr (K == Form::Pure)
    return &pure<F>;
  else if constexpr (K == Form::Lab)
    return F;
  else
    return &now<F>;
}

template <class R>
constexpr RestoreFn restore_of() {
  if constexpr (requires(const Call &c, const json &j, Seat &s) { R::load(c, j, s); })
    return &R::load;
  else
    return nullptr;
}

struct Spec {
  OpenFn open;
  json (*sample)(Feed &);
  RestoreFn restore;
};

#define AGENT_TOOL_SPEC(T, id, cn, F, C, keep, R) Spec{open_of<Form::F, &T>(), &Sample##T, restore_of<R>()},
constexpr Per<Tool, Spec> kSpecs{{{AGENT_TOOLS(AGENT_TOOL_SPEC)}}};
#undef AGENT_TOOL_SPEC

void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

// 收尾: 时段封口 (含末段), 原文按 kOutChars 截断
void seal(Act &a, double t) {
  if (!a.laps.empty())
    a.laps.back().span.end(t);
  a.span.end(t);
  clip(a.out.text);
}

Act &last(std::vector<Step> &tl) {
  assert(!tl.empty() && tl.back().act);
  return *tl.back().act;
}

} // namespace

// =============================================================================
// 统一调用底座 Rig: open → pump / answer → 收尾; abort 随时作废
// =============================================================================
const Act *Rig::acting() const {
  const Step *sp = timeline.empty() ? nullptr : &timeline.back();
  return sp && sp->act && !sp->act->span.ended() ? &*sp->act : nullptr;
}

Seat Rig::seat(const Gate &gate, std::string_view session, bool dry, bool gpu) {
  return {desk, notes, gate, step(), catalog::label(desk.scope()), std::string(session), dry, gpu};
}

void Rig::flush() {
  if (doomed.empty() || lab.busy())
    return;
  for (const std::string &f : doomed)
    desk.discard(f);
  doomed.clear();
}

bool Rig::open_act(Seat &s, double t) {
  Act &a = last(timeline);
  a.span.begin(t);
  const ToolMeta &m = def(a.call.tool);
  Prep p = kSpecs[a.call.tool].open(a.call, s);
  a.out = std::move(p.out);
  if (a.out.res)
    a.out.fx = a.out.res->effect();
  if (p.task) {
    lab.send({m.cat, m.cat == Cat::Query ? nullptr : desk.env(), std::move(p.task)}); // 研报库不看作用域
    return false;
  }
  if (m.exec == Exec::Human)
    return false;
  seal(a, t);
  return true;
}

bool Rig::pump_act(double t) {
  while (std::optional<Lab::Tick> k = lab.poll()) {
    if (timeline.empty() || !timeline.back().act)
      continue;
    Act &a = *timeline.back().act;
    if (!a.span.running() || def(a.call.tool).exec != Exec::Lab)
      continue;
    if (k->step == LabStep::End) {
      assert(k->out && "在跑的调用没被取消, 后台须交回收尾");
      k->out(a.out, desk);
      seal(a, t);
      flush();
      return true;
    }
    if (!a.laps.empty())
      a.laps.back().span.end(t);
    a.laps.push_back({k->step, {}});
    a.laps.back().span.begin(t);
  }
  flush();
  return false;
}

void Rig::answer_act(double t) {
  Act &a = last(timeline);
  assert(def(a.call.tool).exec == Exec::Human && a.span.running() && a.out.res);
  a.out = Done(a.out.res);
  seal(a, t);
}

void Rig::abort_act(double t) {
  Act &a = last(timeline);
  assert(a.span.running());
  lab.cancel();
  if (!a.out.fx.staged.empty())
    doomed.push_back(a.out.fx.staged);
  a.out = a.out.res && a.out.res->abort() ? Done(a.out.res, false) : Fail("研究员取消了这个调用, 没做完");
  seal(a, t);
  flush();
}

void Rig::restore(const json &saved, Seat &s) {
  assert(saved.size() == timeline.size());
  for (size_t i = 0; i < timeline.size(); ++i)
    if (std::optional<Act> &a = timeline[i].act; a && saved[i].at("act").contains("res")) {
      const RestoreFn fn = kSpecs[a->call.tool].restore;
      assert(fn && "存了结果的工具须给 load");
      a->out.res = fn(a->call, saved[i].at("act").at("res"), s);
    }
}

// =============================================================================
// 随机输入的取材
// =============================================================================
namespace {

constexpr int kSpotTries = 64;    // 挑当日有逐笔的票: 随机试几次
constexpr double kSpotSeen = 0.7; // 工作台有 view_trades 明细时取它的段的概率
constexpr std::string_view kCsRoots[] = {"CsRank", "CsNormRank", "CsZ"};

// 叶名 档.层.列 的层段是 grid
bool on_grid(const std::string &code, const std::string &grid) {
  const size_t p = code.find('.');
  return p != std::string::npos && code.compare(p + 1, grid.size() + 1, grid + ".") == 0;
}

} // namespace

int Feed::below(int n) {
  assert(n > 0);
  return std::uniform_int_distribution<int>(0, n - 1)(rng);
}
bool Feed::coin(double p) { return std::uniform_real_distribution<double>(0.0, 1.0)(rng) < p; }

std::string Feed::pick(Tool t, std::string_view key) {
  const ArgDefs as = doc(t).args;
  const auto a = std::ranges::find_if(as, [&](const ArgDef &x) { return key == x.key; });
  assert(a != as.end() && !a->choices.empty() && "参数表里须有这个带可选集的参数");
  const int e = a->empty_ok(), i = below(static_cast<int>(a->choices.size()) + e) - e;
  return i < 0 ? std::string() : std::string(a->choices[static_cast<size_t>(i)]);
}

std::string Feed::leaf() { return ctx.leaves.empty() ? std::string("bar.1d.close") : one(ctx.leaves).code; }

std::vector<std::string> Feed::leaves(int n) {
  std::vector<std::string> v;
  for (const Leaf &l : ctx.leaves)
    v.push_back(l.code);
  std::shuffle(v.begin(), v.end(), rng);
  v.resize(std::min(v.size(), static_cast<size_t>(n)));
  return v;
}

std::vector<std::string> Feed::grid() {
  std::vector<std::string> v;
  for (const Leaf &l : ctx.leaves)
    if (on_grid(l.code, ctx.grid))
      v.push_back(l.code);
  return v;
}

std::string Feed::factor() {
  std::vector<const Result *> ok;
  for (const Result &r : tried)
    if (r.visible && r.best())
      ok.push_back(&r);
  if (!ok.empty() && coin(0.8))
    return one(ok)->name;
  const std::vector<std::string> g = grid();
  const std::string x = g.empty() ? leaf() : one(g);
  std::vector<std::string> roots;
  for (const Op &o : ctx.ops)
    if (std::ranges::contains(kCsRoots, std::string_view(o.name)))
      roots.push_back(o.name + "(" + x + ")");
    else if (o.name == "TsRankRoll")
      roots.push_back("TsRankRoll(" + x + ", d=" + std::to_string(ctx.ts_root_d) + ")");
  return roots.empty() ? "CsRank(" + x + ")" : one(roots);
}

std::string Feed::day() { return ctx.days.empty() ? std::string() : misc::dash(one(ctx.days)); }

std::pair<std::string, std::string> Feed::span(int lo, int hi) {
  assert(lo >= 1 && lo <= hi);
  const int D = static_cast<int>(ctx.days.size());
  if (D == 0)
    return {};
  const int n = std::min(D, lo + below(hi - lo + 1)), d0 = below(D - n + 1);
  return {misc::dash(ctx.days[static_cast<size_t>(d0)]), misc::dash(ctx.days[static_cast<size_t>(d0 + n - 1)])};
}

std::string Feed::code() { return ctx.codes.empty() ? std::string("000001.SZ") : one(ctx.codes); }
std::string Feed::clock() {
  if (ctx.clock.size() <= 1)
    return {};
  return ctx.clock[static_cast<size_t>(ctx.open + below(static_cast<int>(ctx.clock.size()) - ctx.open))];
}

std::string Feed::tod(int cap, const std::string &around) {
  assert(cap >= 1);
  const int seg = static_cast<int>(ctx.clock.size()), lo = ctx.open;
  if (seg <= 1)
    return {};
  const int n = 1 + below(std::min(cap, seg - lo));
  int k0 = lo + below(seg - lo - n + 1);
  if (const auto it = std::find(ctx.clock.begin() + lo, ctx.clock.end(), around); it != ctx.clock.end())
    k0 = std::clamp(static_cast<int>(it - ctx.clock.begin()) - n / 2, lo, seg - n);
  return ctx.clock[static_cast<size_t>(k0)] + "-" + ctx.clock[static_cast<size_t>(k0 + n - 1)];
}

Spot Feed::spot() {
  std::vector<Spot> seen;
  for (const Step &sp : timeline)
    if (const TradesRes *t = sp.act ? As<Tool::ViewTrades>(*sp.act) : nullptr)
      for (const TradesRes::Seg &s : t->list)
        seen.push_back({s.code, s.date, ctx.clock.size() > 1 ? s.time.substr(0, 5) : std::string()});
  if (!seen.empty() && coin(kSpotSeen))
    return one(seen);
  const size_t D = ctx.days.size(), N = ctx.codes.size();
  assert(ctx.traded.size() == D * N);
  for (int i = 0; i < kSpotTries && D > 0 && N > 0; ++i) {
    const size_t d = static_cast<size_t>(below(static_cast<int>(D))), a = static_cast<size_t>(below(static_cast<int>(N)));
    if (ctx.traded[d * N + a])
      return {ctx.codes[a], misc::dash(ctx.days[d]), clock()};
  }
  return {code(), day(), clock()};
}

std::vector<std::string> Feed::refs(int n) {
  std::vector<std::string> out;
  for (size_t i = timeline.size(); i-- > 0 && static_cast<int>(out.size()) < n;)
    if (const std::optional<Act> &a = timeline[i].act; a && a->span.ended() && a->out.ok)
      out.push_back(StepTag(static_cast<int>(i) + 1));
  return out;
}

json SampleArgs(Tool t, Feed &p) { return kSpecs[t].sample(p); }

} // namespace GUI::Agent
