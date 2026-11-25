// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Look.hpp"  // IWYU pragma: keep
#include "gui/task_agent/tools/Misc.hpp"  // IWYU pragma: keep
#include "gui/task_agent/tools/Note.hpp"  // IWYU pragma: keep
#include "gui/task_agent/tools/Query.hpp" // IWYU pragma: keep
#include "gui/task_agent/tools/Trial.hpp" // IWYU pragma: keep

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <numeric>
#include <type_traits>
#include <utility>
#include <variant>

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
    if (const TrialRes *x = AsTrial(a))
      ++t.verdicts[x->st.res.verdict];
    t.adopted += a.call.tool == Tool::SubmitFactor && a.out.ok;
  }
  return t;
}

bool Finished(Steps ss) {
  for (size_t i = ss.size(); i-- > 0;) {
    if (const std::optional<Act> &a = ss[i].act; a && a->call.tool == Tool::Finish && a->span.ended())
      return true;
    if (ss[i].span.began())
      return false;
  }
  return false;
}

std::set<std::string> AdoptedOf(Steps ss) {
  std::set<std::string> out;
  for (const Step &sp : ss)
    if (const std::optional<Act> &a = sp.act; a && a->call.tool == Tool::SubmitFactor && a->span.ended() && a->out.ok)
      out.insert(a->call.str("name"));
  return out;
}

std::set<std::string> StagedOf(Steps ss) {
  std::set<std::string> out;
  for (const Step &sp : ss)
    if (const TrialRes *x = sp.act && sp.act->span.ended() ? AsTrial(*sp.act) : nullptr; x && x->st.ok())
      out.insert(x->st.file);
  return out;
}

// =============================================================================
// 工具实现表 (AGENT_TOOLS 唯一展开, 编译期核对 ImplFn 的下标 = ToolMeta::exec)
// =============================================================================
namespace {

// 纯后台工具: 备料 = 带上调用, 收尾 = 搬进结果
template <PureFn F>
LabTask pure(Act &a, Seat &) {
  return [c = a.call](Bay &bay) -> Commit { return [out = F(c, bay)](Act &x, Lab &) mutable { x.out = std::move(out); }; };
}

template <auto F>
constexpr ImplFn impl() {
  if constexpr (std::is_same_v<decltype(F), PureFn>)
    return LabFn(&pure<F>);
  else
    return ImplFn(F);
}

struct Spec {
  ImplFn fn;
  json (*sample)(Feed &);
};

#define AGENT_TOOL_SPEC(T, id, cn, E, C, keep) Spec{impl<&T>(), &Sample##T},
constexpr Per<Tool, Spec> kSpecs{{{AGENT_TOOLS(AGENT_TOOL_SPEC)}}};
#undef AGENT_TOOL_SPEC

constexpr bool specs_match() {
  for (const Tool t : kVals<Tool>)
    if (static_cast<Exec>(kSpecs[t].fn.index()) != def(t).exec)
      return false;
  return true;
}
static_assert(specs_match(), "实现形 = 清单的执行");

void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

// 收尾: 时段封口, 原文按 kOutChars 截断
void seal(Act &a, double t) {
  if (TrialRes *r = AsTrial(a))
    r->search.end(t), r->run.end(t);
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
  return {lab, notes, gate, step(), catalog::label(lab.scope()), std::string(session), dry, gpu};
}

void Rig::flush() {
  if (doomed.empty() || lab.busy())
    return;
  for (const std::string &f : doomed)
    lab.discard(f);
  doomed.clear();
}

bool Rig::open_act(Seat &s, double t) {
  Act &a = last(timeline);
  a.span.begin(t);
  const ImplFn &fn = kSpecs[a.call.tool].fn;
  if (const SyncFn *f = std::get_if<SyncFn>(&fn)) {
    a.out = (*f)(a.call, s);
    seal(a, t);
    return true;
  }
  if (const HumanFn *f = std::get_if<HumanFn>(&fn)) {
    a.out.res = (*f)(a.call);
    return false;
  }
  LabTask task = std::get<LabFn>(fn)(a, s);
  if (!task) {
    seal(a, t);
    return true;
  }
  lab.send(a.call.tool, std::move(task));
  return false;
}

bool Rig::pump_act(double t) {
  while (std::optional<Lab::Tick> k = lab.poll()) {
    if (timeline.empty() || !timeline.back().act)
      continue;
    Act &a = *timeline.back().act;
    if (!a.span.running() || def(a.call.tool).exec != Exec::Lab)
      continue;
    if (k->step == LabStep::End) {
      assert(k->done && "在跑的调用没被取消, 后台须交回收尾");
      k->done(a, lab);
      seal(a, t);
      flush();
      return true;
    }
    TrialRes *r = AsTrial(a);
    assert(r && "只有 eval_factor 分段");
    r->search.end(t), r->run.end(t);
    (k->step == LabStep::Search ? r->search : r->run).begin(t);
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
  if (TrialRes *r = AsTrial(a); r && r->st.ok()) {
    doomed.push_back(std::exchange(r->st.file, {}));
    r->st.res = Result{.name = r->st.cand.name,
                       .name_cn = r->st.cand.name_cn,
                       .tmpl = r->st.cand.expr,
                       .note = r->st.cand.note,
                       .error = "研究员取消了这次回测",
                       .verdict = Verdict::Unstaged,
                       .visible = false};
    a.out = {false, r->text(), {}, a.out.res};
  } else {
    a.out = Fail("研究员取消了这个调用, 没做完");
  }
  seal(a, t);
  flush();
}

// =============================================================================
// 随机输入的取材
// =============================================================================
namespace {

constexpr int kSpotTries = 64;    // 挑当日有逐笔的票: 随机试几次
constexpr double kSpotSeen = 0.7; // 工作台有 view_trades 明细时取它的段的概率
constexpr std::string_view kCsRoots[] = {"CsRank", "CsNormRank", "CsZ"};

std::string dash(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}

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

std::string Feed::day() { return ctx.days.empty() ? std::string() : dash(one(ctx.days)); }

std::pair<std::string, std::string> Feed::span(int lo, int hi) {
  assert(lo >= 1 && lo <= hi);
  const int D = static_cast<int>(ctx.days.size());
  if (D == 0)
    return {};
  const int n = std::min(D, lo + below(hi - lo + 1)), d0 = below(D - n + 1);
  return {dash(ctx.days[static_cast<size_t>(d0)]), dash(ctx.days[static_cast<size_t>(d0 + n - 1)])};
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
    if (const auto *t = sp.act ? dynamic_cast<const TradesRes *>(sp.act->out.res.get()) : nullptr)
      for (const TradesRes::Seg &s : t->list)
        seen.push_back({s.code, s.date, ctx.clock.size() > 1 ? s.time.substr(0, 5) : std::string()});
  if (!seen.empty() && coin(kSpotSeen))
    return one(seen);
  const size_t D = ctx.days.size(), N = ctx.codes.size();
  assert(ctx.traded.size() == D * N);
  for (int i = 0; i < kSpotTries && D > 0 && N > 0; ++i) {
    const size_t d = static_cast<size_t>(below(static_cast<int>(D))), a = static_cast<size_t>(below(static_cast<int>(N)));
    if (ctx.traded[d * N + a])
      return {ctx.codes[a], dash(ctx.days[d]), clock()};
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
