// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/All.hpp"

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
  RestoreFn restore;
};

#define AGENT_TOOL_SPEC(T, id, cn, F, C, keep, R) Spec{open_of<Form::F, &T>(), restore_of<R>()},
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
    lab.send({m.cat, m.cat == Cat::Doc ? nullptr : desk.env(), std::move(p.task)}); // 研报库不看作用域
    return false;
  }
  if (m.form == Form::Human)
    return false;
  seal(a, t);
  return true;
}

bool Rig::pump_act(double t) {
  while (std::optional<Lab::Tick> k = lab.poll()) {
    if (timeline.empty() || !timeline.back().act)
      continue;
    Act &a = *timeline.back().act;
    if (!a.span.running() || !def(a.call.tool).lab())
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
  assert(def(a.call.tool).form == Form::Human && a.span.running() && a.out.res);
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

} // namespace GUI::Agent
