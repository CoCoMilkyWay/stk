// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Hyp.hpp"   // IWYU pragma: keep (实现表经 AGENT_TOOLS 拼名引用)
#include "gui/task_agent/tools/Look.hpp"  // IWYU pragma: keep (实现表经 AGENT_TOOLS 拼名引用)
#include "gui/task_agent/tools/Misc.hpp"  // IWYU pragma: keep (实现表经 AGENT_TOOLS 拼名引用)
#include "gui/task_agent/tools/Note.hpp"  // IWYU pragma: keep (实现表经 AGENT_TOOLS 拼名引用)
#include "gui/task_agent/tools/Query.hpp" // IWYU pragma: keep (实现表经 AGENT_TOOLS 拼名引用)
#include "gui/task_agent/tools/Trial.hpp"

#include <array>
#include <cassert>
#include <cstdio>
#include <utility>
#include <variant>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

// =============================================================================
// 实现表: 下标 = Tool; 一行 = 执行 + 取材 (+ 后台两钩)
// =============================================================================
namespace {

struct Impl {
  RunFn run;
  json (*sample)(Feed &);
  StageFn stage = nullptr;
  SettleFn settle = nullptr;
};

#define AGENT_TOOL_IMPL(T, E, C) Impl{RunFn(&T), &Sample##T},
constexpr std::array<Impl, kToolCount> kImpls = [] {
  std::array<Impl, kToolCount> t{AGENT_TOOLS(AGENT_TOOL_IMPL)};
  t[static_cast<size_t>(Tool::EvalFactor)].stage = StageEvalFactor;
  t[static_cast<size_t>(Tool::EvalFactor)].settle = SettleEvalFactor;
  return t;
}();
#undef AGENT_TOOL_IMPL

constexpr bool impls_match() {
  for (size_t i = 0; i < kToolCount; ++i) {
    const Impl &m = kImpls[i];
    if (m.run.index() != static_cast<size_t>(kToolKinds[i].exec) || ((m.stage || m.settle) && kToolKinds[i].exec != Exec::Lab))
      return false;
  }
  return true;
}
static_assert(impls_match(), "实现的签名 = 清单的执行; 两钩只挂后台工具");

const Impl &impl(Tool t) { return kImpls[static_cast<size_t>(t)]; }

void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

} // namespace

// =============================================================================
// 调用的一生
// =============================================================================
Exec Begin(Act &a, Seat &s, double t) {
  a.span.begin(t);
  const Impl &m = impl(a.call.tool);
  switch (def(a.call.tool).exec()) {
  case Exec::Sync:
    a.out = std::get<SyncFn>(m.run)(a.call, s);
    return Exec::Sync;
  case Exec::Lab: {
    if (m.stage && !m.stage(a, s))
      return Exec::Sync;
    const Trial *k = std::get_if<Trial>(&a.extra);
    s.lab.send(a.call, std::get<LabFn>(m.run), k ? k->st.file : std::string(), s.gpu);
    return Exec::Lab;
  }
  case Exec::Human:
    a.extra.emplace<Asked>(QuestionsOf(a.call));
    return Exec::Human;
  }
  assert(false && "未知执行");
  return Exec::Sync;
}

void Enter(Act &a, LabStep step, double t) {
  Trial *k = std::get_if<Trial>(&a.extra);
  if (!k)
    return;
  k->search.end(t), k->run.end(t);
  if (step == LabStep::Search)
    k->search.begin(t);
  else if (step == LabStep::Run)
    k->run.begin(t);
}

void Settle(Act &a, ToolOut out, Seat &s) {
  assert(def(a.call.tool).exec() == Exec::Lab && a.span.running());
  if (const SettleFn f = impl(a.call.tool).settle)
    return f(a, std::move(out), s);
  a.out = std::move(out);
}

void Answer(Act &a) {
  assert(def(a.call.tool).exec() == Exec::Human && a.span.running());
  a.out = std::get<HumanFn>(impl(a.call.tool).run)(std::get<Asked>(a.extra));
}

std::string Abort(Act &a) {
  assert(a.span.running());
  std::string file;
  if (Trial *k = std::get_if<Trial>(&a.extra); k && k->st.ok()) {
    file = std::exchange(k->st.file, {});
    k->st.res = Result{.error = "研究员取消了这次回测", .verdict = Verdict::Unstaged};
  }
  a.out = Fail("研究员取消了这个调用, 没做完");
  return file;
}

void End(Act &a, State &st, Ref at, double t) {
  if (Trial *k = std::get_if<Trial>(&a.extra))
    k->search.end(t), k->run.end(t);
  a.span.end(t);
  Apply(st, a, at);
  clip(a.out.text);
}

// =============================================================================
// 随机输入的取材
// =============================================================================
namespace {

std::string dash(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}

} // namespace

int Feed::below(int n) {
  assert(n > 0);
  return std::uniform_int_distribution<int>(0, n - 1)(rng);
}
bool Feed::coin(double p) { return std::uniform_real_distribution<double>(0.0, 1.0)(rng) < p; }

std::string Feed::leaf() { return ctx.leaves.empty() ? std::string("bar.1d.close") : one(ctx.leaves).code; }

std::string Feed::factor() {
  std::vector<const Result *> ok;
  for (const Result &r : tried)
    if (r.visible && (r.best() || r.top))
      ok.push_back(&r);
  if (!ok.empty() && coin(0.8))
    return one(ok)->name;
  return "CsRank(" + leaf() + ")";
}

std::string Feed::day() { return ctx.days.empty() ? std::string() : dash(one(ctx.days)); }
std::string Feed::code() { return ctx.codes.empty() ? std::string("000001.SZ") : one(ctx.codes); }
std::string Feed::clock() {
  if (ctx.clock.size() <= 1)
    return {};
  return ctx.clock[static_cast<size_t>(ctx.open + below(static_cast<int>(ctx.clock.size()) - ctx.open))];
}

std::string Feed::hyp(bool supported) {
  const std::vector<Hyp> &hs = board.all();
  if (hs.empty())
    return "H1";
  if (supported)
    for (auto it = hs.rbegin(); it != hs.rend(); ++it)
      if (it->stance == Stance::Supported && it->evals < kHypEvals)
        return it->id;
  return one(hs).id;
}

std::vector<std::string> Feed::refs(int n) {
  std::vector<std::string> out;
  for (size_t i = timeline.size(); i-- > 0 && static_cast<int>(out.size()) < n;)
    for (size_t j = timeline[i].acts.size(); j-- > 0 && static_cast<int>(out.size()) < n;)
      if (const Act &a = timeline[i].acts[j]; a.span.ended() && a.out.ok)
        out.push_back(RefText({static_cast<int>(i) + 1, static_cast<int>(j) + 1}));
  return out;
}

json SampleArgs(Tool t, Feed &p) { return impl(t).sample(p); }

} // namespace GUI::Agent
