// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Hyp.hpp"     // IWYU pragma: keep
#include "gui/task_agent/tools/Look.hpp"    // IWYU pragma: keep
#include "gui/task_agent/tools/Misc.hpp"    // IWYU pragma: keep
#include "gui/task_agent/tools/Note.hpp"    // IWYU pragma: keep
#include "gui/task_agent/tools/Query.hpp"   // IWYU pragma: keep
#include "gui/task_agent/tools/Session.hpp" // IWYU pragma: keep
#include "gui/task_agent/tools/Trial.hpp"   // IWYU pragma: keep

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <numeric>
#include <utility>
#include <variant>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

// =============================================================================
// 计数 Tally 与单趟增量账本 State
// =============================================================================
int Tally::n(Exec e) const {
  int s = 0;
  for (const Tool t : kVals<Tool>)
    s += def(t).exec == e ? n(t) : 0;
  return s;
}

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

void OpenRound(State &s) { s.flags.done = false; }

void CloseReply(State &s, const Round &rd) {
  assert(rd.span.ended());
  s.tally.llm_s += rd.span.len(0.0);
  s.tally.usage += rd.reply.usage;
}

void ApplyAct(State &s, const Act &a, Ref at) {
  assert(a.span.ended());
  const Call &c = a.call;
  ++s.tally.calls[c.tool];
  s.tally.act_s += a.span.len(0.0);
  s.tally.fails[c.tool] += !a.out.ok;
  if (const TrialRes *x = AsTrial(a))
    ++s.tally.verdicts[x->st.res.verdict];
  switch (c.tool) {
  case Tool::Hypothesize:
    s.board.add(c, at);
    break;
  case Tool::Judge:
    if (a.out.ok)
      s.board.judge(c, at);
    break;
  case Tool::EvalFactor:
    if (const TrialRes *x = AsTrial(a); x && x->st.ok()) {
      if (const std::string h = c.str("hyp"); !h.empty() && s.board.find(h))
        s.board.vary(c);
      s.staged.insert(x->st.file);
    }
    break;
  case Tool::SubmitFactor:
    if (a.out.ok) {
      s.adopted.insert(c.str("name"));
      ++s.tally.adopted;
    }
    break;
  case Tool::Finish:
    s.flags.done = true;
    break;
  default:
    break;
  }
}

State StateOf(Rounds rs) {
  State s;
  for (size_t i = 0; i < rs.size(); ++i) {
    const Round &rd = rs[i];
    OpenRound(s);
    if (rd.span.ended())
      CloseReply(s, rd);
    for (size_t j = 0; j < rd.acts.size(); ++j)
      if (const Act &a = rd.acts[j]; a.span.ended())
        ApplyAct(s, a, {static_cast<int>(i) + 1, static_cast<int>(j) + 1});
  }
  return s;
}

Tally TallyAt(const State &s, Rounds rs, double now) {
  Tally t = s.tally;
  if (rs.empty())
    return t;
  const Round &rd = rs.back();
  if (rd.span.running())
    t.llm_s += rd.span.len(now);
  for (const Act &a : rd.acts)
    if (a.span.running())
      ++t.calls[a.call.tool], t.act_s += a.span.len(now);
  return t;
}

Tally TallyOf(Rounds rs, double now) {
  Tally t;
  for (const Round &rd : rs) {
    t.llm_s += rd.span.len(now), t.usage += rd.reply.usage;
    for (const Act &a : rd.acts) {
      if (!a.span.began())
        continue;
      ++t.calls[a.call.tool], t.act_s += a.span.len(now);
      if (!a.span.ended())
        continue;
      t.fails[a.call.tool] += !a.out.ok;
      if (const TrialRes *x = AsTrial(a))
        ++t.verdicts[x->st.res.verdict];
      t.adopted += a.call.tool == Tool::SubmitFactor && a.out.ok;
    }
  }
  return t;
}

BatchCheck VetBatch(std::string_view reply, bool ask) {
  BatchCheck out;
  ParseCalls(reply, ask, out.calls, out.error);
  return out;
}

// =============================================================================
// 工具实现表 (AGENT_TOOLS 唯一展开, 零手工下标补丁, 编译期核对签名 = ToolMeta::exec)
// =============================================================================
namespace {

struct Spec {
  ImplFn fn;
  json (*sample)(Feed &);
};

#define AGENT_TOOL_SPEC(T, id, cn, E, C, keep) Spec{ImplFn(T), &Sample##T},
constexpr Per<Tool, Spec> kSpecs{{{AGENT_TOOLS(AGENT_TOOL_SPEC)}}};
#undef AGENT_TOOL_SPEC

constexpr Exec exec_of(const ImplFn &f) {
  return std::holds_alternative<SyncFn>(f)    ? Exec::Sync
         : std::holds_alternative<HumanFn>(f) ? Exec::Human
                                              : Exec::Lab;
}
constexpr bool specs_match() {
  for (const Tool t : kVals<Tool>)
    if (exec_of(kSpecs[t].fn) != def(t).exec)
      return false;
  return true;
}
static_assert(specs_match(), "实现的签名 = 清单的执行");

void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

void finish_act(Act &a, State &st, Ref at, double t) {
  if (TrialRes *r = AsTrial(a))
    r->search.end(t), r->run.end(t);
  a.span.end(t);
  if (!IsAgentTool(a.call.tool))
    return;
  ApplyAct(st, a, at);
  clip(a.out.text);
}

} // namespace

// =============================================================================
// 统一调用底座 Rig: open ⇄ step / settle / answer / abort
// =============================================================================
Seat Rig::seat(size_t i, const Gate &gate, std::string_view session, bool dry, bool gpu, llm::Options llm) {
  return {lab, notes, gate, st, timeline, {round(), static_cast<int>(i) + 1}, catalog::label(lab.scope()), std::string(session), dry, gpu, std::move(llm)};
}

void Rig::flush() {
  if (doomed.empty() || lab.busy())
    return;
  for (const std::string &f : doomed)
    lab.discard(f);
  doomed.clear();
}

Exec Rig::open_act(Act &a, Seat &s, double t) {
  a.span.begin(t);
  const ImplFn &fn = kSpecs[a.call.tool].fn;
  if (const auto *sync = std::get_if<SyncFn>(&fn)) {
    a.out = (*sync)(a.call, s);
    finish_act(a, st, s.me, t);
    return Exec::Sync;
  }
  if (const auto *direct = std::get_if<LabFn>(&fn)) {
    const Call call = a.call;
    const LabFn f = *direct;
    lab.send(call.tool, [call, f](Bay &bay) { return f(call, bay); });
    return Exec::Lab;
  }
  if (const auto *staged = std::get_if<StagedLab>(&fn)) {
    std::optional<LabTask> task = staged->open(a, s);
    if (!task) {
      finish_act(a, st, s.me, t);
      return Exec::Sync;
    }
    lab.send(a.call.tool, std::move(*task));
    return Exec::Lab;
  }
  a.out.res = std::make_shared<AskRes>(QuestionsOf(a.call));
  return Exec::Human;
}

void Rig::step_act(Act &a, LabStep step, double t) {
  TrialRes *r = AsTrial(a);
  if (!r)
    return;
  r->search.end(t), r->run.end(t);
  if (step == LabStep::Search)
    r->search.begin(t);
  else if (step == LabStep::Run)
    r->run.begin(t);
}

void Rig::settle_act(Act &a, ToolOut out, Seat &s, double t) {
  assert(def(a.call.tool).exec == Exec::Lab && a.span.running());
  if (const auto *staged = std::get_if<StagedLab>(&kSpecs[a.call.tool].fn))
    staged->close(a, std::move(out), s);
  else
    a.out = std::move(out);
  finish_act(a, st, s.me, t);
}

void Rig::answer_act(Act &a, Ref at, double t) {
  assert(def(a.call.tool).exec == Exec::Human && a.span.running());
  auto r = std::static_pointer_cast<AskRes>(a.out.res);
  assert(r);
  a.out = std::get<HumanFn>(kSpecs[a.call.tool].fn)(std::move(r));
  finish_act(a, st, at, t);
}

void Rig::abort_act(Act &a, Ref at, double t) {
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
  finish_act(a, st, at, t);
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
  for (const Round &rd : timeline)
    for (const Act &a : rd.acts)
      if (const auto *t = dynamic_cast<const TradesRes *>(a.out.res.get()))
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

std::string Feed::hyp() {
  const std::vector<Hyp> &hs = board.all();
  return hs.empty() ? std::string() : hs.back().id;
}

std::vector<std::string> Feed::refs(int n) {
  std::vector<std::string> out;
  for (size_t i = timeline.size(); i-- > 0 && static_cast<int>(out.size()) < n;)
    for (size_t j = timeline[i].acts.size(); j-- > 0 && static_cast<int>(out.size()) < n;)
      if (const Act &a = timeline[i].acts[j]; a.span.ended() && a.out.ok)
        out.push_back(RefText({static_cast<int>(i) + 1, static_cast<int>(j) + 1}));
  return out;
}

json SampleArgs(Tool t, Feed &p) { return kSpecs[t].sample(p); }

} // namespace GUI::Agent
