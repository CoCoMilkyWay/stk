// Timeline — 见头文件
#include "gui/task_agent/Timeline.hpp"

#include <algorithm>
#include <cassert>
#include <numeric>
#include <variant>

namespace GUI::Agent {

void Apply(State &s, const Act &a, Ref at) {
  assert(a.span.ended());
  const Call &c = a.call;
  switch (c.tool) {
  case Tool::SearchLeaves: {
    const Shown &x = std::get<Shown>(a.got);
    s.seen.insert(x.leaves.begin(), x.leaves.end());
    break;
  }
  case Tool::Hypothesize:
    s.board.add(c, at);
    break;
  case Tool::Judge:
    if (a.ok)
      s.board.judge(c, at);
    break;
  case Tool::EvalFactor:
    if (const Trial &k = std::get<Trial>(a.got); k.st.ok())
      s.board.vary(c, k.knob, at);
    break;
  case Tool::SubmitFactor:
    if (a.ok)
      s.adopted.insert(c.str("name"));
    break;
  default:
    break;
  }
}

State StateOf(Rounds rs) {
  State s;
  for (size_t i = 0; i < rs.size(); ++i)
    for (size_t j = 0; j < rs[i].acts.size(); ++j)
      if (const Act &a = rs[i].acts[j]; a.span.ended())
        Apply(s, a, {static_cast<int>(i) + 1, static_cast<int>(j) + 1});
  return s;
}

int Tally::n(Run r) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.run == r ? n(d.id) : 0;
  return s;
}

int Tally::n(Cat c) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.cat == c ? n(d.id) : 0;
  return s;
}

int Tally::failed(Cat c) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.cat == c ? failed(d.id) : 0;
  return s;
}

int Tally::all() const { return std::accumulate(calls.begin(), calls.end(), 0); }
int Tally::failed() const { return std::accumulate(fails.begin(), fails.end(), 0); }

Tally TallyOf(Rounds rs, double now) {
  Tally t;
  for (const Round &rd : rs) {
    t.llm_s += rd.span.len(now), t.usage += rd.reply.usage;
    for (const Act &a : rd.acts) {
      if (!a.span.began())
        continue;
      const size_t k = static_cast<size_t>(a.call.tool);
      ++t.calls[k], t.act_s += a.span.len(now);
      if (!a.span.ended())
        continue;
      t.fails[k] += !a.ok;
      if (const Trial *x = std::get_if<Trial>(&a.got))
        ++t.verdicts[static_cast<size_t>(x->st.res.verdict)];
      t.adopted += a.call.tool == Tool::SubmitFactor && a.ok;
    }
  }
  return t;
}

Flags FlagsOf(Rounds rs, bool test) {
  const auto asks = [](const Act &a) { return a.call.tool == Tool::AskHuman; };
  const auto staged = [](const Act &a) {
    const Trial *k = std::get_if<Trial>(&a.got);
    return k && k->st.ok();
  };
  const auto finished = [](const Act &a) { return a.call.tool == Tool::Finish && a.span.ended(); };
  Flags f{.reviewed = test};
  const Round *last = nullptr; // 末个有调用的轮
  for (const Round &rd : rs) {
    f.reviewed = f.reviewed || rd.kind == Msg::Review;
    f.asked = f.asked || std::ranges::any_of(rd.acts, asks);
    if (!rd.acts.empty())
      last = &rd;
  }
  f.evaled = last && std::ranges::any_of(last->acts, staged);
  f.done = f.reviewed && !rs.empty() && std::ranges::any_of(rs.back().acts, finished);
  return f;
}

} // namespace GUI::Agent
