// build_context — 拼上下文: 照工作台快照拼出 system / 首条 / 本条增量 / 完整上下文 TurnsOf (远处折成结论); 展示见 ui/session/BuildContext.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Session.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

int visible_count(const std::vector<Result> &rs) {
  return static_cast<int>(std::ranges::count_if(rs, [](const Result &r) { return r.visible; }));
}

Message first_of(const Seat &s, bool ask, const std::string &remark) {
  return FirstMessage(s.lab.ctx(), visible_count(s.lab.results()), NotesBrief(s.notes, s.scope), ask, remark);
}

} // namespace

std::vector<Step> PendingOf(const Seat &s, bool ask, const std::string &memo, const std::string &remark) {
  std::vector<Step> ss(s.timeline.begin(), s.timeline.end());
  if (ss.empty())
    ss.emplace_back();
  for (size_t i = 0; i < ss.size(); ++i) {
    Step &sp = ss[i];
    if (sp.user.empty()) {
      Message m = i == 0 ? first_of(s, ask, remark) : ss[i - 1].act ? ResultMessage(*ss[i - 1].act, static_cast<int>(i), s.lab.test())
                                                                    : RetryMessage(ss[i - 1].error);
      sp.kind = m.kind, sp.user = std::move(m.text);
    }
    if (sp.act && !sp.reply.ok())
      sp.reply = {.text = CallJson(sp.act->call), .end = llm::End::Ok}, sp.span = {0.0, 0.0};
  }
  if (const std::optional<Act> &a = ss.back().act; a && a->span.ended()) {
    Message m = ResultMessage(*a, static_cast<int>(ss.size()), s.lab.test());
    ss.push_back({.kind = m.kind, .user = std::move(m.text)});
  }
  if (!memo.empty())
    ss.back().memo += (ss.back().memo.empty() ? "" : "\n\n") + memo;
  return ss;
}

std::string BuildContextRes::text() const { return body; }

ToolOut BuildContext(const Call &c, Seat &s) {
  const std::string kind = c.str("kind"), view = c.str("view");
  const bool ask = c.str("ask") == "yes";
  const std::string memo = c.str("memo"), remark = c.str("remark");

  auto r = std::make_shared<BuildContextRes>();
  r->kind = kind, r->view = view;
  r->done = s.st.done;
  r->next_step = NextStep(s.lab.test());
  r->system = SystemMessage(s.lab.ctx(), s.gate, ask).text;

  std::vector<Step> ss;
  if (kind == "first" || kind == "test") {
    Message m = kind == "test" ? TestMessage(ask, remark) : first_of(s, ask, remark);
    ss = {Step{.kind = m.kind, .user = std::move(m.text), .memo = memo}};
  } else {
    ss = PendingOf(s, ask, memo, remark);
  }

  const llm::Turns ts = TurnsOf(ss);
  r->step = static_cast<int>(ss.size());
  r->turns = ts.size();
  r->folded = FoldOf(ss);
  r->first = TextOf(ss.front());
  if (ts.size() >= 2)
    r->prev_reply = ts[ts.size() - 2].text;
  r->current = TextOf(ss.back());

  if (view == "system")
    r->body = r->system;
  else if (view == "delta")
    r->body = r->current;
  else
    r->body = llm::Flat(ts);

  return Done(std::move(r));
}

json SampleBuildContext(Feed &p) {
  return {{"kind", p.timeline.empty() ? "first" : "result"}, {"view", p.coin(0.5) ? "full" : "delta"}, {"ask", "yes"}, {"memo", ""}, {"remark", ""}};
}

} // namespace GUI::Agent
