// build_context — 拼上下文: 照工作台快照拼出 system / 首条 / 研究树 / 本条增量 / 完整上下文 TurnsOf; 展示见 ui/session/BuildContext.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Session.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

std::string calls_json(const std::vector<Act> &acts) {
  json arr = json::array();
  for (const Act &a : acts)
    arr.push_back({{"tool", def(a.call.tool).id}, {"args", a.call.args}});
  return json{{"calls", std::move(arr)}}.dump();
}

int visible_count(const std::vector<Result> &rs) {
  return static_cast<int>(std::ranges::count_if(rs, [](const Result &r) { return r.visible; }));
}

} // namespace

std::string BuildContextRes::text() const { return body; }

ToolOut BuildContext(const Call &c, Seat &s) {
  const std::string kind = c.str("kind"), view = c.str("view");
  const bool ask = c.str("ask") == "yes";
  const std::string memo = c.str("memo"), remark = c.str("remark");
  const Context &ctx = s.lab.ctx();

  auto r = std::make_shared<BuildContextRes>();
  r->kind = kind, r->view = view;
  r->done = s.st.done;
  r->next_step = NextStep(s.lab.test());
  r->system = SystemMessage(ctx, s.gate, ask).text;

  const auto make_first = [&] {
    return FirstMessage(ctx, visible_count(s.lab.results()), NotesBrief(s.notes, s.scope), ask, remark);
  };

  std::vector<Round> rs(s.timeline.begin(), s.timeline.end());
  if (kind == "first" || kind == "test") {
    Message m = kind == "test" ? TestMessage(ask, remark) : make_first();
    rs = {Round{.kind = m.kind, .user = std::move(m.text), .memo = memo}};
  } else {
    if (rs.empty() || (rs.size() == 1 && rs[0].user.empty() && rs[0].acts.empty())) {
      Message m = make_first();
      rs = {Round{.kind = m.kind, .user = std::move(m.text), .memo = memo}};
    } else if (!rs.back().acts.empty()) {
      if (rs.size() == 1 && rs[0].user.empty())
        rs[0].kind = Msg::First, rs[0].user = make_first().text;
      if (!rs.back().reply.ok())
        rs.back().reply = {.text = calls_json(rs.back().acts), .end = llm::End::Ok};
      Message nm = RoundMessage(rs.back().acts, static_cast<int>(rs.size()), s.lab.test());
      rs.push_back(Round{.kind = nm.kind, .user = std::move(nm.text), .memo = memo});
    } else if (!memo.empty()) {
      rs.back().memo += (rs.back().memo.empty() ? "" : "\n\n") + memo;
    }
  }

  const llm::Turns ts = TurnsOf(rs);
  r->round = static_cast<int>(rs.size());
  r->turns = ts.size();
  r->first = TextOf(rs.front());
  r->tree = TreeOf(rs);
  if (ts.size() >= 2)
    r->prev_reply = ts[ts.size() - 2].text;
  r->current = TextOf(rs.back());

  if (view == "system")
    r->body = r->system;
  else if (view == "tree")
    r->body = r->tree.empty() ? "(无研究树: 尚在首轮或前轮无已执行调用)" : r->tree;
  else if (view == "delta")
    r->body = r->current;
  else
    r->body = llm::Flat(ts);

  return Done(std::move(r));
}

json SampleBuildContext(Feed &p) {
  return {{"kind", p.timeline.size() > 1 || !p.timeline.back().acts.empty() ? "round" : "first"},
          {"view", p.coin(0.5) ? "full" : "delta"},
          {"ask", "yes"},
          {"memo", ""},
          {"remark", ""}};
}

} // namespace GUI::Agent
