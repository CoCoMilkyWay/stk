// probe_brain — 试跑大模型: 三拍对仗 OpenProbeBrain (备上下文与模型选项, GUI) → LabTask (llm::send + VetBatch, Lab) → CloseProbeBrain (封存, GUI);
//               展示见 ui/session/ProbeBrain.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Bay.hpp"
#include "gui/task_agent/tools/Session.hpp"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

struct BrainYield final : Res {
  llm::Reply reply;
  BatchCheck check;
  std::string text() const override { return {}; }
  void draw() const override {}
};

std::string calls_json(const std::vector<Act> &acts) {
  json arr = json::array();
  for (const Act &a : acts)
    arr.push_back({{"tool", def(a.call.tool).id}, {"args", a.call.args}});
  return json{{"calls", std::move(arr)}}.dump();
}

} // namespace

std::string ProbeBrainRes::text() const {
  if (!error.empty())
    return "试跑未成: " + error;
  return reply.text;
}

std::optional<LabTask> OpenProbeBrain(Act &a, Seat &s) {
  const Call &c = a.call;
  auto r = std::make_shared<ProbeBrainRes>();
  r->ask = c.str("ask") == "yes";
  r->opt = s.llm;
  if (const std::string b = c.str("backend"); !b.empty())
    r->opt.backend = IdOf<llm::Backend>(b);
  if (const std::string m = c.str("model"); !m.empty())
    r->opt.model = m;

  if (llm::key(r->opt.backend).empty()) {
    r->error = std::string("后端 ") + def(r->opt.backend).name + " 的 key 未填 (output/config/agent/keys.json)";
    a.out = Done(std::move(r), false);
    return std::nullopt;
  }
  if (r->opt.model.empty()) {
    r->error = "模型为空 (先在 Config 页选好模型, 或在 model 参数里指定)";
    a.out = Done(std::move(r), false);
    return std::nullopt;
  }
  if (r->opt.net.proxy && (r->opt.net.host.empty() || r->opt.net.port <= 0 || r->opt.net.port >= 65536)) {
    r->error = "HTTP 代理地址或端口不合法 (检查 Config 页)";
    a.out = Done(std::move(r), false);
    return std::nullopt;
  }

  const Context &ctx = s.lab.ctx();
  std::string system = SystemMessage(ctx, s.gate, r->ask).text;
  llm::Turns turns;
  const std::string prompt = c.str("prompt"), memo = c.str("memo");
  if (!prompt.empty()) {
    turns.push_back({true, memo.empty() ? prompt : prompt + MemoText(memo)});
  } else {
    const int tried = static_cast<int>(std::ranges::count_if(s.lab.results(), [](const Result &x) { return x.visible; }));
    const auto make_first = [&] { return FirstMessage(ctx, tried, NotesBrief(s.notes, s.scope), r->ask, {}); };
    std::vector<Round> rs(s.timeline.begin(), s.timeline.end());
    if (rs.empty() || (rs.size() == 1 && rs[0].user.empty() && rs[0].acts.empty())) {
      Message m = make_first();
      rs = {Round{.kind = m.kind, .user = std::move(m.text), .memo = memo}};
    } else if (!rs.back().acts.empty()) {
      if (rs.size() == 1 && rs[0].user.empty())
        rs[0].kind = Msg::First, rs[0].user = make_first().text;
      if (!rs.back().reply.ok())
        rs.back().reply = {.text = calls_json(rs.back().acts), .end = llm::End::Ok};
      Message nm = RoundMessage(rs.back().acts, static_cast<int>(rs.size()), s.st.board, s.lab.test());
      rs.push_back(Round{.kind = nm.kind, .user = std::move(nm.text), .memo = memo});
    } else if (!memo.empty()) {
      rs.back().memo += (rs.back().memo.empty() ? "" : "\n\n") + memo;
    }
    turns = TurnsOf(rs);
  }

  r->system_chars = system.size();
  r->turns = turns.size();
  r->prompt_text = llm::Flat(turns);
  const llm::Options opt = r->opt;
  const bool ask = r->ask;
  a.out.res = std::move(r);

  return LabTask([opt, system = std::move(system), turns = std::move(turns), ask](Bay &bay) -> ToolOut {
    auto y = std::make_shared<BrainYield>();
    y->reply = llm::send(opt, system, turns, [](llm::Part, std::string_view) {}, bay.cancel);
    if (bay.cancelled())
      return {};
    if (y->reply.ok())
      y->check = VetBatch(y->reply.text, ask);
    return {y->reply.ok(), {}, {}, std::move(y)};
  });
}

void CloseProbeBrain(Act &a, ToolOut out, Seat &) {
  auto r = std::static_pointer_cast<ProbeBrainRes>(a.out.res);
  assert(r);
  if (auto *y = static_cast<BrainYield *>(out.res.get())) {
    r->reply = std::move(y->reply);
    r->check = std::move(y->check);
  }
  if (!r->reply.ok())
    r->error = "回复失败 (" + std::string(def(r->reply.end).id) + (r->reply.error.empty() ? "" : ": " + r->reply.error) + ")";
  const bool ok = r->reply.ok() && r->check.ok();
  a.out = Done(std::move(r), ok);
}

json SampleProbeBrain(Feed &) {
  return {{"backend", ""}, {"model", ""}, {"ask", "yes"}, {"memo", ""}, {"prompt", ""}};
}

} // namespace GUI::Agent
