// probe_brain — 试跑大模型: 三拍对仗 OpenProbeBrain (备上下文与模型选项, GUI) → LabTask (新开一段对话 llm::send + ParseCall, Lab) → CloseProbeBrain (封存, GUI);
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
  CallCheck check;
  std::string text() const override { return {}; }
  void draw() const override {}
};

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
    turns = TurnsOf(PendingOf(s, r->ask, memo));
  }

  r->system_chars = system.size();
  r->turns = turns.size();
  r->prompt_text = llm::Flat(turns);
  const llm::Options opt = r->opt;
  const bool ask = r->ask;
  a.out.res = std::move(r);

  return LabTask([opt, system = std::move(system), turns = std::move(turns), ask](Bay &bay) -> ToolOut {
    auto y = std::make_shared<BrainYield>();
    llm::Link link;
    y->reply = llm::send(link, opt, system, turns, true, [](llm::Part, std::string_view) {}, bay.cancel);
    if (bay.cancelled())
      return {};
    if (y->reply.ok())
      y->check = ParseCall(y->reply.text, ask);
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
