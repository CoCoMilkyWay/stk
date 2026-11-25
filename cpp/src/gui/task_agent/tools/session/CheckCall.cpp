// check_call — 核一条回复: 跑 ParseCall + eval_factor 落盘核验 (lab.vet); 展示见 ui/session/CheckCall.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Session.hpp"

#include <utility>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string CheckCallRes::text() const {
  if (!error.empty())
    return retry_text;
  std::string s = "校验通过 (ask=" + std::string(ask ? "yes" : "no") + "): `" + StepTag(step) + "` [" + def(def(call.tool).exec).cn + "] `" + call.text() + "`";
  if (!pre_err.empty())
    s += " — 预检未过: " + pre_err;
  return s + "\n";
}

ToolOut CheckCall(const Call &c, Seat &s) {
  auto r = std::make_shared<CheckCallRes>();
  r->ask = c.str("ask") == "yes";
  CallCheck chk = ParseCall(c.str("reply"), r->ask);
  if (!chk.ok()) {
    r->error = std::move(chk.error);
    r->retry_text = RetryMessage(r->error).text;
    return Done(std::move(r), false);
  }
  r->step = s.me;
  r->call = std::move(chk.call);
  if (r->call.tool == Tool::EvalFactor)
    r->pre_err = s.lab.vet(Cand{r->call.str("name"), r->call.str("name_cn"), r->call.str("expr"), r->call.str("note")});
  return Done(std::move(r), true);
}

json SampleCheckCall(Feed &p) {
  return {{"reply", json{{"tool", "view_feature"}, {"args", {{"exprs", p.leaves(2)}}}}.dump(2)}, {"ask", "yes"}};
}

} // namespace GUI::Agent
