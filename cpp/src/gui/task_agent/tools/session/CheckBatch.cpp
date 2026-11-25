// check_batch — 核整批调用: 跑 VetBatch (ParseCalls) + eval_factor 落盘核验 (lab.vet); 展示见 ui/session/CheckBatch.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Session.hpp"

#include <utility>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string CheckBatchRes::text() const {
  if (!error.empty())
    return retry_text;
  std::string s = "校验通过: " + std::to_string(items.size()) + " 个调用 (ask=" + (ask ? "yes" : "no") + ")\n\n";
  for (size_t i = 0; i < items.size(); ++i) {
    const Item &x = items[i];
    s += std::to_string(i + 1) + ". `" + RefText(x.at) + "` [" + def(def(x.call.tool).exec).cn + "] `" + x.call.text() + "`";
    if (!x.pre_err.empty())
      s += " — 预检未过: " + x.pre_err;
    s += "\n";
  }
  return s;
}

ToolOut CheckBatch(const Call &c, Seat &s) {
  const bool ask = c.str("ask") == "yes";
  BatchCheck chk = VetBatch(c.str("reply"), ask);

  auto r = std::make_shared<CheckBatchRes>();
  r->ask = ask;
  if (!chk.ok()) {
    r->error = std::move(chk.error);
    r->retry_text = RetryMessage(r->error).text;
    return Done(std::move(r), false);
  }
  const int round = static_cast<int>(s.timeline.size());
  for (size_t i = 0; i < chk.calls.size(); ++i) {
    Call &call = chk.calls[i];
    std::string pre;
    if (call.tool == Tool::EvalFactor)
      pre = s.lab.vet(Cand{call.str("name"), call.str("name_cn"), call.str("expr"), call.str("hyp"), call.str("note")});
    r->items.push_back({{round, static_cast<int>(i) + 1}, std::move(call), std::move(pre)});
  }
  return Done(std::move(r), true);
}

json SampleCheckBatch(Feed &p) {
  json calls = json::array();
  calls.push_back({{"tool", "list_tried"}, {"args", {{"top", 10}}}});
  calls.push_back({{"tool", "view_feature"}, {"args", {{"exprs", p.leaves(2)}}}});
  return {{"reply", json{{"calls", std::move(calls)}}.dump(2)}, {"ask", "yes"}};
}

} // namespace GUI::Agent
