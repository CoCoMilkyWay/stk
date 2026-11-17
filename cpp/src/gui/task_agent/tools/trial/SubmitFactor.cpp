// submit_factor — 提交入库: 输入 name → SubmitRes (入库 / 达门槛不拷 / 未入库的原因) → 原文; 展示见 ui/trial/SubmitFactor.cpp
#include "gui/task_agent/tools/Trial.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string SubmitRes::text() const {
  if (!why.empty())
    return "未入库: " + why;
  return test ? "达门槛 (自检会话, 不拷进正式库)" : dry ? "达门槛 (工作台, 不拷进正式库)"
                                                        : "已入库";
}

ToolOut SubmitFactor(const Call &c, Seat &s) {
  auto r = std::make_shared<SubmitRes>();
  const Adopted x = s.lab.adopt(c.str("name"), s.gate, s.dry);
  r->name = x.name, r->why = x.error, r->test = s.lab.test(), r->dry = s.dry;
  for (const Result &t : s.lab.results())
    if (t.name == r->name)
      r->cand = t;
  const bool ok = x.ok();
  return Done(std::move(r), ok);
}

json SampleSubmitFactor(Feed &p) {
  std::vector<std::string> names;
  for (const Result &r : p.tried)
    if (r.visible && r.best())
      names.push_back(r.name);
  return {{"name", names.empty() ? std::string("bench_0") : p.one(names)}};
}

} // namespace GUI::Agent
