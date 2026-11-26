// finish — 收尾: 输入 name → FinishRes (提交: 入库 / 达门槛不拷 / 未入库的原因; 结束) → 原文; 展示见 ui/misc/Finish.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/Misc.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

std::string submit_line(const FinishRes &r) {
  if (r.name.empty())
    return "不提交";
  if (!r.why.empty())
    return "`" + r.name + "` 未入库: " + r.why;
  return "`" + r.name + "` " + (r.test ? "达门槛 (自检会话, 不拷进正式库)" : r.dry ? "达门槛 (工作台, 不拷进正式库)"
                                                                                   : "已入库");
}

} // namespace

std::string FinishRes::text() const {
  if (!ended)
    return "没收尾: " + why + "; 会话照常, 改 name 重交";
  return submit_line(*this) + "\n本条回复的调用做完后会话结束";
}

std::string FinishRes::title() const { return ended ? submit_line(*this) + " · 会话结束" : "没收尾: " + why; }

Effect FinishRes::effect() const {
  if (!ended)
    return {};
  return {.submitted = name, .adopted = !name.empty() && why.empty() ? name : std::string(), .finish = true};
}

ToolOut Finish(const Call &c, Seat &s) {
  auto r = std::make_shared<FinishRes>();
  r->name = c.str("name"), r->test = s.desk.test(), r->dry = s.dry;
  if (r->name.empty())
    return Done(std::move(r));
  if (const Result *t = s.desk.tried(r->name))
    r->cand = *t;
  r->ended = r->cand.has_value();
  r->why = s.desk.adopt(r->name, s.gate, s.dry).error;
  const bool ok = r->ended;
  return Done(std::move(r), ok);
}

// 多半交达门槛的可见候选 (没有就退到有权重的, 会给出差在哪); 偶尔不提交
json SampleFinish(Feed &p) {
  std::vector<std::string> pass, near;
  for (const Result &r : p.tried)
    if (r.visible && r.best())
      (GateMiss(p.gate, r).empty() ? pass : near).push_back(r.name);
  const std::vector<std::string> &v = pass.empty() ? near : pass;
  return {{"name", v.empty() || p.coin(0.2) ? std::string() : p.one(v)}};
}

} // namespace GUI::Agent
