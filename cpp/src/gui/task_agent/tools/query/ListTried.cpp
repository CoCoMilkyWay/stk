// list_tried — 列已试: 输入 top / skip → TriedRes (本作用域可见的已评候选, score 降序) → 原文; 展示见 ui/query/ListTried.cpp
#include "gui/task_agent/tools/Query.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string TriedRes::text() const {
  std::string s;
  for (const Result &r : rows)
    s += "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`\n" + Detail(r) + GateLine(r, gate);
  s += ListTail(all, skip, static_cast<int>(rows.size()));
  return s.empty() ? "(无)\n" : s;
}

ToolOut ListTried(const Call &c, Seat &s) {
  auto res = std::make_shared<TriedRes>();
  TriedRes &R = *res;
  R.skip = c.num("skip"), R.gate = s.gate;
  const int top = c.num("top");
  for (const Result &r : s.lab.results())
    if (r.visible && R.all++ >= R.skip && static_cast<int>(R.rows.size()) < top)
      R.rows.push_back(r);
  return Done(std::move(res));
}

json SampleListTried(Feed &p) { return {{"top", 10 + p.below(20)}, {"skip", p.coin(0.2) ? 10 : 0}}; }

} // namespace GUI::Agent
