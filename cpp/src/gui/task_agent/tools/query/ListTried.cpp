// list_tried — 列已试: 输入 query / top / skip → TriedRes (本作用域可见、命中 query 的已评候选, score (中位 rIC) 降序) → 原文; 展示见 ui/query/ListTried.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/Query.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string TriedRes::text() const {
  std::string s = ListHead(query, all);
  for (const Result &r : rows)
    s += "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n" + Detail(r) + GateLine(r, gate);
  s += ListTail(all, skip, static_cast<int>(rows.size()));
  return rows.empty() && !all ? s + "(无)\n" : s;
}

std::string TriedRes::title() const {
  return (query.empty() ? "" : "query " + query + " · ") + "共 " + std::to_string(all) + " 个 · 列 " + std::to_string(rows.size()) + " 个" +
         (rows.empty() ? "" : " · 首个 " + rows.front().name + " " + Brief(rows.front()));
}

ToolOut ListTried(const Call &c, Seat &s) {
  auto res = std::make_shared<TriedRes>();
  TriedRes &R = *res;
  R.query = c.str("query"), R.skip = c.num("skip"), R.gate = s.gate;
  const std::vector<std::string> ws = text::words(R.query);
  const int top = c.num("top");
  for (const Result &r : s.desk.results())
    if (r.visible && Hits(r, ws) && R.all++ >= R.skip && static_cast<int>(R.rows.size()) < top)
      R.rows.push_back(r);
  return Done(std::move(res));
}

json SampleListTried(Feed &p) { return {{"query", ""}, {"top", 10 + p.below(20)}, {"skip", p.coin(0.2) ? 10 : 0}}; }

} // namespace GUI::Agent
