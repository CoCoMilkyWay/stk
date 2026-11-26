// list_library — 列正式库: 输入 query / top / skip → LibraryRes (同评估期、命中 query、本持有期有统计的正式因子, score (中位 rIC) 降序) → 原文; 展示见 ui/query/ListLibrary.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/Query.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string LibraryRes::text() const {
  std::string s = ListHead(query, all);
  for (const Result &r : rows) {
    const Weight &w = *r.best();
    s += "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n- 最佳权重 " + Fills(r.tmpl, w.expr) + ": " + SignLine(r) +
         " · " + Text(w) + "\n";
  }
  s += ListTail(all, skip, static_cast<int>(rows.size()));
  if (none)
    s += "(另有 " + std::to_string(none) + " 个在本持有期无统计, 不列)\n";
  return rows.empty() && !all && !none ? s + "(无)\n" : s;
}

std::string LibraryRes::title() const {
  return (query.empty() ? "" : "query " + query + " · ") + "共 " + std::to_string(all) + " 个 · 列 " + std::to_string(rows.size()) + " 个" +
         (none ? " · 另有 " + std::to_string(none) + " 个无统计" : "") + (rows.empty() ? "" : " · 首个 " + rows.front().name + " " + Text(*rows.front().best()));
}

ToolOut ListLibrary(const Call &c, Seat &s) {
  auto res = std::make_shared<LibraryRes>();
  LibraryRes &R = *res;
  R.query = c.str("query"), R.skip = c.num("skip");
  const std::vector<std::string> ws = text::words(R.query);
  const int top = c.num("top");
  for (const Result &r : s.desk.ctx().library) {
    if (!r.visible || !Hits(r, ws))
      continue;
    if (!r.best()) {
      ++R.none;
      continue;
    }
    if (R.all++ >= R.skip && static_cast<int>(R.rows.size()) < top)
      R.rows.push_back(r);
  }
  return Done(std::move(res));
}

json SampleListLibrary(Feed &p) { return {{"query", ""}, {"top", 10 + p.below(20)}, {"skip", 0}}; }

} // namespace GUI::Agent
