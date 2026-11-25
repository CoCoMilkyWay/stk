// list_library — 列正式库: 输入 top / skip → LibraryRes (同评估期、本持有期有统计的正式因子, score (中位 rIC) 降序) → 原文; 展示见 ui/query/ListLibrary.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/Query.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string LibraryRes::text() const {
  std::string s;
  for (const Result &r : rows) {
    const Weight &w = *r.best();
    s += "- " + r.name + " (" + r.name_cn + "): `" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n  - 最佳权重 " + Fills(r.tmpl, w.expr) + ": " + SignLine(r) + " · " + Text(w) + "\n";
  }
  s += ListTail(all, skip, static_cast<int>(rows.size()));
  if (none)
    s += "(另有 " + std::to_string(none) + " 个在本持有期无统计, 不列)\n";
  return s.empty() ? "(空)\n" : s;
}

std::string LibraryRes::title() const {
  return "共 " + std::to_string(all) + " 个 · 列 " + std::to_string(rows.size()) + " 个" + (none ? " · 另有 " + std::to_string(none) + " 个无统计" : "") +
         (rows.empty() ? "" : " · 首个 " + rows.front().name + " " + Text(*rows.front().best()));
}

ToolOut ListLibrary(const Call &c, Seat &s) {
  auto res = std::make_shared<LibraryRes>();
  LibraryRes &R = *res;
  R.skip = c.num("skip");
  const int top = c.num("top");
  for (const Result &r : s.desk.ctx().library) {
    if (!r.visible)
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

json SampleListLibrary(Feed &p) { return {{"top", 10 + p.below(20)}, {"skip", 0}}; }

} // namespace GUI::Agent
