// list_notes 的展示: 每类一节 (条数 + 逐条: 编号 / 时间 / 作用域 / 附议 / 正文)
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void NotesRes::draw() const {
  for (const Group &g : groups) {
    heading(std::string(def(g.kind).cn) + " · " + std::to_string(g.hit) + " 条" + (here ? " · 本作用域" : "") + (query ? " · query 命中" : ""));
    if (g.items.empty()) {
      ImGui::TextDisabled("(无)");
      continue;
    }
    if (!begin_table(def(g.kind).id, {"编号", "时间", "作用域", "标题 / 正文"}, 0.0f))
      continue;
    for (const Item &x : g.items) {
      ImGui::TableNextRow();
      cell(x.id), cell(x.time), cell_dim(x.scope);
      ImGui::TableNextColumn();
      wrapped(kColHead, x.title);
      if (!x.relation.empty())
        wrapped(color(TaskStatus::Kind::Warn), x.relation);
      for (const std::string &b : x.body)
        wrapped_dim(b);
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
