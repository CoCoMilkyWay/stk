// search_leaves 的展示: 命中概况 + 组行表 (悬浮看公式与说明全文)
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void LeavesRes::draw() const {
  heading("查 \"" + query + "\"");
  if (!hit) {
    wrapped(color(TaskStatus::Kind::Warn), "无命中; 各词单独命中: " + alone);
    return;
  }
  ImGui::Text("命中 %zu 个叶, %zu 组%s", hit, groups, groups > static_cast<size_t>(n) ? (", 列前 " + std::to_string(n) + " 组").c_str() : "");
  if (loose)
    wrapped(color(TaskStatus::Kind::Warn), "没有同时含全部词的, 列含任一词的 (各词单独命中: " + alone + ")");
  if (!begin_table("##leaves", {"叶", "中文名", "落盘值", "公式", "说明"}, 0.0f))
    return;
  for (const Row &r : rows) {
    ImGui::TableNextRow();
    cell(r.leaf), cell(r.name_cn), cell_dim(r.value);
    ImGui::TableNextColumn();
    wrapped(r.formula);
    ImGui::TableNextColumn();
    wrapped_dim(r.desc);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
