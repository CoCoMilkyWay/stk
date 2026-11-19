// list_library 的展示: 正式因子表 (最佳权重统计 / 换手 / 思路 / 模板)
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void LibraryRes::draw() const {
  heading("正式库 " + std::to_string(all) + " 个 · 这里第 " + std::to_string(skip + 1) + " 个起 " + std::to_string(rows.size()) + " 个" +
          (none ? " · 另 " + std::to_string(none) + " 个本持有期无统计" : std::string()));
  if (rows.empty() || !begin_table("##lib", {"名", "中文", "中位 rIC", "rIC", "mono", "多空 bp", "年换手", "思路 / 模板"}, 0.0f))
    return;
  for (const Result &r : rows) {
    const Weight &w = *r.best();
    ImGui::TableNextRow();
    cell(r.name), cell(r.name_cn);
    cell_num(w.roll.ric_med, "%.4f", true), cell_num(w.st.ric, "%.4f", true), cell_num(w.st.mono, "%.3f", true), cell_num(w.st.ls * 1e4, "%+.2f", true),
        cell_num(w.turn, "%.0f");
    ImGui::TableNextColumn();
    if (!r.note.empty())
      wrapped_dim(r.note);
    wrapped(kColExpr, r.tmpl);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
