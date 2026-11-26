// 观察的展示: 与原文同一组节 (LookRes::secs) 逐节画 —— 日段提示 → 标题 → 各节 (节名 → 概况行 → 图 → 表)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <string>

namespace GUI::Agent {

using namespace ui;

void LookRes::draw() const {
  if (!period.empty())
    wrapped(color(TaskStatus::Kind::Warn), period);
  std::string h = headline();
  std::erase(h, '`');
  heading(h);
  const std::vector<Sec> ss = secs();
  for (size_t i = 0; i < ss.size(); ++i) {
    const Sec &x = ss[i];
    ImGui::PushID(static_cast<int>(i));
    if (!x.title.empty()) {
      std::string t = x.title;
      std::erase(t, '`');
      heading(t);
    }
    for (const std::string &l : x.lines)
      x.warn ? wrapped(color(TaskStatus::Kind::Warn), l) : wrapped(l);
    if (x.plot >= 0)
      plot(x.plot);
    if (grid("##tab", x.tab.head)) {
      for (size_t r = 0; r < x.tab.rows.size(); ++r) {
        ImGui::TableNextRow();
        if (static_cast<int>(r) == x.tab.mark)
          ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, ImGui::GetColorU32(ImGuiCol_TableHeaderBg));
        for (const Num &c : x.tab.rows[r])
          cell_tint(c.s, c.tint);
      }
      ImGui::EndTable();
    }
    ImGui::PopID();
  }
}

} // namespace GUI::Agent
