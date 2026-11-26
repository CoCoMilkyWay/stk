// list_tried 的展示: 候选表 (结局 / 最佳权重统计 / 是否达门槛 / 模板; 悬浮看结局全文)
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void TriedRes::draw() const {
  heading("已试候选 " + (query.empty() ? std::string() : "· query " + query + " 命中 ") + std::to_string(all) + " 个 · 这里第 " + std::to_string(skip + 1) + " 个起 " + std::to_string(rows.size()) + " 个");
  if (rows.empty() || !begin_table("##tried", {"名", "中文", "结局", "中位 rIC", "每期毛多空 bp", "每期净多空 bp", "年换手", "门槛", "模板"}, 0.0f))
    return;
  for (const Result &r : rows) {
    const Weight *w = r.best();
    ImGui::TableNextRow();
    cell(r.name), cell(r.name_cn), cell(def(r.verdict).cn);
    tip_long(Detail(r));
    cell_num(w ? w->score : NAN, "%.4f", true), cell_num(w ? w->gross * 1e4 : NAN, "%+.2f", true), cell_num(w ? w->net * 1e4 : NAN, "%+.2f", true),
        cell_num(w ? w->turn : NAN, "%.0f");
    ImGui::TableNextColumn();
    if (r.verdict == Verdict::Weights) {
      const std::string miss = GateMiss(gate, r);
      ImGui::TextColored(color(miss.empty() ? TaskStatus::Kind::Ready : TaskStatus::Kind::Muted), "%s", miss.empty() ? "达" : "未达");
      if (!miss.empty())
        tip_long(miss);
    }
    ImGui::TableNextColumn();
    wrapped(kColExpr, r.tmpl);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
