// search_docs 的展示: 命中概况 + 阶段 → 流派树 + 机构 / 年份 / 价值 + 研报表 (结论与片段)
#include "gui/task_agent/tools/Doc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

namespace {
void ranks(const char *id, const Ranks &v) {
  if (!grid(id, {"", "篇"}))
    return;
  for (const auto &[k, n] : v) {
    ImGui::TableNextRow();
    cell(k), cell(std::to_string(n));
  }
  ImGui::EndTable();
}
} // namespace

void DocsRes::draw() const {
  heading("命中 " + std::to_string(hit) + " 篇 " + cond);
  if (loose || !hit)
    wrapped(color(TaskStatus::Kind::Warn), "各条件单独命中: " + alone);
  if (!hit)
    return;
  if (ImGui::TreeNodeEx("阶段 → 流派", ImGuiTreeNodeFlags_DefaultOpen)) {
    for (const Stage &s : tree)
      if (ImGui::TreeNode((s.name + " (" + std::to_string(s.n) + ")").c_str())) {
        for (const auto &[a, n] : s.sub)
          ImGui::BulletText("%s  %d", a.c_str(), n);
        ImGui::TreePop();
      }
    ImGui::TreePop();
  }
  if (ImGui::BeginTable("##facets", 3, ImGuiTableFlags_SizingStretchSame)) {
    ImGui::TableNextColumn(), ImGui::TextDisabled("机构"), ranks("##brokers", brokers);
    ImGui::TableNextColumn(), ImGui::TextDisabled("年份"), ranks("##years", years);
    ImGui::TableNextColumn(), ImGui::TextDisabled("价值"), ranks("##values", values);
    ImGui::EndTable();
  }
  heading("研报 (前 " + std::to_string(docs.size()) + " 篇)");
  if (!begin_table("##docs", {"id", "价值", "主阶段", "因子", "命中", "流派 / 结论 / 片段"}, 0.0f))
    return;
  for (const Doc &d : docs) {
    ImGui::TableNextRow();
    cell(d.id);
    ImGui::TableNextColumn();
    ImGui::TextColored(color(d.value == "high" ? TaskStatus::Kind::Ready : d.value == "medium" ? TaskStatus::Kind::None
                                                                                               : TaskStatus::Kind::Muted),
                       "%s", d.value.c_str());
    cell(d.primary), cell(std::to_string(d.factors)), full ? cell(std::to_string(d.hits)) : cell_dim("—");
    ImGui::TableNextColumn();
    wrapped_dim(d.approaches);
    if (!d.finding.empty())
      wrapped(d.finding);
    for (const std::string &s : d.snippets)
      wrapped(kColExpr, s);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
