// read_doc 的展示: 标注卡 (阶段 / 结论 / 引用 / 全部因子) · 目录表 · 原文一段
#include "gui/task_agent/tools/Doc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void ReadRes::draw() const {
  heading(id + " · " + part);
  if (part == "outline") {
    ImGui::Text("全文 %zu 字 · %zu 节", chars, total);
    if (grid("##secs", {"节", "标题", "字数"})) {
      for (const Sec &x : secs) {
        ImGui::TableNextRow();
        cell(std::to_string(x.k)), cell(x.title), cell(std::to_string(x.chars));
      }
      ImGui::EndTable();
    }
    return;
  }
  if (part == "text") {
    ImGui::Text("第 %zu 节%s起%s", sec, skip ? (" 第 " + std::to_string(skip) + " 字").c_str() : "",
                end ? " · 全文完" : (" · 未完: 接着读 from=" + std::to_string(next_sec) + " skip=" + std::to_string(next_skip)).c_str());
    box(body, false, 60.0f, false);
    return;
  }
  if (from == 0) {
    wrapped_dim(meta);
    ImGui::SeparatorText("涉及阶段");
    for (const std::string &s : stages)
      ImGui::BulletText("%s", s.c_str());
    ImGui::SeparatorText(("结论 (" + std::to_string(findings.size()) + ")").c_str());
    for (size_t i = 0; i < findings.size(); ++i)
      wrapped(std::to_string(i + 1) + ". " + findings[i]);
    if (!pre_in.empty() || !pre_out.empty() || !cited.empty()) {
      ImGui::SeparatorText("引用");
      for (const std::string &s : pre_in)
        ImGui::BulletText("前作 (库内) %s", s.c_str());
      for (const std::string &s : pre_out)
        ImGui::BulletText("前作 (库外) %s", s.c_str());
      for (const std::string &s : cited)
        ImGui::BulletText("被引 %s", s.c_str());
    }
  }
  ImGui::SeparatorText(("因子 (共 " + std::to_string(total) + "; 原文按字数上限列到哪算哪, 这里列全)").c_str());
  for (const Factor &f : factors) {
    wrapped(std::to_string(f.no) + ". " + f.head);
    ImGui::Indent();
    wrapped(kColExpr, f.formula);
    wrapped_dim(f.evidence);
    ImGui::Unindent();
  }
}

} // namespace GUI::Agent
