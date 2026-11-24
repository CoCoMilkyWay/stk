// judge 的展示: 判定 (或证据不成的原因) + 证据核对表 + 各栏
#include "gui/task_agent/tools/Hyp.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <string_view>

namespace GUI::Agent {

using namespace ui;

void JudgeRes::draw() const {
  heading(hyp + (title.empty() ? std::string() : " " + title));
  if (!miss.empty())
    wrapped(color(TaskStatus::Kind::Error), miss + " (判定没改)");
  else
    ImGui::TextColored(color(stance == Stance::Supported ? TaskStatus::Kind::Ready : stance == Stance::Refuted ? TaskStatus::Kind::Error
                                                                                                               : TaskStatus::Kind::Warn),
                       "判 %s · 变体 %d", def(stance).cn, evals);
  if (!evidence.empty()) {
    heading("证据");
    if (grid("##ev", {"编号", "调用", "观察", "做成", "关键数"})) {
      for (const Ev &e : evidence) {
        ImGui::TableNextRow();
        cell(e.ref);
        if (e.call.empty()) {
          cell_dim("(对不上做过的调用)"), cell(""), cell(""), cell("");
          continue;
        }
        cell(e.call), cell(e.look ? "是" : ""), cell(e.ok ? "是" : "否"), cell_dim(e.gist);
      }
      ImGui::EndTable();
    }
  }
  for (const ArgDef &a : doc(Tool::Judge).args) {
    const std::string_view k = a.key;
    if (k == "hyp" || k == "verdict" || k == "evidence" || !fields.contains(a.key))
      continue;
    const std::string &v = fields.at(a.key).get_ref<const std::string &>();
    if (v.empty())
      continue;
    ImGui::TextColored(kColHead, "%s", a.cn);
    ImGui::Indent();
    wrapped(v);
    ImGui::Unindent();
  }
}

} // namespace GUI::Agent
