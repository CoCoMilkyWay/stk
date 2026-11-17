// hypothesize 的展示: 编号 + 机制假设的各栏 (谁在做什么 / 为什么 / 预期 / 算落空)
#include "gui/task_agent/tools/Hyp.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void HypRes::draw() const {
  heading(id + " (待检验)");
  for (const ArgDef &a : def(Tool::Hypothesize).args) {
    if (!fields.contains(a.key))
      continue;
    ImGui::TextColored(kColHead, "%s", a.cn);
    ImGui::Indent();
    wrapped(fields.at(a.key).get<std::string>());
    ImGui::Unindent();
  }
}

} // namespace GUI::Agent
