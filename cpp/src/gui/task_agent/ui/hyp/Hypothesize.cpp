// hypothesize 的展示: 编号 + 机制假设的各栏 (谁在做什么 / 为什么 / 预期 / 算落空)
#include "gui/task_agent/tools/Hyp.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void HypRes::draw() const {
  heading(id + " (待检验)");
  for (const ArgDef &a : doc(Tool::Hypothesize).args) {
    if (!fields.contains(a.key))
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
