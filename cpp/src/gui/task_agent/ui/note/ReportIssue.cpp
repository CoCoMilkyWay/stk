// report_issue 的展示: 类型 (bug / idea / question) 着色 + 记录卡
#include "gui/task_agent/tools/Note.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void IssueNoteRes::draw() const {
  if (fields.contains("type")) {
    const std::string t = fields.at("type").get<std::string>();
    ImGui::TextColored(color(t == "bug" ? TaskStatus::Kind::Error : t == "idea" ? TaskStatus::Kind::Ready
                                                                                : TaskStatus::Kind::Warn),
                       "%s", t.c_str());
  }
  DrawNoteCard(*this);
}

} // namespace GUI::Agent
