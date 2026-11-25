// 记录工具 (request_feature / request_tool / note_insight / report_issue) 的共用展示: 编号 / 类型标 / 附议 / 未落盘提示 + 各栏
#include "gui/task_agent/tools/Note.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <string_view>

namespace GUI::Agent {

using namespace ui;

void NoteRes::draw() const {
  if (!miss.empty()) {
    wrapped(color(TaskStatus::Kind::Error), miss);
    return;
  }
  if (kind == NoteKind::Issue && fields.contains("type")) {
    const std::string &t = fields.at("type").get_ref<const std::string &>();
    ImGui::TextColored(color(t == "bug" ? TaskStatus::Kind::Error : t == "idea" ? TaskStatus::Kind::Ready
                                                                                : TaskStatus::Kind::Warn),
                       "%s", t.c_str());
  }
  heading(std::string(def(kind).cn) + " " + id + (ref.empty() ? "" : " (附议 " + ref + ")"));
  if (dry)
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "工作台: 编号是预览, 没落盘");
  for (const ArgDef &a : doc(def(kind).tool).args) {
    const std::string_view k = a.key;
    if (k == "ref" || (kind == NoteKind::Issue && k == "type") || !fields.contains(a.key))
      continue;
    ImGui::TextColored(kColHead, "%s", a.cn);
    ImGui::Indent();
    wrapped(fields.at(a.key).get_ref<const std::string &>());
    ImGui::Unindent();
  }
}

} // namespace GUI::Agent
