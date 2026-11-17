// request_feature 的展示 (及四个记录工具共用的记录卡): 编号 / 附议 / 工作台未落盘提示 + 各栏
#include "gui/task_agent/tools/Note.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <string_view>

namespace GUI::Agent {

using namespace ui;

void DrawNoteCard(const NoteRes &r) {
  if (!r.miss.empty()) {
    wrapped(color(TaskStatus::Kind::Error), r.miss);
    return;
  }
  heading(std::string(def(r.kind).cn) + " " + r.id + (r.ref.empty() ? "" : " (附议 " + r.ref + ")"));
  if (r.dry)
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "工作台: 编号是预览, 没落盘");
  for (const ArgDef &a : def(def(r.kind).tool).args) {
    if (std::string_view(a.key) == "ref" || !r.fields.contains(a.key))
      continue;
    ImGui::TextColored(kColHead, "%s", a.cn);
    ImGui::Indent();
    wrapped(r.fields.at(a.key).get<std::string>());
    ImGui::Unindent();
  }
}

void FeatureNoteRes::draw() const { DrawNoteCard(*this); }

} // namespace GUI::Agent
