// report_issue — 报问题: 输入 type / title / detail / ref → IssueNoteRes (编号或 ref 不成的原因) → 原文; 展示见 ui/note/ReportIssue.cpp
#include "gui/task_agent/tools/Note.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

ToolOut ReportIssue(const Call &c, Seat &s) { return FileNote(c, s); }

json SampleReportIssue(Feed &p) {
  static const char *kTypes[] = {"bug", "idea", "question"};
  return {{"type", kTypes[p.below(3)]}, {"title", "示例: 某个结果不好读"}, {"detail", "现在怎样、改成怎样、为什么"}, {"ref", ""}};
}

} // namespace GUI::Agent
