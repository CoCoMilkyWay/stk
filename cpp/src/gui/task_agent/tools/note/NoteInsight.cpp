// note_insight — 记发现: 输入 title / evidence / reach / use / ref → NoteRes (编号或 ref 不成的原因) → 原文; 展示见 ui/note/Note.cpp
#include "gui/task_agent/tools/Note.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

ToolOut NoteInsight(const Call &c, Seat &s) { return FileNote(c, s); }

json SampleNoteInsight(Feed &p) {
  const std::vector<std::string> refs = p.refs(1);
  return {{"title", "示例: 尾盘信号在次日开盘前兑现完"},
          {"evidence", (refs.empty() ? StepTag(1) : refs.front()) + " view_response 的 T+1 收盘点与当日收盘点持平"},
          {"reach", "本作用域; 其他持有期没验证"},
          {"use", "尾盘类因子不宜配长持有期"},
          {"ref", ""}};
}

} // namespace GUI::Agent
