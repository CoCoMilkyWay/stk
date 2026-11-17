// request_tool 的展示: 记录卡
#include "gui/task_agent/tools/Note.hpp"

namespace GUI::Agent {

void ToolNoteRes::draw() const { DrawNoteCard(*this); }

} // namespace GUI::Agent
