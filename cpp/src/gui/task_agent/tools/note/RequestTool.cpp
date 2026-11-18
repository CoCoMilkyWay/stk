// request_tool — 申请工具: 输入 title / want / purpose / gap / ref → ToolNoteRes (编号或 ref 不成的原因) → 原文; 展示见 ui/note/RequestTool.cpp
#include "gui/task_agent/tools/Note.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

ToolOut RequestTool(const Call &c, Seat &s) { return FileNote(c, s); }

json SampleRequestTool(Feed &) {
  return {{"title", "示例: 看因子的年换手与净值"},
          {"want", "给 factor, 看年换手、扣成本后的多空净值、按金额段的冲击"},
          {"purpose", "判断低换手要求下哪个变体更好"},
          {"gap", "eval_factor 只给年换手一个数, 看不到成本后的结果"},
          {"ref", ""}};
}

} // namespace GUI::Agent
