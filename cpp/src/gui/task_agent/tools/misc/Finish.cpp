// finish — 结束: 无参数 → FinishRes → 原文; 展示见 ui/misc/Finish.cpp
#include "gui/task_agent/tools/Misc.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string FinishRes::text() const { return "本条回复的调用做完后会话结束"; }

ToolOut Finish(const Call &, Seat &) { return Done(std::make_shared<FinishRes>()); }

json SampleFinish(Feed &) { return json::object(); }

} // namespace GUI::Agent
