// finish 的展示
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void FinishRes::draw() const { wrapped(color(TaskStatus::Kind::Ready), text() + " (工作台里不结束什么)"); }

} // namespace GUI::Agent
