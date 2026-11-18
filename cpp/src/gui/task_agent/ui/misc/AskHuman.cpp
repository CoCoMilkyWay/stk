// ask_human 的展示: 各题与所选 (只读)
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void AskRes::draw() const { draw_asked(asked); }

} // namespace GUI::Agent
