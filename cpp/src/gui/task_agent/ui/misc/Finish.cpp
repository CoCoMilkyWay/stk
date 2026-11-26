// finish 的展示: 提交结局 + 候选的结局全文 + 结束
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void FinishRes::draw() const {
  if (!name.empty())
    heading("提交 " + name);
  const TaskStatus::Kind kind = !ended || !why.empty() ? TaskStatus::Kind::Error : test || dry ? TaskStatus::Kind::Warn
                                                                                               : TaskStatus::Kind::Ready;
  wrapped(color(kind), text());
  if (dry)
    wrapped_dim("工作台只查门槛不拷进正式库, 也不结束什么");
  if (!cand)
    return;
  wrapped(kColExpr, cand->tmpl);
  if (!cand->note.empty())
    wrapped_dim(cand->note);
  wrapped(Detail(*cand));
}

} // namespace GUI::Agent
