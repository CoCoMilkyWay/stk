// submit_factor 的展示: 入库结局 + 候选的结局全文
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void SubmitRes::draw() const {
  heading("提交 " + name);
  wrapped(color(!why.empty() ? TaskStatus::Kind::Error : test || dry ? TaskStatus::Kind::Warn
                                                                     : TaskStatus::Kind::Ready),
          text());
  if (dry)
    wrapped_dim("工作台只查门槛, 不拷进正式库");
  if (!cand)
    return;
  wrapped(kColExpr, cand->tmpl);
  if (!cand->note.empty())
    wrapped_dim(cand->note);
  wrapped(Detail(*cand));
}

} // namespace GUI::Agent
