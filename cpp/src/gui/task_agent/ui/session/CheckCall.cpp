// check_call 的展示: 通过 / 不合协议结论 + 解析出的调用与预检 (编号 / 工具 / 执行 / 参数 / 预检状态)
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void CheckCallRes::draw() const {
  if (!error.empty()) {
    heading("不合协议 (不执行, 下一条 = 重交要求)");
    wrapped(color(TaskStatus::Kind::Error), error);
    fold("retry", ImGuiTreeNodeFlags_DefaultOpen, "发给大脑的重交要求 · " + chars(retry_text), retry_text, false, kBoxLines, false);
    return;
  }
  heading("协议校验通过");
  ImGui::TextDisabled("ask=%s", ask ? "yes" : "no");
  if (grid("##call", {"编号", "工具", "执行", "预检", "调用"})) {
    const ToolMeta &m = def(call.tool);
    ImGui::TableNextRow();
    cell(StepTag(step));
    cell(std::string(m.id) + " (" + m.cn + ")");
    cell_dim(def(m.exec).cn);
    ImGui::TableNextColumn();
    if (pre_err.empty())
      ImGui::TextColored(color(TaskStatus::Kind::Ready), "可跑");
    else
      wrapped(color(TaskStatus::Kind::Warn), pre_err);
    ImGui::TableNextColumn();
    wrapped(call.text());
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
