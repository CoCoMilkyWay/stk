// check_batch 的展示: 通过 / 整批不合协议结论 + 逐调用解析与预检表 (编号 / 工具 / 执行 / 参数 / 预检状态)
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void CheckBatchRes::draw() const {
  if (!error.empty()) {
    heading("整批不合协议 (一个都不执行, 下一条 = 重交要求)");
    wrapped(color(TaskStatus::Kind::Error), error);
    fold("retry", ImGuiTreeNodeFlags_DefaultOpen, "发给大脑的重发要求 · " + chars(retry_text), retry_text, false, kBoxLines, false);
    return;
  }
  heading("协议校验通过 · " + std::to_string(items.size()) + " 个调用");
  ImGui::TextDisabled("ask=%s", ask ? "yes" : "no");
  if (grid("##batch", {"编号", "工具", "执行", "预检", "调用"})) {
    for (const Item &x : items) {
      const ToolMeta &m = def(x.call.tool);
      ImGui::TableNextRow();
      cell(RefText(x.at));
      cell(std::string(m.id) + " (" + m.cn + ")");
      cell_dim(def(m.exec).cn);
      ImGui::TableNextColumn();
      if (x.pre_err.empty())
        ImGui::TextColored(color(TaskStatus::Kind::Ready), "可跑");
      else
        wrapped(color(TaskStatus::Kind::Warn), x.pre_err);
      ImGui::TableNextColumn();
      wrapped(x.call.text());
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
