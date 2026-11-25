// step_state 的展示: 账本与续跑判定 Cont + 预算与回退损失 + 停下报告折叠
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void StepStateRes::draw() const {
  heading("状态机快照 · 第 " + std::to_string(round) + " 轮 · 调用 " + std::to_string(cur) + "/" + std::to_string(acts));
  ImGui::TextColored(color(TaskStatus::Kind::Ready), "Cont::%s (%s) — %s", cont_id.c_str(), cont_cn.c_str(), hint.c_str());
  ImGui::TextDisabled("done=%s · 假设 %zu · 调用 %d (失败 %d) · 候选 %d · 入库 %d · token %s", flags.done ? "true" : "false", hyps, tally.all(), tally.failed(),
                      tally.staged(), tally.adopted, kilo(tally.usage.prompt + tally.usage.completion).c_str());
  if (!budget_miss.empty())
    wrapped(color(TaskStatus::Kind::Warn), "预算已满: " + budget_miss);

  heading("回退到第 " + std::to_string(rewind_k) + " 轮的损失预估");
  ImGui::Text("丢弃 %d 轮 · 候选文件 %d 个 · 全局记录 %d 条 (已入正式库不动)", loss_rounds, loss_files, loss_notes);

  if (!report.empty())
    fold("report", action == "report" ? ImGuiTreeNodeFlags_DefaultOpen : ImGuiTreeNodeFlags_None, "停下报告 · " + chars(report), report, false, kBoxLines, false);
}

} // namespace GUI::Agent
