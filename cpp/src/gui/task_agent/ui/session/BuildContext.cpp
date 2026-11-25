// build_context 的展示: 轮次与账本标志 + 各段字数表 + 分段折叠 (系统提示 / 首条 / 研究树 / 上一批回复 / 本条消息 / 下一步)
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <iterator>
#include <utility>

namespace GUI::Agent {

using namespace ui;

void BuildContextRes::draw() const {
  heading("上下文构成 · 第 " + std::to_string(round) + " 轮 · kind=" + kind + " · view=" + view + " · Turns=" + std::to_string(turns));
  ImGui::TextDisabled("done=%s", done ? "true" : "false");
  if (grid("##ctx_parts", {"段", "字数", "说明"})) {
    const std::pair<const char *, const std::string *> rows[] = {
        {"System", &system}, {"First", &first}, {"研究树", &tree}, {"上一批回复", &prev_reply}, {"本条增量", &current}, {"下一步", &next_step}};
    const char *notes[] = {"静态系统提示 (会话内不变)", "首条消息", "前 N-1 轮折叠 (TurnsOf)", "上轮模型回复 JSON", "外部 agent 常规收到的增量", "RoundMessage 动态生成"};
    for (size_t i = 0; i < std::size(rows); ++i) {
      ImGui::TableNextRow();
      cell(rows[i].first);
      cell(rows[i].second->empty() ? "—" : chars(*rows[i].second));
      cell_dim(notes[i]);
    }
    ImGui::EndTable();
  }
  if (!next_step.empty())
    fold("next", ImGuiTreeNodeFlags_DefaultOpen, "下一步指导语 · " + chars(next_step), next_step, false, kBoxLines, false);
  if (!tree.empty())
    fold("tree", ImGuiTreeNodeFlags_None, "研究树 · " + chars(tree), tree, false, kBoxLines, false);
  if (!prev_reply.empty())
    fold("prev", ImGuiTreeNodeFlags_None, "上一批回复 · " + chars(prev_reply), prev_reply, true, kBoxLines, false);
  if (!current.empty())
    fold("cur", ImGuiTreeNodeFlags_None, "本条增量 · " + chars(current), current, false, kBoxLines, false);
  if (!first.empty())
    fold("first", ImGuiTreeNodeFlags_None, "首条消息 · " + chars(first), first, true, kBoxLines, false);
  if (!system.empty())
    fold("sys", ImGuiTreeNodeFlags_None, "系统提示 · " + chars(system), system, true, kBoxLines, false);
}

} // namespace GUI::Agent
