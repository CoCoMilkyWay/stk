// build_context 的展示: 步与账本标志 + 各段字数表 + 分段折叠 (系统提示 / 首条 / 上一条回复 / 本条消息 / 下一步)
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <iterator>
#include <utility>

namespace GUI::Agent {

using namespace ui;

void BuildContextRes::draw() const {
  heading("上下文构成 · 第 " + std::to_string(step) + " 步 · kind=" + kind + " · view=" + view + " · Turns=" + std::to_string(turns));
  const std::string fold_text = folded > 1 ? "第 2–" + std::to_string(folded) + " 条消息折成结论" : std::string("无");
  ImGui::TextDisabled("done=%s · 折叠: %s", done ? "true" : "false", fold_text.c_str());
  if (grid("##ctx_parts", {"段", "字数", "说明"})) {
    const std::pair<const char *, const std::string *> rows[] = {
        {"System", &system}, {"First", &first}, {"上一条回复", &prev_reply}, {"本条增量", &current}, {"下一步", &next_step}};
    const char *notes[] = {"静态系统提示 (会话内不变)", "首条消息", "上一步大脑交的调用 JSON", "有状态的大脑 / 外部 agent 连着时收到的", "ResultMessage 动态生成"};
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
  if (!prev_reply.empty())
    fold("prev", ImGuiTreeNodeFlags_None, "上一条回复 · " + chars(prev_reply), prev_reply, true, kBoxLines, false);
  if (!current.empty())
    fold("cur", ImGuiTreeNodeFlags_None, "本条增量 · " + chars(current), current, false, kBoxLines, false);
  if (!first.empty())
    fold("first", ImGuiTreeNodeFlags_None, "首条消息 · " + chars(first), first, true, kBoxLines, false);
  if (!system.empty())
    fold("sys", ImGuiTreeNodeFlags_None, "系统提示 · " + chars(system), system, true, kBoxLines, false);
}

} // namespace GUI::Agent
