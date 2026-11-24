// probe_brain 的展示: 后端/模型/用时/token + 思考过程 + VetBatch 校验结果表 + 发出的上下文
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void ProbeBrainRes::draw() const {
  heading(std::string(def(opt.backend).name) + " / " + (opt.model.empty() ? std::string("(未选模型)") : opt.model) +
          (opt.net.proxy ? " · 代理 " + opt.net.host + ":" + std::to_string(opt.net.port) : " · 直连"));
  if (!error.empty()) {
    wrapped(color(TaskStatus::Kind::Error), error);
  } else {
    std::string meta = "结局 " + std::string(def(reply.end).id) + " · 用时 " + dur(reply.seconds) + " · Turns " + std::to_string(turns) +
                       " · System " + kilo(static_cast<double>(system_chars)) + "B";
    if (reply.usage.prompt > 0)
      meta += " · token 输入 " + kilo(reply.usage.prompt) + " (缓存 " + kilo(reply.usage.cached) + ") / 输出 " + kilo(reply.usage.completion);
    ImGui::TextColored(color(reply.ok() ? TaskStatus::Kind::Ready : TaskStatus::Kind::Error), "%s", meta.c_str());
  }

  if (!reply.thinking.empty())
    fold("think", ImGuiTreeNodeFlags_None, "思考过程 · " + chars(reply.thinking), reply.thinking, true, kThinkLines, false);

  if (reply.ok()) {
    if (!check.ok()) {
      heading("VetBatch 未通过 (ask=" + std::string(ask ? "yes" : "no") + ")");
      wrapped(color(TaskStatus::Kind::Error), check.error);
    } else {
      heading("VetBatch 通过 · " + std::to_string(check.calls.size()) + " 个调用");
      if (grid("##probe_calls", {"#", "工具", "执行", "调用"})) {
        for (size_t i = 0; i < check.calls.size(); ++i) {
          const Call &c = check.calls[i];
          const ToolMeta &m = def(c.tool);
          ImGui::TableNextRow();
          cell(std::to_string(i + 1));
          cell(std::string(m.id) + " (" + m.cn + ")");
          cell_dim(def(m.exec).cn);
          ImGui::TableNextColumn();
          wrapped(c.text());
        }
        ImGui::EndTable();
      }
    }
  }

  if (!prompt_text.empty())
    fold("prompt", ImGuiTreeNodeFlags_None, "发出的上下文 · " + chars(prompt_text), prompt_text, true, kBoxLines, false);
}

} // namespace GUI::Agent
