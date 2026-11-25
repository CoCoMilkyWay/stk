// probe_brain 的展示: 后端/模型/用时/token + 思考过程 + ParseCall 校验结果 + 发出的上下文
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
      heading("ParseCall 未通过 (ask=" + std::string(ask ? "yes" : "no") + ")");
      wrapped(color(TaskStatus::Kind::Error), check.error);
    } else {
      const ToolMeta &m = def(check.call.tool);
      heading("ParseCall 通过 · " + std::string(m.id) + " (" + m.cn + ") · " + def(m.exec).cn);
      wrapped(check.call.text());
    }
  }

  if (!prompt_text.empty())
    fold("prompt", ImGuiTreeNodeFlags_None, "发出的上下文 · " + chars(prompt_text), prompt_text, true, kBoxLines, false);
}

} // namespace GUI::Agent
