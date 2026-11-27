// Config 页 — 见 gui/task_agent/TaskAgent.hpp
#include "disk/Disk.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/ui/Pages.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace GUI::Agent::page {

using namespace ui;

namespace {

std::string model_label(const llm::Model &m) { return m.name == m.id ? m.id : m.name + "  (" + m.id + ")"; }

} // namespace

void render_config(Ui &s) {
  Options &o = s.opt;
  ImGui::TextColored(kColHead, "大脑");
  ImGui::SetNextItemWidth(120);
  if (ImGui::Combo("后端", &o.backend, [](void *, int i) { return kBackends[i].name; }, nullptr, kBackendN))
    o.model = first_model(s, o.backend);
  ImGui::SameLine();
  if (has_key(o.backend))
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "key 已填");
  else
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "key 未填 (output/config/agent/keys.json)");
  ImGui::BeginDisabled(s.sniff.any());
  int via = o.via_proxy[o.backend];
  bool toggled = ImGui::RadioButton("直连", &via, 0);
  ImGui::SameLine();
  toggled |= ImGui::RadioButton("代理", &via, 1);
  if (toggled && static_cast<bool>(via) != o.via_proxy[o.backend]) {
    o.via_proxy[o.backend] = via;
    sniff(s, o.backend);
  }
  ImGui::SameLine();
  ImGui::SetNextItemWidth(140);
  input_text("##proxy_host", o.proxy_host);
  bool addr = ImGui::IsItemDeactivatedAfterEdit();
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  ImGui::InputInt("HTTP 代理##proxy_port", &o.proxy_port, 0, 0);
  o.proxy_port = std::clamp(o.proxy_port, 1, 65535);
  addr |= ImGui::IsItemDeactivatedAfterEdit();
  if (addr)
    for (int b = 0; b < kBackendN; ++b)
      if (o.via_proxy[b])
        sniff(s, b);
  ImGui::EndDisabled();
  tip("各后端分别选直连 / 代理, 代理地址共用 (HTTPS 后端 CONNECT, Devin 子进程经 HTTPS_PROXY); 改了即重新嗅探模型");
  const std::vector<llm::Model> &ms = s.sniff.got[o.backend].list;
  const std::string &sniff_err = s.sniff.got[o.backend].error;
  const auto cur = std::find_if(ms.begin(), ms.end(), [&](const llm::Model &m) { return m.id == o.model; });
  ImGui::SetNextItemWidth(320);
  ImGui::BeginDisabled(ms.empty());
  if (ImGui::BeginCombo("模型", cur == ms.end() ? o.model.c_str() : model_label(*cur).c_str())) {
    for (const llm::Model &m : ms)
      if (ImGui::Selectable(model_label(m).c_str(), m.id == o.model))
        o.model = m.id;
    ImGui::EndCombo();
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (s.sniff.busy(o.backend)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "嗅探中");
  } else if (has_key(o.backend)) {
    if (ImGui::SmallButton("重新嗅探"))
      sniff(s, o.backend);
    ImGui::SameLine();
    if (!sniff_err.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "嗅探失败: %s", sniff_err.c_str());
    else if (ms.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "该后端无可用模型");
    else
      ImGui::TextDisabled("%zu 个可用", ms.size());
  }
  ImGui::TextDisabled("下次 New / 继续 生效 (内置会话继续时换了后端或模型, 对话历史照时间线重开); 外部 agent 经本机 MCP 接口 %s 接入", s.remote->listen().c_str());

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "评估");
  const bool gpu_ok = catalog::gpu();
  if (!gpu_ok)
    o.gpu = 0;
  else if (o.gpu < 0)
    o.gpu = 1;
  ImGui::BeginDisabled(!gpu_ok);
  ImGui::RadioButton("CPU", &o.gpu, 0);
  ImGui::SameLine();
  ImGui::RadioButton("GPU", &o.gpu, 1);
  ImGui::EndDisabled();
  ImGui::TextDisabled("会话目录 %s/<池>/<层>/<持有期>; 下次 New / 继续 生效", disk::path(disk::AgentSessions, {}).c_str());

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "预算");
  ImGui::TextDisabled("每段 (New / 继续 起) 的上限: 到了本段即停下等你 (按 继续 开启下一段); 0 = 不限; 下次 New / 继续 生效");
  const auto limit = [](const char *label, int &v) {
    ImGui::SetNextItemWidth(120);
    ImGui::InputInt(label, &v, 1, 10);
    v = std::max(v, 0);
  };
  limit("步##budget_steps", o.budget.steps);
  ImGui::SameLine();
  limit("分钟##budget_minutes", o.budget.minutes);

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "入库门槛");
  ImGui::TextDisabled("agent 提交的候选至少一个权重达到才进正式因子库; 下次开会话生效 (New / 外部 agent 的 open)");
  ImGui::SetNextItemWidth(120);
  ImGui::InputDouble((std::string(kGateName) + " >=").c_str(), &o.gate.min, 0.0, 0.0, "%.3f");
  tip("全期 mono = 评估期 20 组每期超额均值对组号的 Spearman (Search 的目标是它的滚动窗版: 中位窗 mono)");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "提问");
  ImGui::Checkbox("允许 agent 向你提问 (ask_human)", &o.ask);
  tip("开: agent 可在关键分岔向你提问 (每题带推荐), 等你选答后接着做\n关: 工具表里没有 ask_human, agent 自定方向, 全自动挖掘");
  ImGui::SameLine();
  ImGui::TextDisabled("下次开会话生效 (New / 外部 agent 的 open)");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "提示词");
  ImGui::SetNextItemWidth(120);
  const bool switched = ImGui::Combo("模板", &o.prompt, [](void *, int i) { return kMsgs[i].id; }, nullptr, static_cast<int>(kNum<Msg>));
  const Msg msg = static_cast<Msg>(o.prompt);
  if (switched || !s.prompt_loaded) {
    s.prompt_text = LoadPrompt(msg);
    s.prompt_loaded = true;
  }
  ImGui::SameLine();
  if (ImGui::Button("Reload"))
    s.prompt_text = LoadPrompt(msg);
  ImGui::SameLine();
  if (ImGui::Button("Save"))
    SavePrompt(msg, s.prompt_text);
  ImGui::SameLine();
  ImGui::TextDisabled("%s ({{键}} 占位; 保存后, 下次组消息生效)", disk::path(disk::PromptFile, {.name = def(msg).id}).c_str());
  input_multiline("##prompt", s.prompt_text, ImVec2(-1, -1));
}

} // namespace GUI::Agent::page
