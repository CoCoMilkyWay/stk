// Task Agent — 见头文件
#include "gui/task_agent/TaskAgent.hpp"
#include "disk/Disk.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_agent/ui/Pages.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent::page {

// ---- 模型嗅探 ----
std::string first_model(const Ui &s, int backend) { return s.sniff.got[backend].list.empty() ? std::string() : s.sniff.got[backend].list.front().id; }
void sniff(Ui &s, int backend) { s.sniff.start(backend, net_of(s.opt, backend)); }
bool model_ok(const Ui &s) {
  const int b = s.opt.backend;
  return !s.sniff.busy(b) && s.sniff.got[b].error.empty() && !s.opt.model.empty(); // 按当前连接方式嗅探成功且已选
}

// 回测上 GPU (Config 页未定 = 有 GPU 即用)
bool use_gpu(const Ui &s) { return catalog::gpu() && s.opt.gpu != 0; }
// Tools 页有调用在跑 (含等研究员答)
bool bench_busy(const Ui &s) { return s.bench && s.bench->busy(); }

namespace {

// 子行: Mine | Tools (主栏 "Tools/<类>/<工具>", 按类相邻, 一工具一行) | Config
enum TabIdx {
  TAB_MINE = 0,
  TAB_TOOL0,
  TAB_CONFIG = TAB_TOOL0 + static_cast<int>(kNum<Tool>),
  TAB_COUNT
};
constexpr std::array<Tool, kNum<Tool>> kTabTools = [] {
  std::array<Tool, kNum<Tool>> a{};
  size_t n = 0;
  for (const Cat c : kVals<Cat>)
    for (const Tool t : kVals<Tool>)
      if (def(t).cat == c)
        a[n++] = t;
  return a;
}();
bool is_tool_tab(int idx) { return idx >= TAB_TOOL0 && idx < TAB_CONFIG; }
Tool tool_of(int idx) {
  assert(is_tool_tab(idx));
  return kTabTools[idx - TAB_TOOL0];
}
// 主栏子行名与各任务同为大驼峰: view_feature → ViewFeature
std::string pascal(std::string_view id) {
  std::string s;
  bool up = true;
  for (const char c : id) {
    if (c != '_')
      s += up ? static_cast<char>(std::toupper(static_cast<unsigned char>(c))) : c;
    up = c == '_';
  }
  return s;
}

// 收齐的嗅探结果进表; 嗅探成功而当前后端所选模型不在表里 → 取表首 (失败则留着所选, 重嗅成功再核)
void poll_sniff(Ui &s) {
  for (int b = 0; b < kBackendN; ++b) {
    if (!s.sniff.poll(b) || b != s.opt.backend || !s.sniff.got[b].error.empty())
      continue;
    const std::vector<llm::Model> &ms = s.sniff.got[b].list;
    if (std::none_of(ms.begin(), ms.end(), [&](const llm::Model &m) { return m.id == s.opt.model; }))
      s.opt.model = first_model(s, b);
  }
}

// ---- 选项落盘 (output/config/agent/ui.json) ----
// 无文件 = 默认值; 有文件即按字段清单读回 (形状或值域不合即终止, 删文件恢复默认)
void load_ui(Ui &s) {
  const bool saved = disk::exists(disk::AgentUiFile);
  if (saved)
    nlohmann::json::parse(disk::read(disk::AgentUiFile)).get_to(s.opt);
  const Options &o = s.opt;
  assert(o.backend >= 0 && o.backend < kBackendN && o.proxy_port > 0 && o.proxy_port < 65536 && o.gpu >= -1 && o.gpu <= 1 && o.prompt >= 0 &&
         o.prompt < static_cast<int>(kNum<Msg>) && o.sc.years >= 1 && o.budget.steps >= 0 && o.budget.minutes >= 0 && "ui.json 的值越界");
  if (!catalog::gpu())
    s.opt.gpu = 0;
  if (saved)
    s.opt_saved = s.opt;
}
void save_ui(Ui &s) {
  if (s.opt_saved && *s.opt_saved == s.opt)
    return;
  disk::write(disk::AgentUiFile, {}, nlohmann::ordered_json(s.opt).dump(2));
  s.opt_saved = s.opt;
}

// 首帧 Update (帧首全部任务先 Update 再 Draw): 建会话台与外部 agent 接口 (即监听), 读选项, 各后端嗅探模型
void ensure(Ui &s) {
  if (s.mine)
    return;
  s.mine = std::make_unique<Mine>();
  s.remote = std::make_unique<Remote>();
  load_ui(s);
  s.sniff.cancel.store(false, std::memory_order_relaxed);
  for (int b = 0; b < kBackendN; ++b)
    sniff(s, b);
}
} // namespace

} // namespace GUI::Agent::page

namespace GUI::Tasks {

TaskHandle CreateAgentTask() {
  using namespace Agent;
  using namespace Agent::page;
  auto state = std::make_shared<Ui>();

  TaskHandle handle;
  handle.name = "Agent";
  handle.storage = state;
  handle.tabs = {"Mine"};
  for (const Tool t : kTabTools)
    handle.tabs.push_back(std::string("Tools/") + def(def(t).cat).en + "/" + pascal(def(t).id));
  handle.tabs.push_back("Config");

  // 每帧 (无论是否选中): 收嗅探结果; 推进会话 (含评估队列与会话日志); 收工作台的后台结果; 外部 agent 的 MCP 请求交会话 (开会话的规则取 Config 此刻的)
  handle.Update = [state](SharedData &data) {
    Ui &s = *state;
    ensure(s);
    catalog::fit(data, s.opt.sc);
    poll_sniff(s);
    s.mine->update();
    if (s.bench)
      s.bench->update();
    s.remote->update(data, *s.mine, setup_of(s.opt), leg_of(s.opt, use_gpu(s)), bench_busy(s));
    follow(s);
    save_ui(s);
  };

  handle.Status = [state](const SharedData &, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    const Ui &s = *state;
    if (!s.mine)
      return {};
    if (idx == TAB_CONFIG) {
      const int b = s.opt.backend;
      if (!has_key(b))
        return {TaskStatus::Kind::Warn, "no key"};
      if (s.sniff.busy(b))
        return {TaskStatus::Kind::Busy, "sniffing"};
      if (!s.sniff.got[b].error.empty())
        return {TaskStatus::Kind::Error, "sniff failed"};
      return s.opt.model.empty() ? TaskStatus{TaskStatus::Kind::Warn, "no model"} : TaskStatus{};
    }
    if (is_tool_tab(idx))
      return s.bench && s.bench->running() == tool_of(idx) ? TaskStatus{TaskStatus::Kind::Busy, "running"} : TaskStatus{};
    if (idx != TAB_MINE)
      return {};
    const Mine &m = *s.mine;
    if (m.phase() == Phase::Choose)
      return {TaskStatus::Kind::Warn, "answer"};
    if (m.phase() == Phase::Think)
      return {TaskStatus::Kind::Busy, m.remote() ? "agent" : "asking"};
    if (m.phase() == Phase::Act)
      return {TaskStatus::Kind::Busy, StepTag(m.step()) + (m.acting() ? std::string(" ") + def(m.acting()->call.tool).id : std::string())};
    if (m.phase() == Phase::Pause)
      return {TaskStatus::Kind::Ready, "paused"};
    return {};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    Ui &s = *state;
    assert(s.mine && "帧首 Update 已建");
    ImGui::BeginChild("AgentTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_MINE:
      render_mine(data, s);
      break;
    case TAB_CONFIG:
      render_config(s);
      break;
    default:
      if (!s.bench)
        s.bench = std::make_unique<Bench>();
      s.bench->draw(data, tool_of(idx), s.mine->held() ? s.mine->setup.sc : s.opt.sc, s.opt.gate, use_gpu(s), s.mine->live() || !s.mine->idle());
      break;
    }
    ImGui::EndChild();
    save_ui(s);
  };

  handle.Destroy = [state]() {
    Ui &s = *state;
    s.sniff.quit();
    s.remote.reset(); // 析构: 停网络线程, 未答的请求随连接关掉
    s.bench.reset();
    s.mine.reset(); // 析构: 对话线程取消并 join, 评估 worker Stop
    s.seen_phase = Phase::None;
  };

  return handle;
}

} // namespace GUI::Tasks
