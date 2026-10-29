// Task Agent — 见头文件
#include "agent/ui/TaskAgent.hpp"
#include "agent/Bridge.hpp"
#include "agent/Chat.hpp"
#include "agent/Config.hpp"
#include "agent/Prompt.hpp"

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace agent {

namespace {

namespace fs = std::filesystem;
using GUI::TaskStatus;

enum TabIdx {
  TAB_MINE = 0,
  TAB_CONFIG,
  TAB_LOG,
  TAB_COUNT
};

constexpr ImVec4 kColHead(0.4f, 0.8f, 1.0f, 1.0f);

struct State {
  std::unique_ptr<Bridge> bridge; // 首次 Draw 建 (Destroy 释放: 两个 worker 与常驻块)
  std::unique_ptr<Chat> chat;

  // 作用域选择 (下次 New 生效)
  std::string pool;
  int level = -1, hold = 0;

  // Config (下次 New / Eval 生效)
  int backend = 0;
  std::string model = kZhipuModels[0];
  int gpu = -1; // -1 = 未定 (有 GPU 默认 GPU)
  int prompt_idx = 0;
  std::string prompt_text;
  bool prompt_loaded = false;

  // 会话 (New 定)
  bool live = false;
  Scope sc;
  std::string dir; // kWorkDir/<池>/<层>/<持有期>
  Context ctx;
  std::string editor; // 下一条 user (可改)
  std::vector<Staged> batch;
  bool batch_pending = false; // 本批在评, 评完在 Update 里组反馈
  std::vector<Result> results;
  uint64_t seen_evals = 0;
  std::string message;
};

std::string factors_dir(const State &s) { return (fs::path(s.dir) / "factors").string(); }

const char *key_of(int backend) { return backend == static_cast<int>(llm::Backend::Zhipu) ? kZhipuKey : kCursorKey; }

std::string stamp() {
  const std::time_t now = std::time(nullptr);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", std::localtime(&now));
  return b;
}

// ---- std::string 输入框 (imgui 缓冲随串扩) ----
int resize_cb(ImGuiInputTextCallbackData *d) {
  if (d->EventFlag == ImGuiInputTextFlags_CallbackResize) {
    auto *s = static_cast<std::string *>(d->UserData);
    s->resize(static_cast<size_t>(d->BufTextLen));
    d->Buf = s->data();
  }
  return 0;
}
bool input_text(const char *id, std::string &s) {
  return ImGui::InputText(id, s.data(), s.capacity() + 1, ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}
bool input_multiline(const char *id, std::string &s, ImVec2 size) {
  return ImGui::InputTextMultiline(id, s.data(), s.capacity() + 1, size, ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}
// 只读多行 (可选中复制)
void text_box(const char *id, std::string s, ImVec2 size) { ImGui::InputTextMultiline(id, s.data(), s.size() + 1, size, ImGuiInputTextFlags_ReadOnly); }

void tip(const char *s) {
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    ImGui::SetTooltip("%s", s);
}

// ---- 会话 ----
void new_session(const SharedData &data, State &s) {
  s.sc = Scope{s.pool, s.level, s.hold};
  s.ctx = s.bridge->context(data, s.sc);
  s.dir = (fs::path(kWorkDir) / s.pool / s.ctx.level / s.ctx.hold).string();
  s.chat->open(llm::Options{static_cast<llm::Backend>(s.backend), s.model, SystemPrompt(s.ctx)}, (fs::path(s.dir) / "chat" / stamp()).string());
  s.results = s.bridge->results(data, s.sc, factors_dir(s));
  s.seen_evals = s.bridge->evals();
  s.editor = FirstMessage(s.ctx, s.results);
  s.batch.clear();
  s.batch_pending = false;
  s.message.clear();
  s.live = true;
}

// 末轮回复 → 候选落进会话目录 → 排队评估; 解析失败 → 编辑框填重发要求
void eval_reply(const SharedData &data, State &s) {
  std::string reply;
  {
    std::lock_guard<std::mutex> lock(s.chat->mutex);
    assert(!s.chat->turns.empty());
    reply = s.chat->turns.back().reply.text;
  }
  std::vector<Cand> cs;
  std::string err;
  s.message.clear();
  if (!ParseReply(reply, cs, err)) {
    s.editor = RetryMessage(err);
    return;
  }
  s.batch.clear();
  std::vector<std::string> files;
  for (Cand &c : cs) {
    Staged st{std::move(c), {}, {}};
    st.file = s.bridge->stage(data, s.sc, factors_dir(s), st.cand, st.err);
    if (!st.file.empty())
      files.push_back(st.file);
    s.batch.push_back(std::move(st));
  }
  if (!files.empty() && !s.bridge->eval(data, s.sc, factors_dir(s), files, s.gpu == 1)) {
    s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
    return;
  }
  s.batch_pending = true;
}

// ---- Mine ----
void scope_row(const SharedData &data, State &s, bool locked) {
  ImGui::TextColored(kColHead, "1. Scope:");
  ImGui::SameLine();
  ImGui::BeginDisabled(locked);
  ImGui::SetNextItemWidth(160);
  if (ImGui::BeginCombo("##pool", s.pool.c_str())) {
    for (const Choice &p : Bridge::pools()) {
      const std::string label = p.name + "  " + Bridge::pool_cn(p.name);
      if (ImGui::Selectable(label.c_str(), p.name == s.pool))
        s.pool = p.name;
    }
    ImGui::EndCombo();
  }
  const std::vector<Choice> lvs = Bridge::levels(s.pool);
  if (std::none_of(lvs.begin(), lvs.end(), [&](const Choice &c) { return c.key == s.level; }))
    s.level = lvs.front().key;
  for (const Choice &l : lvs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((l.name + "##lv").c_str(), s.level == l.key))
      s.level = l.key;
  }
  const std::vector<Choice> hs = s.bridge->holds(data, s.level);
  ImGui::SameLine();
  ImGui::TextDisabled("|  Hold");
  if (hs.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("(该层无可搜持有期: 标签未建)");
    ImGui::EndDisabled();
    return;
  }
  if (std::none_of(hs.begin(), hs.end(), [&](const Choice &c) { return c.key == s.hold; }))
    s.hold = hs.front().key;
  for (const Choice &h : hs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((h.name + "##hd").c_str(), s.hold == h.key))
      s.hold = h.key;
  }
  ImGui::EndDisabled();
  ImGui::TextDisabled("   %s", Bridge::hold_desc(s.hold, s.level).c_str());
}

void session_row(const SharedData &data, State &s) {
  Chat &chat = *s.chat;
  const EvalState es = s.bridge->state();
  const bool chat_busy = chat.busy();
  bool last_ok = false;
  int rounds = 0;
  {
    std::lock_guard<std::mutex> lock(chat.mutex);
    rounds = static_cast<int>(chat.turns.size());
    last_ok = rounds > 0 && chat.turns.back().reply.ok();
  }
  const bool has_hold = !s.bridge->holds(data, s.level).empty();
  const bool has_key = *key_of(s.backend) != '\0';

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !chat_busy && !es.busy && has_hold && has_key;
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0)))
    new_session(data, s);
  ImGui::EndDisabled();
  tip(has_key ? "按当前作用域与 Config 开新对话\n• 系统提示 = 特征叶 / 算子表 / 正式因子库\n• 首条 = 本作用域已试过的候选"
              : "Config 页所选后端的 key 未填 (agent/Config.hpp)");
  ImGui::SameLine();
  const bool can_ask = s.live && !chat_busy && !es.busy && !s.batch_pending && !s.editor.empty();
  ImGui::BeginDisabled(!can_ask);
  if (ImGui::Button("Ask", ImVec2(60, 0))) {
    chat.ask(s.editor);
    s.editor.clear();
  }
  ImGui::EndDisabled();
  tip("把编辑框里的消息发出");
  ImGui::SameLine();
  const bool can_eval = s.live && !chat_busy && !es.busy && !s.batch_pending && last_ok;
  ImGui::BeginDisabled(!can_eval);
  if (ImGui::Button("Eval", ImVec2(60, 0)))
    eval_reply(data, s);
  ImGui::EndDisabled();
  tip("解析末轮回复: 候选落进会话目录 → 逐个 Search → Run; 评完把反馈填进编辑框");
  ImGui::SameLine();
  ImGui::BeginDisabled(!chat_busy && !es.busy);
  if (ImGui::Button("Cancel", ImVec2(60, 0))) {
    if (chat_busy)
      chat.cancel();
    if (es.busy) {
      s.bridge->cancel();
      s.batch_pending = false;
    }
  }
  ImGui::EndDisabled();

  ImGui::SameLine();
  if (!s.live) {
    ImGui::TextDisabled("无会话");
  } else {
    ImGui::TextDisabled("%s / %s / %s · %s %s · %d 轮", s.sc.pool.c_str(), s.ctx.level.c_str(), s.ctx.hold.c_str(),
                        llm::kBackendNames[static_cast<int>(chat.options().backend)], chat.options().model.c_str(), rounds);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", s.dir.c_str());
  }
  if (chat_busy) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "asking");
  }
  if (es.busy) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "eval %d/%d %s %s %.0f%%", es.done, es.total, es.stage.c_str(), es.file.c_str(), es.frac * 100.0);
  }
  if (!es.message.empty() || !s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "| %s", (s.message.empty() ? es.message : s.message).c_str());
  }
}

void reply_panel(State &s) {
  Chat &chat = *s.chat;
  std::string text, thinking, head;
  {
    std::lock_guard<std::mutex> lock(chat.mutex);
    if (chat.busy()) {
      text = chat.live_text, thinking = chat.live_thinking;
      head = "回复 (收取中)";
    } else if (!chat.turns.empty()) {
      const llm::Reply &r = chat.turns.back().reply;
      text = r.text, thinking = r.thinking;
      char b[160];
      std::snprintf(b, sizeof(b), "回复 #%d: %s · %.0fs · 输入 %d (缓存 %d) / 输出 %d", static_cast<int>(chat.turns.size()), r.finish.c_str(), r.seconds,
                    r.usage.prompt, r.usage.cached, r.usage.completion);
      head = b;
    }
  }
  ImGui::TextDisabled("%s", head.empty() ? "回复" : head.c_str());
  if (!thinking.empty() && ImGui::TreeNode("思考")) {
    text_box("##thinking", std::move(thinking), ImVec2(-1, ImGui::GetTextLineHeight() * 12));
    ImGui::TreePop();
  }
  text_box("##reply", std::move(text), ImVec2(-1, -1));
}

void results_table(const SharedData &data, State &s) {
  ImGui::TextDisabled("候选 %zu (会话目录, 本持有期 score 降序)", s.results.size());
  constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                                     ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable;
  if (!ImGui::BeginTable("##results", 10, kFlags, ImVec2(-1, -1)))
    return;
  ImGui::TableSetupScrollFreeze(2, 1);
  for (const char *h : {"名", "中文", "结局", "score", "多空bp", "rIC", "IR", "t", "mono", "模板"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  for (const Result &r : s.results) {
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name.c_str());
    if (!r.note.empty() && ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", r.note.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name_cn.c_str());
    ImGui::TableNextColumn();
    const Weight *w = r.best();
    if (!r.error.empty()) {
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Error), "BROKEN");
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", r.error.c_str());
    } else if (!w) {
      ImGui::TextDisabled("%s", r.ws.empty() && r.searched ? "无显著" : "未评");
    } else {
      ImGui::PushID(r.file.c_str());
      if (ImGui::SmallButton("Adopt")) {
        std::string err;
        s.message = s.bridge->adopt(data, s.sc, factors_dir(s), r.file, err) ? "已采纳 " + r.file : err;
      }
      ImGui::PopID();
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("拷进正式因子目录 (含权重与统计); %zu 个权重\n%s", r.ws.size(), w->expr.c_str());
    }
    ImGui::TableNextColumn();
    if (w) {
      ImGui::Text("%.3g", w->score);
      ImGui::TableNextColumn();
      ImGui::Text("%.1f", w->ls * 1e4);
      ImGui::TableNextColumn();
      ImGui::Text("%.4f", w->ric);
      ImGui::TableNextColumn();
      ImGui::Text("%.2f", w->ir);
      ImGui::TableNextColumn();
      ImGui::Text("%.1f", w->t);
      ImGui::TableNextColumn();
      ImGui::Text("%.2f", w->mono);
    } else {
      for (int i = 0; i < 5; ++i)
        ImGui::TableNextColumn();
    }
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.tmpl.c_str());
  }
  ImGui::EndTable();
}

void render_mine(const SharedData &data, State &s) {
  const bool locked = s.chat->busy() || s.bridge->state().busy;
  scope_row(data, s, locked);
  session_row(data, s);
  ImGui::Separator();
  const float w = ImGui::GetContentRegionAvail().x;
  ImGui::BeginChild("##left", ImVec2(w * 0.5f, 0), false);
  const float h = ImGui::GetContentRegionAvail().y;
  ImGui::TextDisabled("下一条 (可改; Eval 评完自动填反馈)");
  input_multiline("##editor", s.editor, ImVec2(-1, h * 0.4f));
  reply_panel(s);
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##right", ImVec2(0, 0), false);
  results_table(data, s);
  ImGui::EndChild();
}

// ---- Config ----
void render_config(State &s) {
  ImGui::TextColored(kColHead, "LLM");
  ImGui::SetNextItemWidth(120);
  if (ImGui::Combo("后端", &s.backend, llm::kBackendNames, llm::kBackendCount))
    s.model = s.backend == static_cast<int>(llm::Backend::Zhipu) ? kZhipuModels[0] : kCursorModels[0];
  ImGui::SameLine();
  if (*key_of(s.backend))
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Ready), "key 已填");
  else
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "key 未填 (agent/Config.hpp)");
  ImGui::SetNextItemWidth(200);
  if (ImGui::BeginCombo("##model_preset", "预设")) {
    const bool zhipu = s.backend == static_cast<int>(llm::Backend::Zhipu);
    const auto list = [&](const char *const *ms, size_t n) {
      for (size_t i = 0; i < n; ++i)
        if (ImGui::Selectable(ms[i], s.model == ms[i]))
          s.model = ms[i];
    };
    if (zhipu)
      list(kZhipuModels, std::size(kZhipuModels));
    else
      list(kCursorModels, std::size(kCursorModels));
    ImGui::EndCombo();
  }
  ImGui::SameLine();
  ImGui::SetNextItemWidth(260);
  input_text("模型", s.model);
  ImGui::TextDisabled("下次 New 生效");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "评估");
  const bool gpu_ok = Bridge::gpu_available();
  if (!gpu_ok)
    s.gpu = 0;
  else if (s.gpu < 0)
    s.gpu = 1;
  ImGui::BeginDisabled(!gpu_ok);
  ImGui::RadioButton("CPU", &s.gpu, 0);
  ImGui::SameLine();
  ImGui::RadioButton("GPU", &s.gpu, 1);
  ImGui::EndDisabled();
  ImGui::TextDisabled("会话目录 %s/<池>/<层>/<持有期>", kWorkDir);

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "提示词");
  ImGui::SetNextItemWidth(120);
  if (ImGui::Combo("模板", &s.prompt_idx, kPrompts, static_cast<int>(std::size(kPrompts))) || !s.prompt_loaded) {
    s.prompt_text = LoadPrompt(kPrompts[s.prompt_idx]);
    s.prompt_loaded = true;
  }
  ImGui::SameLine();
  if (ImGui::Button("Reload"))
    s.prompt_text = LoadPrompt(kPrompts[s.prompt_idx]);
  ImGui::SameLine();
  if (ImGui::Button("Save"))
    SavePrompt(kPrompts[s.prompt_idx], s.prompt_text);
  ImGui::SameLine();
  ImGui::TextDisabled("%s/%s.md ({{键}} 占位; 下次 New / Eval 生效)", kPromptDir, kPrompts[s.prompt_idx]);
  input_multiline("##prompt", s.prompt_text, ImVec2(-1, -1));
}

// ---- Log ----
void render_log(State &s) {
  Chat &chat = *s.chat;
  if (!chat.opened()) {
    ImGui::TextDisabled("无会话");
    return;
  }
  ImGui::TextDisabled("%s", chat.log_dir().c_str());
  ImGui::BeginChild("##log", ImVec2(0, 0), false);
  std::lock_guard<std::mutex> lock(chat.mutex);
  for (size_t i = 0; i < chat.turns.size(); ++i) {
    const Turn &t = chat.turns[i];
    const llm::Reply &r = t.reply;
    char head[192];
    std::snprintf(head, sizeof(head), "#%zu  %s · %.0fs · 输入 %d (缓存 %d) / 输出 %d###turn%zu", i + 1, r.finish.c_str(), r.seconds, r.usage.prompt, r.usage.cached,
                  r.usage.completion, i);
    if (!ImGui::CollapsingHeader(head))
      continue;
    ImGui::PushID(static_cast<int>(i));
    if (ImGui::TreeNode("user")) {
      ImGui::TextWrapped("%s", t.user.c_str());
      ImGui::TreePop();
    }
    if (!r.thinking.empty() && ImGui::TreeNode("思考")) {
      ImGui::TextWrapped("%s", r.thinking.c_str());
      ImGui::TreePop();
    }
    ImGui::TextWrapped("%s", r.text.c_str());
    ImGui::PopID();
  }
  if (chat.busy())
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "收取中: %zu 字思考, %zu 字正文", chat.live_thinking.size(), chat.live_text.size());
  ImGui::EndChild();
}

} // namespace

GUI::TaskHandle CreateAgentTask() {
  auto state = std::make_shared<State>();

  GUI::TaskHandle handle;
  handle.name = "Agent";
  handle.storage = state;
  handle.tabs = {"Mine", "Config", "Log"};

  // 每帧 (无论是否选中): 推进评估队列; 评完的文件变了 → 重扫结果; 本批评完 → 组反馈进编辑框
  handle.Update = [state](SharedData &data) {
    State &s = *state;
    if (!s.bridge)
      return;
    s.bridge->update(data);
    if (!s.live)
      return;
    if (s.bridge->evals() != s.seen_evals) {
      s.seen_evals = s.bridge->evals();
      s.results = s.bridge->results(data, s.sc, factors_dir(s));
    }
    if (s.batch_pending && !s.bridge->state().busy) {
      s.batch_pending = false;
      s.editor = RoundMessage(s.ctx, s.batch, s.results);
    }
  };

  handle.Status = [state](const SharedData &, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    const State &s = *state;
    if (!s.bridge)
      return {};
    if (idx == TAB_CONFIG)
      return *key_of(s.backend) ? TaskStatus{} : TaskStatus{TaskStatus::Kind::Warn, "no key"};
    if (idx != TAB_MINE)
      return {};
    if (s.chat->busy())
      return {TaskStatus::Kind::Busy, "asking"};
    if (const EvalState es = s.bridge->state(); es.busy)
      return {TaskStatus::Kind::Busy, "eval " + std::to_string(es.done) + "/" + std::to_string(es.total)};
    return s.live ? TaskStatus{TaskStatus::Kind::Ready, "live"} : TaskStatus{};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    State &s = *state;
    if (!s.bridge) {
      s.bridge = std::make_unique<Bridge>();
      s.chat = std::make_unique<Chat>();
    }
    if (s.pool.empty())
      s.pool = Bridge::pools().front().name;
    ImGui::BeginChild("AgentTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_MINE:
      render_mine(data, s);
      break;
    case TAB_CONFIG:
      render_config(s);
      break;
    case TAB_LOG:
      render_log(s);
      break;
    default:
      break;
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    State &s = *state;
    if (s.chat)
      s.chat->cancel();
    if (s.bridge)
      s.bridge->cancel();
    s.chat.reset(); // 析构 join worker
    s.bridge.reset();
    s.live = false;
    s.batch_pending = false;
  };

  return handle;
}

} // namespace agent
