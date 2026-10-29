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
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
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

// 会话阶段 (AGENT.md 状态图): 澄清 = Digest (agent 消化提问) → Choose (人选答案); 挖掘 = Mine; 结束 = Report
enum class Phase : uint8_t { None,
                             Digest,
                             Choose,
                             Mine,
                             Report };

struct Submit {
  int round = 0;
  Submitted s;
};
struct Issue {
  int round = 0;
  std::string text;
};

struct State {
  std::unique_ptr<Bridge> bridge; // 首次 Draw 建 (Destroy 释放: 两个 worker 与常驻块)
  std::unique_ptr<Chat> chat;

  // 作用域选择 (下次 New 生效)
  std::string pool;
  int level = -1, hold = 0;

  // Config (下次 New 生效)
  int backend = 0;
  std::string model = kZhipuModels[0];
  int gpu = -1; // -1 = 未定 (有 GPU 默认 GPU)
  Gate gate{kGateScore, kGateT, kGateMono, kGateValid};
  int prompt_idx = 0;
  std::string prompt_text;
  bool prompt_loaded = false;

  // 会话 (New 定)
  Phase phase = Phase::None;
  Scope sc;
  Gate sgate;
  bool sgpu = false;
  std::string dir; // kWorkDir/<池>/<层>/<持有期>
  Context ctx;
  uint64_t seen_turns = 0; // 已处理到的 chat.done()
  bool stop = false;       // 按了 Stop: 本步做完即结束
  // 澄清
  std::vector<Question> questions;
  std::vector<std::vector<bool>> picks;
  std::string note;
  // 挖掘
  std::vector<Staged> batch;
  bool batch_pending = false;       // 本批在评, 评完在 Update 里发反馈
  std::vector<Submitted> submitted; // 上一条回复的提交结局, 进下一条反馈
  std::vector<Submit> submits;      // 全会话
  std::vector<Issue> issues;
  std::set<std::string> adopted; // 本会话入库的英文名
  std::vector<Result> results;
  uint64_t seen_evals = 0;
  // 结束
  std::string reason, report;
  std::string message;
};

bool live(const State &s) { return s.phase == Phase::Digest || s.phase == Phase::Choose || s.phase == Phase::Mine; }

std::string factors_dir(const State &s) { return (fs::path(s.dir) / "factors").string(); }

const char *key_of(int backend) { return backend == static_cast<int>(llm::Backend::Zhipu) ? kZhipuKey : kCursorKey; }

std::string stamp() {
  const std::time_t now = std::time(nullptr);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", std::localtime(&now));
  return b;
}

int rounds_of(Chat &chat) {
  std::lock_guard<std::mutex> lock(chat.mutex);
  return static_cast<int>(chat.turns.size());
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
std::string num(double v) {
  char b[32];
  std::snprintf(b, sizeof(b), "%.4g", v);
  return b;
}

// 发一条 user 并记流水; kind = first / answer / round / retry
void ask(State &s, const char *kind, std::string text) {
  s.chat->event("ask", {{"round", std::to_string(rounds_of(*s.chat) + 1)}, {"kind", kind}, {"chars", std::to_string(text.size())}});
  s.chat->ask(std::move(text));
}

// 一个候选文件的结局 (流水用)
std::string brief(const Result &r) {
  if (!r.error.empty())
    return "BROKEN: " + r.error;
  const Weight *w = r.best();
  if (!w)
    return r.ws.empty() && r.searched ? "无显著" : "未评估";
  return std::to_string(r.ws.size()) + " 个权重, 最佳 score " + num(w->score) + " ls " + num(w->ls * 1e4) + "bp rIC " + num(w->ric) + " t " + num(w->t) + " mono " +
         num(w->mono) + " 有效率 " + num(w->valid);
}

void new_session(const SharedData &data, State &s) {
  s.sc = Scope{s.pool, s.level, s.hold};
  s.sgate = s.gate;
  s.sgpu = s.gpu == 1;
  s.ctx = s.bridge->context(data, s.sc);
  s.dir = (fs::path(kWorkDir) / s.pool / s.ctx.level / s.ctx.hold).string();
  s.chat->open(llm::Options{static_cast<llm::Backend>(s.backend), s.model, SystemPrompt(s.ctx, s.sgate)}, (fs::path(s.dir) / "chat" / stamp()).string());
  s.results = s.bridge->results(data, s.sc, factors_dir(s));
  s.seen_evals = s.bridge->evals();
  s.seen_turns = s.chat->done();
  s.stop = false;
  s.questions.clear(), s.picks.clear(), s.note.clear();
  s.batch.clear(), s.submitted.clear(), s.submits.clear(), s.issues.clear(), s.adopted.clear();
  s.batch_pending = false;
  s.reason.clear(), s.report.clear(), s.message.clear();
  s.phase = Phase::Digest;
  s.chat->event("open", {{"pool", s.sc.pool},
                         {"level", s.ctx.level},
                         {"hold", s.ctx.hold},
                         {"backend", llm::kBackendNames[s.backend]},
                         {"model", s.model},
                         {"eval", s.sgpu ? "GPU" : "CPU"},
                         {"gate", "score " + num(s.sgate.score) + " t " + num(s.sgate.t) + " mono " + num(s.sgate.mono) + " valid " + num(s.sgate.valid)},
                         {"tried", std::to_string(s.results.size())}});
  ask(s, "first", FirstMessage(s.ctx, s.results));
}

std::string report_of(const State &s, int rounds) {
  std::string r = "# 结束报告\n\n- 结束原因: " + s.reason + "\n- 轮数: " + std::to_string(rounds) + "\n\n";
  std::string in, out;
  int n_in = 0, n_out = 0;
  for (const Submit &x : s.submits) {
    const std::string line = "- 第 " + std::to_string(x.round) + " 轮 " + x.s.name + (x.s.err.empty() ? "" : ": " + x.s.err) + "\n";
    (x.s.err.empty() ? in : out) += line;
    ++(x.s.err.empty() ? n_in : n_out);
  }
  r += "## 入库 (" + std::to_string(n_in) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(n_out) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(s.issues.size()) + ")\n\n";
  for (const Issue &x : s.issues)
    r += "- 第 " + std::to_string(x.round) + " 轮: " + x.text + "\n";
  if (s.issues.empty())
    r += "(无)\n";
  return r;
}

// 结束: 在跑的回复 / 评估作废, 出报告落进会话日志
void finish(State &s, std::string reason) {
  assert(live(s));
  if (s.chat->busy())
    s.chat->cancel();
  s.bridge->cancel();
  s.batch_pending = false;
  s.reason = std::move(reason);
  s.report = report_of(s, rounds_of(*s.chat));
  s.chat->event("finish", {{"reason", s.reason}, {"rounds", std::to_string(rounds_of(*s.chat))}, {"adopted", std::to_string(s.adopted.size())},
                           {"issues", std::to_string(s.issues.size())}});
  std::ofstream f(fs::path(s.chat->log_dir()) / "report.md", std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << s.report;
  s.phase = Phase::Report;
}

// 提交一个已评候选: 会话目录里有 → 达门槛 → 拷进正式因子库
Submitted submit_one(const SharedData &data, State &s, const std::string &name) {
  Submitted x{name, {}};
  const auto it = std::find_if(s.results.begin(), s.results.end(), [&](const Result &r) { return r.name == name; });
  if (it == s.results.end()) {
    x.err = "会话目录里没有这个候选";
    return x;
  }
  x.err = GateMiss(s.sgate, *it);
  if (x.err.empty() && s.bridge->adopt(data, s.sc, factors_dir(s), it->file, x.err))
    s.adopted.insert(name);
  return x;
}

void on_digest(State &s, const std::string &text, int round) {
  std::string err;
  if (!ParseQuestions(text, s.questions, err)) {
    s.chat->event("parse_fail", {{"round", std::to_string(round)}, {"phase", "digest"}, {"error", err}});
    return s.stop ? finish(s, "按 Stop") : ask(s, "retry", RetryMessage(err));
  }
  s.chat->event("questions", {{"round", std::to_string(round)}, {"n", std::to_string(s.questions.size())}});
  s.picks.clear();
  for (const Question &q : s.questions)
    s.picks.emplace_back(q.options.size(), false);
  s.phase = Phase::Choose;
  if (s.stop)
    finish(s, "按 Stop");
}

void on_mine(const SharedData &data, State &s, const std::string &text, int round) {
  MineReply m;
  std::string err;
  if (!ParseMine(text, m, err)) {
    s.chat->event("parse_fail", {{"round", std::to_string(round)}, {"phase", "mine"}, {"error", err}});
    return s.stop ? finish(s, "按 Stop") : ask(s, "retry", RetryMessage(err));
  }
  s.chat->event("mine_reply", {{"round", std::to_string(round)},
                               {"factors", std::to_string(m.factors.size())},
                               {"submit", std::to_string(m.submit.size())},
                               {"issues", std::to_string(m.issues.size())},
                               {"done", m.done ? "true" : "false"}});
  for (std::string &x : m.issues) {
    s.chat->event("issue", {{"round", std::to_string(round)}, {"text", x}});
    s.issues.push_back({round, std::move(x)});
  }
  s.submitted.clear();
  for (const std::string &name : m.submit) {
    s.submitted.push_back(submit_one(data, s, name));
    s.submits.push_back({round, s.submitted.back()});
    s.chat->event("submit", {{"round", std::to_string(round)}, {"name", name}, {"result", s.submitted.back().err.empty() ? "入库" : s.submitted.back().err}});
  }
  if (m.done)
    return finish(s, "agent 宣布挖完");
  if (s.stop)
    return finish(s, "按 Stop");
  s.batch.clear();
  std::vector<std::string> files;
  for (Cand &c : m.factors) {
    Staged st{std::move(c), {}, {}};
    st.file = s.bridge->stage(data, s.sc, factors_dir(s), st.cand, st.err);
    if (!st.file.empty())
      files.push_back(st.file);
    s.chat->event("stage", {{"round", std::to_string(round)}, {"name", st.cand.name}, {"expr", st.cand.expr}, {"note", st.cand.note},
                            {"result", st.err.empty() ? "落文件 " + st.file : "未落文件: " + st.err}});
    s.batch.push_back(std::move(st));
  }
  s.results = s.bridge->results(data, s.sc, factors_dir(s));
  s.chat->event("eval_begin", {{"round", std::to_string(round)}, {"files", std::to_string(files.size())}});
  s.bridge->eval(data, s.sc, factors_dir(s), files, s.sgpu);
  s.batch_pending = true;
}

// 每帧推进: 评完发反馈; 收齐一轮按阶段处理
void step(const SharedData &data, State &s) {
  if (s.phase != Phase::Digest && s.phase != Phase::Mine)
    return;
  if (s.batch_pending) {
    if (s.bridge->state().busy)
      return;
    s.batch_pending = false;
    for (const Staged &st : s.batch) {
      if (st.file.empty())
        continue;
      const auto it = std::find_if(s.results.begin(), s.results.end(), [&](const Result &r) { return r.file == st.file; });
      assert(it != s.results.end());
      s.chat->event("eval", {{"name", st.cand.name}, {"result", brief(*it)}});
    }
    s.chat->event("eval_end");
    if (s.stop)
      return finish(s, "按 Stop");
    ask(s, "round", RoundMessage(s.ctx, s.submitted, s.batch, s.results));
    return;
  }
  if (s.chat->busy() || s.chat->done() == s.seen_turns)
    return;
  s.seen_turns = s.chat->done();
  llm::Reply r;
  int round = 0;
  {
    std::lock_guard<std::mutex> lock(s.chat->mutex);
    assert(!s.chat->turns.empty());
    r = s.chat->turns.back().reply;
    round = static_cast<int>(s.chat->turns.size());
  }
  s.chat->event("reply", {{"round", std::to_string(round)},
                          {"finish", r.finish},
                          {"seconds", num(r.seconds)},
                          {"prompt_tokens", std::to_string(r.usage.prompt)},
                          {"cached_tokens", std::to_string(r.usage.cached)},
                          {"completion_tokens", std::to_string(r.usage.completion)},
                          {"chars", std::to_string(r.text.size())},
                          {"thinking_chars", std::to_string(r.thinking.size())}});
  if (!r.ok())
    return finish(s, "回复失败 (" + r.finish + ")");
  if (s.phase == Phase::Digest)
    on_digest(s, r.text, round);
  else
    on_mine(data, s, r.text, round);
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
  const bool has_hold = !s.bridge->holds(data, s.level).empty();
  const bool has_key = *key_of(s.backend) != '\0';

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !live(s) && !chat_busy && !es.busy && has_key && has_hold;
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0))) {
    if (s.bridge->ready(data, Scope{s.pool, s.level, s.hold}, s.gpu == 1))
      new_session(data, s);
    else
      s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
  }
  ImGui::EndDisabled();
  tip(has_key ? "按当前作用域与 Config 开新会话: 打包规则发给 agent, 它先提问\n资产轴与标签就绪才开得成" : "Config 页所选后端的 key 未填 (agent/Config.hpp)");
  ImGui::SameLine();
  ImGui::BeginDisabled(s.phase != Phase::Choose);
  if (ImGui::Button("Start", ImVec2(60, 0))) {
    for (size_t i = 0; i < s.questions.size(); ++i) {
      std::string sel;
      for (size_t j = 0; j < s.questions[i].options.size(); ++j)
        if (s.picks[i][j])
          sel += (sel.empty() ? "" : "; ") + s.questions[i].options[j];
      chat.event("pick", {{"question", s.questions[i].prompt}, {"picked", sel}});
    }
    chat.event("start", {{"note", s.note}});
    ask(s, "answer", AnswerMessage(s.questions, s.picks, s.note));
    s.phase = Phase::Mine;
  }
  ImGui::EndDisabled();
  tip("把选好的答案与附言发给 agent, 开始全自动挖掘");
  ImGui::SameLine();
  ImGui::BeginDisabled(!live(s) || s.stop);
  if (ImGui::Button("Stop", ImVec2(60, 0))) {
    chat.event("stop");
    s.stop = true;
    if (s.phase == Phase::Choose)
      finish(s, "按 Stop");
  }
  ImGui::EndDisabled();
  tip("当前这步做完再结束");
  ImGui::SameLine();
  ImGui::BeginDisabled(!live(s));
  if (ImGui::Button("Cancel", ImVec2(60, 0))) {
    chat.event("cancel");
    finish(s, "按 Cancel");
  }
  ImGui::EndDisabled();
  tip("立即结束, 在跑的回复 / 回测作废");

  ImGui::SameLine();
  static constexpr const char *kPhaseNames[] = {"无会话", "澄清", "选答案", "挖掘", "已结束"};
  ImGui::TextDisabled("%s", kPhaseNames[static_cast<int>(s.phase)]);
  if (s.phase != Phase::None) {
    ImGui::SameLine();
    ImGui::TextDisabled("· %s / %s / %s · %s %s · %d 轮 · 入库 %zu", s.sc.pool.c_str(), s.ctx.level.c_str(), s.ctx.hold.c_str(),
                        llm::kBackendNames[static_cast<int>(chat.options().backend)], chat.options().model.c_str(), rounds_of(chat), s.adopted.size());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", chat.log_dir().c_str());
  }
  if (chat_busy) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "asking");
  }
  if (es.busy) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "eval %d/%d %s %s %.0f%%", es.done, es.total, es.stage.c_str(), es.file.c_str(), es.frac * 100.0);
  }
  if (s.stop && live(s)) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "stopping");
  }
  if (!es.message.empty() || !s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "| %s", (s.message.empty() ? es.message : s.message).c_str());
  }
}

// 澄清: 逐题点选 + 附言
void questions_panel(State &s) {
  ImGui::TextDisabled("agent 的问题 (%zu); 选完按 Start", s.questions.size());
  ImGui::BeginChild("##questions", ImVec2(-1, ImGui::GetContentRegionAvail().y * 0.6f), true);
  for (size_t i = 0; i < s.questions.size(); ++i) {
    const Question &q = s.questions[i];
    ImGui::PushID(static_cast<int>(i));
    ImGui::TextWrapped("%zu. %s%s", i + 1, q.prompt.c_str(), q.multiple ? " (可多选)" : "");
    for (size_t j = 0; j < q.options.size(); ++j) {
      ImGui::PushID(static_cast<int>(j));
      bool on = s.picks[i][j];
      if (q.multiple ? ImGui::Checkbox(q.options[j].c_str(), &on) : ImGui::RadioButton(q.options[j].c_str(), on)) {
        if (!q.multiple) {
          s.picks[i].assign(q.options.size(), false);
          on = true;
        }
        s.picks[i][j] = on;
      }
      ImGui::PopID();
    }
    ImGui::Spacing();
    ImGui::PopID();
  }
  ImGui::EndChild();
  ImGui::TextDisabled("附言 (可空)");
  input_multiline("##note", s.note, ImVec2(-1, -1));
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

void results_table(State &s) {
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
    } else if (s.adopted.count(r.name)) {
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Ready), "入库");
    } else {
      ImGui::TextUnformatted("有权重");
    }
    if (w && ImGui::IsItemHovered())
      ImGui::SetTooltip("%zu 个权重\n%s", r.ws.size(), w->expr.c_str());
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
  scope_row(data, s, live(s));
  session_row(data, s);
  ImGui::Separator();
  const float w = ImGui::GetContentRegionAvail().x;
  ImGui::BeginChild("##left", ImVec2(w * 0.5f, 0), false);
  if (s.phase == Phase::Choose) {
    questions_panel(s);
  } else {
    if (s.phase == Phase::Report)
      text_box("##report", s.report, ImVec2(-1, ImGui::GetContentRegionAvail().y * 0.4f));
    reply_panel(s);
  }
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##right", ImVec2(0, 0), false);
  results_table(s);
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
  ImGui::TextDisabled("会话目录 %s/<池>/<层>/<持有期>; 下次 New 生效", kWorkDir);

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "入库门槛");
  ImGui::TextDisabled("agent 提交的候选至少一个权重全部达到才进正式因子库; 下次 New 生效");
  const auto field = [](const char *label, double &v, const char *fmt) {
    ImGui::SetNextItemWidth(120);
    ImGui::InputDouble(label, &v, 0.0, 0.0, fmt);
  };
  field("score >=", s.gate.score, "%.3g");
  ImGui::SameLine();
  field("t >=", s.gate.t, "%.2f");
  ImGui::SameLine();
  field("mono >=", s.gate.mono, "%.2f");
  ImGui::SameLine();
  field("有效率 >=", s.gate.valid, "%.2f");

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
  ImGui::TextDisabled("%s/%s.md ({{键}} 占位; 下次组消息生效)", kPromptDir, kPrompts[s.prompt_idx]);
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

  // 每帧 (无论是否选中): 推进评估队列; 评完的文件变了 → 重扫结果; 推进会话
  handle.Update = [state](SharedData &data) {
    State &s = *state;
    if (!s.bridge)
      return;
    s.bridge->update(data);
    if (s.phase == Phase::None)
      return;
    if (s.bridge->evals() != s.seen_evals) {
      s.seen_evals = s.bridge->evals();
      s.results = s.bridge->results(data, s.sc, factors_dir(s));
    }
    step(data, s);
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
    if (s.phase == Phase::Choose)
      return {TaskStatus::Kind::Warn, "selecting"};
    if (s.chat->busy())
      return {TaskStatus::Kind::Busy, "asking"};
    if (const EvalState es = s.bridge->state(); es.busy)
      return {TaskStatus::Kind::Busy, "eval " + std::to_string(es.done) + "/" + std::to_string(es.total)};
    if (s.phase == Phase::Report)
      return {TaskStatus::Kind::Ready, "done"};
    return {};
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
    s.phase = Phase::None;
    s.batch_pending = false;
  };

  return handle;
}

} // namespace agent
