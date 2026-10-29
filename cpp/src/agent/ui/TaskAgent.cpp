// Task Agent — 见头文件
#include "agent/ui/TaskAgent.hpp"
#include "agent/Config.hpp"
#include "agent/Mine.hpp"

#include "imgui.h"
#include "misc/fs.hpp"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace agent {

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, mono, ric)

namespace {

using GUI::TaskStatus;
using Phase = Mine::Phase;

enum TabIdx {
  TAB_MINE = 0,
  TAB_CONFIG,
  TAB_LOG,
  TAB_COUNT
};

constexpr ImVec4 kColHead(0.4f, 0.8f, 1.0f, 1.0f);

bool has_key(int backend) { return !llm::key(static_cast<llm::Backend>(backend)).empty(); }

// Mine / Config 页的选项 (kUiFile 读写同一份字段清单): 作用域 / 连接 / 模型 / 评估后端 / 入库门槛 (下次 New 生效) + Config 页在看的模板
struct Options {
  std::string pool;
  int level = -1, hold = 0;
  int backend = 0;
  std::string model; // 该后端嗅探表里的 id; 空 = 还没嗅探到
  std::array<bool, llm::kBackendCount> via_proxy = [] {
    std::array<bool, llm::kBackendCount> a{};
    for (int b = 0; b < llm::kBackendCount; ++b)
      a[b] = kBackends[b].proxy;
    return a;
  }(); // 每后端直连 / 代理, 代理地址共用
  std::string proxy_host = kProxyHost;
  int proxy_port = kProxyPort;
  int gpu = -1; // -1 = 未定 (有 GPU 默认 GPU)
  Gate gate{kGateMono, kGateRic};
  int prompt = 0; // Msg 下标
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, pool, level, hold, backend, model, via_proxy, proxy_host, proxy_port, gpu, gate, prompt)

llm::Net net_of(const Options &o, int backend) { return {o.via_proxy[backend], o.proxy_host, o.proxy_port}; }

// 模型嗅探: 每后端一个后台 GET models (按该后端当前连接方式); cancel 须声明在 jobs 前 (future 析构 join 时它还活着)
struct Sniffer {
  std::atomic<bool> cancel{false};
  std::array<std::future<llm::Models>, llm::kBackendCount> jobs;
  std::array<llm::Models, llm::kBackendCount> got;

  bool busy(int b) const { return jobs[b].valid(); }
  bool any() const {
    return std::any_of(jobs.begin(), jobs.end(), [](const auto &f) { return f.valid(); });
  }
  // 须不在嗅探 (赋值新 future 会先 join 被替换的那个); 无 key 不嗅
  void start(int b, const llm::Net &net) {
    assert(!busy(b));
    got[b] = {};
    if (!has_key(b))
      return;
    if (net.proxy && net.host.empty()) {
      got[b].error = "代理地址为空";
      return;
    }
    jobs[b] = std::async(std::launch::async, [this, b, net] { return llm::models(static_cast<llm::Backend>(b), net, cancel); });
  }
  // 收齐 → 结果进 got, true
  bool poll(int b) {
    if (!busy(b) || jobs[b].wait_for(std::chrono::seconds(0)) != std::future_status::ready)
      return false;
    got[b] = jobs[b].get();
    return true;
  }
  void quit() {
    cancel.store(true, std::memory_order_relaxed);
    for (std::future<llm::Models> &f : jobs)
      f = {}; // 释放即 join
  }
};

struct State {
  std::unique_ptr<Mine> mine; // 首次 Draw 建 (Destroy 释放: 对话线程 / 评估 worker 与常驻块)
  Options opt;
  std::string opt_saved; // kUiFile 当前内容; 选项序列化后与之不同才写
  Sniffer sniff;
  // Config 页提示词编辑
  std::string prompt_text;
  bool prompt_loaded = false;
  // Mine 页
  ImGuiTextFilter leaf_filter, op_filter;
  Phase seen_phase = Phase::None; // 刚进 Choose: 左栏切到问题页一次
  std::string message;
};

// ---- 模型嗅探 ----
std::string first_model(const State &s, int backend) { return s.sniff.got[backend].list.empty() ? std::string() : s.sniff.got[backend].list.front().id; }
void sniff(State &s, int backend) { s.sniff.start(backend, net_of(s.opt, backend)); }
// 收齐的嗅探结果进表; 嗅探成功而当前后端所选模型不在表里 → 取表首 (失败则留着所选, 重嗅成功再核)
void poll_sniff(State &s) {
  for (int b = 0; b < llm::kBackendCount; ++b) {
    if (!s.sniff.poll(b) || b != s.opt.backend || !s.sniff.got[b].error.empty())
      continue;
    const std::vector<llm::Model> &ms = s.sniff.got[b].list;
    if (std::none_of(ms.begin(), ms.end(), [&](const llm::Model &m) { return m.id == s.opt.model; }))
      s.opt.model = first_model(s, b);
  }
}
std::string model_label(const llm::Model &m) { return m.name == m.id ? m.id : m.name + "  (" + m.id + ")"; }
bool model_ok(const State &s) {
  const int b = s.opt.backend;
  return !s.sniff.busy(b) && s.sniff.got[b].error.empty() && !s.opt.model.empty(); // 按当前连接方式嗅探成功且已选
}

// ---- 选项落盘 (kUiFile) ----
// 池 / 层 / 持有期 / 模型失效由各自的选择逻辑回落; GPU 不在了回落 CPU
void load_ui(State &s) {
  std::ifstream f(kUiFile);
  if (f.is_open()) {
    const nlohmann::json j = nlohmann::json::parse(f, nullptr, false);
    assert(!j.is_discarded() && j.is_object() && "kUiFile 须是 JSON 对象");
    j.get_to(s.opt);
  }
  const Options &o = s.opt;
  assert(o.backend >= 0 && o.backend < llm::kBackendCount);
  assert(o.proxy_port > 0 && o.proxy_port < 65536);
  assert(o.gpu >= -1 && o.gpu <= 1);
  assert(o.prompt >= 0 && o.prompt < static_cast<int>(std::size(kPrompts)));
  if (!catalog::gpu())
    s.opt.gpu = 0;
  s.opt_saved = nlohmann::ordered_json(s.opt).dump(2);
}
void save_ui(State &s) {
  std::string text = nlohmann::ordered_json(s.opt).dump(2);
  if (text == s.opt_saved)
    return;
  misc::atomic_write(kUiFile, text.data(), text.size());
  s.opt_saved = std::move(text);
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
bool input_text(const char *id, std::string &s, const char *hint = nullptr) {
  return ImGui::InputTextWithHint(id, hint, s.data(), s.capacity() + 1, ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}
bool input_multiline(const char *id, std::string &s, ImVec2 size) {
  return ImGui::InputTextMultiline(id, s.data(), s.capacity() + 1, size, ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}
// 只读多行 (可选中复制; ReadOnly 不写缓冲, 原串直接给)
void text_box(const char *id, const std::string &s, ImVec2 size) {
  ImGui::InputTextMultiline(id, const_cast<char *>(s.c_str()), s.size() + 1, size, ImGuiInputTextFlags_ReadOnly);
}

void tip(const char *s) {
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    ImGui::SetTooltip("%s", s);
}

std::string reply_head(const llm::Reply &r) {
  char b[160];
  std::snprintf(b, sizeof(b), "%s · %.0fs · 输入 %d (缓存 %d) / 输出 %d", llm::kEndNames[static_cast<int>(r.end)], r.seconds, r.usage.prompt, r.usage.cached,
                r.usage.completion);
  return b;
}

// ---- Mine ----
void scope_row(const SharedData &data, State &s, bool locked) {
  Options &o = s.opt;
  ImGui::TextColored(kColHead, "1. Scope:");
  ImGui::SameLine();
  ImGui::BeginDisabled(locked);
  ImGui::SetNextItemWidth(160);
  if (ImGui::BeginCombo("##pool", o.pool.c_str())) {
    for (const Choice &p : catalog::pools()) {
      const std::string label = p.name + "  " + catalog::pool_cn(p.name);
      if (ImGui::Selectable(label.c_str(), p.name == o.pool))
        o.pool = p.name;
    }
    ImGui::EndCombo();
  }
  const std::vector<Choice> lvs = catalog::levels(o.pool);
  if (std::none_of(lvs.begin(), lvs.end(), [&](const Choice &c) { return c.key == o.level; }))
    o.level = lvs.front().key;
  for (const Choice &l : lvs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((l.name + "##lv").c_str(), o.level == l.key))
      o.level = l.key;
  }
  const std::vector<Choice> hs = catalog::holds(data, o.level);
  ImGui::SameLine();
  ImGui::TextDisabled("|  Hold");
  if (hs.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("(该层无可搜持有期: 标签未建)");
    ImGui::EndDisabled();
    return;
  }
  if (std::none_of(hs.begin(), hs.end(), [&](const Choice &c) { return c.key == o.hold; }))
    o.hold = hs.front().key;
  for (const Choice &h : hs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((h.name + "##hd").c_str(), o.hold == h.key))
      o.hold = h.key;
  }
  ImGui::EndDisabled();
  ImGui::TextDisabled("   %s", catalog::hold_desc(o.hold, o.level).c_str());
}

void session_row(const SharedData &data, State &s) {
  Mine &m = *s.mine;
  const Options &o = s.opt;
  const EvalState es = m.lab.state();
  const bool chat_busy = m.chat.busy();
  const bool has_hold = !catalog::holds(data, o.level).empty();
  const bool keyed = has_key(o.backend);

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !m.live() && !chat_busy && es.step == Step::Idle && keyed && model_ok(s) && has_hold;
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0))) {
    const Scope sc{o.pool, o.level, o.hold};
    if (catalog::ready(data, sc, o.gpu == 1)) {
      s.message.clear();
      m.open(data, {sc, o.gate, o.gpu == 1, {static_cast<llm::Backend>(o.backend), net_of(o, o.backend), o.model, {}}});
    } else {
      s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
    }
  }
  ImGui::EndDisabled();
  tip(!keyed         ? "所选后端的 key 未填 (config/agent/keys.json)"
      : !model_ok(s) ? "模型未就绪 (Config 页: 嗅探中 / 嗅探失败 / 无可用模型)"
                     : "按当前作用域与 Config 开新会话: 打包规则发给 agent, 它先提问\n资产轴与标签就绪才开得成");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Choose);
  if (ImGui::Button("Start", ImVec2(60, 0)))
    m.start();
  ImGui::EndDisabled();
  tip("把选好的答案与附言发给 agent, 开始全自动挖掘");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live() || m.stopping());
  if (ImGui::Button("Stop", ImVec2(60, 0)))
    m.stop();
  ImGui::EndDisabled();
  tip("当前这步做完再结束");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live());
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    m.cancel();
  ImGui::EndDisabled();
  tip("立即结束, 在跑的回复 / 回测作废");

  ImGui::SameLine();
  ImGui::TextDisabled("%s", Mine::kPhaseNames[static_cast<int>(m.phase())]);
  if (m.phase() != Phase::None) {
    const Mine::Setup &st = m.setup;
    ImGui::SameLine();
    ImGui::TextDisabled("· %s / %s / %s · %s %s · %d 轮 · 入库 %zu", st.sc.pool.c_str(), m.ctx.level.c_str(), m.ctx.hold.c_str(), llm::def(st.llm.backend).name,
                        st.llm.model.c_str(), m.rounds(), m.adopted.size());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", m.log_dir().c_str());
  }
  if (chat_busy) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "asking");
  }
  if (es.step != Step::Idle) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "eval %d/%d %s %s %.0f%%", es.done, es.total, kStepNames[static_cast<int>(es.step)], es.file.c_str(),
                       es.frac * 100.0);
  }
  if (m.stopping()) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "stopping");
  }
  if (!es.message.empty() || !s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "| %s", (s.message.empty() ? es.message : s.message).c_str());
  }
}

// 澄清: 逐题点选 (已按推荐预选; 末项 "其他" 自己写意见) + 附言
void questions_panel(Mine &m) {
  ImGui::TextDisabled("agent 的问题 (%zu); 已按推荐预选, 只改不同意的 (可选 \"其他\" 自己写), 改完按 Start", m.questions.size());
  ImGui::BeginChild("##questions", ImVec2(-1, ImGui::GetContentRegionAvail().y * 0.6f), true);
  for (size_t i = 0; i < m.questions.size(); ++i) {
    const Question &q = m.questions[i];
    Pick &p = m.picks[i];
    ImGui::PushID(static_cast<int>(i));
    ImGui::TextWrapped("%zu. %s%s", i + 1, q.prompt.c_str(), q.multiple ? " (可多选)" : "");
    for (size_t j = 0; j < q.options.size(); ++j) {
      ImGui::PushID(static_cast<int>(j));
      bool on = p.on[j];
      if (q.multiple ? ImGui::Checkbox(q.options[j].c_str(), &on) : ImGui::RadioButton(q.options[j].c_str(), on)) {
        if (!q.multiple) {
          p.on.assign(q.options.size(), false);
          p.other = false;
          on = true;
        }
        p.on[j] = on;
      }
      if (q.recommended[j]) {
        ImGui::SameLine();
        ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Ready), "推荐");
      }
      ImGui::PopID();
    }
    // 其他: 打字即选中; 单选与选项互斥
    const auto pick_other = [&] {
      if (!q.multiple)
        p.on.assign(q.options.size(), false);
      p.other = true;
    };
    bool other = p.other;
    if (q.multiple ? ImGui::Checkbox("其他##other", &other) : ImGui::RadioButton("其他##other", other)) {
      if (q.multiple)
        p.other = other;
      else
        pick_other();
    }
    ImGui::SameLine();
    ImGui::SetNextItemWidth(-1);
    if (input_text("##other_text", p.text, "自己的意见") && !p.text.empty() && !p.other)
      pick_other();
    ImGui::Spacing();
    ImGui::PopID();
  }
  ImGui::EndChild();
  ImGui::TextDisabled("附言 (可空)");
  input_multiline("##note", m.note, ImVec2(-1, -1));
}

void reply_panel(const Chat &chat) {
  std::lock_guard<std::mutex> lock(chat.mutex);
  const bool live = chat.busy();
  const llm::Reply *r = live || chat.turns.empty() ? nullptr : &chat.turns.back().reply;
  const std::string &text = r ? r->text : chat.live_text;
  const std::string &thinking = r ? r->thinking : chat.live_thinking;
  if (live)
    ImGui::TextDisabled("回复 (收取中)");
  else if (r)
    ImGui::TextDisabled("回复 #%zu: %s", chat.turns.size(), reply_head(*r).c_str());
  else
    ImGui::TextDisabled("回复");
  if (!thinking.empty() && ImGui::TreeNode("思考")) {
    text_box("##thinking", thinking, ImVec2(-1, ImGui::GetTextLineHeight() * 12));
    ImGui::TreePop();
  }
  text_box("##reply", text, ImVec2(-1, -1));
}

void results_table(const Mine &m) {
  const std::vector<Result> &results = m.lab.results();
  ImGui::TextDisabled("候选 %zu (会话目录, 本持有期 score 降序)", results.size());
  // 不可拖宽: FixedFit 列每帧按内容重算宽, 新行再长也不截断
  constexpr ImGuiTableFlags kFlags =
      ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY | ImGuiTableFlags_ScrollX;
  if (!ImGui::BeginTable("##results", 8, kFlags, ImVec2(-1, -1)))
    return;
  ImGui::TableSetupScrollFreeze(2, 1);
  for (const char *h : {"名", "中文", "结局", "mono", "多空bp", "rIC", "score", "模板"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  for (const Result &r : results) {
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
      if (r.has_top && ImGui::IsItemHovered())
        ImGui::SetTooltip("%s\n目标最高 %s%s", r.top.miss.c_str(), r.top.expr.c_str(), r.top.sign < 0 ? " (取反)" : "");
    } else if (m.adopted.count(r.name)) {
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Ready), "入库");
    } else {
      ImGui::TextUnformatted("有权重");
    }
    if (w && ImGui::IsItemHovered())
      ImGui::SetTooltip("%zu 个权重\n%s", r.ws.size(), w->expr.c_str());
    ImGui::TableNextColumn();
    if (w) {
      ImGui::Text("%.3f", w->mono);
      ImGui::TableNextColumn();
      ImGui::Text("%.2f", w->ls * 1e4);
      ImGui::TableNextColumn();
      ImGui::Text("%.4f", w->ric);
      ImGui::TableNextColumn();
      ImGui::Text("%.3g", w->score);
    } else if (r.has_top) {
      ImGui::TextDisabled("%.3f", r.top.mono);
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%.2f", r.top.ls * 1e4);
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%.4f", r.top.ric);
      ImGui::TableNextColumn();
      ImGui::TextDisabled("-");
    } else {
      for (int i = 0; i < 3; ++i)
        ImGui::TableNextColumn();
    }
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.tmpl.c_str());
  }
  ImGui::EndTable();
}

// ---- 参考: 本会话发给 agent 的特征表 / 算子表 ----
constexpr ImGuiTableFlags kRefFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                                      ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable;

void leaves_panel(State &s) {
  const std::vector<Leaf> &leaves = s.mine->ctx.leaves;
  s.leaf_filter.Draw("过滤##leaf", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个叶, 模板里写全名; 悬浮看公式", leaves.size());
  if (!ImGui::BeginTable("##leaves", 3, kRefFlags, ImVec2(-1, -1)))
    return;
  ImGui::TableSetupScrollFreeze(1, 1);
  for (const char *h : {"叶", "中文名", "说明"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  for (const Leaf &l : leaves) {
    if (!s.leaf_filter.PassFilter((l.code + ' ' + l.name_cn + ' ' + l.desc).c_str()))
      continue;
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(l.code.c_str());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", l.formula.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(l.name_cn.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(l.desc.c_str());
  }
  ImGui::EndTable();
}

void ops_panel(State &s) {
  const std::vector<Op> &ops = s.mine->ctx.ops;
  s.op_filter.Draw("过滤##op", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个算子; 悬浮看签名与定义", ops.size());
  if (!ImGui::BeginTable("##ops", 4, kRefFlags, ImVec2(-1, -1)))
    return;
  ImGui::TableSetupScrollFreeze(1, 1);
  for (const char *h : {"算子", "中文名", "输出域", "说明"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  for (const Op &o : ops) {
    if (!s.op_filter.PassFilter((o.name + ' ' + o.name_cn + ' ' + o.note).c_str()))
      continue;
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(o.name.c_str());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("签名: %s\n定义: %s", o.sig.c_str(), o.tex.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(o.name_cn.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(o.out.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(o.note.c_str());
  }
  ImGui::EndTable();
}

// 左栏: 问题 / 回复 + 本会话的特征表 / 算子表
void left_panel(State &s) {
  Mine &m = *s.mine;
  const Phase ph = m.phase();
  const bool to_questions = ph == Phase::Choose && s.seen_phase != Phase::Choose;
  s.seen_phase = ph;
  if (ph == Phase::None)
    return reply_panel(m.chat);
  if (!ImGui::BeginTabBar("##left_tabs"))
    return;
  if (ImGui::BeginTabItem(ph == Phase::Choose ? "问题###talk" : "回复###talk", nullptr, to_questions ? ImGuiTabItemFlags_SetSelected : ImGuiTabItemFlags_None)) {
    if (ph == Phase::Choose) {
      questions_panel(m);
    } else {
      if (ph == Phase::Report)
        text_box("##report", m.report, ImVec2(-1, ImGui::GetContentRegionAvail().y * 0.4f));
      reply_panel(m.chat);
    }
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem("特征")) {
    leaves_panel(s);
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem("算子")) {
    ops_panel(s);
    ImGui::EndTabItem();
  }
  ImGui::EndTabBar();
}

void render_mine(const SharedData &data, State &s) {
  scope_row(data, s, s.mine->live());
  session_row(data, s);
  ImGui::Separator();
  const float w = ImGui::GetContentRegionAvail().x;
  ImGui::BeginChild("##left", ImVec2(w * 0.5f, 0), false);
  left_panel(s);
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##right", ImVec2(0, 0), false);
  results_table(*s.mine);
  ImGui::EndChild();
}

// ---- Config ----
void render_config(State &s) {
  Options &o = s.opt;
  ImGui::TextColored(kColHead, "LLM");
  ImGui::SetNextItemWidth(120);
  if (ImGui::Combo("后端", &o.backend, [](void *, int i) { return kBackends[i].name; }, nullptr, llm::kBackendCount))
    o.model = first_model(s, o.backend);
  ImGui::SameLine();
  if (has_key(o.backend))
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Ready), "key 已填");
  else
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "key 未填 (config/agent/keys.json)");
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
    for (int b = 0; b < llm::kBackendCount; ++b)
      if (o.via_proxy[b])
        sniff(s, b);
  ImGui::EndDisabled();
  tip("各后端分别选直连 / 代理, 代理地址共用 (HTTP CONNECT); 改了即重新嗅探模型");
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
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Busy), "嗅探中");
  } else if (has_key(o.backend)) {
    if (ImGui::SmallButton("重新嗅探"))
      sniff(s, o.backend);
    ImGui::SameLine();
    if (!sniff_err.empty())
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Error), "嗅探失败: %s", sniff_err.c_str());
    else if (ms.empty())
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "该后端无可用模型");
    else
      ImGui::TextDisabled("%zu 个可用", ms.size());
  }
  ImGui::TextDisabled("下次 New 生效");

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
  ImGui::TextDisabled("会话目录 %s/<池>/<层>/<持有期>; 下次 New 生效", kWorkDir);

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "入库门槛");
  ImGui::TextDisabled("agent 提交的候选至少一个权重全部达到才进正式因子库; 下次 New 生效");
  const auto field = [](const char *label, double &v, const char *fmt) {
    ImGui::SetNextItemWidth(120);
    ImGui::InputDouble(label, &v, 0.0, 0.0, fmt);
  };
  field("mono >=", o.gate.mono, "%.2f");
  ImGui::SameLine();
  field("rIC >=", o.gate.ric, "%.3f");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "提示词");
  ImGui::SetNextItemWidth(120);
  const bool switched = ImGui::Combo("模板", &o.prompt, kPrompts, static_cast<int>(std::size(kPrompts)));
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
  ImGui::TextDisabled("%s/%s.md ({{键}} 占位; 保存后, 下次组消息生效)", kPromptDir, kPrompts[o.prompt]);
  input_multiline("##prompt", s.prompt_text, ImVec2(-1, -1));
}

// ---- Log ----
void render_log(const Mine &m) {
  if (m.phase() == Phase::None) {
    ImGui::TextDisabled("无会话");
    return;
  }
  const Chat &chat = m.chat;
  ImGui::TextDisabled("%s", m.log_dir().c_str());
  ImGui::BeginChild("##log", ImVec2(0, 0), false);
  std::lock_guard<std::mutex> lock(chat.mutex);
  for (size_t i = 0; i < chat.turns.size(); ++i) {
    const Turn &t = chat.turns[i];
    const llm::Reply &r = t.reply;
    const std::string head = "#" + std::to_string(i + 1) + "  " + reply_head(r) + "###turn" + std::to_string(i);
    if (!ImGui::CollapsingHeader(head.c_str()))
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
    if (!r.why.empty())
      ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Error), "%s", r.why.c_str());
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

  // 每帧 (无论是否选中): 收嗅探结果; 推进会话 (含评估队列与会话日志)
  handle.Update = [state](SharedData &) {
    State &s = *state;
    if (!s.mine)
      return;
    poll_sniff(s);
    s.mine->update();
  };

  handle.Status = [state](const SharedData &, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    const State &s = *state;
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
    if (idx != TAB_MINE)
      return {};
    const Mine &m = *s.mine;
    if (m.phase() == Phase::Choose)
      return {TaskStatus::Kind::Warn, "selecting"};
    if (m.chat.busy())
      return {TaskStatus::Kind::Busy, "asking"};
    if (const EvalState es = m.lab.state(); es.step != Step::Idle)
      return {TaskStatus::Kind::Busy, "eval " + std::to_string(es.done) + "/" + std::to_string(es.total)};
    if (m.phase() == Phase::Report)
      return {TaskStatus::Kind::Ready, "done"};
    return {};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    State &s = *state;
    if (!s.mine) {
      s.mine = std::make_unique<Mine>();
      load_ui(s);
      s.sniff.cancel.store(false, std::memory_order_relaxed);
      for (int b = 0; b < llm::kBackendCount; ++b)
        sniff(s, b);
    }
    const std::vector<Choice> pools = catalog::pools();
    if (std::none_of(pools.begin(), pools.end(), [&](const Choice &p) { return p.name == s.opt.pool; }))
      s.opt.pool = pools.front().name;
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
      render_log(*s.mine);
      break;
    default:
      break;
    }
    ImGui::EndChild();
    save_ui(s);
  };

  handle.Destroy = [state]() {
    State &s = *state;
    s.sniff.quit();
    s.mine.reset(); // 析构: 对话线程取消并 join, 评估 worker Stop
    s.seen_phase = Phase::None;
  };

  return handle;
}

} // namespace agent
