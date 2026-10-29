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
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace agent {

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Scope, pool, level, hold)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, mono, ric)

namespace {

using GUI::TaskStatus;
using Phase = Mine::Phase;

enum TabIdx {
  TAB_MINE = 0,
  TAB_CONFIG,
  TAB_COUNT
};

constexpr ImVec4 kColHead(0.4f, 0.8f, 1.0f, 1.0f);

bool has_key(int backend) { return !llm::key(static_cast<llm::Backend>(backend)).empty(); }

// Mine / Config 页的选项 (kUiFile 读写同一份字段清单): 作用域 / 连接 / 模型 / 评估后端 / 入库门槛 (下次 New 生效) + Config 页在看的模板
struct Options {
  Scope sc;
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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, sc, backend, model, via_proxy, proxy_host, proxy_port, gpu, gate, prompt)

llm::Net net_of(const Options &o, int backend) { return {o.via_proxy[backend], o.proxy_host, o.proxy_port}; }
Mine::Setup setup_of(const Options &o) { return {o.sc, o.gate, o.gpu == 1, {static_cast<llm::Backend>(o.backend), net_of(o, o.backend), o.model, {}}}; }

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
  Phase seen_phase = Phase::None; // 刚进 Choose: 切到过程页一次
  size_t flow_rounds = 0;         // 过程页上一帧画过的轮数
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
  assert(o.prompt >= 0 && o.prompt < kMsgCount);
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
  return ImGui::InputTextMultiline(id, s.data(), s.capacity() + 1, size, ImGuiInputTextFlags_CallbackResize | ImGuiInputTextFlags_WordWrap, resize_cb, &s);
}

void tip(const char *s) {
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    ImGui::SetTooltip("%s", s);
}

// ---- Mine ----
void scope_row(const SharedData &data, State &s, bool locked) {
  Scope &o = s.opt.sc;
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
  const bool has_hold = !catalog::holds(data, o.sc.level).empty();
  const bool keyed = has_key(o.backend);

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !m.live() && !m.chat.busy() && !m.lab.busy() && keyed && model_ok(s) && has_hold;
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0))) {
    if (catalog::ready(data, o.sc, o.gpu == 1)) {
      s.message.clear();
      m.open(data, setup_of(o));
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

  if (m.phase() != Phase::None) {
    const Mine::Setup &st = m.setup;
    ImGui::SameLine();
    ImGui::TextDisabled("%s / %s / %s · %s %s · %s", st.sc.pool.c_str(), m.ctx.level.c_str(), m.ctx.hold.c_str(), llm::def(st.llm.backend).name,
                        st.llm.model.c_str(), st.gpu ? "GPU" : "CPU");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", m.log_dir().c_str());
  }
  if (!s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(GUI::StatusColor(TaskStatus::Kind::Warn), "| %s", s.message.c_str());
  }
}

// ---- 显示用的量 ----
std::string dur(double sec) {
  const int t = static_cast<int>(sec + 0.5);
  char b[32];
  if (t < 60)
    std::snprintf(b, sizeof(b), "%ds", t);
  else if (t < 3600)
    std::snprintf(b, sizeof(b), "%dm%02ds", t / 60, t % 60);
  else
    std::snprintf(b, sizeof(b), "%dh%02dm", t / 3600, t / 60 % 60);
  return b;
}
std::string kilo(double n) {
  char b[32];
  if (n < 1e3)
    std::snprintf(b, sizeof(b), "%.0f", n);
  else if (n < 1e6)
    std::snprintf(b, sizeof(b), "%.1fk", n / 1e3);
  else
    std::snprintf(b, sizeof(b), "%.2fM", n / 1e6);
  return b;
}
// UTF-8 字数
std::string chars(const std::string &s) {
  return kilo(static_cast<double>(std::count_if(s.begin(), s.end(), [](char c) { return (static_cast<unsigned char>(c) & 0xC0) != 0x80; }))) + " 字";
}
const char *spinner() {
  static constexpr const char *kSpin[] = {"|", "/", "-", "\\"};
  return kSpin[static_cast<int>(ImGui::GetTime() * 8.0) % 4];
}
ImVec4 color(TaskStatus::Kind k) { return k == TaskStatus::Kind::None ? ImGui::GetStyleColorVec4(ImGuiCol_Text) : GUI::StatusColor(k); }

// ---- 自动换行的文本块 ----
void wrapped(const std::string &s) {
  ImGui::PushTextWrapPos(0.0f);
  ImGui::TextUnformatted(s.c_str(), s.c_str() + s.size());
  ImGui::PopTextWrapPos();
}
void wrapped(const ImVec4 &c, const std::string &s) {
  ImGui::PushStyleColor(ImGuiCol_Text, c);
  wrapped(s);
  ImGui::PopStyleColor();
}
void wrapped_dim(const std::string &s) { wrapped(ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), s); }
void copy_button(const char *id, const std::string &s) {
  ImGui::SameLine();
  if (ImGui::SmallButton(id))
    ImGui::SetClipboardText(s.c_str());
}
constexpr float kThinkLines = 12.0f, kBoxLines = 20.0f; // 思考 / 在收正文框 · 其余文本框的最大行高

// 可折叠的文本块: 标题行 + 复制; 展开 = 全文, 高过 lines 行就在框内滚 (不把时间线顶走); tail = 停在底部时跟着新内容走 (初开即在底部)
void fold(const char *id, ImGuiTreeNodeFlags flags, const std::string &label, const std::string &body, bool dim, float lines, bool tail) {
  const bool open = ImGui::TreeNodeEx(id, flags, "%s", label.c_str());
  copy_button((std::string("复制##") + id).c_str(), body);
  if (!open)
    return;
  const ImGuiStyle &st = ImGui::GetStyle();
  const float w = ImGui::GetContentRegionAvail().x - 2.0f * st.WindowPadding.x;
  const float h = ImGui::CalcTextSize(body.c_str(), body.c_str() + body.size(), false, w).y + 2.0f * st.WindowPadding.y + 2.0f;
  ImGui::BeginChild("##box", ImVec2(-1, std::min(h, lines * ImGui::GetTextLineHeightWithSpacing())), ImGuiChildFlags_Borders);
  const bool follow = tail && ImGui::GetScrollY() >= ImGui::GetScrollMaxY() - 2.0f;
  dim ? wrapped_dim(body) : wrapped(body);
  if (follow)
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
  ImGui::TreePop();
}

// ---- 现在在干什么 + 会话累计 ----
void now_row(const Mine &m, const EvalState &es, const llm::Reply &live) {
  const Phase ph = m.phase();
  if (ph == Phase::None) {
    ImGui::TextDisabled("无会话: 选好作用域按 New");
    return;
  }
  const double t = m.now();
  double llm_s = 0.0, eval_s = 0.0;
  double tok_in = 0.0, tok_cached = 0.0, tok_out = 0.0;
  Mine::Tally all;
  for (const Mine::Round &rd : m.timeline) {
    all += Mine::tally(rd);
    if (rd.t_reply >= 0) {
      const llm::Reply &r = rd.reply;
      llm_s += r.seconds;
      tok_in += r.usage.prompt, tok_cached += r.usage.cached, tok_out += r.usage.completion;
    }
    if (const Mined *b = std::get_if<Mined>(&rd.got); b && b->t_eval0 >= 0)
      eval_s += (b->t_eval1 >= 0 ? b->t_eval1 : t) - b->t_eval0;
  }
  std::string what;
  TaskStatus::Kind kind = TaskStatus::Kind::Busy;
  float frac = -1.0f; // 回测总进度 (−1 = 不画)
  if (m.chat.busy() && !m.timeline.empty()) {
    const double since = t - m.timeline.back().t_ask;
    llm_s += since;
    what = std::string(spinner()) + (live.think_s < 0 ? " agent 思考中" : " agent 输出中") + " · 第 " + std::to_string(m.timeline.size()) + " 轮 · " + dur(since) +
           " · 思考 " + chars(live.thinking) + (live.text.empty() ? "" : " · 正文 " + chars(live.text));
  } else {
    switch (ph) {
    case Phase::Choose:
      what = "等你选答案: 已按推荐预选, 改完按 Start", kind = TaskStatus::Kind::Warn;
      break;
    case Phase::Eval:
      if (es.step == Step::Idle) {
        what = std::string(spinner()) + " 回测收尾";
      } else {
        char b[64];
        std::snprintf(b, sizeof(b), " %.0f%%", es.frac * 100.0);
        what = std::string(spinner()) + " 回测 " + std::to_string(es.done + 1) + "/" + std::to_string(es.total) + " · " + name(es.step) + " " + es.file + b + " · 本轮 " +
               dur(t - std::get<Mined>(m.timeline.back().got).t_eval0);
        frac = es.total > 0 ? static_cast<float>((es.done + es.frac) / es.total) : 0.0f;
      }
      break;
    case Phase::Report:
      what = "已结束: " + m.reason, kind = TaskStatus::Kind::Ready;
      break;
    default:
      what = "处理回复";
      break;
    }
  }
  if (m.stopping())
    what += " · 本步做完即停", kind = TaskStatus::Kind::Warn;
  if (frac >= 0.0f) {
    ImGui::ProgressBar(frac, ImVec2(120, 0), "");
    ImGui::SameLine();
  }
  ImGui::TextColored(color(kind), "%s", what.c_str());
  ImGui::TextDisabled("用时 %s · LLM %s · 回测 %s · token 输入 %s (缓存 %s) / 输出 %s · %zu 轮 · 候选 %d · 入库 %d", dur(t).c_str(), dur(llm_s).c_str(), dur(eval_s).c_str(),
                      kilo(tok_in).c_str(), kilo(tok_cached).c_str(), kilo(tok_out).c_str(), m.timeline.size(), all.staged, all.adopted);
  if (!es.message.empty())
    wrapped(color(TaskStatus::Kind::Warn), es.message);
}

// 澄清: 逐题点选 (已按推荐预选; 末项 "其他" 自己写意见) + 附言; edit = 在 Choose 的末轮. 返回 = 按了 Start
bool asked_block(Asked &a, bool edit) {
  if (edit)
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "已按推荐预选, 只改不同意的 (可选 \"其他\" 自己写), 改完按 Start");
  for (size_t i = 0; i < a.qs.size(); ++i) {
    const Question &q = a.qs[i];
    Pick &p = a.picks[i];
    ImGui::PushID(static_cast<int>(i));
    wrapped(std::to_string(i + 1) + ". " + q.prompt + (q.multiple ? " (可多选)" : ""));
    ImGui::Indent();
    // 单选: 选项与 "其他" 互斥
    const auto pick = [&](size_t j) {
      if (q.multiple) {
        p.on[j] = !p.on[j];
        return;
      }
      p.on.assign(q.options.size(), false);
      p.other = false;
      p.on[j] = true;
    };
    const auto pick_other = [&] {
      if (!q.multiple)
        p.on.assign(q.options.size(), false);
      p.other = true;
    };
    for (size_t j = 0; j < q.options.size(); ++j) {
      ImGui::PushID(static_cast<int>(j));
      bool on = p.on[j];
      ImGui::BeginDisabled(!edit);
      const bool hit = q.multiple ? ImGui::Checkbox("##o", &on) : ImGui::RadioButton("##o", on);
      ImGui::EndDisabled();
      ImGui::SameLine();
      if (q.recommended[j]) {
        ImGui::TextColored(color(TaskStatus::Kind::Ready), "推荐");
        ImGui::SameLine();
      }
      p.on[j] || edit ? wrapped(q.options[j]) : wrapped_dim(q.options[j]);
      if (edit && (hit || ImGui::IsItemClicked()))
        pick(j);
      ImGui::PopID();
    }
    // 其他: 打字即选中
    bool other = p.other;
    ImGui::BeginDisabled(!edit);
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
    ImGui::EndDisabled();
    ImGui::Unindent();
    ImGui::Spacing();
    ImGui::PopID();
  }
  if (edit) {
    ImGui::TextDisabled("附言 (可空)");
    input_multiline("##note", a.note, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 4));
    return ImGui::Button("Start", ImVec2(80, 0));
  }
  if (!a.note.empty())
    wrapped_dim("附言: " + a.note);
  return false;
}

// 结局的颜色 (入库另标绿)
TaskStatus::Kind kind_of(const Mine &m, const Result &r) {
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return TaskStatus::Kind::Error;
  case Verdict::Weights:
    return m.adopted.count(r.name) ? TaskStatus::Kind::Ready : TaskStatus::Kind::None;
  case Verdict::Null:
    return r.top ? TaskStatus::Kind::Warn : TaskStatus::Kind::Muted;
  case Verdict::Pending:
    return TaskStatus::Kind::Muted;
  }
  return TaskStatus::Kind::None;
}

// 候选标题行上的一句结局
std::string verdict(const Mine &m, const Result &r) {
  switch (r.verdict) {
  case Verdict::Weights:
    return Text(r.best()->st) + (m.adopted.count(r.name) ? " · 已入库" : "");
  case Verdict::Null:
    return r.top ? "无显著: " + r.top->miss : std::string(name(r.verdict));
  default:
    return name(r.verdict);
  }
}

// 一个候选文件的结局全文 (一行; 悬浮看全部权重 / 目标最高的候选)
void result_line(const Mine &m, const Result &r) {
  wrapped(color(kind_of(m, r)), Brief(r) + (m.adopted.count(r.name) ? " · 已入库" : ""));
  if (!ImGui::IsItemHovered())
    return;
  if (r.verdict == Verdict::Weights) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 50.0f);
    for (const Weight &x : r.ws) {
      ImGui::TextUnformatted(x.expr.c_str());
      ImGui::TextDisabled("    %s", Text(x).c_str());
    }
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  } else if (r.top) {
    ImGui::SetTooltip("%s", r.top->expr.c_str());
  }
}

// 时间线的树: 组 (每 kGroupRounds 轮, 超过一组才分) → 轮 → 候选 / 文本块; 每层标题即摘要.
// 新一轮来时只留最近 kOpenRounds 轮 (及其所在组) 展开, 更早的自动折叠; 人手动开合的保持到下一轮来
constexpr size_t kGroupRounds = 10, kOpenRounds = 2;
constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAvailWidth | ImGuiTreeNodeFlags_DrawLinesFull;

// 一轮里的一个候选: 标题 = 名 + 回测状态 / 用时 + 结局; 展开 = 表达式, 思路, 结局全文
void cand_block(const Mine &m, const Trial &k, size_t i, const EvalState &es) {
  const Staged &st = k.st;
  const Result &res = st.res;
  const bool settled = k.t1 >= 0 || res.verdict == Verdict::Unstaged;
  ImGui::PushID(static_cast<int>(i));
  ImGui::PushStyleColor(ImGuiCol_Text, kColHead);
  const bool open = ImGui::TreeNodeEx("cand", ImGuiTreeNodeFlags_DefaultOpen | ImGuiTreeNodeFlags_DrawLinesFull, "%s  %s", st.cand.name.c_str(), st.cand.name_cn.c_str());
  ImGui::PopStyleColor();
  ImGui::SameLine();
  if (settled) {
    std::string took;
    if (k.t0 >= 0) {
      const double search_s = (k.t_run >= 0 ? k.t_run : k.t1) - k.t0;
      if (search_s > 0)
        took = "Search " + dur(search_s);
      if (k.t_run >= 0)
        took += (took.empty() ? "" : " · ") + std::string("Run ") + dur(k.t1 - k.t_run);
    }
    if (!took.empty()) {
      ImGui::TextDisabled("%s ·", took.c_str());
      ImGui::SameLine();
    }
    ImGui::TextColored(color(kind_of(m, res)), "%s", verdict(m, res).c_str());
  } else if (k.t0 >= 0 && m.live()) {
    const bool here = es.file == st.file;
    ImGui::TextColored(color(TaskStatus::Kind::Busy), "%s %s %.0f%% · %s", spinner(), k.t_run >= 0 ? "Run" : "Search", here ? es.frac * 100.0 : 0.0, dur(m.now() - k.t0).c_str());
  } else {
    ImGui::TextDisabled("%s", m.live() ? "排队" : "未评");
  }
  if (open) {
    wrapped(ImVec4(0.9f, 0.8f, 0.55f, 1.0f), st.cand.expr);
    if (!st.cand.note.empty())
      wrapped_dim(st.cand.note);
    if (res.verdict == Verdict::Unstaged)
      wrapped(color(TaskStatus::Kind::Error), st.err);
    else if (settled)
      result_line(m, res);
    ImGui::TreePop();
  }
  ImGui::PopID();
}

// 挖掘: 候选 (回测状态 / 结局) + 提交 + issues
void mined_block(const Mine &m, const Mined &b, const EvalState &es) {
  const size_t n = b.batch.size();
  if (n > 0) {
    std::string label = "候选 " + std::to_string(n);
    TaskStatus::Kind kind = TaskStatus::Kind::None;
    if (b.t_eval1 >= 0)
      label += " · 回测 " + dur(b.t_eval1 - b.t_eval0);
    else if (b.t_eval0 >= 0 && m.live())
      label = std::string(spinner()) + " " + label + " · 回测中 " + dur(m.now() - b.t_eval0), kind = TaskStatus::Kind::Busy;
    else
      label += " · 回测中断", kind = TaskStatus::Kind::Warn;
    ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
    const bool open = ImGui::TreeNodeEx("cands", kNode | ImGuiTreeNodeFlags_DefaultOpen, "%s", label.c_str());
    ImGui::PopStyleColor();
    if (open) {
      for (size_t i = 0; i < n; ++i)
        cand_block(m, b.batch[i], i, es);
      ImGui::TreePop();
    }
  }
  for (const Submitted &x : b.submitted) {
    ImGui::Bullet();
    if (x.err.empty())
      ImGui::TextColored(color(TaskStatus::Kind::Ready), "提交 %s → 入库", x.name.c_str());
    else
      wrapped(color(TaskStatus::Kind::Warn), "提交 " + x.name + " → 未入库: " + x.err);
  }
  for (const std::string &x : b.issues) {
    ImGui::Bullet();
    wrapped(color(TaskStatus::Kind::Warn), "issue: " + x);
  }
  if (b.done)
    ImGui::TextColored(color(TaskStatus::Kind::Ready), "agent 宣布挖完");
}

// 一轮的标题 (折起来也看得出这轮干了什么)
std::string round_head(const Mine &m, size_t i, bool receiving, bool thinking_now, TaskStatus::Kind &kind) {
  const Mine::Round &rd = m.timeline[i];
  std::string h = "第 " + std::to_string(i + 1) + " 轮 · " + def(rd.kind).cn + " · +" + dur(rd.t_ask);
  kind = TaskStatus::Kind::None;
  if (receiving) {
    kind = TaskStatus::Kind::Busy;
    return h + " · " + spinner() + (thinking_now ? " 思考中 " : " 输出中 ") + dur(m.now() - rd.t_ask);
  }
  const bool replied = rd.t_reply >= 0;
  if (replied)
    h += " · 回复 " + dur(rd.reply.seconds);
  if (!rd.err.empty()) {
    kind = TaskStatus::Kind::Error;
    return h + (replied && !rd.reply.ok() ? " · 回复失败" : " · 不合协议");
  }
  if (const Asked *a = std::get_if<Asked>(&rd.got))
    return h + " · " + std::to_string(a->qs.size()) + " 个问题";
  const Mined *b = std::get_if<Mined>(&rd.got);
  if (!b)
    return h;
  const Mine::Tally c = Mine::tally(rd);
  h += " · 候选 " + std::to_string(c.staged) + (c.unstaged ? " (未落 " + std::to_string(c.unstaged) + ")" : "");
  if (b->t_eval0 >= 0 && b->t_eval1 < 0 && m.live())
    h += " · " + std::string(spinner()) + " 回测中", kind = TaskStatus::Kind::Busy;
  else if (c.staged > 0)
    h += " · 有权重 " + std::to_string(c.hits);
  if (c.submits > 0)
    h += " · 提交 " + std::to_string(c.submits) + " (入库 " + std::to_string(c.adopted) + ")";
  if (c.adopted > 0 && kind == TaskStatus::Kind::None)
    kind = TaskStatus::Kind::Ready;
  if (c.issues > 0)
    h += " · issue " + std::to_string(c.issues);
  if (b->done)
    h += " · 宣布挖完";
  return h;
}

// 一轮: 发出 → 思考 → 回复 (结构化 + 原文) → 回测; 在收的一轮画 live. fresh = 新一轮刚来 (重定开合). 返回 = 按了 Start
bool round_block(Mine &m, size_t i, const EvalState &es, const llm::Reply &live, bool fresh) {
  Mine::Round &rd = m.timeline[i];
  const bool replied = rd.t_reply >= 0;
  const llm::Reply &r = replied ? rd.reply : live;
  const bool receiving = !replied && m.chat.busy();
  const bool thinking_now = receiving && live.think_s < 0;
  const double since = m.now() - rd.t_ask;
  const double think_s = thinking_now ? since : r.think_s;

  TaskStatus::Kind kind = TaskStatus::Kind::None;
  const std::string head = round_head(m, i, receiving, thinking_now, kind);
  if (fresh)
    ImGui::SetNextItemOpen(i + kOpenRounds >= m.timeline.size(), ImGuiCond_Always);
  ImGui::PushID(static_cast<int>(i));
  ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
  const bool open = ImGui::TreeNodeEx("round", kNode, "%s", head.c_str());
  ImGui::PopStyleColor();
  if (!open) {
    ImGui::PopID();
    return false;
  }
  fold("send", ImGuiTreeNodeFlags_None, "发出 " + chars(rd.user), rd.user, false, kBoxLines, false);
  if (!r.thinking.empty() || thinking_now) {
    const std::string label = thinking_now ? std::string(spinner()) + " 思考中 " + dur(think_s) : "思考了 " + dur(think_s);
    fold("think", ImGuiTreeNodeFlags_DefaultOpen, label + " · " + chars(r.thinking), r.thinking, true, kThinkLines, true);
  }
  if (receiving && !thinking_now)
    fold("text_live", ImGuiTreeNodeFlags_DefaultOpen, std::string(spinner()) + " 输出中 " + dur(since - think_s) + " · " + chars(r.text), r.text, false, kThinkLines,
         true);
  bool start = false;
  if (replied) {
    std::string line = "回复 · " + dur(r.seconds) + " (思考 " + dur(r.think_s) + ")";
    if (r.usage.prompt > 0)
      line += " · token 输入 " + kilo(r.usage.prompt) + " (缓存 " + kilo(r.usage.cached) + ") / 输出 " + kilo(r.usage.completion);
    ImGui::TextColored(color(r.ok() ? TaskStatus::Kind::None : TaskStatus::Kind::Error), "%s", line.c_str());
    if (!rd.err.empty())
      wrapped(color(TaskStatus::Kind::Error), rd.err);
    if (Asked *a = std::get_if<Asked>(&rd.got))
      start = asked_block(*a, m.phase() == Phase::Choose && i + 1 == m.timeline.size());
    else if (const Mined *b = std::get_if<Mined>(&rd.got))
      mined_block(m, *b, es);
    fold("text", ImGuiTreeNodeFlags_None, "原文 " + chars(r.text), r.text, false, kBoxLines, false);
  }
  ImGui::TreePop();
  ImGui::PopID();
  return start;
}

// 一组轮的标题: 轮次 / 起始时刻 / 候选 / 有权重 / 入库
std::string group_head(const Mine &m, size_t g0, size_t g1) {
  Mine::Tally c;
  for (size_t i = g0; i < g1; ++i)
    c += Mine::tally(m.timeline[i]);
  return "第 " + std::to_string(g0 + 1) + "–" + std::to_string(g1) + " 轮 · +" + dur(m.timeline[g0].t_ask) + " · 候选 " + std::to_string(c.staged) + " · 有权重 " +
         std::to_string(c.hits) + " · 入库 " + std::to_string(c.adopted);
}

// 过程: 全会话 (分组 → 逐轮) + 结束报告; 停在底部时跟着新内容走. seen = 上一帧画过的轮数 (变了 = 新一轮来, 重定开合)
void timeline_panel(Mine &m, const EvalState &es, const llm::Reply &live, bool to_end, size_t &seen) {
  ImGui::BeginChild("##flow", ImVec2(0, 0), false);
  const bool follow = to_end || ImGui::GetScrollY() >= ImGui::GetScrollMaxY() - 2.0f;
  bool start = false;
  if (m.phase() == Phase::None) {
    ImGui::TextDisabled("选好作用域按 New: 系统把规则打包发给 agent, 它先提问");
  } else {
    const size_t n = m.timeline.size();
    const bool fresh = n != seen, grouped = n > kGroupRounds;
    seen = n;
    for (size_t g0 = 0; g0 < n; g0 += kGroupRounds) {
      const size_t g1 = std::min(n, g0 + kGroupRounds);
      ImGui::PushID(static_cast<int>(g0));
      bool open = true;
      if (grouped) {
        if (fresh)
          ImGui::SetNextItemOpen(g1 + kOpenRounds > n, ImGuiCond_Always);
        open = ImGui::TreeNodeEx("group", kNode | ImGuiTreeNodeFlags_Framed, "%s", group_head(m, g0, g1).c_str());
      }
      if (open) {
        for (size_t i = g0; i < g1; ++i)
          start |= round_block(m, i, es, live, fresh);
        if (grouped)
          ImGui::TreePop();
      }
      ImGui::PopID();
    }
  }
  if (start)
    m.start();
  if (m.phase() == Phase::Report) {
    ImGui::SeparatorText(("结束 · +" + dur(m.now())).c_str());
    wrapped(color(TaskStatus::Kind::Ready), m.reason);
    fold("report", ImGuiTreeNodeFlags_DefaultOpen, "结束报告", m.report, false, kBoxLines, false);
  }
  if (follow)
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
}

// 表随页宽缩放: 不可拖宽, 前面的列 FixedFit 每帧按内容重算宽 (新行再长也不截断), 末列 (长文本) 吃剩下的宽并自动换行
constexpr ImGuiTableFlags kTableFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY;
bool begin_table(const char *id, std::initializer_list<const char *> heads) {
  if (!ImGui::BeginTable(id, static_cast<int>(heads.size()), kTableFlags, ImVec2(-1, -1)))
    return false;
  ImGui::TableSetupScrollFreeze(0, 1);
  int i = 0;
  for (const char *h : heads)
    ImGui::TableSetupColumn(h, ++i == static_cast<int>(heads.size()) ? ImGuiTableColumnFlags_WidthStretch : ImGuiTableColumnFlags_None);
  ImGui::TableHeadersRow();
  return true;
}

void results_table(const Mine &m) {
  const std::vector<Result> &results = m.lab.results();
  ImGui::TextDisabled("候选 %zu (会话目录, 本持有期 score 降序)", results.size());
  if (!begin_table("##results", {"名", "中文", "结局", "mono", "多空bp", "rIC", "score", "模板"}))
    return;
  for (const Result &r : results) {
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name.c_str());
    if (!r.note.empty() && ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", r.note.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name_cn.c_str());
    ImGui::TableNextColumn();
    ImGui::TextColored(color(kind_of(m, r)), "%s", m.adopted.count(r.name) ? "入库" : name(r.verdict));
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", (Brief(r) + (r.top ? "\n目标最高 " + r.top->expr : "")).c_str());
    // 统计列: 最佳权重; 无显著则目标最高的候选 (灰)
    const Weight *w = r.best();
    const Stat *st = w ? &w->st : r.top ? &r.top->st
                                        : nullptr;
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(w ? ImGuiCol_Text : ImGuiCol_TextDisabled));
    ImGui::TableNextColumn();
    if (st)
      ImGui::Text("%.3f", st->mono);
    ImGui::TableNextColumn();
    if (st)
      ImGui::Text("%.2f", st->ls * 1e4);
    ImGui::TableNextColumn();
    if (st)
      ImGui::Text("%.4f", st->ric);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%.3g", w->score);
    ImGui::PopStyleColor();
    ImGui::TableNextColumn();
    wrapped(r.tmpl);
  }
  ImGui::EndTable();
}

// ---- 参考: 本会话发给 agent 的特征表 / 算子表 ----
void leaves_panel(State &s) {
  const std::vector<Leaf> &leaves = s.mine->ctx.leaves;
  s.leaf_filter.Draw("过滤##leaf", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个叶, 模板里写全名; 悬浮看公式", leaves.size());
  if (!begin_table("##leaves", {"叶", "中文名", "说明"}))
    return;
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
    wrapped(l.desc);
  }
  ImGui::EndTable();
}

void ops_panel(State &s) {
  const std::vector<Op> &ops = s.mine->ctx.ops;
  s.op_filter.Draw("过滤##op", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个算子; 悬浮看签名与定义", ops.size());
  if (!begin_table("##ops", {"算子", "中文名", "输出域", "说明"}))
    return;
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
    wrapped(o.note);
  }
  ImGui::EndTable();
}

// 过程 (全宽时间线) / 候选表 / 本会话的特征表 / 算子表; 刚进 Choose 切回过程并滚到底 (问题在那)
void render_mine(const SharedData &data, State &s) {
  Mine &m = *s.mine;
  scope_row(data, s, m.live());
  session_row(data, s);
  const EvalState es = m.lab.live();
  const llm::Reply live = m.chat.live();
  now_row(m, es, live);
  ImGui::Separator();
  const Phase ph = m.phase();
  const bool to_choose = ph == Phase::Choose && s.seen_phase != Phase::Choose;
  s.seen_phase = ph;
  if (!ImGui::BeginTabBar("##mine_tabs"))
    return;
  if (ImGui::BeginTabItem("过程###flow", nullptr, to_choose ? ImGuiTabItemFlags_SetSelected : ImGuiTabItemFlags_None)) {
    timeline_panel(m, es, live, to_choose, s.flow_rounds);
    ImGui::EndTabItem();
  }
  const std::string cands = "候选 (" + std::to_string(m.lab.results().size()) + ")###cands";
  if (ImGui::BeginTabItem(cands.c_str())) {
    results_table(m);
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
  const bool switched = ImGui::Combo("模板", &o.prompt, [](void *, int i) { return kMsgs[i].id; }, nullptr, kMsgCount);
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
  ImGui::TextDisabled("%s/%s.md ({{键}} 占位; 保存后, 下次组消息生效)", kPromptDir, def(msg).id);
  input_multiline("##prompt", s.prompt_text, ImVec2(-1, -1));
}

} // namespace

GUI::TaskHandle CreateAgentTask() {
  auto state = std::make_shared<State>();

  GUI::TaskHandle handle;
  handle.name = "Agent";
  handle.storage = state;
  handle.tabs = {"Mine", "Config"};

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
    if (const EvalState es = m.lab.live(); es.step != Step::Idle)
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
    if (std::none_of(pools.begin(), pools.end(), [&](const Choice &p) { return p.name == s.opt.sc.pool; }))
      s.opt.sc.pool = pools.front().name;
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
