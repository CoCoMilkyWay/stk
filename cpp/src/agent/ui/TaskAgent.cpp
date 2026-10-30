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
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <future>
#include <initializer_list>
#include <memory>
#include <set>
#include <string>
#include <string_view>
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

// Mine / Config 页的选项 (kUiFile 读写同一份字段清单): 作用域 / 连接 / 模型 / 评估后端 / 入库门槛 / 可否提问 (下次 New 生效) + Config 页在看的模板
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
  bool ask = true; // agent 可用 ask_human 向人提一次问
  int prompt = 0;  // Msg 下标
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, sc, backend, model, via_proxy, proxy_host, proxy_port, gpu, gate, ask, prompt)

llm::Net net_of(const Options &o, int backend) { return {o.via_proxy[backend], o.proxy_host, o.proxy_port}; }
Mine::Setup setup_of(const Options &o) {
  return {o.sc, o.gate, o.gpu == 1, o.ask, {static_cast<llm::Backend>(o.backend), net_of(o, o.backend), o.model, {}}};
}

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
  // 过程页的跟随: pinned = 钉在底部跟新内容 (人往上滚 / 点开合即放开, 回到底部再钉); jump = 点了状态行, 跳到当前轮
  bool pinned = true, jump = false;
  float flow_y = 0.0f; // 上一帧的滚动位置
  // 两个 worker 在跑那件事的样子 (每帧 live 同步)
  Chat::Live chat_live;
  Lab::Live lab_live;
  // 在收那轮: 增量总量与它上次变的时刻 (量静默)
  size_t live_n = 0;
  double live_t = 0.0;
  // 在收那轮思考块的自动开合: 思考时开, 转正文收; 人点过就不再动
  size_t think_round = SIZE_MAX;
  bool think_open = false, think_touched = false;
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
// 无文件 / 读不出 / 字段对不上 / 取值越界 ⇒ 整份回默认值, 首次 save_ui 按当前字段清单重写
// 池 / 层 / 持有期 / 模型失效由各自的选择逻辑回落; GPU 不在了回落 CPU
void load_ui(State &s) {
  std::ifstream f(kUiFile);
  bool ok = false;
  if (f.is_open()) {
    const nlohmann::json j = nlohmann::json::parse(f, nullptr, false);
    try {
      j.get_to(s.opt);
      ok = true;
    } catch (const nlohmann::json::exception &) {
    }
  }
  const Options &o = s.opt;
  ok = ok && o.backend >= 0 && o.backend < llm::kBackendCount && o.proxy_port > 0 && o.proxy_port < 65536 && o.gpu >= -1 && o.gpu <= 1 &&
       o.prompt >= 0 && o.prompt < kMsgCount;
  if (!ok)
    s.opt = Options{};
  if (!catalog::gpu())
    s.opt.gpu = 0;
  s.opt_saved = ok ? nlohmann::ordered_json(s.opt).dump(2) : std::string();
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
// 悬浮看长文 (限宽换行)
void tip_long(const std::string &s) {
  if (!ImGui::IsItemHovered())
    return;
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(ImGui::GetFontSize() * 50.0f);
  ImGui::TextUnformatted(s.c_str(), s.c_str() + s.size());
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
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
                     : "按当前作用域与 Config 开新会话: 打包规则与工具表发给 agent, 它调工具看数据、回测、提交\n资产轴与标签就绪才开得成");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Choose);
  if (ImGui::Button("Start", ImVec2(60, 0)))
    m.start();
  ImGui::EndDisabled();
  tip("把选好的答案与附言作为 ask_human 的结果交给 agent, 接着执行余下的调用");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live() || m.stopping());
  if (ImGui::Button("Stop", ImVec2(60, 0)))
    m.stop();
  ImGui::EndDisabled();
  tip("在做的调用做完再结束");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live());
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    m.cancel();
  ImGui::EndDisabled();
  tip("立即结束, 在跑的回复 / 回测作废");

  if (m.phase() != Phase::None) {
    const Mine::Setup &st = m.setup;
    ImGui::SameLine();
    ImGui::TextDisabled("%s · %s %s · %s · %s", catalog::label(st.sc).c_str(), llm::def(st.llm.backend).name, st.llm.model.c_str(), st.gpu ? "GPU" : "CPU",
                        st.ask ? "可提问" : "不提问");
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
// 状态色; 进行中的随时间明暗呼吸 (一眼看出哪在动)
ImVec4 color(TaskStatus::Kind k) {
  if (k == TaskStatus::Kind::None)
    return ImGui::GetStyleColorVec4(ImGuiCol_Text);
  ImVec4 c = GUI::StatusColor(k);
  if (k == TaskStatus::Kind::Busy)
    c.w *= 0.7f + 0.3f * static_cast<float>(std::cos(ImGui::GetTime() * 4.0));
  return c;
}

// 截到 n 字 (UTF-8), 截了补 …
std::string clip(std::string_view s, size_t n) {
  size_t i = 0;
  for (size_t k = 0; i < s.size(); ++i)
    if ((static_cast<unsigned char>(s[i]) & 0xC0) != 0x80 && k++ == n)
      break;
  return i < s.size() ? std::string(s.substr(0, i)) + "…" : std::string(s);
}
std::string_view trim(std::string_view s) {
  const size_t b = s.find_first_not_of(" \t\r");
  return b == std::string_view::npos ? std::string_view() : s.substr(b, s.find_last_not_of(" \t\r") - b + 1);
}
// 思考在想什么: 末个独占一行的 **小标题** / # 标题; 没有则 (live) 末个非空行的开头
constexpr size_t kTopicChars = 48;
std::string topic(std::string_view s, bool live) {
  std::string_view tail;
  for (size_t end = s.size(); end > 0;) {
    const size_t nl = s.rfind('\n', end - 1);
    const size_t b = nl == std::string_view::npos ? 0 : nl + 1;
    const std::string_view line = trim(s.substr(b, end - b));
    end = b == 0 ? 0 : nl;
    if (line.empty())
      continue;
    if (line.size() > 4 && line.starts_with("**") && line.ends_with("**"))
      return clip(trim(line.substr(2, line.size() - 4)), kTopicChars);
    if (const size_t h = line.find_first_not_of('#'); h > 0 && h <= 6 && h < line.size() && line[h] == ' ')
      return clip(trim(line.substr(h)), kTopicChars);
    if (tail.empty())
      tail = line;
  }
  return live ? clip(tail, kTopicChars) : std::string();
}

// 在收那轮此刻的样子 (每帧量): 等首包 → 思考 → 正文; quiet = 多久没来新增量
constexpr double kQuietS = 20.0; // 静默超过这么久标黄
struct Flow {
  enum Stage : uint8_t { Idle,
                         Wait,
                         Think,
                         Write } stage = Idle;
  double since = 0.0, quiet = 0.0, writing = 0.0; // 本轮发出至今 / 距上个增量 / 正文已写
  std::string topic;
  bool stalled() const { return stage != Idle && quiet > kQuietS; }
};
// 阶段由末轮的 Span 定, 静默由镜像 s.chat_live 的增量量
Flow flow_of(State &s, const Mine &m) {
  Flow f;
  if (m.timeline.empty() || !m.timeline.back().span.running())
    return f;
  const Mine::Round &rd = m.timeline.back();
  const llm::Reply &live = s.chat_live.reply;
  const double t = m.now();
  const size_t n = live.thinking.size() + live.text.size();
  if (s.live_t < rd.span.t0)
    s.live_n = 0, s.live_t = rd.span.t0;
  if (n != s.live_n)
    s.live_n = n, s.live_t = t;
  f.since = rd.span.len(t);
  f.quiet = t - s.live_t;
  f.writing = rd.write.len(t);
  f.stage = rd.write.began() ? Flow::Write : rd.think.began() ? Flow::Think
                                                              : Flow::Wait;
  if (f.stage == Flow::Think)
    f.topic = topic(live.thinking, true);
  return f;
}
// 阶段一句 (进行时) + 用时 + 静默
std::string doing(const Flow &f) {
  std::string h = f.stage == Flow::Wait    ? "等待首包… " + dur(f.since)
                  : f.stage == Flow::Think ? "思考中… " + dur(f.since)
                                           : "输出中… " + dur(f.writing);
  if (f.stalled())
    h += " · 已 " + dur(f.quiet) + " 无新内容";
  return h;
}

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

// 可折叠的文本块: 标题行 + 复制; 展开 = 全文, 高过 lines 行就在框内滚 (不把时间线顶走); tail = 停在底部时跟着新内容走 (初开即在底部).
// 返回 = 人点了开合
bool fold(const char *id, ImGuiTreeNodeFlags flags, const std::string &label, const std::string &body, bool dim, float lines, bool tail) {
  const bool open = ImGui::TreeNodeEx(id, flags, "%s", label.c_str());
  const bool toggled = ImGui::IsItemToggledOpen();
  copy_button((std::string("复制##") + id).c_str(), body);
  if (!open)
    return toggled;
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
  return toggled;
}

// 在做的调用 (Act / Choose 中; 否则空)
const Act *acting(const Mine &m) {
  if (m.phase() != Phase::Act && m.phase() != Phase::Choose)
    return nullptr;
  const std::vector<Act> &as = m.timeline.back().acts;
  return m.acting() < as.size() ? &as[m.acting()] : nullptr;
}
const char *step_name(const Trial &k) { return k.run.began() ? "Run" : "Search"; }

// ---- 现在在干什么 (点了跳到当前轮) + 会话累计 ----
void now_row(State &s, const Mine &m, const Flow &f) {
  const Phase ph = m.phase();
  if (ph == Phase::None) {
    ImGui::TextDisabled("无会话: 选好作用域按 New");
    return;
  }
  const double t = m.now();
  const Mine::Total &all = m.total;
  const llm::Reply &live = s.chat_live.reply;
  const Lab::Live &ll = s.lab_live;
  const Act *a = acting(m);
  double llm_s = all.llm_s, act_s = all.act_s;
  if (const Mine::Round &rd = m.timeline.back(); rd.span.running())
    llm_s += rd.span.len(t);
  if (a && a->span.running())
    act_s += a->span.len(t);
  std::string what;
  TaskStatus::Kind kind = TaskStatus::Kind::Busy;
  float frac = -1.0f; // 在做那步的进度 (−1 = 不画)
  if (f.stage != Flow::Idle) {
    what = "第 " + std::to_string(m.timeline.size()) + " 轮 · agent " + doing(f);
    if (!f.topic.empty())
      what += " · " + f.topic;
    if (!live.thinking.empty())
      what += " · 思考 " + chars(live.thinking);
    if (!live.text.empty())
      what += " · 正文 " + chars(live.text);
    if (f.stalled())
      kind = TaskStatus::Kind::Warn;
  } else {
    switch (ph) {
    case Phase::Choose:
      what = "等你选答案: 已按推荐预选, 改完按 Start", kind = TaskStatus::Kind::Warn;
      break;
    case Phase::Act: {
      if (!a) {
        what = "处理回复";
        break;
      }
      what = "第 " + std::to_string(m.timeline.size()) + " 轮 · 调用 " + std::to_string(m.acting() + 1) + "/" + std::to_string(m.timeline.back().acts.size()) + " " +
             clip(a->call.text(), kTopicChars);
      if (const Trial *k = std::get_if<Trial>(&a->got)) {
        char pct[32];
        std::snprintf(pct, sizeof(pct), " %.0f%%", ll.frac * 100.0);
        what += std::string(" · ") + step_name(*k) + pct;
        frac = static_cast<float>(ll.frac);
      } else {
        what += " · 执行中…";
      }
      what += " " + dur(a->span.len(t));
      break;
    }
    case Phase::Report:
      what = "已结束: " + m.reason, kind = TaskStatus::Kind::Ready;
      break;
    default:
      what = "处理回复";
      break;
    }
  }
  if (m.stopping())
    what += " · 在做的调用做完即停", kind = TaskStatus::Kind::Warn;
  if (frac >= 0.0f) {
    ImGui::ProgressBar(frac, ImVec2(120, 0), "");
    ImGui::SameLine();
  }
  ImGui::TextColored(color(kind), "%s", what.c_str());
  if (!m.timeline.empty()) {
    if (ImGui::IsItemClicked())
      s.jump = true;
    if (ImGui::IsItemHovered()) {
      ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);
      ImGui::SetTooltip("点击跳到当前轮");
    }
  }
  ImGui::TextDisabled("用时 %s · LLM %s · 执行 %s · token 输入 %s (缓存 %s) / 输出 %s · %zu 轮 · 调用 %d (观察 %d) · 候选 %d · 入库 %d", dur(t).c_str(), dur(llm_s).c_str(),
                      dur(act_s).c_str(), kilo(all.usage.prompt).c_str(), kilo(all.usage.cached).c_str(), kilo(all.usage.completion).c_str(), m.timeline.size(),
                      all.tally.calls, all.tally.looks, all.tally.staged, all.tally.adopted);
  if (!ll.message.empty())
    wrapped(color(TaskStatus::Kind::Warn), ll.message);
}

// ask_human: 逐题点选 (已按推荐预选; 末项 "其他" 自己写意见) + 附言; edit = Choose 中在问的那个调用. 返回 = 按了 Start
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

using Names = std::set<std::string>; // 入库的英文名

// 结局的颜色 (入库另标绿; agent 看不见的灰)
TaskStatus::Kind kind_of(const Names &in, const Result &r) {
  if (!r.visible)
    return TaskStatus::Kind::Muted;
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return TaskStatus::Kind::Error;
  case Verdict::Weights:
    return in.count(r.name) ? TaskStatus::Kind::Ready : TaskStatus::Kind::None;
  case Verdict::Null:
    return r.top ? TaskStatus::Kind::Warn : TaskStatus::Kind::Muted;
  case Verdict::Pending:
    return TaskStatus::Kind::Muted;
  }
  return TaskStatus::Kind::None;
}

// 结局两档 (同 Prompt 的 Brief / Detail): 一行 + 入库标记 / 多行 (去末换行)
std::string brief(const Names &in, const Result &r) { return Brief(r) + (in.count(r.name) ? " · 已入库" : ""); }
std::string detail(const Result &r) {
  std::string s = Detail(r);
  while (!s.empty() && s.back() == '\n')
    s.pop_back();
  return s;
}

// 一个候选文件的结局: 一行, 悬浮看全文
void result_line(const Names &in, const Result &r) {
  wrapped(color(kind_of(in, r)), brief(in, r));
  tip_long(detail(r));
}

// 时间线的树: 组 (每 kGroupRounds 轮, 超过一组才分) → 轮 → 调用 / 文本块; 每层标题即摘要.
// 新一轮来时只留最近 kOpenRounds 轮 (及其所在组) 展开, 更早的自动折叠; 人手动开合的保持到下一轮来
constexpr size_t kGroupRounds = 10, kOpenRounds = 2;
constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAvailWidth | ImGuiTreeNodeFlags_DrawLinesFull;
constexpr size_t kCallChars = 96; // 调用一行在标题里的字数

// eval_factor 调用: 标题 = 名 + 回测状态 / 用时 + 结局一行 (悬浮看表达式); 展开 = 表达式, 思路, 结局全文.
// 默认收起; 定论那刻 (换节点 id, 开合重定) 未落 / BROKEN / 有参数组的自动展开
void cand_block(const Mine &m, const Act &a, const Lab::Live &ll) {
  const Trial &k = std::get<Trial>(a.got);
  const Staged &st = k.st;
  const Result &res = st.res;
  const bool done = a.span.ended();
  if (done)
    ImGui::SetNextItemOpen(res.verdict == Verdict::Unstaged || res.verdict == Verdict::Broken || res.verdict == Verdict::Weights, ImGuiCond_Once);
  ImGui::PushStyleColor(ImGuiCol_Text, kColHead);
  const bool open = ImGui::TreeNodeEx(done ? "settled" : "cand", ImGuiTreeNodeFlags_DrawLinesFull, "%s  %s", st.cand.name.c_str(), st.cand.name_cn.c_str());
  ImGui::PopStyleColor();
  if (!open && ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 50.0f);
    ImGui::TextColored(ImVec4(0.9f, 0.8f, 0.55f, 1.0f), "%s", st.cand.expr.c_str());
    if (!st.cand.note.empty())
      ImGui::TextDisabled("%s", st.cand.note.c_str());
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  }
  ImGui::SameLine();
  const double t = m.now();
  if (done) {
    std::string took;
    if (k.search.began())
      took = "Search " + dur(k.search.len(t));
    if (k.run.began())
      took += (took.empty() ? "" : " · ") + std::string("Run ") + dur(k.run.len(t));
    if (!took.empty()) {
      ImGui::TextDisabled("%s ·", took.c_str());
      ImGui::SameLine();
    }
    ImGui::TextColored(color(kind_of(m.adopted, res)), "%s", brief(m.adopted, res).c_str());
  } else if (a.span.running() && m.live()) {
    ImGui::TextColored(color(TaskStatus::Kind::Busy), "%s… %.0f%% · %s", step_name(k), ll.frac * 100.0, dur(a.span.len(t)).c_str());
  } else {
    ImGui::TextDisabled("%s", m.live() ? "排队" : "未评");
  }
  if (open) {
    wrapped(ImVec4(0.9f, 0.8f, 0.55f, 1.0f), st.cand.expr);
    if (!st.cand.note.empty())
      wrapped_dim(st.cand.note);
    if (done)
      wrapped(color(kind_of(m.adopted, res)), detail(res));
    ImGui::TreePop();
  }
}

// 一个调用: eval_factor 同候选; ask_human = 问答 (edit = 在问它); 其余 = 调用一行 + 状态 / 用时, 展开 = 交给 agent 的结果. 返回 = 按了 Start
bool act_block(const Mine &m, Act &a, size_t i, bool edit, const Lab::Live &ll) {
  ImGui::PushID(static_cast<int>(i));
  bool start = false;
  const double t = m.now();
  if (std::holds_alternative<Trial>(a.got)) {
    cand_block(m, a, ll);
  } else if (Asked *q = std::get_if<Asked>(&a.got)) {
    ImGui::PushStyleColor(ImGuiCol_Text, color(edit ? TaskStatus::Kind::Warn : TaskStatus::Kind::None));
    const bool open = ImGui::TreeNodeEx("ask", kNode | ImGuiTreeNodeFlags_DefaultOpen, "问研究员 · %zu 个问题%s", q->qs.size(), edit ? " · 等你答" : "");
    ImGui::PopStyleColor();
    if (open) {
      start = asked_block(*q, edit);
      ImGui::TreePop();
    }
  } else {
    const ToolDef &d = def(a.call.tool);
    std::string label = std::string(d.cn) + " · " + clip(a.call.text(), kCallChars);
    TaskStatus::Kind kind = TaskStatus::Kind::None;
    if (a.span.ended())
      label += " · " + dur(a.span.len(t)) + (a.ok ? "" : " · 没做成"), kind = a.ok ? TaskStatus::Kind::None : TaskStatus::Kind::Warn;
    else if (a.span.running() && m.live())
      label += " · 执行中… " + dur(a.span.len(t)), kind = TaskStatus::Kind::Busy;
    else
      label += m.live() ? " · 排队" : " · 未做", kind = TaskStatus::Kind::Muted;
    ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
    fold("act", ImGuiTreeNodeFlags_None, label, a.out, false, kBoxLines, false);
    ImGui::PopStyleColor();
  }
  ImGui::PopID();
  return start;
}

// 一轮的标题 (折起来也看得出这轮干了什么)
std::string round_head(const Mine &m, size_t i, const Flow *live, TaskStatus::Kind &kind) {
  const Mine::Round &rd = m.timeline[i];
  std::string h = "第 " + std::to_string(i + 1) + " 轮 · " + def(rd.kind).cn + " · +" + dur(rd.span.t0);
  kind = TaskStatus::Kind::None;
  if (live) {
    kind = live->stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy;
    return h + " · " + doing(*live) + (live->topic.empty() ? "" : " · " + live->topic);
  }
  const bool replied = rd.span.ended();
  if (replied)
    h += " · 回复 " + dur(rd.span.len(m.now()));
  if (!rd.err.empty()) {
    kind = TaskStatus::Kind::Error;
    return h + (replied && !rd.reply.ok() ? " · 回复失败" : " · 不合协议");
  }
  if (rd.acts.empty())
    return h;
  const Mine::Tally &c = rd.tally;
  h += " · 调用 " + std::to_string(c.calls);
  if (c.looks > 0)
    h += " · 观察 " + std::to_string(c.looks);
  if (c.staged + c.unstaged > 0)
    h += " · 候选 " + std::to_string(c.staged) + (c.unstaged ? " (未落 " + std::to_string(c.unstaged) + ")" : "") + " · 有权重 " + std::to_string(c.hits);
  if (c.submits > 0)
    h += " · 提交 " + std::to_string(c.submits) + " (入库 " + std::to_string(c.adopted) + ")";
  if (c.issues > 0)
    h += " · issue " + std::to_string(c.issues);
  if (std::any_of(rd.acts.begin(), rd.acts.end(), [](const Act &a) { return a.call.tool == Tool::Finish; }))
    h += " · 宣布挖完";
  if (i + 1 == m.timeline.size() && m.phase() == Phase::Choose)
    h += " · 等你答", kind = TaskStatus::Kind::Warn;
  else if (i + 1 == m.timeline.size() && m.phase() == Phase::Act)
    h += " · 执行中… " + std::to_string(m.acting()) + "/" + std::to_string(rd.acts.size()), kind = TaskStatus::Kind::Busy;
  else if (c.adopted > 0)
    kind = TaskStatus::Kind::Ready;
  return h;
}

// 一轮: 发出 → 思考 → 回复 (逐个调用 + 原文); 在收的一轮画 live. fresh = 新一轮刚来 (重定开合); s.jump = 末轮展开并滚到它. 返回 = 按了 Start
bool round_block(State &s, Mine &m, size_t i, const Flow &f, bool fresh) {
  Mine::Round &rd = m.timeline[i];
  const bool replied = rd.span.ended(), last = i + 1 == m.timeline.size();
  const llm::Reply &r = replied ? rd.reply : s.chat_live.reply;
  const bool receiving = !replied && f.stage != Flow::Idle;

  TaskStatus::Kind kind = TaskStatus::Kind::None;
  const std::string head = round_head(m, i, receiving ? &f : nullptr, kind);
  if (s.jump && last)
    ImGui::SetNextItemOpen(true, ImGuiCond_Always);
  else if (fresh)
    ImGui::SetNextItemOpen(i + kOpenRounds >= m.timeline.size(), ImGuiCond_Always);
  ImGui::PushID(static_cast<int>(i));
  ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
  const bool open = ImGui::TreeNodeEx("round", kNode, "%s", head.c_str());
  ImGui::PopStyleColor();
  if (s.jump && last)
    ImGui::SetScrollHereY(0.0f);
  if (!open) {
    ImGui::PopID();
    return false;
  }
  fold("send", ImGuiTreeNodeFlags_None, "发出 " + chars(rd.user), rd.user, false, kBoxLines, false);
  if (receiving && f.stage == Flow::Wait)
    ImGui::TextColored(color(f.stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy), "%s", doing(f).c_str());
  if (!r.thinking.empty()) {
    const bool now = receiving && f.stage == Flow::Think;
    if (last) {
      if (s.think_round != i)
        s.think_round = i, s.think_open = false, s.think_touched = false;
      if (!s.think_touched && s.think_open != now) {
        ImGui::SetNextItemOpen(now, ImGuiCond_Always);
        s.think_open = now;
      }
    }
    std::string label = now ? doing(f) : "思考了 " + dur(rd.think.len(m.now()));
    if (const std::string t = now ? f.topic : topic(r.thinking, false); !t.empty())
      label += " · " + t;
    if (fold("think", ImGuiTreeNodeFlags_None, label + " · " + chars(r.thinking), r.thinking, true, kThinkLines, true) && last)
      s.think_touched = true;
  }
  if (receiving && f.stage == Flow::Write)
    fold("text_live", ImGuiTreeNodeFlags_DefaultOpen, doing(f) + " · " + chars(r.text), r.text, false, kThinkLines, true);
  bool start = false;
  if (replied) {
    std::string line = "回复 · " + dur(rd.span.len(m.now())) + (rd.think.began() ? " (思考 " + dur(rd.think.len(m.now())) + ")" : "");
    if (r.usage.prompt > 0)
      line += " · token 输入 " + kilo(r.usage.prompt) + " (缓存 " + kilo(r.usage.cached) + ") / 输出 " + kilo(r.usage.completion);
    ImGui::TextColored(color(r.ok() ? TaskStatus::Kind::None : TaskStatus::Kind::Error), "%s", line.c_str());
    if (!rd.err.empty())
      wrapped(color(TaskStatus::Kind::Error), rd.err);
    for (size_t j = 0; j < rd.acts.size(); ++j)
      start |= act_block(m, rd.acts[j], j, last && m.phase() == Phase::Choose && j == m.acting(), s.lab_live);
    fold("text", ImGuiTreeNodeFlags_None, "原文 " + chars(r.text), r.text, false, kBoxLines, false);
  }
  ImGui::TreePop();
  ImGui::PopID();
  return start;
}

// 一组轮的标题: 轮次 / 起始时刻 / 调用 / 观察 / 候选 / 有权重 / 入库
std::string group_head(const Mine &m, size_t g0, size_t g1) {
  Mine::Tally c;
  for (size_t i = g0; i < g1; ++i)
    c += m.timeline[i].tally;
  return "第 " + std::to_string(g0 + 1) + "–" + std::to_string(g1) + " 轮 · +" + dur(m.timeline[g0].span.t0) + " · 调用 " + std::to_string(c.calls) + " · 观察 " +
         std::to_string(c.looks) + " · 候选 " + std::to_string(c.staged) + " · 有权重 " + std::to_string(c.hits) + " · 入库 " + std::to_string(c.adopted);
}

// 结束汇总: 计数 + 本会话有参数组的候选 (入库的在前, 再按 score 降序; 先列 kSummaryRows 个, 其余折起) + 结束报告
constexpr size_t kSummaryRows = 5;
void summary_block(const Mine &m) {
  const Mine::Tally &c = m.total.tally;
  std::vector<const Result *> hits;
  for (const Mine::Round &rd : m.timeline)
    for (const Act &a : rd.acts)
      if (const Trial *k = std::get_if<Trial>(&a.got); k && k->st.res.verdict == Verdict::Weights)
        hits.push_back(&k->st.res);
  std::stable_sort(hits.begin(), hits.end(), [&](const Result *a, const Result *b) {
    const bool ia = m.adopted.count(a->name) > 0, ib = m.adopted.count(b->name) > 0;
    return ia != ib ? ia : a->best()->score > b->best()->score;
  });
  ImGui::SeparatorText(("结束 · +" + dur(m.now())).c_str());
  wrapped(color(TaskStatus::Kind::Ready), m.reason);
  ImGui::TextDisabled("%zu 轮 · 调用 %d (观察 %d) · 候选 %d · 有参数组 %d · 提交 %d · 入库 %d", m.timeline.size(), c.calls, c.looks, c.staged, c.hits, c.submits, c.adopted);
  const auto row = [&](size_t i) {
    const Result &r = *hits[i];
    ImGui::Bullet();
    ImGui::TextColored(kColHead, "%s  %s", r.name.c_str(), r.name_cn.c_str());
    ImGui::SameLine();
    result_line(m.adopted, r);
  };
  for (size_t i = 0; i < std::min(hits.size(), kSummaryRows); ++i)
    row(i);
  if (hits.size() > kSummaryRows && ImGui::TreeNodeEx("more", kNode, "其余 %zu 个", hits.size() - kSummaryRows)) {
    for (size_t i = kSummaryRows; i < hits.size(); ++i)
      row(i);
    ImGui::TreePop();
  }
  fold("report", ImGuiTreeNodeFlags_None, "结束报告", m.report, false, kBoxLines, false);
}

// 过程: 全会话 (分组 → 逐轮) + 结束汇总. 钉在底部时跟新内容走; 放开后右下角给 "回到最新".
// s.flow_rounds = 上一帧画过的轮数 (变了 = 新一轮来, 重定开合)
void timeline_panel(State &s, Mine &m, const Flow &f, bool to_end) {
  ImGui::BeginChild("##flow", ImVec2(0, 0), false);
  const float y = ImGui::GetScrollY();
  if (to_end || y >= ImGui::GetScrollMaxY() - 2.0f)
    s.pinned = true;
  else if (y < s.flow_y - 0.5f)
    s.pinned = false;
  if (ImGui::IsWindowHovered(ImGuiHoveredFlags_ChildWindows) && ImGui::IsMouseClicked(ImGuiMouseButton_Left))
    s.pinned = false; // 点开合 = 在读: 不把读的地方拖走
  bool start = false;
  if (m.phase() == Phase::None) {
    ImGui::TextDisabled("选好作用域按 New: 系统把规则与工具表打包发给 agent, 它每条回复发一串工具调用 (看数据 / 回测 / 提交), 系统执行后把结果交回");
  } else {
    const size_t n = m.timeline.size();
    const bool fresh = n != s.flow_rounds, grouped = n > kGroupRounds;
    s.flow_rounds = n;
    for (size_t g0 = 0; g0 < n; g0 += kGroupRounds) {
      const size_t g1 = std::min(n, g0 + kGroupRounds);
      ImGui::PushID(static_cast<int>(g0));
      bool open = true;
      if (grouped) {
        if (s.jump && g1 == n)
          ImGui::SetNextItemOpen(true, ImGuiCond_Always);
        else if (fresh)
          ImGui::SetNextItemOpen(g1 + kOpenRounds > n, ImGuiCond_Always);
        const bool busy = g1 == n && (m.chat.busy() || m.lab.busy());
        ImGui::PushStyleColor(ImGuiCol_Text, color(busy ? TaskStatus::Kind::Busy : TaskStatus::Kind::None));
        open = ImGui::TreeNodeEx("group", kNode | ImGuiTreeNodeFlags_Framed, "%s", group_head(m, g0, g1).c_str());
        ImGui::PopStyleColor();
      }
      if (open) {
        for (size_t i = g0; i < g1; ++i)
          start |= round_block(s, m, i, f, fresh);
        if (grouped)
          ImGui::TreePop();
      }
      ImGui::PopID();
    }
  }
  if (start) {
    m.start();
    s.pinned = true;
  }
  if (m.phase() == Phase::Report)
    summary_block(m);
  if (s.jump)
    s.jump = false, s.pinned = false;
  else if (s.pinned)
    ImGui::SetScrollHereY(1.0f);
  const bool behind = !s.pinned && ImGui::GetScrollY() < ImGui::GetScrollMaxY() - 2.0f;
  const ImVec2 corner(ImGui::GetWindowPos().x + ImGui::GetWindowWidth() - ImGui::GetStyle().ScrollbarSize, ImGui::GetWindowPos().y + ImGui::GetWindowHeight());
  s.flow_y = ImGui::GetScrollY();
  ImGui::EndChild();
  // 浮在时间线右下角 (单开子窗口: 叠在内容上也点得到); 点了下一帧钉回底部
  if (behind) {
    const ImGuiStyle &st = ImGui::GetStyle();
    const char *label = "回到最新";
    const ImVec2 sz(ImGui::CalcTextSize(label).x + 2.0f * st.FramePadding.x, ImGui::GetFrameHeight());
    ImGui::SetCursorScreenPos(ImVec2(corner.x - sz.x - st.WindowPadding.x, corner.y - sz.y - st.WindowPadding.y));
    ImGui::BeginChild("##latest", sz, ImGuiChildFlags_None, ImGuiWindowFlags_NoScrollbar | ImGuiWindowFlags_NoBackground);
    if (ImGui::Button(label))
      s.pinned = true;
    ImGui::EndChild();
  }
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

// 特征 / 算子 / 候选 三页的数据 = Lab (会话中候选随评估实时更新). 无在跑会话时随作用域重绑; 绑不上 (该层无可搜持有期 / 评估在收尾) = 空表
struct View {
  const Context &ctx;
  const std::vector<Result> &results;
  const Names &adopted;
};

View view_of(const SharedData &data, State &s) {
  Mine &m = *s.mine;
  const Scope &sc = s.opt.sc;
  if (!m.live() && !m.lab.busy() && m.lab.scope() != sc && !catalog::holds(data, sc.level).empty())
    m.lab.bind(data, sc);
  static const Context kNoCtx;
  static const std::vector<Result> kNoResults;
  static const Names kNone;
  if (m.lab.scope() != sc)
    return {kNoCtx, kNoResults, kNone};
  return {m.lab.ctx(), m.lab.results(), m.phase() != Phase::None && m.setup.sc == sc ? m.adopted : kNone};
}

void results_table(const View &v) {
  const std::vector<Result> &results = v.results;
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
    ImGui::TextColored(color(kind_of(v.adopted, r)), "%s", v.adopted.count(r.name) ? "入库" : name(r.verdict));
    tip_long(detail(r));
    // 统计列: 最佳权重; 无显著则代表点 (灰)
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

// ---- 参考: 发给 agent 的特征表 / 算子表 ----
void leaves_panel(State &s, const Context &c) {
  const std::vector<Leaf> &leaves = c.leaves;
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

void ops_panel(State &s, const Context &c) {
  const std::vector<Op> &ops = c.ops;
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

// 过程 (全宽时间线) / 候选表 / 特征表 / 算子表 (后三页不必开会话, 随作用域); 刚进 Choose 切回过程并滚到底 (问题在那)
void render_mine(const SharedData &data, State &s) {
  Mine &m = *s.mine;
  scope_row(data, s, m.live());
  session_row(data, s);
  m.chat.live(s.chat_live);
  m.lab.live(s.lab_live);
  const Flow f = flow_of(s, m);
  now_row(s, m, f);
  ImGui::Separator();
  const Phase ph = m.phase();
  const bool to_choose = ph == Phase::Choose && s.seen_phase != Phase::Choose;
  s.seen_phase = ph;
  if (!ImGui::BeginTabBar("##mine_tabs"))
    return;
  if (ImGui::BeginTabItem("过程###flow", nullptr, to_choose || s.jump ? ImGuiTabItemFlags_SetSelected : ImGuiTabItemFlags_None)) {
    timeline_panel(s, m, f, to_choose);
    ImGui::EndTabItem();
  }
  const View v = view_of(data, s);
  const std::string cands = "候选 (" + std::to_string(v.results.size()) + ")###cands";
  if (ImGui::BeginTabItem(cands.c_str())) {
    results_table(v);
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem("特征")) {
    leaves_panel(s, v.ctx);
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem("算子")) {
    ops_panel(s, v.ctx);
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
  ImGui::TextColored(kColHead, "提问");
  ImGui::Checkbox("允许 agent 向你提问 (ask_human)", &o.ask);
  tip("开: agent 可在任一轮提一次问题 (每题带推荐), 等你选答后接着做\n关: 工具表里没有 ask_human, agent 自定因子与方向, 全自动挖掘");
  ImGui::SameLine();
  ImGui::TextDisabled("下次 New 生效");

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
    if (m.phase() == Phase::Act)
      return {TaskStatus::Kind::Busy, "call " + std::to_string(m.acting() + 1) + "/" + std::to_string(m.timeline.back().acts.size())};
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
