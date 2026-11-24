// Task Agent — 见头文件
#include "gui/task_agent/TaskAgent.hpp"
#include "disk/Disk.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Remote.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Bench.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <future>
#include <initializer_list>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace GUI::Agent {

namespace {

using namespace ui;
using Phase = Mine::Phase;

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

constexpr int kBackendN = static_cast<int>(kNum<llm::Backend>); // 界面按下标 (int) 选后端

bool has_key(int backend) { return !llm::key(static_cast<llm::Backend>(backend)).empty(); }

// Mine / Tools / Config 页的选项 (ui.json 读写同一份字段清单): 作用域 (Mine 与 Tools 页共用, 含开放的叶 / 算子) / 大脑 / 连接 / 模型 / 评估后端 / 预算 (下次 New / 继续生效) /
// 入库门槛 / 可否提问 / 会话约束 (下次开会话生效: New 或外部 agent 的 open) + Config 页在看的模板
struct Options {
  Scope sc;
  bool mcp = false; // 大脑 = 外部 agent (经 MCP open 接上); 否则 = 大模型 (下面的后端 / 模型)
  int backend = 0;
  std::string model; // 该后端嗅探表里的 id; 空 = 还没嗅探到
  std::array<bool, kBackendN> via_proxy = [] {
    std::array<bool, kBackendN> a{};
    for (int b = 0; b < kBackendN; ++b)
      a[b] = kBackends[b].proxy;
    return a;
  }(); // 每后端直连 / 代理, 代理地址共用
  std::string proxy_host = kProxyHost;
  int proxy_port = kProxyPort;
  int gpu = -1; // -1 = 未定 (有 GPU 默认 GPU)
  Gate gate{kGateRic};
  Budget budget;
  bool ask = true;    // agent 可用 ask_human 向人提一次问
  bool strict = true; // 守会话约束
  int prompt = 0;     // Msg 下标
  bool operator==(const Options &) const = default;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, sc, mcp, backend, model, via_proxy, proxy_host, proxy_port, gpu, gate, budget, ask, strict, prompt)

llm::Net net_of(const Options &o, int backend) { return {o.via_proxy[backend], o.proxy_host, o.proxy_port}; }
Mine::Setup setup_of(const Options &o) { return {.sc = o.sc, .gate = o.gate, .ask = o.ask, .strict = o.strict}; }
Mine::Leg leg_of(const Options &o, bool gpu) {
  return {.llm = {static_cast<llm::Backend>(o.backend), net_of(o, o.backend), o.model}, .mcp = o.mcp, .gpu = gpu, .budget = o.budget};
}

// 模型嗅探: 每后端一个后台 llm::models (按该后端当前连接方式); cancel 须声明在 jobs 前 (future 析构 join 时它还活着)
struct Sniffer {
  std::atomic<bool> cancel{false};
  std::array<std::future<llm::Models>, kBackendN> jobs;
  std::array<llm::Models, kBackendN> got;

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

struct Ui {
  std::unique_ptr<Mine> mine;     // 首帧建 (Destroy 释放: 对话线程 / 评估 worker 与常驻块)
  std::unique_ptr<Bench> bench;   // Tools 页的工作台 (首次进 Tools 页建)
  std::unique_ptr<Remote> remote; // 首帧建即监听: 外部 agent 经 MCP 当会话 mine 的大脑
  Options opt;
  std::optional<Options> opt_saved; // 上次落盘的选项; operator== 判变, 改了才序列化写盘
  Sniffer sniff;
  // Config 页提示词编辑
  std::string prompt_text;
  bool prompt_loaded = false;
  // Mine 页
  ImGuiTextFilter leaf_filter, op_filter, tool_filter, note_filter, cand_filter;
  std::vector<catalog::PoolRow> pool_rows; // 池下拉 (打开时扫)
  Phase seen_phase = Phase::None;          // 刚进 Choose: 切到过程页一次
  size_t flow_rounds = 0;                  // 过程页上一帧画过的轮数
  std::string message;
  // 本会话要求 (只随下次 New / Test 发, 不落 ui.json); fold_remark = 刚开会话, 下一帧把它折起
  std::string remark;
  bool fold_remark = false;
  // 过程页的跟随: pinned = 钉在底部跟新内容 (人往上滚 / 点开合即放开, 回到底部再钉); jump = 点了状态行, 跳到当前轮
  bool pinned = true, jump = false;
  float flow_y = 0.0f; // 上一帧的滚动位置
  // 两个 worker 在跑那件事的样子 (每帧 live 同步)
  ChatLive chat_live;
  LabLive lab_live;
  // 在收那轮: 增量总量与它上次变的时刻 (量静默)
  size_t live_n = 0;
  double live_t = 0.0;
  // 在收那轮思考块的自动开合: 思考时开, 转正文收; 人点过就不再动
  size_t think_round = SIZE_MAX;
  bool think_open = false, think_touched = false;
  // 载入菜单的会话一览 (打开菜单时扫)
  std::vector<Saved> saved;
  // 插话的草稿 (按 "插话" 进 Mine::memo 排队)
  std::string memo;
  // 跳到某个调用: go = 要去的 (过程页展开它所在的组 / 轮并滚过去, 用掉即清); flash = 刚跳到的, 高亮 kFlashS 秒
  std::optional<Ref> go;
  Ref flash;
  double flash_t = -1.0;
  // 过程页各轮展开着的 "agent 看到的上下文" (收起即丢): 键 = 轮; sig = 算它时的样子 (轮数 / 这轮发出时刻 / 消息与插话字节数), 变了重算
  struct Ctx {
    std::tuple<size_t, double, size_t, size_t> sig;
    std::string body;
  };
  std::unordered_map<size_t, Ctx> ctx;
  // 回退确认框: 回到第 k 轮; loss = 会丢的; open = 下一帧打开
  struct Rewind {
    int k = 0;
    Mine::Loss loss;
    bool open = false;
  } rewind;
  // 候选页: 选中的候选文件与它的 Search 日志 (换了选中才重读)
  std::string pick, pick_log, pick_log_of;
  // 记录页: 只看待办 / 在写的回复草稿
  bool only_open = false;
  std::string reply;
};

// ---- 模型嗅探 ----
std::string first_model(const Ui &s, int backend) { return s.sniff.got[backend].list.empty() ? std::string() : s.sniff.got[backend].list.front().id; }
void sniff(Ui &s, int backend) { s.sniff.start(backend, net_of(s.opt, backend)); }
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
std::string model_label(const llm::Model &m) { return m.name == m.id ? m.id : m.name + "  (" + m.id + ")"; }
bool model_ok(const Ui &s) {
  const int b = s.opt.backend;
  return !s.sniff.busy(b) && s.sniff.got[b].error.empty() && !s.opt.model.empty(); // 按当前连接方式嗅探成功且已选
}

// 回测上 GPU (Config 页未定 = 有 GPU 即用)
bool use_gpu(const Ui &s) { return catalog::gpu() && s.opt.gpu != 0; }
// Tools 页有调用在跑 (含等研究员答)
bool bench_busy(const Ui &s) { return s.bench && s.bench->busy(); }
// 作用域 (含开放的叶 / 算子; Mine 与 Tools 页共用一份) 锁着: Mine 页有会话, 或 Tools 页有调用在跑
bool scope_locked(const Ui &s) { return s.mine->held() || bench_busy(s); }

// ---- 选项落盘 (output/config/agent/ui.json) ----
bool ui_json_valid(const nlohmann::json &j) {
  if (!j.is_object() || !j.contains("sc") || !j["sc"].is_object() || !j.contains("gate") || !j["gate"].is_object() || !j.contains("budget") ||
      !j["budget"].is_object())
    return false;
  const auto &sc = j["sc"], &gt = j["gate"], &bg = j["budget"];
  const bool scope_ok = sc.contains("pool") && sc["pool"].is_string() && sc.contains("level") && sc["level"].is_number_integer() && sc.contains("hold") &&
                        sc["hold"].is_number_integer() && sc.contains("lob") && sc["lob"].is_boolean() && sc.contains("years") && sc["years"].is_number_integer() &&
                        sc.contains("leaves") && sc["leaves"].is_object() && sc.contains("ops") && sc["ops"].is_object();
  return scope_ok && gt.contains("ric") && gt["ric"].is_number() && bg.contains("rounds") && bg["rounds"].is_number_integer() && bg.contains("minutes") &&
         bg["minutes"].is_number_integer() && bg.contains("ktokens") && bg["ktokens"].is_number_integer() && j.contains("backend") &&
         j["backend"].is_number_integer() && j.contains("model") && j["model"].is_string() && j.contains("via_proxy") && j["via_proxy"].is_array() &&
         j["via_proxy"].size() == kBackendN && j.contains("proxy_host") && j["proxy_host"].is_string() && j.contains("proxy_port") &&
         j["proxy_port"].is_number_integer() && j.contains("gpu") && j["gpu"].is_number_integer() && j.contains("ask") && j["ask"].is_boolean() &&
         j.contains("strict") && j["strict"].is_boolean() && j.contains("mcp") && j["mcp"].is_boolean() && j.contains("prompt") && j["prompt"].is_number_integer();
}
void load_ui(Ui &s) {
  bool ok = false;
  if (disk::exists(disk::AgentUiFile)) {
    const nlohmann::json j = nlohmann::json::parse(disk::read(disk::AgentUiFile), nullptr, false);
    if (ui_json_valid(j)) {
      j.get_to(s.opt);
      s.opt.prompt = std::clamp(s.opt.prompt, 0, static_cast<int>(kNum<Msg>) - 1);
      ok = true;
    }
  }
  const Options &o = s.opt;
  ok = ok && o.backend >= 0 && o.backend < kBackendN && o.proxy_port > 0 && o.proxy_port < 65536 && o.gpu >= -1 && o.gpu <= 1 &&
       o.prompt >= 0 && o.prompt < static_cast<int>(kNum<Msg>) && o.sc.years >= 1 && o.budget.rounds >= 0 && o.budget.minutes >= 0 && o.budget.ktokens >= 0;
  if (!ok)
    s.opt = Options{};
  if (!catalog::gpu())
    s.opt.gpu = 0;
  if (ok)
    s.opt_saved = s.opt;
  else
    s.opt_saved.reset();
}
void save_ui(Ui &s) {
  if (s.opt_saved && *s.opt_saved == s.opt)
    return;
  disk::write(disk::AgentUiFile, {}, nlohmann::ordered_json(s.opt).dump(2));
  s.opt_saved = s.opt;
}

// 首帧 (Update 或 Draw 先到者): 建会话台与外部 agent 接口 (即监听), 读选项, 各后端嗅探模型
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
// 作用域的池 / 层不在目录里 (改过名 / 删了) → 取首个
void fix_scope(Scope &sc) {
  const std::vector<Choice> pools = catalog::pools();
  const std::vector<Choice> lvs = catalog::levels();
  if (std::none_of(pools.begin(), pools.end(), [&](const Choice &p) { return p.name == sc.pool; }))
    sc.pool = pools.front().name;
  if (std::none_of(lvs.begin(), lvs.end(), [&](const Choice &l) { return l.key == sc.level; }))
    sc.level = lvs.front().key;
}

// 思考在想什么: 末个独占一行的 **小标题** / # 标题; 没有则 (live) 末个非空行的开头
constexpr size_t kTopicChars = 48;
std::string topic(std::string_view s, bool live) {
  std::string_view tail;
  for (size_t end = s.size(); end > 0;) {
    const size_t nl = s.rfind('\n', end - 1);
    const size_t b = nl == std::string_view::npos ? 0 : nl + 1;
    const std::string_view line = text::trim(s.substr(b, end - b));
    end = b == 0 ? 0 : nl;
    if (line.empty())
      continue;
    if (line.size() > 4 && line.starts_with("**") && line.ends_with("**"))
      return text::head(text::trim(line.substr(2, line.size() - 4)), kTopicChars);
    if (const size_t h = line.find_first_not_of('#'); h > 0 && h <= 6 && h < line.size() && line[h] == ' ')
      return text::head(text::trim(line.substr(h)), kTopicChars);
    if (tail.empty())
      tail = line;
  }
  return live ? text::head(tail, kTopicChars) : std::string();
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
  bool mcp = false; // 大脑是外部 agent: 只有等它交批, 没有流
  bool stalled() const { return stage != Idle && !mcp && quiet > kQuietS; }
};
// 阶段由末轮的 Span 定, 静默由镜像 s.chat_live 的增量量 (外部 agent: 恒为等)
Flow flow_of(Ui &s, const Mine &m) {
  Flow f;
  if (m.timeline.empty() || !m.timeline.back().span.running())
    return f;
  const Round &rd = m.timeline.back();
  if (m.leg.mcp) {
    f.stage = Flow::Wait, f.mcp = true, f.since = rd.span.len(m.now());
    return f;
  }
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
  std::string h = f.mcp                    ? "等外部 agent 交这一批… " + dur(f.since)
                  : f.stage == Flow::Wait  ? "等待首包… " + dur(f.since)
                  : f.stage == Flow::Think ? "思考中… " + dur(f.since)
                                           : "输出中… " + dur(f.writing);
  if (f.stalled())
    h += " · 已 " + dur(f.quiet) + " 无新内容";
  return h;
}

// 在做的调用 (Act / Choose 中; 否则空)
const Act *acting(const Mine &m) {
  if (m.phase() != Phase::Act && m.phase() != Phase::Choose)
    return nullptr;
  const std::vector<Act> &as = m.timeline.back().acts;
  return m.acting() < as.size() ? &as[m.acting()] : nullptr;
}
const char *step_name(const TrialRes &k) { return k.run.began() ? "Run" : "Search"; }

// ---- 会话行: New / Test / 载入 / 继续 / 交答案 / Stop / Cancel / 关闭 ----
void load_session(const SharedData &data, Ui &s, const Saved &x) {
  if (!catalog::ready(data, x.sc, false)) {
    s.message = "这段会话的作用域未就绪 (资产轴未扫描或标签库未建)";
    return;
  }
  s.message.clear();
  s.mine->load(data, x.dir);
  s.opt.sc = s.mine->setup.sc;
  s.fold_remark = true, s.flow_rounds = 0, s.pinned = true;
}

// 载入菜单: 本作用域存过的会话, 新到旧; 一行 = 时刻 / 轮数 / 停下原因 / 候选与入库 / 大脑, 悬浮看目录与要求
void saved_menu(const SharedData &data, Ui &s) {
  if (s.saved.empty()) {
    ImGui::TextDisabled("本作用域 (%s) 还没有存过的会话", catalog::label(s.opt.sc).c_str());
    return;
  }
  for (const Saved &x : s.saved) {
    const nlohmann::ordered_json &b = x.brief;
    const std::string why = b.at("live").get<bool>() ? "没停稳就退出了" : b.at("reason").get<std::string>();
    const std::string label = stamp_text(x.stamp) + (x.test ? " · 自检" : "") + " · " + std::to_string(b.at("rounds").get<int>()) + " 轮 · " + text::head(why, 24) + " · 候选 " +
                              std::to_string(b.at("cands").get<int>()) + " · 入库 " + std::to_string(b.at("adopted").size()) + " · " + b.at("brain").get<std::string>() + "###" + x.dir;
    if (ImGui::Selectable(label.c_str())) {
      load_session(data, s, x);
      ImGui::CloseCurrentPopup();
    }
    const std::string remark = b.at("remark").get<std::string>();
    tip_long(x.dir + "\n用时 " + dur(b.at("elapsed").get<double>()) + " · 假设 " + std::to_string(b.at("hyps").get<int>()) + " · 有参数组 " + std::to_string(b.at("weights").get<int>()) +
             " · 停下原因: " + why + (remark.empty() ? "" : "\n本会话要求: " + remark));
  }
}

void session_row(const SharedData &data, Ui &s) {
  Mine &m = *s.mine;
  const Options &o = s.opt;
  const bool has_hold = !catalog::holds(data, o.sc.level).empty();
  const bool keyed = has_key(o.backend);
  const bool bound = m.lab.scope() == o.sc;
  const bool has_leaves = bound && !m.lab.ctx().leaves.empty(), has_ops = bound && !m.lab.ctx().ops.empty();
  const char *llm_not = bench_busy(s)  ? "Tools 页的调用在跑 (不并跑, 等它做完或取消)"
                        : o.mcp        ? nullptr
                        : !keyed       ? "所选后端的 key 未填 (output/config/agent/keys.json)"
                        : !model_ok(s) ? "模型未就绪 (Config 页: 嗅探中 / 嗅探失败 / 无可用模型)"
                                       : nullptr;

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !m.live() && m.idle() && !llm_not && has_hold && has_leaves && has_ops;
  const char *why_not = llm_not                   ? llm_not
                        : has_hold && !has_leaves ? "没有开放的特征叶 (本层无叶, 或特征页一个没勾)"
                        : has_hold && !has_ops    ? "没有开放的算子 (算子页一个没勾)"
                                                  : nullptr;
  const auto begin_session = [&](bool test) {
    if (!catalog::ready(data, o.sc, o.gpu == 1)) {
      s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
      return;
    }
    s.message.clear();
    Mine::Setup st = setup_of(o);
    st.test = test;
    st.remark = std::string(text::trim(s.remark));
    m.open(data, std::move(st), leg_of(o, use_gpu(s)));
    s.fold_remark = true;
  };
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0)))
    begin_session(false);
  ImGui::EndDisabled();
  tip(why_not ? why_not
              : "按当前作用域与 Config 开新会话: 打包规则与工具表发给大脑 (大模型; 或外部 agent: 等它经 MCP 调 open 接上), 它调工具看数据、回测、提交\n"
                "资产轴与标签就绪才开得成; 停着的会话已存盘, 换下也能再载入");
  ImGui::SameLine();
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("Test", ImVec2(60, 0)))
    begin_session(true);
  ImGui::EndDisabled();
  tip(why_not ? why_not
              : "开一段自检会话: 同样的系统提示, 首条换成 test 模板, 让 agent 把每个工具都调一遍, 问题记 report_issue, 末了总评\n"
                "候选 / 日志 / 全局记录进 global/test 目录, submit_factor 只查门槛不拷进正式库");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.live() || !m.idle() || bench_busy(s));
  if (ImGui::Button("载入", ImVec2(60, 0))) {
    s.saved = SavedSessions(o.sc);
    ImGui::OpenPopup("##saved");
  }
  ImGui::EndDisabled();
  tip("载入本作用域 (池 / 层 / 持有期) 存过的一段会话: 停在它停下处, 可继续, 也可右键某一轮回退");
  if (ImGui::BeginPopup("##saved")) {
    saved_menu(data, s);
    ImGui::EndPopup();
  }
  ImGui::SameLine();
  const bool can_resume = m.phase() == Phase::Pause && m.idle() && !llm_not;
  ImGui::BeginDisabled(!can_resume);
  if (ImGui::Button("继续", ImVec2(60, 0))) {
    if (!catalog::ready(data, m.setup.sc, o.gpu == 1)) {
      s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
    } else {
      s.message.clear();
      m.resume(data, leg_of(o, use_gpu(s)));
      s.pinned = true;
    }
  }
  ImGui::EndDisabled();
  const std::string resume_tip = m.phase() != Phase::Pause ? std::string("会话停着时可用: 接着停下处走")
                                 : llm_not                 ? std::string(llm_not)
                                                           : "继续 = " + m.hint() + "\n大脑 (可换) / 后端 / 模型 / 评估后端 / 预算按 Config 当前的; 本段预算从此刻起算";
  tip(resume_tip.c_str());
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Choose);
  if (ImGui::Button("交答案", ImVec2(60, 0)))
    m.start();
  ImGui::EndDisabled();
  tip("agent 用 ask_human 提问时可用: 把选好的答案与附言交给 agent, 接着执行余下的调用");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live() || m.stopping());
  if (ImGui::Button("Stop", ImVec2(60, 0)))
    m.stop();
  ImGui::EndDisabled();
  tip("停下: 在收回复的等它收齐 (不执行其中的调用), 在做的调用做完; 之后可继续 / 回退");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live());
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    m.cancel();
  ImGui::EndDisabled();
  tip("立即停下: 在收的回复作废; 在跑的调用作废 (落了文件的回测删掉候选); 之后可继续 / 回退");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Pause || !m.idle());
  if (ImGui::Button("关闭", ImVec2(60, 0)))
    m.close();
  ImGui::EndDisabled();
  tip("放下这段停着的会话 (已存盘, 可再载入); 作用域与特征 / 算子的勾选随之解锁");

  if (m.held()) {
    const Mine::Setup &st = m.setup;
    ImGui::SameLine();
    ImGui::TextDisabled("%s%s · 近 %d 年 · %s · %s · %s · %s", st.test ? "自检 · " : "", catalog::label(st.sc).c_str(), st.sc.years, m.brain().c_str(), m.leg.gpu ? "GPU" : "CPU",
                        st.ask ? "可提问" : "不提问", st.strict ? "守约束" : "不守约束");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", m.log.dir().c_str());
  }
  if (!s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", s.message.c_str());
  }
}

// ---- 工具用量条: 按类拼接, 类内一工具一段 (同色相深浅相间), 段宽 = 次数占比; 类段够宽写类名与占比, 悬浮看各类各工具的次数 / 占比 / 失败数 (表格) ----
constexpr Per<Cat, ImVec4> kCatCols{{{{0.35f, 0.78f, 0.45f, 1.0f}, {0.85f, 0.80f, 0.35f, 1.0f}, {0.95f, 0.62f, 0.25f, 1.0f}, {0.38f, 0.62f, 0.95f, 1.0f}, {0.72f, 0.52f, 0.92f, 1.0f}, {0.58f, 0.58f, 0.58f, 1.0f}}}};

ImU32 shade(ImVec4 c, int i) {
  const float f = 1.0f - 0.2f * static_cast<float>(i % 3);
  return ImGui::ColorConvertFloat4ToU32(ImVec4(c.x * f, c.y * f, c.z * f, c.w));
}

void tools_bar(const Tally &c) {
  const int all = c.all();
  const float w = ImGui::GetContentRegionAvail().x, h = ImGui::GetTextLineHeight();
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  ImGui::InvisibleButton("##tools_bar", ImVec2(w, h));
  const bool hovered = ImGui::IsItemHovered();
  ImDrawList *dl = ImGui::GetWindowDrawList();
  dl->AddRectFilled(p0, ImVec2(p0.x + w, p0.y + h), ImGui::GetColorU32(ImGuiCol_FrameBg));
  if (all == 0) {
    if (hovered)
      ImGui::SetTooltip("还没有调用");
    return;
  }
  Per<Cat, int> per{};
  for (const Cat k : kVals<Cat>)
    per[k] = c.n(k);
  const float mx = ImGui::GetIO().MousePos.x;
  const auto pct = [&](int n) { return 100.0 * n / all; };
  std::optional<Tool> hit;
  float x = p0.x;
  for (const Cat k : kVals<Cat>) {
    if (per[k] == 0)
      continue;
    const float k0 = x;
    int i = 0;
    for (const Tool t : kVals<Tool>) {
      if (def(t).cat != k || c.n(t) == 0)
        continue;
      const float x1 = x + w * static_cast<float>(c.n(t)) / static_cast<float>(all);
      dl->AddRectFilled(ImVec2(x, p0.y), ImVec2(x1, p0.y + h), shade(kCatCols[k], i++));
      if (hovered && mx >= x && mx < x1)
        hit = t;
      x = x1;
    }
    char label[32];
    std::snprintf(label, sizeof(label), "%s %.0f%%", def(k).cn, pct(per[k]));
    if (const float tw = ImGui::CalcTextSize(label).x; tw + 4.0f <= x - k0)
      dl->AddText(ImVec2((k0 + x - tw) * 0.5f, p0.y), IM_COL32(20, 20, 20, 255), label);
    if (k0 > p0.x)
      dl->AddLine(ImVec2(k0, p0.y), ImVec2(k0, p0.y + h), ImGui::GetColorU32(ImGuiCol_WindowBg), 2.0f);
  }
  if (!hovered)
    return;
  ImGui::BeginTooltip();
  constexpr ImGuiTableFlags kTipTable = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_PadOuterX;
  if (ImGui::BeginTable("##tools_tip", 5, kTipTable)) {
    for (const char *head : {"工具", "中文名", "次数", "占比", "失败"})
      ImGui::TableSetupColumn(head);
    ImGui::TableHeadersRow();
    const ImVec4 bad = color(TaskStatus::Kind::Error), dim = ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled);
    const auto right = [](const ImVec4 &col, const char *s) {
      ImGui::SetCursorPosX(ImGui::GetCursorPosX() + ImGui::GetColumnWidth() - ImGui::CalcTextSize(s).x);
      ImGui::TextColored(col, "%s", s);
    };
    const auto row = [&](const ImVec4 &col, const char *name, const char *cn, int n, int fail) {
      char b[32];
      ImGui::TableNextRow();
      ImGui::TableNextColumn(), ImGui::TextColored(col, "%s", name);
      ImGui::TableNextColumn(), ImGui::TextColored(col, "%s", cn);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%d", n), right(col, b);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%.0f%%", pct(n)), right(col, b);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%d", fail), right(fail ? bad : dim, b);
    };
    row(kColHead, "全部", "", all, c.failed());
    for (const Cat k : kVals<Cat>) {
      if (per[k] == 0)
        continue;
      row(kCatCols[k], def(k).cn, "", per[k], c.failed(k));
      for (const Tool t : kVals<Tool>) {
        if (def(t).cat != k || c.n(t) == 0)
          continue;
        const ToolMeta &m = def(t);
        const std::string name = std::string("  ") + m.id;
        row(hit == t ? kColHead : ImGui::GetStyleColorVec4(ImGuiCol_Text), name.c_str(), m.cn, c.n(t), c.failed(t));
      }
    }
    ImGui::EndTable();
  }
  ImGui::EndTooltip();
}

// 假设板一句: 个数 (各判定的个数) + 待判的
std::string board_brief(const Board &b) {
  Per<Stance, int> n{};
  std::string owe;
  for (const Hyp &h : b.all()) {
    ++n[h.stance];
    if (h.pending)
      owe += (owe.empty() ? "" : " ") + h.id;
  }
  std::string s;
  for (const Stance st : kVals<Stance>)
    if (n[st])
      s += (s.empty() ? "" : " · ") + std::string(def(st).cn) + " " + std::to_string(n[st]);
  return std::to_string(b.all().size()) + (s.empty() ? "" : " (" + s + ")") + (owe.empty() ? "" : " · 待判 " + owe);
}

// ---- 现在在干什么 (点了跳到当前轮) + 会话累计 (一行要点, 悬浮看全) ----
void now_row(Ui &s, const Mine &m, const Flow &f) {
  const Phase ph = m.phase();
  if (ph == Phase::None) {
    ImGui::TextDisabled("无会话: 选好作用域按 New, 或载入存过的");
    return;
  }
  const double t = m.now();
  const Tally all = m.tally();
  const llm::Reply &live = s.chat_live.reply;
  const LabLive &ll = s.lab_live;
  const Act *a = acting(m);
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
      what = "等你选答案: 已按推荐预选, 改完按 交答案", kind = TaskStatus::Kind::Warn;
      break;
    case Phase::Act: {
      if (!a) {
        what = "处理回复";
        break;
      }
      what = "第 " + std::to_string(m.timeline.size()) + " 轮 · 调用 " + std::to_string(m.acting() + 1) + "/" + std::to_string(m.timeline.back().acts.size()) + " " +
             text::head(a->call.text(), kTopicChars);
      if (const TrialRes *k = AsTrial(*a)) {
        char pct[32];
        std::snprintf(pct, sizeof(pct), " %.0f%%", ll.frac * 100.0);
        what += std::string(" · ") + step_name(*k) + pct;
        frac = static_cast<float>(ll.frac);
      } else if (def(a->call.tool).exec == Exec::Lab) {
        char pct[32];
        std::snprintf(pct, sizeof(pct), " %.0f%%", ll.frac * 100.0);
        what += std::string(" · 执行中") + (ll.frac > 0.0 ? pct : "…");
        frac = static_cast<float>(ll.frac);
      } else {
        what += " · 执行中…";
      }
      what += " " + dur(a->span.len(t));
      break;
    }
    case Phase::Pause:
      what = "停着: " + m.reason + " · 继续 = " + m.hint(), kind = TaskStatus::Kind::Ready;
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
  ImGui::TextDisabled("用时 %s · token %s · %zu 轮 · 候选 %d · 入库 %d · 假设 %s", dur(t).c_str(), kilo(all.usage.prompt + all.usage.completion).c_str(), m.timeline.size(),
                      all.staged(), all.adopted, board_brief(m.st.board).c_str());
  tip_long("用时 " + dur(t) + " (LLM " + dur(all.llm_s) + " · 执行 " + dur(all.act_s) + ")\ntoken 输入 " + kilo(all.usage.prompt) + " (缓存 " + kilo(all.usage.cached) + ") / 输出 " +
           kilo(all.usage.completion) + "\n" + std::to_string(m.timeline.size()) + " 轮 · 调用 " + std::to_string(all.all()) + " (观察 " + std::to_string(all.n(Cat::Look)) +
           " · 查研报 " + std::to_string(all.docs()) + " · 没做成 " + std::to_string(all.failed()) + ")\n候选 " + std::to_string(all.staged()) + " (有参数组 " +
           std::to_string(all.n(Verdict::Weights)) + ") · 提交 " + std::to_string(all.n(Tool::SubmitFactor)) + " · 入库 " + std::to_string(all.adopted) + "\n假设 " +
           board_brief(m.st.board) + (m.memo.empty() ? "" : "\n插话已排队, 随下一条发出"));
  tools_bar(all);
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
void cand_block(const Mine &m, const Act &a, const std::string &ref, const LabLive &ll) {
  const TrialRes &k = *AsTrial(a);
  const Staged &st = k.st;
  const Result &res = st.res;
  const bool done = a.span.ended();
  if (done)
    ImGui::SetNextItemOpen(res.verdict == Verdict::Unstaged || res.verdict == Verdict::Broken || res.verdict == Verdict::Weights, ImGuiCond_Once);
  ImGui::PushStyleColor(ImGuiCol_Text, kColHead);
  const bool open = ImGui::TreeNodeEx(done ? "settled" : "cand", ImGuiTreeNodeFlags_DrawLinesFull, "%s 回测 %s  %s", ref.c_str(), st.cand.name.c_str(), st.cand.name_cn.c_str());
  ImGui::PopStyleColor();
  if (!open && ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(ImGui::GetFontSize() * 50.0f);
    ImGui::TextColored(kColExpr, "%s", st.cand.expr.c_str());
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
    ImGui::TextColored(color(kind_of(m.st.adopted, res)), "%s", brief(m.st.adopted, res).c_str());
  } else if (a.span.running() && m.live()) {
    ImGui::TextColored(color(TaskStatus::Kind::Busy), "%s… %.0f%% · %s", step_name(k), ll.frac * 100.0, dur(a.span.len(t)).c_str());
  } else {
    ImGui::TextDisabled("%s", m.live() ? "排队" : "未评");
  }
  if (open) {
    if (done) {
      k.draw();
      if (!a.out.text.empty())
        fold("raw", ImGuiTreeNodeFlags_None, "原文 · " + chars(a.out.text), a.out.text, true, kBoxLines, false);
    } else {
      wrapped(kColExpr, st.cand.expr);
      if (!st.cand.note.empty())
        wrapped_dim(st.cand.note);
    }
    ImGui::TreePop();
  }
}

// 假设 / 判定的字段 (调用参数) 一行一个 "标签: 值"; 标题 / 编号 / 判定 / 证据另有地方写
std::string fields_text(Tool t, const nlohmann::ordered_json &args) {
  std::string s = FieldsText(t, args, {"title", "hyp", "verdict", "evidence"});
  while (!s.empty() && s.back() == '\n')
    s.pop_back();
  return s;
}

// 判定的颜色: 支持绿 / 落空红 / 不清黄 / 待检验灰
TaskStatus::Kind kind_of(Stance st) {
  switch (st) {
  case Stance::Supported:
    return TaskStatus::Kind::Ready;
  case Stance::Refuted:
    return TaskStatus::Kind::Error;
  case Stance::Unclear:
    return TaskStatus::Kind::Warn;
  case Stance::Open:
    break;
  }
  return TaskStatus::Kind::Muted;
}

// 调用编号的跳转钮 (同一组里 id 区分)
void ref_button(Ui &s, Ref r, int id) {
  ImGui::PushID(id);
  if (ImGui::SmallButton(RefText(r).c_str()))
    s.go = r;
  ImGui::PopID();
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("跳到这个调用");
}
// 证据列: 能解析的编号各一个跳转钮 (id 从 base 起)
void evidence_buttons(Ui &s, const nlohmann::ordered_json &ev, int base) {
  int id = base;
  for (const nlohmann::ordered_json &e : ev) {
    Ref r;
    if (!RefOf(e.get<std::string>(), r))
      continue;
    if (id > base)
      ImGui::SameLine();
    ref_button(s, r, id++);
  }
}

// 提出于 r 的假设 (回退后可能已不在)
const Hyp *hyp_at(const Board &b, Ref r) {
  const std::vector<Hyp> &hs = b.all();
  const auto it = std::find_if(hs.begin(), hs.end(), [&](const Hyp &h) { return h.at == r; });
  return it == hs.end() ? nullptr : &*it;
}

constexpr double kFlashS = 2.0; // 跳到的调用高亮几秒

// 一个调用 (标题带调用编号 R<轮>#<序>): eval_factor 同候选; ask_human = 问答 (edit = 在问它); hypothesize / judge = 编号 + 判定一行, 展开 = 各字段 (+ 证据跳转);
// 其余 = 调用一行 + 状态 / 用时, 展开 = 交给 agent 的结果. s.go 指它 → 滚到它并高亮一会. 返回 = 按了交答案
bool act_block(Ui &s, const Mine &m, Act &a, size_t r, size_t i, bool edit) {
  const Ref ref{static_cast<int>(r) + 1, static_cast<int>(i) + 1};
  const std::string rt = RefText(ref);
  if (s.go && *s.go == ref) {
    ImGui::SetScrollHereY(0.25f);
    s.flash = ref, s.flash_t = ImGui::GetTime();
  }
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  ImGui::PushID(static_cast<int>(i));
  bool start = false;
  const double t = m.now();
  std::string state;
  TaskStatus::Kind kind = TaskStatus::Kind::None;
  if (a.span.ended())
    state = " · " + dur(a.span.len(t)) + (a.out.ok ? "" : " · 没做成"), kind = a.out.ok ? TaskStatus::Kind::None : TaskStatus::Kind::Warn;
  else if (a.span.running() && m.live())
    state = " · 执行中… " + dur(a.span.len(t)), kind = TaskStatus::Kind::Busy;
  else
    state = m.live() ? " · 排队" : " · 未做", kind = TaskStatus::Kind::Muted;
  if (AsTrial(a)) {
    cand_block(m, a, rt, s.lab_live);
  } else if (AskRes *q = AsAsk(a)) {
    ImGui::PushStyleColor(ImGuiCol_Text, color(edit ? TaskStatus::Kind::Warn : TaskStatus::Kind::None));
    const bool open = ImGui::TreeNodeEx("ask", kNode | ImGuiTreeNodeFlags_DefaultOpen, "%s 问研究员 · %zu 个问题%s", rt.c_str(), q->asked.qs.size(), edit ? " · 等你答" : "");
    ImGui::PopStyleColor();
    if (open) {
      if (edit) {
        start = edit_asked(q->asked);
      } else {
        q->draw();
        if (!a.out.text.empty())
          fold("raw", ImGuiTreeNodeFlags_None, "原文 · " + chars(a.out.text), a.out.text, true, kBoxLines, false);
      }
      ImGui::TreePop();
    }
  } else if (a.call.tool == Tool::Hypothesize || a.call.tool == Tool::Judge) {
    const bool judge = a.call.tool == Tool::Judge;
    std::string label = rt + " ";
    if (judge) {
      const Stance st = IdOf<Stance>(a.call.str("verdict"));
      label += "判 " + a.call.str("hyp") + " → " + def(st).cn;
      kind = a.out.ok ? kind_of(st) : TaskStatus::Kind::Warn;
    } else {
      const Hyp *h = hyp_at(m.st.board, ref);
      label += "提假设 " + (h ? h->id + " · " : std::string()) + text::head(a.call.str("title"), kCallChars);
      if (a.out.ok && h)
        kind = kind_of(h->stance);
    }
    ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
    const bool open = ImGui::TreeNodeEx("hyp", ImGuiTreeNodeFlags_None, "%s", (label + state).c_str());
    ImGui::PopStyleColor();
    if (open) {
      if (judge) {
        ImGui::TextDisabled("证据");
        ImGui::SameLine();
        evidence_buttons(s, a.call.args.at("evidence"), 0);
      }
      if (a.out.res) {
        a.out.res->draw();
        if (!a.out.text.empty())
          fold("raw", ImGuiTreeNodeFlags_None, "原文 · " + chars(a.out.text), a.out.text, true, kBoxLines, false);
      } else {
        if (!judge)
          wrapped(a.call.str("title"));
        wrapped_dim(fields_text(a.call.tool, a.call.args));
        wrapped(color(a.out.ok ? TaskStatus::Kind::None : TaskStatus::Kind::Warn), a.out.text);
      }
      ImGui::TreePop();
    }
  } else {
    const ToolMeta &tm = def(a.call.tool);
    const std::string label = rt + " " + tm.cn + " · " + text::head(a.call.text(), kCallChars) + state;
    if (a.out.res) {
      ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
      const bool open = ImGui::TreeNodeEx("act", ImGuiTreeNodeFlags_None, "%s", label.c_str());
      ImGui::PopStyleColor();
      copy_button("复制##act", a.out.text);
      if (open) {
        a.out.res->draw();
        if (!a.out.text.empty())
          fold("raw", ImGuiTreeNodeFlags_None, "原文 · " + chars(a.out.text), a.out.text, true, kBoxLines, false);
        ImGui::TreePop();
      }
    } else {
      ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
      fold("act", ImGuiTreeNodeFlags_None, label, a.out.text, false, kBoxLines, false);
      ImGui::PopStyleColor();
    }
  }
  ImGui::PopID();
  if (s.flash == ref && ImGui::GetTime() - s.flash_t < kFlashS) {
    const ImVec2 p1(p0.x + ImGui::GetContentRegionAvail().x, ImGui::GetCursorScreenPos().y);
    ImGui::GetWindowDrawList()->AddRectFilled(p0, p1, ImGui::GetColorU32(ImGuiCol_TextSelectedBg));
  }
  return start;
}

// 一轮的标题 (折起来也看得出这轮干了什么)
std::string round_head(const Mine &m, size_t i, const Flow *live, TaskStatus::Kind &kind) {
  const Round &rd = m.timeline[i];
  std::string h = "第 " + std::to_string(i + 1) + " 轮 · " + def(rd.kind).cn;
  kind = TaskStatus::Kind::None;
  if (!rd.span.began()) {
    kind = TaskStatus::Kind::Warn;
    return h + " · 待发 (按 继续 发出)";
  }
  h += " · +" + dur(rd.span.t0);
  if (live) {
    kind = live->stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy;
    return h + " · " + doing(*live) + (live->topic.empty() ? "" : " · " + live->topic);
  }
  const bool replied = rd.span.ended();
  if (replied)
    h += " · 回复 " + dur(rd.span.len(m.now()));
  if (!rd.error.empty()) {
    kind = TaskStatus::Kind::Error;
    return h + (rd.reply.ok() ? " · 不合协议" : " · 没收成");
  }
  if (rd.acts.empty())
    return h;
  const Tally c = TallyOf(Rounds(m.timeline).subspan(i, 1), m.now());
  h += " · 调用 " + std::to_string(c.all());
  if (const int n = c.n(Cat::Look); n > 0)
    h += " · 观察 " + std::to_string(n);
  if (const int n = c.docs(); n > 0)
    h += " · 查研报 " + std::to_string(n);
  if (c.n(Tool::EvalFactor) > 0) {
    const int u = c.n(Verdict::Unstaged);
    h += " · 候选 " + std::to_string(c.staged()) + (u ? " (未落 " + std::to_string(u) + ")" : "") + " · 有权重 " + std::to_string(c.n(Verdict::Weights));
  }
  if (const int n = c.n(Tool::SubmitFactor); n > 0)
    h += " · 提交 " + std::to_string(n) + " (入库 " + std::to_string(c.adopted) + ")";
  if (const int n = c.n(Cat::Note); n > 0)
    h += " · 记录 " + std::to_string(n);
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

// agent 在第 i 轮看到的整份上下文 (system 之外): 照时间线现算, 展开时算一次 (时间线变了重算)
void context_block(Ui &s, const Mine &m, size_t i) {
  const bool open = ImGui::TreeNodeEx("context", ImGuiTreeNodeFlags_None, "agent 看到的上下文 (首条 + 研究树 + 上一轮的回复 + 本条; system 之外)");
  if (!open) {
    s.ctx.erase(i);
    return;
  }
  const Round &rd = m.timeline[i];
  const auto sig = std::tuple(m.timeline.size(), rd.span.t0, rd.user.size(), rd.memo.size());
  Ui::Ctx &c = s.ctx[i];
  if (c.body.empty() || c.sig != sig)
    c = {sig, llm::Flat(TurnsOf(Rounds(m.timeline).first(i + 1)))};
  copy_button("复制##context", c.body);
  box(c.body, false, kBoxLines, false);
  ImGui::TreePop();
}

// 一轮: 发出 → 思考 → 回复 (逐个调用 + 原文); 在收的一轮画 live; 待发的只有消息. fresh = 新一轮刚来 (重定开合); s.jump = 末轮展开并滚到它; s.go 在这轮 = 展开.
// 停着时标题右键回退. 返回 = 按了交答案
bool round_block(Ui &s, Mine &m, size_t i, const Flow &f, bool fresh) {
  Round &rd = m.timeline[i];
  const bool replied = rd.span.ended(), last = i + 1 == m.timeline.size();
  const llm::Reply &r = replied || !rd.span.began() || m.leg.mcp ? rd.reply : s.chat_live.reply;
  const bool receiving = rd.span.running() && f.stage != Flow::Idle;
  const int k = static_cast<int>(i) + 1;

  TaskStatus::Kind kind = TaskStatus::Kind::None;
  const std::string head = round_head(m, i, receiving ? &f : nullptr, kind);
  if ((s.jump && last) || (s.go && s.go->round == k))
    ImGui::SetNextItemOpen(true, ImGuiCond_Always);
  else if (fresh)
    ImGui::SetNextItemOpen(i + kOpenRounds >= m.timeline.size(), ImGuiCond_Always);
  ImGui::PushID(static_cast<int>(i));
  ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
  const bool open = ImGui::TreeNodeEx("round", kNode, "%s", head.c_str());
  ImGui::PopStyleColor();
  const bool editable = m.phase() == Phase::Pause && m.idle();
  if (editable && ImGui::IsItemHovered() && !ImGui::IsPopupOpen("##rewind_menu"))
    ImGui::SetTooltip("右键: 从这轮重来 (丢这轮的回复与执行及其后各轮, 继续即重发这条)");
  if (editable && ImGui::BeginPopupContextItem("##rewind_menu")) {
    ImGui::TextDisabled("第 %d 轮 · 回退", k);
    if (ImGui::MenuItem(("从第 " + std::to_string(k) + " 轮重来…").c_str()))
      s.rewind = {.k = k, .loss = m.loss(k), .open = true};
    ImGui::EndPopup();
  }
  if (s.jump && last)
    ImGui::SetScrollHereY(0.0f);
  if (!open) {
    ImGui::PopID();
    return false;
  }
  const std::string sent = TextOf(rd);
  fold("send", ImGuiTreeNodeFlags_None, (rd.span.began() ? "发出 " : "待发 ") + chars(sent) + (rd.memo.empty() ? "" : " · 附插话"), sent, false, kBoxLines, false);
  context_block(s, m, i);
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
    if (!rd.error.empty())
      wrapped(color(TaskStatus::Kind::Error), (r.ok() ? "不合协议: " : "") + rd.error);
    for (size_t j = 0; j < rd.acts.size(); ++j)
      start |= act_block(s, m, rd.acts[j], i, j, last && m.phase() == Phase::Choose && j == m.acting());
    fold("text", ImGuiTreeNodeFlags_None, "原文 " + chars(r.text), r.text, false, kBoxLines, false);
  }
  ImGui::TreePop();
  ImGui::PopID();
  return start;
}

// 一组轮的标题: 轮次 / 起始时刻 / 调用 / 观察 / 查研报 / 候选 / 有权重 / 入库
std::string group_head(const Mine &m, size_t g0, size_t g1) {
  const Tally c = TallyOf(Rounds(m.timeline).subspan(g0, g1 - g0), m.now());
  return "第 " + std::to_string(g0 + 1) + "–" + std::to_string(g1) + " 轮 · +" + dur(m.timeline[g0].span.t0) + " · 调用 " + std::to_string(c.all()) + " · 观察 " +
         std::to_string(c.n(Cat::Look)) + " · 查研报 " + std::to_string(c.docs()) + " · 候选 " + std::to_string(c.staged()) + " · 有权重 " + std::to_string(c.n(Verdict::Weights)) +
         " · 入库 " + std::to_string(c.adopted);
}

// 停下汇总: 原因与继续会做什么 + 计数 + 本会话有参数组的候选 (入库的在前, 再按 score (中位 rIC) 降序; 先列 kSummaryRows 个, 其余折起) + 停下报告
constexpr size_t kSummaryRows = 5;
void summary_block(const Mine &m) {
  const Tally c = m.tally();
  std::vector<const Result *> hits;
  for (const Round &rd : m.timeline)
    for (const Act &a : rd.acts)
      if (const TrialRes *k = AsTrial(a); k && k->st.res.verdict == Verdict::Weights)
        hits.push_back(&k->st.res);
  std::stable_sort(hits.begin(), hits.end(), [&](const Result *a, const Result *b) {
    const bool ia = m.st.adopted.count(a->name) > 0, ib = m.st.adopted.count(b->name) > 0;
    return ia != ib ? ia : a->best()->score > b->best()->score;
  });
  ImGui::SeparatorText(("停下 · +" + dur(m.now())).c_str());
  wrapped(color(TaskStatus::Kind::Ready), m.reason);
  wrapped_dim("按 继续 = " + m.hint() + "; 右键某一轮的标题可回退到那轮; 关闭 = 放下这段 (已存盘, 可再载入)");
  ImGui::TextDisabled("%zu 轮 · 调用 %d (观察 %d · 查研报 %d) · 候选 %d · 有参数组 %d · 提交 %d · 入库 %d", m.timeline.size(), c.all(), c.n(Cat::Look), c.docs(), c.staged(), c.n(Verdict::Weights),
                      c.n(Tool::SubmitFactor), c.adopted);
  const auto row = [&](size_t i) {
    const Result &r = *hits[i];
    ImGui::Bullet();
    ImGui::TextColored(kColHead, "%s  %s", r.name.c_str(), r.name_cn.c_str());
    ImGui::SameLine();
    result_line(m.st.adopted, r);
  };
  for (size_t i = 0; i < std::min(hits.size(), kSummaryRows); ++i)
    row(i);
  if (hits.size() > kSummaryRows && ImGui::TreeNodeEx("more", kNode, "其余 %zu 个", hits.size() - kSummaryRows)) {
    for (size_t i = kSummaryRows; i < hits.size(); ++i)
      row(i);
    ImGui::TreePop();
  }
  fold("report", ImGuiTreeNodeFlags_None, "停下报告", m.report, false, kBoxLines, false);
}

// 过程: 全会话 (分组 → 逐轮) + 停下汇总. 钉在底部时跟新内容走; 放开后右下角给 "回到最新"; s.go = 跳到某个调用 (展开它所在的组 / 轮, 滚过去).
// s.flow_rounds = 上一帧画过的轮数 (变了 = 新一轮来, 重定开合); bottom = 底下留给插话栏的高
void timeline_panel(Ui &s, Mine &m, const Flow &f, bool to_end, float bottom) {
  ImGui::BeginChild("##flow", ImVec2(0, -bottom), false);
  const float y = ImGui::GetScrollY();
  if (to_end || y >= ImGui::GetScrollMaxY() - 2.0f)
    s.pinned = true;
  else if (y < s.flow_y - 0.5f)
    s.pinned = false;
  if (ImGui::IsWindowHovered(ImGuiHoveredFlags_ChildWindows) && ImGui::IsMouseClicked(ImGuiMouseButton_Left))
    s.pinned = false;                  // 点开合 = 在读: 不把读的地方拖走
  const bool going = s.go.has_value(); // 本帧起就要跳的 (本帧里才点的留到下一帧)
  if (going)
    s.pinned = false;
  bool start = false;
  if (m.phase() == Phase::None) {
    ImGui::TextDisabled("选好作用域按 New (或载入存过的): 系统把规则与工具表打包发给 agent, 它每条回复发一串工具调用 (看数据 / 回测 / 提交), 系统执行后把结果交回");
  } else {
    const size_t n = m.timeline.size();
    const bool fresh = n != s.flow_rounds, grouped = n > kGroupRounds;
    s.flow_rounds = n;
    for (size_t g0 = 0; g0 < n; g0 += kGroupRounds) {
      const size_t g1 = std::min(n, g0 + kGroupRounds);
      ImGui::PushID(static_cast<int>(g0));
      bool open = true;
      if (grouped) {
        if ((s.jump && g1 == n) || (s.go && static_cast<size_t>(s.go->round) > g0 && static_cast<size_t>(s.go->round) <= g1))
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
  if (m.phase() == Phase::Pause)
    summary_block(m);
  if (going)
    s.go.reset();
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

// 特征 / 算子 / 候选 三页的数据 = Lab (会话中候选随评估实时更新). 无会话时随作用域重绑; 绑不上 (该层无可搜持有期 / 评估在收尾) = 空表
// 全局记录不随作用域, 随 Lab 的候选目录 (自检的 / 正式的) 读; 研究员在记录页标处理
struct View {
  const Context &ctx;
  const std::vector<Result> &results;
  const Names &adopted;
  const Names &staged; // 本会话落了文件的候选
  bool test;           // 候选与全局记录是自检会话的 (global/test 目录)
  Notes &notes;
  const Lab &lab;
};

View view_of(const SharedData &data, Ui &s) {
  Mine &m = *s.mine;
  const Scope &sc = s.opt.sc;
  if (!m.held() && !m.lab.busy() && m.lab.scope() != sc && !catalog::holds(data, sc.level).empty())
    m.lab.bind(data, sc, false);
  if (!m.notes.loaded(m.lab.test()))
    m.notes.load(m.lab.test());
  static const Context kNoCtx;
  static const std::vector<Result> kNoResults;
  static const Names kNone;
  if (m.lab.scope() != sc)
    return {kNoCtx, kNoResults, kNone, kNone, m.lab.test(), m.notes, m.lab};
  const bool mine = m.held() && m.setup.sc == sc;
  return {m.lab.ctx(), m.lab.results(), mine ? m.st.adopted : kNone, mine ? m.st.staged : kNone, m.lab.test(), m.notes, m.lab};
}

// 候选文件的思路 ("H1 标题 · 思路"): 拆出假设与思路两栏
std::pair<std::string, std::string> split_note(const std::string &note) {
  if (note.size() >= 2 && note[0] == 'H' && note[1] >= '0' && note[1] <= '9')
    if (const size_t sep = note.find(" · "); sep != std::string::npos)
      return {note.substr(0, sep), note.substr(sep + 3)};
  return {{}, note};
}

// 候选表: 列头点了排序 (默认 = 本持有期 score (中位 rIC) 降序); 本会话落的标 ●; 点一行在下面看详情与 Search 日志
void results_table(Ui &s, const View &v) {
  const std::vector<Result> &results = v.results;
  s.cand_filter.Draw("过滤##cand", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("候选 %zu (%s; ● = 本会话落的; 列头点了排序, 点一行看详情)", results.size(), v.test ? "自检的 global/test 目录" : "会话目录");
  const auto picked = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.file == s.pick; });
  const float detail_h = picked == results.end() ? 0.0f : ImGui::GetTextLineHeightWithSpacing() * 16.0f;
  constexpr ImGuiTableFlags kFlags = kTableFlags | ImGuiTableFlags_Sortable;
  if (!ImGui::BeginTable("##results", 11, kFlags, ImVec2(-1, -detail_h)))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  constexpr ImGuiTableColumnFlags kDesc = ImGuiTableColumnFlags_PreferSortDescending;
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_NoSort);
  ImGui::TableSetupColumn("名");
  ImGui::TableSetupColumn("中文");
  ImGui::TableSetupColumn("结局");
  ImGui::TableSetupColumn("中位 rIC", kDesc);
  ImGui::TableSetupColumn("每期毛多空 bp", kDesc);
  ImGui::TableSetupColumn("每期净多空 bp", kDesc);
  ImGui::TableSetupColumn("年换手");
  ImGui::TableSetupColumn("假设");
  ImGui::TableSetupColumn("思路");
  ImGui::TableSetupColumn("模板", ImGuiTableColumnFlags_WidthStretch | ImGuiTableColumnFlags_NoSort);
  ImGui::TableHeadersRow();
  // 统计列: 最佳权重 (中位 rIC + 每期毛 / 净多空 + 年换手)
  const auto hyp_of = [](const Result &r) { return split_note(r.note).first; };
  const auto change_of = [](const Result &r) { return split_note(r.note).second; };
  std::vector<size_t> ix;
  for (size_t i = 0; i < results.size(); ++i)
    if (const Result &r = results[i]; s.cand_filter.PassFilter((r.name + ' ' + r.name_cn + ' ' + r.tmpl + ' ' + r.note).c_str()))
      ix.push_back(i);
  if (const ImGuiTableSortSpecs *sp = ImGui::TableGetSortSpecs(); sp && sp->SpecsCount > 0) {
    const ImGuiTableColumnSortSpecs &c = sp->Specs[0];
    const auto num = [&](const Result &r) -> double {
      const Weight *w = r.best();
      switch (c.ColumnIndex) {
      case 4:
        return w ? w->score : -INFINITY;
      case 5:
        return w ? w->gross : -INFINITY;
      case 6:
        return w ? w->net : -INFINITY;
      case 7:
        return w ? w->turn : -INFINITY;
      default:
        return static_cast<double>(r.verdict);
      }
    };
    const auto str = [&](const Result &r) -> std::string {
      switch (c.ColumnIndex) {
      case 1:
        return r.name;
      case 2:
        return r.name_cn;
      case 8:
        return hyp_of(r);
      default:
        return change_of(r);
      }
    };
    const bool text = c.ColumnIndex == 1 || c.ColumnIndex == 2 || c.ColumnIndex == 8 || c.ColumnIndex == 9;
    const bool up = c.SortDirection == ImGuiSortDirection_Ascending;
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) {
      const Result &x = results[a], &y = results[b];
      return text ? (up ? str(x) < str(y) : str(y) < str(x)) : (up ? num(x) < num(y) : num(y) < num(x));
    });
  }
  for (const size_t i : ix) {
    const Result &r = results[i];
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    if (v.staged.contains(r.file))
      ImGui::TextColored(kColHead, "●");
    ImGui::TableNextColumn();
    if (ImGui::Selectable((r.name + "##" + r.file).c_str(), s.pick == r.file, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
      s.pick = s.pick == r.file ? std::string() : r.file;
    if (!r.note.empty())
      tip_long(r.note);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name_cn.c_str());
    ImGui::TableNextColumn();
    ImGui::TextColored(color(kind_of(v.adopted, r)), "%s", v.adopted.count(r.name) ? "入库" : def(r.verdict).cn);
    tip_long(detail(r));
    const Weight *w = r.best();
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%.4f", w->score);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%+.2f", w->gross * 1e4);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%+.2f", w->net * 1e4);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%.0f", w->turn);
    ImGui::TableNextColumn();
    if (const std::string h = hyp_of(r); !h.empty()) {
      ImGui::TextUnformatted(text::head(h, 16).c_str());
      tip_long(h);
    }
    ImGui::TableNextColumn();
    if (const std::string c = change_of(r); !c.empty()) {
      ImGui::TextUnformatted(text::head(c, 16).c_str());
      tip_long(c);
    }
    ImGui::TableNextColumn();
    wrapped(r.tmpl);
  }
  ImGui::EndTable();
  // 选中的 (本帧可能刚换): 模板 / 思路 / 结局全文 + Search 日志 (换了选中才重读)
  const auto now_picked = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.file == s.pick; });
  if (picked == results.end() || now_picked == results.end())
    return;
  const Result &r = *now_picked;
  if (s.pick_log_of != s.pick)
    s.pick_log = v.lab.search_log(r.file), s.pick_log_of = s.pick;
  ImGui::BeginChild("##pick", ImVec2(-1, -1), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "%s  %s", r.name.c_str(), r.name_cn.c_str());
  ImGui::SameLine();
  if (ImGui::SmallButton("复制模板"))
    ImGui::SetClipboardText(r.tmpl.c_str());
  ImGui::SameLine();
  if (ImGui::SmallButton("重读日志"))
    s.pick_log_of.clear();
  wrapped(ImVec4(0.9f, 0.8f, 0.55f, 1.0f), r.tmpl);
  if (!r.note.empty())
    wrapped_dim(r.note);
  wrapped(color(kind_of(v.adopted, r)), detail(r));
  fold("log", ImGuiTreeNodeFlags_None, "Search 日志 " + chars(s.pick_log), s.pick_log.empty() ? std::string("(没有)") : s.pick_log, true, kBoxLines, false);
  ImGui::EndChild();
}

// 一类全局记录, 新到旧; 研究员逐条标处理 (待办 / 已做 / 不做 / 搁置) 并可回一句, agent 在 list_notes 里看得到; 编号悬浮看出处
void notes_table(Ui &s, const View &v, NoteKind k) {
  const std::vector<Note> &all = v.notes.of(k);
  s.note_filter.Draw("过滤##note", 240);
  ImGui::SameLine();
  ImGui::Checkbox("只看待办", &s.only_open);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 条 · 待办 %d (%s, 新到旧); 标的处理与回复 agent 在 list_notes 里看得到", all.size(), v.notes.open(k), v.test ? "自检的 global/test 目录" : "global 目录");
  if (!begin_table("##notes", {"编号", "时间", "作用域", "附议", "处理", "内容"}))
    return;
  struct Change {
    std::string id;
    NoteState st;
    std::string reply;
  };
  std::optional<Change> change; // 表画完再改 (改写文件)
  for (auto it = all.rbegin(); it != all.rend(); ++it) {
    const Note &x = *it;
    const std::string title = x.title(), body = x.body();
    if ((s.only_open && x.state != NoteState::Open) || !s.note_filter.PassFilter((x.id + ' ' + x.scope + ' ' + title + ' ' + body + ' ' + x.reply).c_str()))
      continue;
    ImGui::PushID(x.id.c_str());
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.id.c_str());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s · 第 %d 轮", x.session.c_str(), x.round);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.time.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.scope.c_str());
    ImGui::TableNextColumn();
    if (!x.ref.empty())
      ImGui::Text("附议 %s", x.ref.c_str());
    if (const int b = v.notes.backers(x); b > 0)
      ImGui::TextColored(color(TaskStatus::Kind::Ready), "%d 条附议", b);
    ImGui::TableNextColumn();
    ImGui::SetNextItemWidth(ImGui::GetFontSize() * 4.0f);
    if (ImGui::BeginCombo("##state", def(x.state).cn)) {
      for (const NoteState st : kVals<NoteState>)
        if (ImGui::Selectable(def(st).cn, x.state == st))
          change = Change{x.id, st, x.reply};
      ImGui::EndCombo();
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("回复")) {
      s.reply = x.reply;
      ImGui::OpenPopup("##reply");
    }
    if (ImGui::BeginPopup("##reply")) {
      ImGui::TextDisabled("给 agent 的一句回复 (list_notes 里随处理一起给; 空 = 不回)");
      input_multiline("##reply_text", s.reply, ImVec2(ImGui::GetFontSize() * 30.0f, ImGui::GetTextLineHeightWithSpacing() * 4.0f));
      if (ImGui::Button("保存", ImVec2(80, 0))) {
        change = Change{x.id, x.state, std::string(text::trim(s.reply))};
        ImGui::CloseCurrentPopup();
      }
      ImGui::SameLine();
      if (ImGui::Button("取消", ImVec2(80, 0)))
        ImGui::CloseCurrentPopup();
      ImGui::EndPopup();
    }
    ImGui::TableNextColumn();
    wrapped(kColHead, title);
    wrapped_dim(body);
    if (x.state != NoteState::Open || !x.reply.empty())
      wrapped(color(x.state == NoteState::Done ? TaskStatus::Kind::Ready : TaskStatus::Kind::Warn), "研究员: " + std::string(def(x.state).cn) + (x.reply.empty() ? "" : " · " + x.reply));
    ImGui::PopID();
  }
  ImGui::EndTable();
  if (change)
    v.notes.set(k, change->id, change->st, std::move(change->reply));
}

// 假设板: 每个假设一行 (判定 / 变体用量 / 提出与最近判定处 / 证据), 调用编号点了跳到过程页里的那个调用; 悬浮假设看全文
void hyps_panel(Ui &s, const Mine &m) {
  if (!m.held()) {
    ImGui::TextDisabled("无会话");
    return;
  }
  const std::vector<Hyp> &hs = m.st.board.all();
  if (hs.empty()) {
    ImGui::TextDisabled("还没有假设: agent 看了数据后用 hypothesize 提, 用观察检验后 judge, 判为支持的才能挂回测");
    return;
  }
  ImGui::TextDisabled("假设 %s · 点调用编号跳到过程页里的那个调用; 悬浮假设看全文", board_brief(m.st.board).c_str());
  if (!begin_table("##hyps", {"编号", "判定", "变体", "提出", "最近判定", "证据", "假设"}))
    return;
  for (size_t i = 0; i < hs.size(); ++i) {
    const Hyp &h = hs[i];
    ImGui::PushID(static_cast<int>(i));
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(h.id.c_str());
    ImGui::TableNextColumn();
    ImGui::TextColored(color(kind_of(h.stance)), "%s%s", def(h.stance).cn, h.pending ? " · 待判" : "");
    ImGui::TableNextColumn();
    ImGui::Text("%d/%d", h.evals, kHypEvals);
    ImGui::TableNextColumn();
    ref_button(s, h.at, 0);
    ImGui::TableNextColumn();
    if (h.judged.round)
      ref_button(s, h.judged, 1);
    else
      ImGui::TextDisabled("—");
    ImGui::TableNextColumn();
    if (h.verdict.is_object() && h.verdict.contains("evidence"))
      evidence_buttons(s, h.verdict.at("evidence"), 2);
    ImGui::TableNextColumn();
    wrapped(h.title());
    tip_long(fields_text(Tool::Hypothesize, h.fields) + (h.verdict.is_object() ? "\n\n最近判定 (" + std::string(def(h.stance).cn) + "):\n" + fields_text(Tool::Judge, h.verdict) : std::string()));
    ImGui::PopID();
  }
  ImGui::EndTable();
}

// 插话栏 (过程页底): 草稿 → 排队, 随下一条发出的消息附上; 排着的可撤回. memo_height = 它占的高
float memo_height(const Mine &m) {
  return m.held() ? ImGui::GetTextLineHeightWithSpacing() * (m.memo.empty() ? 3.2f : 4.4f) + ImGui::GetStyle().ItemSpacing.y : 0.0f;
}
void memo_bar(Ui &s, Mine &m) {
  if (!m.held())
    return;
  if (!m.memo.empty()) {
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "已排队的插话 · %s · 随下一条发出", chars(m.memo).c_str());
    tip_long(m.memo);
    ImGui::SameLine();
    if (ImGui::SmallButton("撤回")) {
      const std::string back = m.unqueue();
      s.memo = back + (s.memo.empty() ? "" : "\n\n" + s.memo);
    }
  }
  input_multiline("##memo", s.memo, ImVec2(-90, ImGui::GetTextLineHeightWithSpacing() * 2.6f));
  ImGui::SameLine();
  const std::string_view text = text::trim(s.memo);
  ImGui::BeginDisabled(text.empty());
  if (ImGui::Button("插话", ImVec2(80, 0))) {
    m.queue(std::string(text));
    s.memo.clear();
  }
  ImGui::EndDisabled();
  tip("给 agent 的意见: 排队, 随下一条发出的消息附上 (不打断在收的回复与在跑的调用; 停着时随继续发出)");
}

// 回退确认框: 写明会丢的轮数 / 候选文件 / 全局记录
void rewind_modal(Ui &s, Mine &m) {
  Ui::Rewind &r = s.rewind;
  if (r.open)
    ImGui::OpenPopup("回退###rewind"), r.open = false;
  ImGui::SetNextWindowSize(ImVec2(ImGui::GetFontSize() * 48.0f, ImGui::GetFontSize() * 12.0f), ImGuiCond_Appearing);
  if (!ImGui::BeginPopupModal("回退###rewind"))
    return;
  if (m.phase() != Phase::Pause || r.k < 1 || r.k > m.round()) {
    ImGui::CloseCurrentPopup();
    ImGui::EndPopup();
    return;
  }
  const std::string k = std::to_string(r.k);
  const Mine::Loss &l = r.loss;
  ImGui::TextColored(kColHead, "%s", ("从第 " + k + " 轮重来").c_str());
  wrapped_dim("状态回到第 " + k + " 条发出时, 按 继续 重发它, agent 重答; 它随附的插话退回排队 (可撤回改写), 要改方向就再插话");
  wrapped(color(TaskStatus::Kind::Warn), "将丢弃: 第 " + k + " 轮起 " + std::to_string(l.rounds) + " 轮的回复与执行 · 候选文件 " + std::to_string(l.files) + " 个 (连同 Search 日志) · 本会话的全局记录 " +
                                             std::to_string(l.notes) + " 条; 已入正式库的不动; 不可撤销");
  ImGui::BeginDisabled(!m.idle());
  if (ImGui::Button("确定", ImVec2(100, 0))) {
    m.rewind(r.k);
    ImGui::CloseCurrentPopup();
    s.flow_rounds = 0, s.pinned = true;
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消", ImVec2(100, 0)))
    ImGui::CloseCurrentPopup();
  ImGui::EndPopup();
}

// ---- 参考: 发给 agent 的特征表 / 算子表 / 工具表 ----
// 人勾开放 (特征 / 算子页同一份): all = 本表全部键, keys = 要改的; 勾满 = 回到全开 (此后新加的也开), 否则白名单
Open pick(Open o, const std::vector<std::string> &all, const std::vector<std::string> &keys, bool on) {
  if (o.all)
    o = {false, std::set<std::string>(all.begin(), all.end())};
  for (const std::string &k : keys) {
    if (on)
      o.on.insert(k);
    else
      o.on.erase(k);
  }
  if (std::all_of(all.begin(), all.end(), [&](const std::string &k) { return o.on.contains(k); }))
    o = {};
  return o;
}
// 全选 / 全清 (只动列出的行); 按了 → next
void pick_bar(const Open &o, const std::vector<std::string> &all, const std::vector<std::string> &shown, bool locked, std::optional<Open> &next) {
  const char *why = locked ? "有会话 (在走或停着) 时不可改; 关闭会话后改, 下次 New 生效" : "只动列出的行 (过滤后); 勾满 = 全开, 此后新加的也开; 否则只开勾了的";
  ImGui::BeginDisabled(locked);
  for (const bool on : {true, false}) {
    ImGui::SameLine();
    if (ImGui::SmallButton(on ? "全选" : "全清"))
      next = pick(o, all, shown, on);
    tip(why);
  }
  ImGui::EndDisabled();
}
// 一行的开放勾选框; 关着的行字变灰 (到 pick_end)
void pick_cell(const Open &o, const std::vector<std::string> &all, const std::string &key, bool locked, std::optional<Open> &next) {
  bool on = o.has(key);
  ImGui::TableNextColumn();
  ImGui::BeginDisabled(locked);
  if (ImGui::Checkbox(("##pick_" + key).c_str(), &on))
    next = pick(next.value_or(o), all, {key}, on);
  ImGui::EndDisabled();
  if (!o.has(key))
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
}
void pick_end(const Open &o, const std::string &key) {
  if (!o.has(key))
    ImGui::PopStyleColor();
}

// 本层全部叶 (来源未开放的不列), 逐行勾开放给 agent 的
void leaves_panel(Ui &s, const Context &c, bool locked) {
  Scope &sc = s.opt.sc;
  const Open &open = sc.leaves_of(c.level);
  s.leaf_filter.Draw("过滤##leaf", 240);
  const auto pass = [&](const Leaf &l) {
    return sc.has(l.src) && s.leaf_filter.PassFilter((l.code + ' ' + l.name_cn + ' ' + l.desc + ' ' + l.family).c_str());
  };
  std::vector<std::string> all, shown;
  Per<Src, int> n{}, on{}; // [Src] 列出的叶 / 其中开放的
  for (const Leaf &l : c.all_leaves) {
    all.push_back(l.code);
    if (!pass(l))
      continue;
    shown.push_back(l.code);
    ++n[l.src];
    on[l.src] += open.has(l.code);
  }
  std::optional<Open> next;
  pick_bar(open, all, shown, locked, next);
  std::string counts;
  for (const Src src : kVals<Src>)
    if (n[src] > 0)
      counts += (counts.empty() ? "" : " · ") + std::string(def(src).id) + " " + std::to_string(on[src]) + "/" + std::to_string(n[src]);
  ImGui::SameLine();
  ImGui::TextDisabled("开放 / 列出: %s 个叶, 模板里写全名; 悬浮看公式", counts.c_str());
  if (begin_table("##leaves", {"开放", "叶", "中文名", "说明"})) {
    for (const Leaf &l : c.all_leaves) {
      if (!pass(l))
        continue;
      ImGui::TableNextRow();
      pick_cell(open, all, l.code, locked, next);
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(l.code.c_str());
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", l.formula.c_str());
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(l.name_cn.c_str());
      ImGui::TableNextColumn();
      wrapped(l.desc);
      pick_end(open, l.code);
    }
    ImGui::EndTable();
  }
  if (!next)
    return;
  if (next->all)
    sc.leaves.erase(c.level);
  else
    sc.leaves[c.level] = std::move(*next);
}

// 全部算子, 逐行勾开放给 agent 的
void ops_panel(Ui &s, const Context &c, bool locked) {
  Open &open = s.opt.sc.ops;
  s.op_filter.Draw("过滤##op", 240);
  const auto pass = [&](const Op &o) { return s.op_filter.PassFilter((o.name + ' ' + o.name_cn + ' ' + o.note).c_str()); };
  std::vector<std::string> all, shown;
  int on = 0;
  for (const Op &o : c.all_ops) {
    all.push_back(o.name);
    if (!pass(o))
      continue;
    shown.push_back(o.name);
    on += open.has(o.name);
  }
  std::optional<Open> next;
  pick_bar(open, all, shown, locked, next);
  ImGui::SameLine();
  ImGui::TextDisabled("开放 / 列出: %d/%zu 个算子; 悬浮看签名与定义", on, shown.size());
  if (begin_table("##ops", {"开放", "算子", "中文名", "输出域", "说明"})) {
    for (const Op &o : c.all_ops) {
      if (!pass(o))
        continue;
      ImGui::TableNextRow();
      pick_cell(open, all, o.name, locked, next);
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
      pick_end(open, o.name);
    }
    ImGui::EndTable();
  }
  if (next)
    open = std::move(*next);
}

// 工具表 = 系统提示 "工具" 节的同一份 (kTools); ask_human 只在可提问的会话里发给 agent
void tools_panel(Ui &s) {
  s.tool_filter.Draw("过滤##tool", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个工具; 悬浮参数看类型与说明, 悬浮说明看返回", kNum<Tool>);
  if (!begin_table("##tools", {"工具", "中文名", "执行", "参数", "说明"}))
    return;
  for (const Tool t : kVals<Tool>) {
    const ToolMeta &m = def(t);
    const ToolDoc &d = doc(t);
    const std::string desc = ToolText(d.desc);
    if (!s.tool_filter.PassFilter((std::string(m.id) + ' ' + m.cn + ' ' + desc).c_str()))
      continue;
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.id);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.cn);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(def(m.exec).cn);
    ImGui::TableNextColumn();
    std::string keys, args;
    for (const ArgDef &a : d.args)
      keys += (keys.empty() ? "" : " ") + std::string(a.key), args += (args.empty() ? "" : "\n") + ArgText(a);
    ImGui::TextUnformatted(keys.empty() ? "—" : keys.c_str());
    if (!args.empty())
      tip_long(args);
    ImGui::TableNextColumn();
    wrapped(desc + (t == Tool::AskHuman ? " (只在可提问的会话里给)" : ""));
    tip_long("返回: " + ToolText(d.out));
  }
  ImGui::EndTable();
}

// 本会话要求: 过程页顶的折叠块 (默认展开, 开会话即折起; 折起时悬浮看全文). 有会话时只读 = 已随首条发出的那份; 其余时候编辑下次 New / Test 要发的
void remark_panel(Ui &s, const Mine &m) {
  const bool live = m.held();
  const std::string &text = live ? m.setup.remark : s.remark;
  if (s.fold_remark)
    ImGui::SetNextItemOpen(false, ImGuiCond_Always), s.fold_remark = false;
  const std::string label = "本会话要求 · " + (text.empty() ? std::string("无") : chars(text)) + "###remark";
  if (!ImGui::CollapsingHeader(label.c_str(), ImGuiTreeNodeFlags_DefaultOpen)) {
    if (!text.empty())
      tip_long(text);
    return;
  }
  if (live) {
    text.empty() ? wrapped_dim("(无)") : wrapped(text);
    return;
  }
  ImGui::TextDisabled("只对下一段会话: 附在首条消息 (自检会话的首条) 末尾发给 agent, 不改模板; 例: 着重测试某些算子 / 特征, 或交代这次的背景");
  input_multiline("##remark", s.remark, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 4));
}

// 过程 (本会话要求 + 全宽时间线 + 插话栏) / 假设板 / 候选表 / 特征表 / 算子表 (这三页不必开会话, 随作用域) / 工具表 (固定);
// 刚进 Choose 切回过程并滚到底 (问题在那); 要跳到某个调用也切回过程; 回退确认框浮在页上. 大脑是大模型还是外部 agent, 这里都一样
void render_mine(SharedData &data, Ui &s) {
  ImGui::TextDisabled("外部 agent 的 MCP 接口 %s: 它调 open 开会话 (没有会话时), 或接上大脑选外部 agent 时 New 开的会话", s.remote->listen().c_str());
  Mine &m = *s.mine;
  scope_row(data, s.opt.sc, s.pool_rows, m.lab, scope_locked(s), "1. Scope:");
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
  if (ImGui::BeginTabItem("过程###flow", nullptr, to_choose || s.jump || s.go ? ImGuiTabItemFlags_SetSelected : ImGuiTabItemFlags_None)) {
    remark_panel(s, m);
    timeline_panel(s, m, f, to_choose, memo_height(m));
    memo_bar(s, m);
    ImGui::EndTabItem();
  }
  const std::string hyps = "假设 (" + std::to_string(m.st.board.all().size()) + ")###hyps";
  if (ImGui::BeginTabItem(hyps.c_str())) {
    hyps_panel(s, m);
    ImGui::EndTabItem();
  }
  const View v = view_of(data, s);
  if (ImGui::BeginTabItem("候选###cands")) {
    if (ImGui::BeginTabBar("##cand_tabs")) {
      const std::string factors = "因子 (" + std::to_string(v.results.size()) + ")###factors";
      if (ImGui::BeginTabItem(factors.c_str())) {
        results_table(s, v);
        ImGui::EndTabItem();
      }
      for (const NoteKind k : kVals<NoteKind>) {
        const std::string label = std::string(def(k).cn) + " (" + std::to_string(v.notes.open(k)) + "/" + std::to_string(v.notes.of(k).size()) + ")###" + def(k).id;
        if (ImGui::BeginTabItem(label.c_str())) {
          notes_table(s, v, k);
          ImGui::EndTabItem();
        }
      }
      ImGui::EndTabBar();
    }
    ImGui::EndTabItem();
  }
  const Scope &sc = s.opt.sc;
  const Open &leaves_open = sc.leaves_of(v.ctx.level);
  int leaves_n = 0, leaves_on = 0, ops_on = 0;
  for (const Leaf &l : v.ctx.all_leaves)
    if (sc.has(l.src))
      ++leaves_n, leaves_on += leaves_open.has(l.code);
  for (const Op &o : v.ctx.all_ops)
    ops_on += sc.ops.has(o.name);
  const std::string leaves = "特征 (" + std::to_string(leaves_on) + "/" + std::to_string(leaves_n) + ")###leaves";
  const std::string ops = "算子 (" + std::to_string(ops_on) + "/" + std::to_string(v.ctx.all_ops.size()) + ")###ops";
  if (ImGui::BeginTabItem(leaves.c_str())) {
    leaves_panel(s, v.ctx, scope_locked(s));
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem(ops.c_str())) {
    ops_panel(s, v.ctx, scope_locked(s));
    ImGui::EndTabItem();
  }
  const std::string tools = "工具 (" + std::to_string(kNum<Tool>) + ")###tools";
  if (ImGui::BeginTabItem(tools.c_str())) {
    tools_panel(s);
    ImGui::EndTabItem();
  }
  ImGui::EndTabBar();
  rewind_modal(s, m);
}

// ---- Config ----
void render_config(Ui &s) {
  Options &o = s.opt;
  ImGui::TextColored(kColHead, "大脑");
  int brain = o.mcp;
  ImGui::RadioButton("大模型 (下面的后端 / 模型)", &brain, 0);
  ImGui::SameLine();
  ImGui::RadioButton("外部 agent (MCP)", &brain, 1);
  o.mcp = brain == 1;
  tip("谁来挖: 会话的状态机 / 消息 / 约束 / 日志 / 界面两者完全同一套, 只差谁发下一批调用\n"
      "大模型: 系统把消息发给所选模型, 收它的回复\n外部 agent: New 开的会话等外部 agent (Devin / Cursor 等, 见仓库 AGENT.md) 经 MCP 调 open 接上, 它交的每批即回复");
  ImGui::SameLine();
  ImGui::TextDisabled("下次 New / 继续 生效 (继续时可换大脑, 对话照时间线接上)");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "LLM");
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
  ImGui::TextDisabled("下次 New / 继续 生效 (继续时换了后端或模型, 对话历史照时间线重开)");

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
  limit("轮##budget_rounds", o.budget.rounds);
  ImGui::SameLine();
  limit("分钟##budget_minutes", o.budget.minutes);
  ImGui::SameLine();
  limit("千 token (输入 + 输出)##budget_ktokens", o.budget.ktokens);

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "入库门槛");
  ImGui::TextDisabled("agent 提交的候选至少一个权重达到才进正式因子库; 下次 New 生效");
  ImGui::SetNextItemWidth(120);
  ImGui::InputDouble("中位 rIC >=", &o.gate.ric, 0.0, 0.0, "%.3f");
  tip("中位 rIC = 滚动窗 (评估期 / 10) rIC 的中位, 同 Search 的目标");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "提问");
  ImGui::Checkbox("允许 agent 向你提问 (ask_human)", &o.ask);
  tip("开: agent 可在任一轮提一次问题 (每题带推荐), 等你选答后接着做\n关: 工具表里没有 ask_human, agent 自定因子与方向, 全自动挖掘");
  ImGui::SameLine();
  ImGui::TextDisabled("下次开会话生效 (New / 外部 agent 的 open)");

  ImGui::Spacing();
  ImGui::TextColored(kColHead, "会话约束");
  ImGui::Checkbox("守会话约束", &o.strict);
  tip("开: eval_factor 须挂判为支持的假设、每个假设至多几个变体; 有落了文件的回测的下一条只诊断; 有变体回测完的假设下一条须 judge; 不守整条退回\n"
      "关: 不查, 只按研究工具表校验参数");
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

} // namespace

} // namespace GUI::Agent

namespace GUI::Tasks {

TaskHandle CreateAgentTask() {
  using namespace Agent;
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
    fix_scope(s.opt.sc);
    poll_sniff(s);
    s.mine->update();
    if (s.bench)
      s.bench->update();
    s.remote->update(data, *s.mine, s.opt.sc, setup_of(s.opt), leg_of(s.opt, use_gpu(s)), bench_busy(s));
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
      return {TaskStatus::Kind::Busy, m.leg.mcp ? "agent" : "asking"};
    if (m.phase() == Phase::Act)
      return {TaskStatus::Kind::Busy, "call " + std::to_string(m.acting() + 1) + "/" + std::to_string(m.timeline.back().acts.size())};
    if (m.phase() == Phase::Pause)
      return {TaskStatus::Kind::Ready, "paused"};
    return {};
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    Ui &s = *state;
    ensure(s);
    fix_scope(s.opt.sc);
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
      s.bench->draw(data, tool_of(idx), s.opt.sc, s.opt.gate, s.opt.gpu == 1, s.mine->live() || !s.mine->idle());
      break;
    }
    ImGui::EndChild();
    save_ui(s);
  };

  handle.Destroy = [state]() {
    Ui &s = *state;
    s.sniff.quit();
    s.remote.reset(); // 析构: 未答的请求答 "程序在退出", 停网络线程
    s.bench.reset();
    s.mine.reset(); // 析构: 对话线程取消并 join, 评估 worker Stop
    s.seen_phase = Phase::None;
  };

  return handle;
}

} // namespace GUI::Tasks
