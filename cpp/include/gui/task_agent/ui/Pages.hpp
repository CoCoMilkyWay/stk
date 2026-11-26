#pragma once

// =============================================================================
// Pages — Agent 任务界面的共享台面 Ui (选项 / 模型嗅探 / 各页的界面态) 与三页入口:
//         Mine (ui/MinePage.cpp) / Tools (工作台, ui/Bench.cpp) / Config (ui/ConfigPage.cpp); 外壳 (页签 / 每帧推进 / 选项落盘) 在 TaskAgent.cpp
// =============================================================================

#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Remote.hpp"
#include "gui/task_agent/ui/Bench.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

struct SharedData;

namespace GUI::Agent::page {

using Phase = Mine::Phase;

constexpr int kBackendN = static_cast<int>(kNum<llm::Backend>); // 界面按下标 (int) 选后端

inline bool has_key(int backend) { return !llm::key(static_cast<llm::Backend>(backend)).empty(); }

// Mine / Tools / Config 页的选项 (ui.json 读写同一份字段清单): 作用域 (Mine 与 Tools 页共用, 含开放的叶 / 算子) / 后端 / 连接 / 模型 / 评估后端 / 预算 (下次 New / 继续生效) /
// 入库门槛 / 可否提问 (下次开会话生效: New 或外部 agent 的 open) + Config 页在看的模板
struct Options {
  Scope sc;
  int backend = 0;   // llm::Backend 下标
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
  bool ask = true; // agent 可用 ask_human 向人提问
  int prompt = 0;  // Msg 下标
  bool operator==(const Options &) const = default;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, sc, backend, model, via_proxy, proxy_host, proxy_port, gpu, gate, budget, ask, prompt)

inline llm::Net net_of(const Options &o, int backend) { return {o.via_proxy[backend], o.proxy_host, o.proxy_port}; }
inline Mine::Setup setup_of(const Options &o) { return {.sc = o.sc, .gate = o.gate, .ask = o.ask}; }
inline Mine::Leg leg_of(const Options &o, bool gpu) {
  return {.llm = {static_cast<llm::Backend>(o.backend), net_of(o, o.backend), o.model}, .gpu = gpu, .budget = o.budget};
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
  std::string message;
  std::string seen_dir; // 界面在跟的会话目录 (空 = 无会话); 换了 = 刚开 / 载入了一段 (New / Test / 载入 / 外部 agent 的 open 同一处跟上)
  // 本会话要求 (只随下次 New / Test 发, 不落 ui.json); fold_remark = 刚开会话, 下一帧把它折起
  std::string remark;
  bool fold_remark = false;
  // 过程页的跟随: pinned = 钉在底部跟新内容 (人往上滚 / 点开合即放开, 回到底部再钉); jump = 点了状态行, 跳到当前一步
  bool pinned = true, jump = false;
  float flow_y = 0.0f; // 上一帧的滚动位置
  // 两个 worker 在跑那件事的样子 (每帧 live 同步)
  ChatLive chat_live;
  LabLive lab_live;
  // 在收那一步: 增量总量与它上次变的时刻 (量静默)
  size_t live_n = 0;
  double live_t = 0.0;
  // 过程页展开着看 prompt (与 agent 往来的原文) 的条目 (键 = 条目标题的 id; 其余看展示)
  std::set<ImGuiID> raw;
  // 载入菜单的会话一览 (打开菜单时扫)
  std::vector<Saved> saved;
  // 插话的草稿 (按 "插话" 进 Mine::memo 排队)
  std::string memo;
  // 过程页各步展开着的 "agent 看到的上下文" (收起即丢): 键 = 步; sig = 算它时的样子 (步数 / 这一步发出时刻 / 消息与插话字节数), 变了重算
  struct Ctx {
    std::tuple<size_t, double, size_t, size_t> sig;
    std::string body;
  };
  std::unordered_map<size_t, Ctx> ctx;
  // 回退确认框: 回到第 k 步; loss = 会丢的; open = 下一帧打开
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

// ---- 嗅探 / 评估后端 / 工作台 (TaskAgent.cpp) ----
std::string first_model(const Ui &s, int backend);
void sniff(Ui &s, int backend);
bool model_ok(const Ui &s);
bool use_gpu(const Ui &s);
bool bench_busy(const Ui &s);

// ---- 页 ----
void follow(Ui &s);                        // 会话换了 (不论哪个入口开的 / 载入的): Mine 页跟上 (ui/MinePage.cpp)
void render_mine(SharedData &data, Ui &s); // ui/MinePage.cpp
void render_config(Ui &s);                 // ui/ConfigPage.cpp

} // namespace GUI::Agent::page
