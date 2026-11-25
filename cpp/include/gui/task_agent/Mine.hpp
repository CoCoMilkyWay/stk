#pragma once

// =============================================================================
// Mine — 挖掘会话 (唯一一套) 的主循环、计数与存档/可读日志 (基于双翼 Chat ⇄ Rig 对仗底座; 无 ImGui, 界面见 TaskAgent)
// =============================================================================
//   主循环 advance (唯一状态 = 时间线 + 停下原因 (非空 = 停着); 阶段由末步推出), 一步一个调用:
//     末步未发出 → 交大脑; 在等回复 / 在跑的调用 (后台 Lab 或等研究员答的 ask_human) → 等;
//     调用未做 → 启动它; 做完 → 下一条入时间线 (它的结果; 不合协议 = 重交要求), finish / 连续不合 / 预算到了即停
//   大脑 (Setup::remote 区分, 只在发出时分叉; 收到一律走 got), 游标 told_ 记它连着收到第几步: 连着的只给本条, 否则 (首次 / 载入 / 回退 / 作废过) 给整份 TurnsOf:
//     内置大模型 = Chat (无状态后端每次全量, 有状态后端连着时只发本条, 见 gui/task_agent/Llm.hpp)
//     外部 agent = 经 MCP 交给它 (gui/task_agent/Remote.hpp), 它 reply 交来的一个调用即回复
//   不合协议的回复照收成一步 (无调用, Step::error = 原因), 下一条即重交要求
//   停下只有 Cancel (在收的回复 / 在跑的调用作废) 与系统停 (finish / 预算 / 连续不合 / 回复失败); 继续 = 接着 advance
//   调用执行统一走 Rig (open_act → pump_act / answer_act, abort_act; 与工作台 Bench 同一底座); 账本即时间线的纯折叠 (tools/Kit.hpp)
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "nlohmann/json.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace GUI::Agent {

struct Budget {
  int steps = 0, minutes = 0; // 0 = 不限
  bool operator==(const Budget &) const = default;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Open, all, on)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Scope, pool, level, hold, lob, years, leaves, ops)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, ric)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Budget, steps, minutes)

// ---- 会话目录日志器 (只 GUI 线程写): log.md 随存档按时间线整份重写 ----
class Journal {
public:
  void open(std::string dir, const std::string &system, const std::string &head);
  void reopen(std::string dir, std::string head);
  const std::string &dir() const { return dir_; }
  const std::string &head() const { return head_; }
  std::string system() const;

  void report(const std::string &text) const;
  void save(const nlohmann::ordered_json &session, const nlohmann::ordered_json &brief, Steps timeline) const;

private:
  std::string dir_, head_;
};

inline constexpr const char *kSessionFile = "session.json";
inline constexpr const char *kBriefFile = "brief.json";
inline constexpr const char *kLogFile = "log.md";

// ---- 会话台面与主循环 (Mine : Rig) ----
class Mine : public Rig {
public:
  struct Setup {
    Scope sc;
    Gate gate;
    bool ask = true;
    bool test = false;
    bool remote = false; // 由外部 agent 经 MCP 驱动 (无内置模型)
    std::string remark;
  };
  struct Leg {
    llm::Options llm;
    bool gpu = false;
    Budget budget;
  };
  enum class Phase : uint8_t { None,
                               Think,
                               Act,
                               Choose,
                               Pause };
  struct Loss {
    int steps = 0, files = 0, notes = 0;
  };

  Chat chat;
  Setup setup;
  Leg leg;
  std::shared_ptr<const std::string> system; // 系统提示 (会话内不变; 与 Chat 请求共享)
  Journal log;
  std::string memo;
  std::string reason, report; // 停下原因 (非空 = 停着; 在跑时恒空) / 停下报告

  double now() const;
  Tally tally() const { return Rig::tally(now()); }

  std::string try_open(const SharedData &data, Setup s, Leg l); // 校验作用域 / 数据就绪 / 开放叶与算子后开会话 (停着的先关闭); 失败返回原因且不动现状
  void load(const SharedData &data, const std::string &dir);
  void resume(const SharedData &data, Leg l);
  void answer(); // 交 ask_human 的答案
  void cancel();
  void close();
  void reply(std::string call); // 外部 agent 交来一个调用 ({"tool", "args"} JSON; 不合协议的也收成一步)
  std::string deliver();        // 交给外部 agent 的本条 (同内置大脑: 连着只给本条, 否则整份上下文); 记下它已收到这一步
  void queue(std::string m);
  std::string unqueue();
  void save() const;
  void update();

  Loss loss(int k) const;
  void rewind(int k);

  Phase phase() const;
  bool live() const { return held_ && reason.empty(); }
  bool held() const { return held_; }
  bool idle() const { return !chat.busy() && !lab.busy(); }
  bool remote() const { return held() && setup.remote; } // 会话由外部 agent 驱动
  std::string brain() const;                             // 大脑一句: 后端 / 模型, 或外部 agent
  std::string hint() const;

private:
  void bind(const SharedData &data);
  void depart();
  int sent() const;
  void advance();
  void post(Message m);
  void send();
  void got(llm::Reply r);
  void drop(std::string why, llm::Reply r = {});
  void mark(std::string md);
  void pause(std::string why);

  void on_chat(Chat::Tick t);
  Seat seat_of();
  bool fresh();

  std::set<std::string> staged_from(int k) const;
  void discard(int k);
  void redo(int k, std::string why);
  void unsend();
  std::string over() const;
  std::string make_log_head() const;
  std::string report_of(const std::string &why) const;

  bool held_ = false;
  int told_ = -1; // 大脑连着收到的末步 (-1 = 没连着: 开 / 载入 / 回退 / 作废回复后)
  struct From {
    int step = 0; // 已发出的步数
    double t = 0.0;
  } from_;
  std::chrono::steady_clock::time_point t0;
  double t_end = -1.0;
};

struct Saved {
  std::string dir, stamp;
  bool test = false;
  Scope sc;
  nlohmann::ordered_json brief;
};
std::vector<Saved> SavedSessions(const Scope &sc);
std::vector<Step> TimelineOf(const std::string &dir); // 存过的会话的时间线 (工作台载入日志时取那次调用之前的部分作快照)

} // namespace GUI::Agent
