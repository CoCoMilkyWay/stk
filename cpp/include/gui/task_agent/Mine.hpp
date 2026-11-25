#pragma once

// =============================================================================
// Mine — 挖掘会话 (唯一一套) 的状态机、计数与存档/可读日志 (基于双翼 Chat ⇄ Rig 对仗底座; 无 ImGui, 界面见 TaskAgent)
// =============================================================================
//   大脑 (Setup::remote 区分; 状态机 / 消息 / 约束 / 日志 / 界面完全同一套):
//     内置大模型 发出 = Chat 问模型 (无状态请求, 由 TurnsOf 折叠上下文), 收成的回复即一批调用
//     外部 agent (Setup::remote) 发出 = 交给它 (经 MCP, gui/task_agent/Remote.hpp 默认交回本条增量, 断链/回退时补研究树), 它 reply 交来的一批 (calls JSON) 即回复
//   双翼对仗:
//     左翼 Chat (问大模型): send(ChatReq) → Think / Write / Restart / End(Reply)
//     右翼 Lab  (问数据核): send(Tool, LabTask) → Search / Run / Look / Docs / End(LabOut)
//   调用执行统一走 Rig (open_act → step_act / settle_act / answer_act / abort_act, 与工作台 Bench 同一底座)
//   唯一真相: timeline (std::vector<Round>); st (Board + Tally + Flags + staged + adopted) 增量维护
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "nlohmann/json.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Agent {

struct Budget {
  int rounds = 0, minutes = 0, ktokens = 0; // 0 = 不限
  bool operator==(const Budget &) const = default;
};

// 停下后按继续的四种动作 (唯一真相表)
enum class Cont : uint8_t { Send,
                            Resend,
                            Acts,
                            Results };
struct ContDef {
  const char *id, *cn;
};
inline constexpr ContDef kConts[] = {{"send", "发出待发消息"}, {"resend", "重发未收成轮"}, {"acts", "续跑余下调用"}, {"results", "发本轮结果"}};
AGENT_ROWS(Cont, kConts, Results);

// 状态机纯函数 (Mine 与 Tools/Session/* 共用)
Cont ContOf(Rounds timeline, size_t cur);
std::string HintOf(Rounds timeline, size_t cur, bool done);
std::string BudgetMiss(const Budget &b, int d_rounds, double d_secs, int d_tokens);
std::string ReportOf(Rounds timeline, const State &st, const Notes &notes, const std::string &dir, const std::string &why, const std::string &remark, bool test, double now);

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Open, all, on)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Scope, pool, level, hold, lob, years, leaves, ops)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, ric)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Budget, rounds, minutes, ktokens)

// ---- 会话目录日志器 (只 GUI 线程写) ----
class Journal {
public:
  void open(std::string dir, const std::string &system, const std::string &head);
  void reopen(std::string dir, std::string head);
  const std::string &dir() const { return dir_; }
  const std::string &head() const { return head_; }
  std::string system() const;

  void append_log(const std::string &md) const;
  void rewrite_log(Rounds timeline, const std::string &stop_reason) const;
  void report(const std::string &text) const;
  void save(const nlohmann::ordered_json &session, const nlohmann::ordered_json &brief) const;

private:
  std::string dir_, head_;
};

inline constexpr const char *kSessionFile = "session.json";
inline constexpr const char *kBriefFile = "brief.json";
inline constexpr const char *kLogFile = "log.md";

// ---- 会话台面与状态机 (Mine : Rig) ----
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
    int rounds = 0, files = 0, notes = 0;
  };

  Chat chat;
  Setup setup;
  Leg leg;
  std::string system;
  std::shared_ptr<const std::string> system_ptr;
  Journal log;
  std::string memo;
  std::string reason, report;

  double now() const;
  Tally tally() const { return Rig::tally(now()); }

  void open(const SharedData &data, Setup s, Leg l);
  std::string try_open(const SharedData &data, Setup s, Leg l); // 校验作用域 / 数据就绪 / 开放叶与算子后开会话 (停着的先关闭); 失败返回原因且不动现状
  void load(const SharedData &data, const std::string &dir);
  void resume(const SharedData &data, Leg l);
  void start();
  void stop();
  void cancel();
  void close();
  std::string reply(std::string calls); // 外部 agent 交来一批 (calls JSON): 通过返回空并开始执行; 不合协议原地返回重发提示 (不污染 timeline)
  void queue(std::string m);
  std::string unqueue();
  void save() const;
  void update();

  Loss loss(int k) const;
  void rewind(int k);

  Phase phase() const { return phase_; }
  bool live() const { return phase_ == Phase::Think || phase_ == Phase::Act || phase_ == Phase::Choose; }
  bool held() const { return phase_ != Phase::None; }
  bool idle() const { return !chat.busy() && !lab.busy(); }
  bool stopping() const { return stop_ && live(); }
  bool remote() const { return held() && setup.remote; }                                                         // 会话由外部 agent 驱动
  bool waiting() const { return phase_ == Phase::Think && !timeline.empty() && timeline.back().span.running(); } // 本条已发出, 在等大脑交这一批
  std::string brain() const;                                                                                     // 大脑一句: 后端 / 模型, 或外部 agent
  size_t acting() const { return cur_; }
  int rev() const { return rev_; } // 时间线代号 (开 / 载入 / 回退递增; 外部 agent 据此判是否需补研究树)
  Cont cont() const;
  std::string hint() const;

private:
  void bind(const SharedData &data);
  void depart();
  bool go(Phase next);
  void post(Message m);
  void send();
  void drop(std::string why, llm::Reply r = {});
  void pause(std::string why);

  void on_chat(Chat::Tick t);
  void on_lab(Lab::Tick t);
  void replied(llm::Reply r);
  std::string take(llm::Reply r);

  void step();
  Seat mine_seat();
  void next();
  void log_act(size_t i);

  std::vector<std::string> staged_from(int k) const;
  void discard(int k);
  void redo(int k, std::string why);
  void unsend();
  std::string over() const;
  std::string make_log_head() const;
  std::string report_of(const std::string &why) const;

  Phase phase_ = Phase::None;
  bool stop_ = false;
  size_t cur_ = 0;
  int retries_ = 0;
  int rev_ = 0;
  std::string retry_reply_, retry_error_; // 本轮内置模型回复未过协议时的原地重试缓存 (不进 timeline)
  struct From {
    int round = 0;
    double t = 0.0;
    int tokens = 0;
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
std::vector<Round> TimelineOf(const std::string &dir); // 存过的会话的时间线 (工作台载入日志时取那次调用之前的部分作快照)

// 存过的会话里某个工具做完的调用 (工作台 "载入日志" 的输入, 带当时交给 agent 的原文作对照)
struct Seen {
  std::string dir, stamp;
  bool test = false;
  Ref at;
  nlohmann::ordered_json args;
  std::string out;
  bool ok = true;
};
std::vector<Seen> SeenCalls(const Scope &sc, Tool t);

} // namespace GUI::Agent
