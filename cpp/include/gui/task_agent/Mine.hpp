#pragma once

// =============================================================================
// Mine — 挖掘会话 (唯一一套) 的状态机、计数与存档/可读日志 (基于双翼 Chat ⇄ Rig 对仗底座; 无 ImGui, 界面见 TaskAgent)
// =============================================================================
//   大脑 (Leg::mcp 二选一, 继续时可换; 状态机 / 消息 / 约束 / 日志 / 界面完全同一套):
//     大模型    发出 = Chat 问模型 (上下文 TurnsOf 纯函数折叠), 收成的回复即一批调用
//     外部 agent 发出 = 交给它 (经 MCP, gui/task_agent/Remote.hpp 取 TextOf), 它 reply 交来的一批 (calls JSON) 即回复
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

// ---- 会话台面与状态机 (Desk : Rig) ----
struct Desk : Rig {
  struct Setup {
    Scope sc;
    Gate gate;
    bool ask = true;
    bool strict = true; // 守会话约束 (RuleMiss + eval_factor 须挂判为支持的假设)
    bool test = false;
    std::string remark;
  };
  struct Leg {
    llm::Options llm;
    bool mcp = false; // 大脑 = 外部 agent (经 MCP 交批); 否则 = 大模型 llm
    bool gpu = false;
    Budget budget;
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

protected:
  std::chrono::steady_clock::time_point t0;
  double t_end = -1.0;
};

class Mine : public Desk {
public:
  enum class Phase : uint8_t { None,
                               Think,
                               Act,
                               Choose,
                               Pause };
  enum class Cont : uint8_t { Send,
                              Resend,
                              Retry,
                              Acts,
                              Results };
  struct Loss {
    int rounds = 0, files = 0, notes = 0;
  };

  void open(const SharedData &data, Setup s, Leg l);
  void load(const SharedData &data, const std::string &dir);
  void resume(const SharedData &data, Leg l);
  void start();
  void stop();
  void cancel();
  void close();
  void reply(std::string calls); // 外部 agent 交来一批 (calls JSON = 大模型的一条回复); 须 waiting()
  void drop();                   // 外部 agent 作废本批余下的调用 (在跑的作废, 没开的不做), 接着发结果
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
  bool mcp() const { return held() && leg.mcp; }                                                                 // 会话的大脑是外部 agent
  bool waiting() const { return phase_ == Phase::Think && !timeline.empty() && timeline.back().span.running(); } // 本条已发出, 在等大脑交这一批
  std::string brain() const;                                                                                     // 大脑一句: 后端 / 模型, 或外部 agent
  size_t acting() const { return cur_; }
  Cont cont() const;
  std::string hint() const;

private:
  void bind(const SharedData &data);
  void depart();
  bool go(Phase next);
  void post(Message m);
  void send();
  void pause(std::string why);

  void on_chat(Chat::Tick t);
  void on_lab(Lab::Tick t);
  void replied(llm::Reply r);

  void step();
  Seat mine_seat();
  void next();
  void log_act(size_t i);

  std::vector<std::string> staged_from(int k) const;
  void discard(int k);
  void unsend();
  std::string over() const;
  std::string make_log_head() const;
  std::string report_of(const std::string &why) const;

  Phase phase_ = Phase::None;
  bool stop_ = false;
  size_t cur_ = 0;
  int retries_ = 0;
  struct From {
    int round = 0;
    double t = 0.0;
    int tokens = 0;
  } from_;
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
