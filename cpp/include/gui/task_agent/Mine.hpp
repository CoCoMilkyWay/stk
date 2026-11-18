#pragma once

// =============================================================================
// Mine — 挖掘会话状态机、工具分派、计数与存档/可读日志 (无 ImGui, 界面见 task_agent/TaskAgent)
// =============================================================================
//   双翼对仗:
//     左翼 Chat (问大模型): send(ChatReq) → Think / Write / Restart / End(Reply)
//     右翼 Lab  (问数据核): send(Call, LabFn) → Search / Run / Look / Docs / End(ToolOut)
//   调用的一生走 Kit (Begin → Enter … → Settle / Answer / Abort → End, 与工作台同一份); Begin 返回这一步落在谁手上 (Exec):
//     Sync   GUI 线程当场完成 (查叶 / 列表 / 全局记录 / 假设与判定 / 提交 / finish), 接着下一个
//     Lab    已投右翼 worker (eval_factor 先核假设并落候选文件), 等 End
//     Human  进 Choose 等研究员点选答案 (ask_human)
//   唯一真相: timeline (std::vector<Round>); st = StateOf(timeline) 增量 Apply, 上下文 TurnsOf(timeline) 按需纯函数折叠
//   会话目录 (output/agent/<main|test>/sessions/<池>/<层>/<持有期>/chat/<时刻>/):
//     system.md              系统提示
//     log.md                 肉眼直接阅读的连续 Markdown 研究日志 (思考折叠块 + 调用清单 + 各工具表格与火花线; 回退时自动重整)
//     report.md              停下总结报告 (入库 / 假设全貌 / 全局记录)
//     session.json / brief.json  原子替换的会话存档与列表摘要
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Tools.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "nlohmann/json.hpp"

#include <array>
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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, mono, ric)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Budget, rounds, minutes, ktokens)

// ---- 计数与流程旗 (时间线的纯函数) ----
struct Tally {
  std::array<int, kToolCount> calls{}, fails{};
  std::array<int, kNum<Verdict>> verdicts{};
  int adopted = 0;
  double llm_s = 0.0, act_s = 0.0;
  llm::Usage usage;

  int n(Tool t) const { return calls[static_cast<size_t>(t)]; }
  int n(Verdict v) const { return verdicts[static_cast<size_t>(v)]; }
  int n(Exec e) const;
  int n(Cat c) const;
  int docs() const { return n(Tool::SearchDocs) + n(Tool::SearchFactors) + n(Tool::ReadDoc); }
  int all() const;
  int failed(Tool t) const { return fails[static_cast<size_t>(t)]; }
  int failed(Cat c) const;
  int failed() const;
  int staged() const { return n(Tool::EvalFactor) - n(Verdict::Unstaged); }
};
Tally TallyOf(Rounds rs, double now);

struct Flags {
  bool evaled = false; // 末个有调用的轮里有落了文件的回测 (下一条回复不能回测)
  bool asked = false;  // 已有 ask_human 调用 (整段会话只问一次)
  bool done = false;   // 末轮的 finish 已做完: 本条做完即结束会话
};
Flags FlagsOf(Rounds rs);

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

// ---- 会话台面与状态机 ----
struct Desk {
  struct Setup {
    Scope sc;
    Gate gate;
    bool ask = true;
    bool test = false;
    std::string remark;
  };
  struct Leg {
    llm::Options llm;
    bool gpu = false;
    Budget budget;
  };

  Chat chat;
  Lab lab;
  Setup setup;
  Leg leg;
  std::string system;
  std::shared_ptr<const std::string> system_ptr;
  Journal log;
  Notes notes;
  std::vector<Round> timeline;
  State st;
  std::string memo;
  std::string reason, report;

  double now() const;
  int round() const { return static_cast<int>(timeline.size()); }
  Tally tally() const { return TallyOf(timeline, now()); }
  Flags flags() const { return FlagsOf(timeline); }

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
  void on_lab(LabTick t);
  void replied(llm::Reply r);

  void step();
  Seat seat();
  void next();
  void ended(Act &a);
  void abort(Act &a);

  std::vector<std::string> staged_from(int k) const;
  void discard(int k);
  void unsend();
  void restate();
  void flush();
  std::string over() const;
  std::string rule_miss(const std::vector<Call> &cs) const;
  std::string make_log_head() const;
  std::string report_of(const std::string &why) const;

  Phase phase_ = Phase::None;
  bool stop_ = false;
  size_t cur_ = 0;
  int retries_ = 0;
  std::vector<std::string> doomed_;
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
std::vector<Round> TimelineOf(const std::string &dir); // 存过的会话的时间线 (工作台的快照取它的前 k 轮)

// 存过的会话里某个工具做完的调用 (工作台 "载入日志" 的输入, 带当时交给 agent 的原文作对照)
struct Seen {
  std::string stamp;
  bool test = false;
  Ref at;
  nlohmann::ordered_json args;
  std::string out;
  bool ok = true;
};
std::vector<Seen> SeenCalls(const Scope &sc, Tool t);

} // namespace GUI::Agent
