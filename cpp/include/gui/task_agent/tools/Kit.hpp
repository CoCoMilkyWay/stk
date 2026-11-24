#pragma once

// =============================================================================
// Kit — 时间线、单趟增量账本、工具三拍契约与统一执行器 Rig: 会话 (Mine, 大脑是大模型或外部 agent) / 工作台 (Bench) 100% 同构同路
// =============================================================================
//   时间线与账本:
//     Round / Act     全会话唯一真相 (Act 仅持 Call + Span + ToolOut, 无旁路 Extra; Res 即唯一结构化载体)
//     State           单趟增量账本 = 假设板 Board + 计数 Tally + 流程旗 Flags + 已落文件 staged + 已入库 adopted
//                     OpenRound / CloseReply / ApplyAct 增量 O(1) 推进; StateOf(Rounds) 纯函数全量重算; TallyAt(now) O(1) 补在跑项秒数
//   三态四形契约 (kSpecs 由 AGENT_TOOLS 拼名, 编译期核对签名 = ToolMeta::exec):
//     SyncFn      (Call, Seat) → ToolOut                   当场一次到位
//     LabFn       (Call, Bay)  → ToolOut                   后台纯计算直通
//     StagedLab   open(Act, Seat) → LabTask ⇄ close(...)   后台三拍 (eval_factor: GUI 核假设落盘 → Lab 回测 → GUI 入表封存)
//     HumanFn     (shared_ptr<AskRes>) → ToolOut           等人三拍 (ask_human: GUI 建问卷 → 人点选 → GUI 封存)
//   Rig             实验台底座 (Lab + Notes + timeline + State + doomed): 统一驱动单调用 open / step / settle / answer / abort
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace GUI::Agent {

// ---- 时段、消息、动作与时间线 ----
struct Span {
  double t0 = -1.0, t1 = -1.0;
  bool began() const { return t0 >= 0.0; }
  bool ended() const { return t1 >= 0.0; }
  bool running() const { return began() && !ended(); }
  double len(double now) const { return !began() ? 0.0 : (ended() ? t1 : now) - t0; }
  void begin(double t) {
    if (!began())
      t0 = t;
  }
  void end(double t) {
    if (running())
      t1 = t;
  }
};

enum class Msg : uint8_t { System,
                           First,
                           Round,
                           Retry,
                           Test };
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首轮"}, {"round", "结果"}, {"retry", "重发"}, {"test", "自检"}};
AGENT_ROWS(Msg, kMsgs, Test);

struct Message {
  Msg kind = Msg::First;
  std::string text;
};

struct Act {
  Call call;
  Span span;
  ToolOut out; // out.res 为唯一结构化载体 (TrialRes / AskRes 在挂起期即驻留)
};

struct Round {
  Msg kind = Msg::First;
  std::string user;
  std::string memo;
  Span span, think, write;
  llm::Reply reply;
  std::string error;
  std::vector<Act> acts;
};
using Rounds = std::span<const Round>;

// ---- 计数、流程旗与单趟增量账本 State ----
struct Tally {
  Per<Tool, int> calls{}, fails{};
  Per<Verdict, int> verdicts{};
  int adopted = 0;
  double llm_s = 0.0, act_s = 0.0;
  llm::Usage usage;

  int n(Tool t) const { return calls[t]; }
  int n(Verdict v) const { return verdicts[v]; }
  int n(Exec e) const;
  int n(Cat c) const;
  int docs() const { return n(Tool::SearchDocs) + n(Tool::SearchFactors) + n(Tool::ReadDoc); }
  int all() const;
  int failed(Tool t) const { return fails[t]; }
  int failed(Cat c) const;
  int failed() const;
  int staged() const { return n(Tool::EvalFactor) - n(Verdict::Unstaged); }
};

struct Flags {
  bool evaled = false; // 末个有调用的轮里有落了文件的回测 (下一条回复不能回测)
  bool asked = false;  // 已有 ask_human 调用 (整段会话只问一次)
  bool done = false;   // 末轮的 finish 已做完: 本条做完即结束会话
};

struct State {
  Board board;
  std::set<std::string> staged;  // 本会话落了文件的候选文件名 (*.json)
  std::set<std::string> adopted; // submit_factor 做成的英文名
  Tally tally;                   // 已收尾轮与调用的精确累计
  Flags flags;
};

void OpenRound(State &s);
void CloseReply(State &s, const Round &rd);
void ApplyAct(State &s, const Act &a, Ref at);
State StateOf(Rounds rs);
Tally TallyAt(const State &s, Rounds rs, double now);
Tally TallyOf(Rounds rs, double now);
// 会话约束 (回测后的下一条只诊断 / 待判的先 judge): 账本 s 之后交来 cs 不守的原因; 守 → 空
std::string RuleMiss(const State &s, const std::vector<Call> &cs);

// ---- 台面与三拍工具签名 ----
struct Seat {
  Lab &lab;
  Notes &notes;
  const Gate &gate;
  const State &st;
  Rounds timeline; // 末轮 = 本调用所在轮
  Ref me;          // 本调用的编号
  std::string scope, session;
  bool free = false; // 不查假设板 (eval_factor 不必挂判为支持的假设): 工作台 / 不守会话约束的会话
  bool dry = false;  // 不落记录: 全局记录只给编号不写, submit_factor 只查门槛不入库 (工作台)
  bool gpu = false;  // eval_factor 的回测上 GPU
};

struct AskRes;
using SyncFn = ToolOut (*)(const Call &, Seat &);
using LabFn = ToolOut (*)(const Call &, Bay &);
struct StagedLab {
  std::optional<LabTask> (*open)(Act &, Seat &);
  void (*close)(Act &, ToolOut, Seat &);
};
using HumanFn = ToolOut (*)(std::shared_ptr<AskRes>);
using ImplFn = std::variant<SyncFn, LabFn, StagedLab, HumanFn>;

// ---- 统一调用底座 Rig (Mine / Bench 共用) ----
struct Rig {
  Lab lab;
  Notes notes;
  std::vector<Round> timeline;
  State st;
  std::vector<std::string> doomed;

  int round() const { return static_cast<int>(timeline.size()); }
  Tally tally(double now) const { return TallyAt(st, timeline, now); }
  const Flags &flags() const { return st.flags; }

  Seat seat(size_t i, const Gate &gate, std::string_view session, bool free, bool dry, bool gpu);
  void restate() { st = StateOf(timeline); }
  void flush();

  Exec open_act(size_t i, Seat &s, double t); // Sync (含未落文件的 eval_factor) 当场收尾并返回 Exec::Sync
  void step_act(size_t i, LabStep step, double t);
  void settle_act(size_t i, ToolOut out, Seat &s, double t);
  void answer_act(size_t i, double t);
  void abort_act(size_t i, double t, std::string_view who = "研究员"); // who = 谁取消的 (写进结果)
};

// ---- 随机输入的取材 ----
struct Spot {
  std::string code, day, clock; // 代码 / 交易日 YYYY-MM-DD / 行末时刻 HH:MM (日层空)
};

struct Feed {
  const Lab &lab;
  const Context &ctx;
  const std::vector<Result> &tried;
  const Gate &gate;
  const Board &board;
  Rounds timeline;
  Ref me;
  std::mt19937_64 &rng;

  int below(int n); // [0, n)
  bool coin(double p);
  template <class T>
  const T &one(const std::vector<T> &v) {
    return v[static_cast<size_t>(below(static_cast<int>(v.size())))];
  }
  std::string leaf();                                       // 开放的叶
  std::vector<std::string> leaves(int n);                   // 至多 n 个互不相同的开放的叶
  std::vector<std::string> grid();                          // 开放的本层叶 (因子须含本层的叶)
  std::string factor();                                     // 可见且有权重的候选名; 没有就拿开放的本层叶套开放的归一根
  std::string day();                                        // 评估期内的交易日 YYYY-MM-DD
  std::pair<std::string, std::string> span(int lo, int hi); // 评估期内连续 lo..hi 个交易日的首末日
  std::string code();                                       // 池内代码
  std::string clock();                                      // 本层某一行的行末时刻 (不含竞价行; 日层空)
  std::string tod(int cap, const std::string &around = {}); // 本层连续 1..cap 行的行末时段 HH:MM-HH:MM (给了 around 就罩住它; 日层空)
  Spot spot();                                              // 一只当日有逐笔的票的一天: 多半取工作台里 view_trades 明细列过的段
  std::string hyp(bool supported);                          // 假设板上的编号 (supported: 能挂变体的优先 = 判为支持、变体有余、没有别轮待判的); 板空给 H1
  std::vector<std::string> refs(int n);                     // 做成了的调用编号 (新到旧)
};
nlohmann::ordered_json SampleArgs(Tool t, Feed &p);

} // namespace GUI::Agent
