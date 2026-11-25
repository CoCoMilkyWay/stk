#pragma once

// =============================================================================
// Kit — 时间线、账本、工具契约与统一执行器 Rig: 会话 (Mine, 大脑是大模型或外部 agent) / 工作台 (Bench) 100% 同构同路
// =============================================================================
//   时间线与账本:
//     Step / Act      全会话唯一真相: 一步 = 发给大脑的一条消息 → 它交来的一个调用 → 执行 (调用编号 #<步>); Act = Call + Span + 分段 Lap + ToolOut
//     ToolOut         一个调用的产出: 原文 / 折叠后的一行 / 账本作用 Effect / 结构化结果 Res (唯一载体)
//     Tally           时间线的纯折叠 TallyOf(Steps, now) (在跑的补到 now); Finished / AdoptedOf / StagedOf 同为纯函数; 账本只读 Effect, 不认具体工具
//   工具契约: 一律 Call 进 → ToolOut 出, 不碰 Act; 四形 = 四个函数类型, 清单 AGENT_TOOLS 按形一行生成全部工具函数的声明 (签名不合即编译错):
//     Sync   SyncSig   (Call, Seat) → ToolOut     当场做完
//     Pure   PureSig   (Call, Bay)  → ToolOut     纯后台, 算完即是结果
//     Lab    LabSig    (Call, Seat) → Prep        GUI 备料 (先给的 ToolOut + 后台任务) → Lab 计算 (Bay) → GUI 收尾 Commit (ToolOut, Desk); 任务空 = 备料即收尾
//     Human  HumanSig  (Call, Seat) → ToolOut     交出问卷 → 人点选 → 交答案即成文
//   台面: 当场 Seat (GUI 线程, 可写候选台 Desk 与全局记录) ⇄ 后台 Bay (Lab 线程, 只读快照 ViewEnv + 计算流), 见 tools/Bay.hpp
//   Rig  实验台底座 (Desk + Lab + Notes + 时间线 + doomed): 末步调用的一生 open → pump (Lab) / answer (Human) → 收尾; abort 随时作废; restore 存档复原
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace GUI::Agent {

// ---- 时段、消息 ----
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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Span, t0, t1)

// 消息的模板 (output/config/agent/prompt/<id>.md); 时间线上一步发出的是哪种由时间线推出 (KindOf)
enum class Msg : uint8_t { System,
                           First,
                           Result,
                           Test,
                           Retry };
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首条"}, {"result", "结果"}, {"test", "自检"}, {"retry", "重交"}};
AGENT_ROWS(Msg, kMsgs, Retry);

// ---- 工具的产出 ----
// 一个调用对会话账本的作用 (由结果给出: 开拍即取一次, 做完再取; 账本 / 回退 / 作废只读它)
struct Effect {
  std::string staged;             // 落下的候选文件 (回退 / 作废时删)
  std::string adopted;            // 入库 (自检 / 工作台 = 达门槛) 的候选英文名
  std::optional<Verdict> verdict; // 回测结局
  bool finish = false;            // 宣布挖完
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Effect, staged, adopted, verdict, finish)

// 工具的结构化结果 (唯一载体: Mine 时间线与 Bench 工作台共用); 要存档的另给 static ResPtr load(Call, json, Seat) (实现表按清单的结果类型探到)
struct Res {
  virtual ~Res() = default;
  virtual std::string text() const = 0;                           // 交给 agent 的原文
  virtual std::string gist() const { return {}; }                 // 上下文折叠后留的一行关键数 (无则空)
  virtual std::string title() const = 0;                          // 时间线上这个调用一行的结论 (对象 + 关键结果)
  virtual Effect effect() const { return {}; }                    // 对会话账本的作用
  virtual bool abort() { return false; }                          // 作废: true = 已改成作废后的样子 (原文照它); false = 只记一句没做完
  virtual nlohmann::ordered_json save() const { return nullptr; } // 存档 (null = 不存)
  virtual void draw() const = 0;                                  // 结构化展示
};
using ResPtr = std::shared_ptr<Res>;

struct ToolOut {
  bool ok = true;
  std::string text, gist, title; // title = 结果的一行结论 (随存档; 没有结果的 = 空)
  Effect fx;
  ResPtr res;
};
template <class R>
ToolOut Done(std::shared_ptr<R> r, bool ok = true) {
  std::string text = r->text(), gist = r->gist(), title = r->title();
  Effect fx = r->effect();
  return {ok, std::move(text), std::move(gist), std::move(title), std::move(fx), std::move(r)};
}
inline ToolOut Fail(std::string why) { return {false, std::move(why), {}, {}, {}, {}}; }

// ---- 动作与时间线 ----
// 后台调用的一段 (eval_factor = Search / Run; 其余后台工具没有分段)
struct Lap {
  LabStep step = LabStep::End;
  Span span;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Lap, step, span)

struct Act {
  Call call;
  Span span;
  std::vector<Lap> laps;
  ToolOut out;
};

inline std::string StepTag(int step) { return "#" + std::to_string(step); } // 调用编号 = 它所在的步

// 会话流程的一笔 (停下 / 继续 / 作废的回复), 记在这一步走到的位置之后; 进可读日志, 界面上与调用同列
enum class LogAt : uint8_t { Sent,    // 发出之后 (回复收成之前)
                             Replied, // 回复收成之后 (调用做完之前)
                             Done };  // 调用做完之后
enum class Beat : uint8_t { Pause,
                            Resume,
                            Drop };
struct BeatDef {
  const char *id, *cn;
};
inline constexpr BeatDef kBeats[] = {{"pause", "停下"}, {"resume", "继续"}, {"drop", "作废回复"}};
AGENT_ROWS(Beat, kBeats, Drop);
struct LogMark {
  LogAt at = LogAt::Sent;
  Beat kind = Beat::Pause;
  std::string line, md; // 一行 (停下原因 / 继续的大脑 / 作废的原因) / 日志里的一节
};

struct Step {
  std::string user;        // 发出的消息 (不含插话)
  std::string memo;        // 随它发出的研究员插话
  Span span, think, write; // 发出 → 回复收齐 / 其中的思考 / 正文
  llm::Reply reply;
  std::string error;      // 非空 = 回复不合协议 (无调用; 下一条 = 重交要求)
  std::optional<Act> act; // 回复交来的调用
  std::vector<LogMark> marks;
};
using Steps = std::span<const Step>;

// 第 i 步发出的是哪种消息: 首条 (自检会话的首条同属此类) / 上一步调用的结果 / 上一条不合协议的重交要求
inline Msg KindOf(Steps ss, size_t i) { return i == 0 ? Msg::First : ss[i - 1].act ? Msg::Result
                                                                                   : Msg::Retry; }

// ---- 账本: 时间线的纯折叠 ----
struct Tally {
  Per<Tool, int> calls{}, fails{};
  Per<Verdict, int> verdicts{};
  int adopted = 0;
  double llm_s = 0.0, act_s = 0.0;
  llm::Usage usage;

  int n(Tool t) const { return calls[t]; }
  int n(Verdict v) const { return verdicts[v]; }
  int n(Cat c) const;
  int docs() const { return n(Tool::SearchDocs) + n(Tool::SearchFactors) + n(Tool::ReadDoc); }
  int all() const;
  int failed(Tool t) const { return fails[t]; }
  int failed(Cat c) const;
  int failed() const;
  int staged() const { return n(Tool::EvalFactor) - n(Verdict::Unstaged); }
};

Tally TallyOf(Steps ss, double now);       // 发出的回复与开始的调用 (在跑的计到 now; 失败 / 结局 / 入库只计做完的)
bool Finished(Steps ss);                   // 末个发出的一步之前 finish 已做完: 会话结束 (再发出一条即作罢)
std::set<std::string> AdoptedOf(Steps ss); // 做成的入库的英文名
std::set<std::string> StagedOf(Steps ss);  // 开拍了的调用落下的候选文件 (回测备料即落, 在跑的也算)

// ---- 台面与四形 ----
struct Seat {
  Desk &desk;
  Notes &notes;
  const Gate &gate;
  int me; // 本调用的编号 (= 步)
  std::string scope, session;
  bool dry = false; // 不落记录: 全局记录只给编号不写, submit_factor 只查门槛不入库 (工作台)
  bool gpu = false; // eval_factor 的回测上 GPU
};

// Lab 形的备料: 先给的产出 (结果的半成品) + 后台任务 (空 = 备料即收尾)
struct Prep {
  ToolOut out;
  LabTask task;
};

using SyncSig = ToolOut(const Call &, Seat &);
using PureSig = ToolOut(const Call &, Bay &);
using LabSig = Prep(const Call &, Seat &);
using HumanSig = ToolOut(const Call &, Seat &);

// ---- 全部工具函数的声明 (清单一行一个 = 形的签名): 定义在 tools/<类>/<工具>.cpp ----
#define AGENT_TOOL_DECL(T, id, cn, F, C, keep, R) F##Sig T;
AGENT_TOOLS(AGENT_TOOL_DECL)
#undef AGENT_TOOL_DECL

// ---- 统一调用底座 Rig (Mine / Bench 共用): 只驱动末步的调用 ----
struct Rig {
  Desk desk;
  Lab lab;
  Notes notes;
  std::vector<Step> timeline;
  std::vector<std::string> doomed;

  int step() const { return static_cast<int>(timeline.size()); }
  Tally tally(double now) const { return TallyOf(timeline, now); }
  const Act *acting() const; // 末步在跑或待做的调用 (没有 = 空)

  Seat seat(const Gate &gate, std::string_view session, bool dry, bool gpu); // 本调用 = 末步的
  void flush();

  bool open_act(Seat &s, double t);                           // 开拍; true = 当场做完 (Sync, 或备料即收尾的 Lab)
  bool pump_act(double t);                                    // 收 Lab 的 tick (分段 / 收尾); true = 末步调用刚做完
  void answer_act(double t);                                  // 交 ask_human 的答案
  void abort_act(double t);                                   // 作废在跑的 (落了文件的回测删候选)
  void restore(const nlohmann::ordered_json &saved, Seat &s); // 存档的时间线 (与 timeline 逐步对应) 里各调用的结果在当前台面上复原
};

} // namespace GUI::Agent
