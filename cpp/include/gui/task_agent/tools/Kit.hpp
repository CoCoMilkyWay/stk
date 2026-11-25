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
#include <random>
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

struct Message {
  Msg kind = Msg::First;
  std::string text;
};

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
  virtual std::string title() const { return {}; }                // 时间线上这个调用一行里代替调用原文的一句 (无则空)
  virtual Effect effect() const { return {}; }                    // 对会话账本的作用
  virtual bool abort() { return false; }                          // 作废: true = 已改成作废后的样子 (原文照它); false = 只记一句没做完
  virtual nlohmann::ordered_json save() const { return nullptr; } // 存档 (null = 不存)
  virtual void draw() const = 0;                                  // 结构化展示
};
using ResPtr = std::shared_ptr<Res>;

struct ToolOut {
  bool ok = true;
  std::string text, gist;
  Effect fx;
  ResPtr res;
};
template <class R>
ToolOut Done(std::shared_ptr<R> r, bool ok = true) {
  std::string text = r->text(), gist = r->gist();
  Effect fx = r->effect();
  return {ok, std::move(text), std::move(gist), std::move(fx), std::move(r)};
}
inline ToolOut Fail(std::string why) { return {false, std::move(why), {}, {}, {}}; }

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

// 只进可读日志的一笔 (停下 / 继续 / 作废的回复), 记在这一步走到的位置之后
enum class LogAt : uint8_t { Sent,    // 发出之后 (回复收成之前)
                             Replied, // 回复收成之后 (调用做完之前)
                             Done };  // 调用做完之后
struct LogMark {
  LogAt at = LogAt::Sent;
  std::string md;
};

struct Step {
  Msg kind = Msg::First;
  std::string user;        // 发出的消息 (不含插话)
  std::string memo;        // 随它发出的研究员插话
  Span span, think, write; // 发出 → 回复收齐 / 其中的思考 / 正文
  llm::Reply reply;
  std::string error;      // 非空 = 回复不合协议 (无调用; 下一条 = 重交要求)
  std::optional<Act> act; // 回复交来的调用
  std::vector<LogMark> marks;
};
using Steps = std::span<const Step>;

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

// ---- 随机输入的取材 ----
struct Spot {
  std::string code, day, clock; // 代码 / 交易日 YYYY-MM-DD / 行末时刻 HH:MM (日层空)
};

struct Feed {
  const Desk &desk;
  const Context &ctx;
  const std::vector<Result> &tried;
  const Gate &gate;
  Steps timeline;
  int me;
  std::mt19937_64 &rng;

  int below(int n); // [0, n)
  bool coin(double p);
  std::string pick(Tool t, std::string_view key); // 参数表里该参数可选集的一项 (缺省为空串的另含空串)
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
  std::vector<std::string> refs(int n);                     // 做成了的调用编号 (新到旧)
};

// ---- 全部工具函数的声明 (清单一行一个: 形的签名 + 随机输入): 定义在 tools/<类>/<工具>.cpp ----
#define AGENT_TOOL_DECL(T, id, cn, F, C, keep, R) \
  F##Sig T;                                       \
  nlohmann::ordered_json Sample##T(Feed &);
AGENT_TOOLS(AGENT_TOOL_DECL)
#undef AGENT_TOOL_DECL

nlohmann::ordered_json SampleArgs(Tool t, Feed &p);

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
