#pragma once

// =============================================================================
// Mine — 挖掘会话状态机 (AGENT.md 状态图, 一个 Phase 一个状态; 无 ImGui, 界面见 task_agent/TaskAgent)
// =============================================================================
//   Phase    等谁    完成信号          转移
//   None     人      open / load       New: 发 first (自检会话发 test) → Think; 载入: → Pause
//   Think    agent   chat End          合格 (一串调用, 且守住会话约束 RuleMiss) → Act; 不合格 → 发 retry, 留 Think (连续 kMaxRetries 条 → Pause); 回复失败 → Pause
//   Act      系统    调用做完          按序执行 (task_agent/Acts): 当场的立即做完; 回测 / 观察等 lab End; ask_human → Choose;
//                                      全部做完: 首次 finish → 发 review (结果 + 复盘清单) → Think; 复盘后再 finish (自检会话首次即是) → Pause;
//                                      没有 finish: 本段预算用完 → 发 review (已复盘过 / 自检 → Pause), 否则发 round (各调用的结果) → Think
//   Choose   人      start             交答案: 作 ask_human 的结果 → Act (接着执行余下的调用)
//   Pause    人      resume            停着 (停下报告已写). 继续 = 接着停下处: 重发末条 (回退后待发 / 回复没收成) / 重发要求 / 余下的调用 /
//                                      下一条 (agent 挖完后继续 = 照常发结果, 让它接着挖); 也可回退 (rewind) / 关闭 / New
//   一问一答对仗: Think (agent 发调用) ↔ Act (系统给结果); 工具表见 gui/task_agent/Tools.hpp, 执行见 gui/task_agent/Acts.hpp
//   两个 worker 对仗: chat (ask → Think / Write / End 带 Reply) ↔ lab (send → Search / Run / Lens / Replay / End 带 Got); update 每帧同样 poll, 各步时刻记进时间线的 Span;
//                    End 一个收一个: replied (解析成调用) ↔ settled (该调用成文); 不在等它们时到的 (被放下的那件) 不收
//   停下: 回复失败 / agent 挖完 / 本段预算 / 连续不合协议 / stop = 在做的调用做完即停 (转移唯一入口 go 处兑现; Choose 中立即) /
//         cancel = 立即: 在收的回复作废; 在跑的调用作废 (结果标取消, 落了文件的回测在实验台停稳后删掉候选, 假设的配额退回)
//   回退 (rewind, 以轮为单位): 每条消息发出时记一份状态快照 (Snap). 回退到第 k 轮 = 丢第 k 轮的回复与执行及其后各轮,
//         删掉丢的轮里落的候选文件与提的全局记录 (正式库不动), 状态恢复成第 k 条发出时的快照, 第 k 条待发 (它随附的插话退回排队); 停在 Pause, 继续即重发
//   上下文: 每次发出都照时间线整份重算 (task_agent/Tree TurnsOf), 对话不留在 Chat 或远端; 载入 / 回退 / 换后端或模型之后无须对账
//   落盘 (task_agent/Save): session.json 在发出消息 / 收齐回复 / 等实验台或人 / 调用收尾 / 停下 / 回退时整份重写; 停着或等人时存的 = 干净 (照存的续),
//         其余 = 中途 (载入时末轮在走的回复作废待重发、执行整轮作废重来)
//   插话 memo: 排队, 随下一条发出的消息附上 (记在那轮的 Round::memo; 成文 Prompt MemoText)
//   GUI 线程: open / load / resume / start / stop / cancel / rewind / close / queue = 人的操作, update 每帧推进 (含会话日志)
//   Desk = 会话状态 (调用经它读写); Mine = Desk + 转移
//   timeline = 全会话唯一真相 (每条发出的消息一轮: 原文 / 插话 / 回复 / 回复里每个调用的执行); UI 只读, 除 Choose 中在问那个调用的 Asked (picks / note)
//   total = timeline 的累计: 计数随事件即加 (调用开始即计其工具); 用时 / token 一段收尾才加 (在走那段由其 Span 现算); 载入 / 回退 / 停下时按 timeline 重算 (retally)
// =============================================================================

#include "gui/task_agent/Board.hpp"
#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Chat.hpp"
#include "gui/task_agent/Journal.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace GUI::Agent {

// 一段 (New / 继续 起) 的上限: 到了先复盘, 复盘过还没 finish 就停下; 0 = 不限
struct Budget {
  int rounds = 0, minutes = 0, ktokens = 0; // 轮数 / 分钟 / 千 token (输入 + 输出)
};

// 落盘 JSON 的字段清单: 作用域 / 门槛 / 预算 (Mine 页的 ui.json 与会话的 session.json 同一份)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Open, all, on)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Scope, pool, level, hold, lob, years, leaves, ops)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, mono, ric)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Budget, rounds, minutes, ktokens)

struct Desk {
  // 一段会话的设定: 作用域 / 门槛 / 可否提问 / 自检 / 要求 New 定, 会话内不变; 后端与模型 / 评估后端 / 预算每段 (New / 继续) 按 Config 当前的定
  struct Setup {
    Scope sc;
    Gate gate;
    bool gpu = false;
    bool ask = true;    // agent 可用 ask_human 向人提一次问
    bool test = false;  // 自检: 首条换成 test (每个工具调一遍); 候选 / 日志 / 全局记录进 global/test 目录, submit_factor 只查门槛不拷
    std::string remark; // 研究员对本会话的要求 / 说明 (空 = 无): 附在首条 (自检的首条) 末尾, 不进系统提示
    llm::Options llm;   // system 由 open 填 (载入取会话目录的 system.md), 会话内不变
    Budget budget;
  };
  // 计数 (+= 累加): calls = 各工具开始执行的次数; fails = 其中没做成的 (结果标 "没做成"); verdicts = eval_factor 的结局 (没落文件 = Unstaged); adopted = submit_factor 入库
  struct Tally {
    std::array<int, kToolCount> calls{};
    std::array<int, kToolCount> fails{};
    std::array<int, kVerdictCount> verdicts{};
    int adopted = 0;
    Tally &operator+=(const Tally &o);
    static Tally of(Tool t);    // 开始一个 t
    static Tally fail(Tool t);  // 一个 t 没做成
    static Tally of(Verdict v); // 一个 eval_factor 有了结局 v
    int n(Tool t) const { return calls[static_cast<size_t>(t)]; }
    int n(Verdict v) const { return verdicts[static_cast<size_t>(v)]; }
    int n(Run r) const; // 该执行方的工具之和
    int n(Cat c) const; // 该用途类的工具之和
    int all() const;
    int failed(Tool t) const { return fails[static_cast<size_t>(t)]; }
    int failed(Cat c) const;
    int failed() const;
    int staged() const { return n(Tool::EvalFactor) - n(Verdict::Unstaged); }
  };
  // 一条消息发出时的会话状态 (回退即恢复; 研究树取其中的假设板): 假设板 / 查过的叶 / 入库 / 约束与流程旗 / 观察窗首日
  struct Snap {
    Board board;
    std::set<std::string> seen, adopted;
    bool evaled = false, done = false, reviewed = false, asked = false;
    int look_from = 0;
  };
  // 一轮 = 一条发出的消息. span = 发出 → 收齐 (未开 = 待发), 其内 think / write = 首个思考增量 / 首个正文增量起 (无思考则 think 未开); acts = 回复里的调用, 按序执行
  struct Round {
    Msg kind = Msg::First;
    std::string user; // 系统生成的消息原文
    std::string memo; // 随它发出的研究员插话 (空 = 无)
    Snap before;      // 发出这条时的状态
    Span span, think, write;
    llm::Reply reply;  // 收齐后有效
    std::string error; // reply.ok() 时 = 不合协议的原因; 否则 = 回复没收成的原因
    std::vector<Act> acts;
    Tally tally;
  };
  struct Total {
    Tally tally;
    double llm_s = 0, act_s = 0; // 已收尾的 Round::span / Act::span 之和
    llm::Usage usage;
  };

  Chat chat;
  Lab lab; // 上下文 = lab.ctx()
  Setup setup;
  Journal log;
  Notes notes; // 全局记录 (open 按 setup.test 读回; 无会话时界面按候选目录重读)
  std::vector<Round> timeline;
  Total total;
  std::set<std::string> adopted; // 本会话入库的英文名
  std::set<std::string> seen;    // 本会话 search_leaves 列出过的叶组里的叶 (eval_factor 的式子只能用它们)
  Board board;                   // 本会话的假设板
  bool evaled = false;           // 上一条执行了的回复里有落了文件的 eval_factor (这一条不能再回测)
  bool done = false;             // agent 调了 finish 且复盘过 (自检会话不复盘): 本条做完即停
  bool reviewed = false;         // 已发过复盘 (首次 finish / 预算用完)
  bool asked = false;            // 已用过 ask_human
  std::string memo;              // 排队的插话 (随下一条发出)
  std::string reason, report;    // 停下原因 / 停下报告

  double now() const;                                             // 会话起算秒 (停着时停在停下时刻)
  int round() const { return static_cast<int>(timeline.size()); } // 在走那轮 (1 起)
  void add(const Tally &d);                                       // 末轮与 total 同加
  Snap snap() const;
  void restore(const Snap &s);
  void retally(); // 各轮 tally 与 total 按 timeline 重算 (只计收尾了的时段)

protected:
  std::chrono::steady_clock::time_point t0;
  double t_end = -1; // 停下时刻 (−1 = 在走)
};

class Mine : public Desk {
public:
  enum class Phase : uint8_t { None,
                               Think,
                               Act,
                               Choose,
                               Pause };
  // 回退会丢的: 轮数 / 候选文件 / 全局记录
  struct Loss {
    int rounds = 0, files = 0, notes = 0;
  };

  void open(const SharedData &data, Setup s);                                     // 须没在跑且两个 worker 都不忙; 作用域须 ready
  void load(const SharedData &data, const std::string &dir);                      // 同上; 读回会话目录 dir, 停在 Pause
  void resume(const SharedData &data, llm::Options llm, bool gpu, Budget budget); // 须 Pause 且都不忙: 按这套后端 / 评估 / 预算接着走 (system 照旧)
  void start();                                                                   // 须 Choose: 交答案
  void stop();
  void cancel();
  void close();              // 须 Pause 且都不忙: 放下会话 (已存盘, 可再载入)
  void queue(std::string m); // 须有会话: 插话排队
  std::string unqueue();     // 撤回排队的插话 (返回原文)
  void save() const;         // session.json + brief.json
  void update();
  // 回退 (须 Pause 且都不忙, 1 ≤ k ≤ round()): 丢第 k 轮的回复与执行及其后各轮, 第 k 条待发
  Loss loss(int k) const;
  void rewind(int k);

  Phase phase() const { return phase_; }
  bool live() const { return phase_ == Phase::Think || phase_ == Phase::Act || phase_ == Phase::Choose; }
  bool held() const { return phase_ != Phase::None; } // 有会话 (在走或停着): 作用域锁定
  bool idle() const { return !chat.busy() && !lab.busy(); }
  bool stopping() const { return stop_ && live(); }
  size_t acting() const { return cur_; } // Act / Choose 中在做的调用 = timeline.back().acts[acting()]
  std::string hint() const;              // 停着时继续会做什么

private:
  bool go(Phase next);  // 转移的唯一入口: 按过 Stop → 停下, false; 否则进 next
  void post(Message m); // 新起一轮 (记下发出时的快照) 并发出
  void send();          // 发出末轮: 带上排队的插话, 照时间线算上下文
  void pause(std::string why);
  void on(Chat::Tick t);
  void on(Lab::Tick t);
  void replied(llm::Reply r);
  void settled(Lab::Got g);
  void step();              // 从 cur_ 起执行, 遇到要等的停下; 全部做完 → next / 停下
  void next(bool more);     // 本条的调用全做完: 发下一条 (复盘 / 结果; more = 挖完后研究员要接着挖); 预算用完且复盘过 → 停下
  void ended(Act &a);       // 一个调用做完: 截断结果, 记流水, 前进
  void abort(Act &a);       // 在跑的调用作废 (Cancel)
  void discard(int k);      // 删第 k 轮起落的候选文件与提的全局记录
  void unsend();            // 末轮回到待发 (回复与执行作废; 消息、插话与快照照旧)
  void flush();             // 删掉作废回测的候选文件 (实验台须不忙)
  std::string over() const; // 本段预算用完 = 用了多少; 否则空
  std::string report_of(const std::string &why) const;

  Phase phase_ = Phase::None;
  bool stop_ = false;
  size_t cur_ = 0;
  int retries_ = 0;                 // 连续不合协议的回复数
  std::vector<std::string> doomed_; // 作废回测的候选文件, 实验台停稳后删
  struct Leg {
    int round = 0;
    double t = 0;
    int tokens = 0;
  } leg_; // 本段的起点 (预算按它算)
};

// 一段存过的会话 (载入用): dir = 会话目录; sc = 它的作用域; brief = 一览用的摘要 (brief.json)
struct Saved {
  std::string dir, stamp;
  bool test = false;
  Scope sc;
  nlohmann::ordered_json brief;
};
std::vector<Saved> SavedSessions(const Scope &sc); // 本作用域 (池 / 层 / 持有期) 存过的会话 (正式 + 自检), 新到旧

} // namespace GUI::Agent
