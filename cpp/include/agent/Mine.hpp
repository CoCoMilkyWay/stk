#pragma once

// =============================================================================
// Mine — 挖掘会话状态机 (AGENT.md 状态图, 一个 Phase 一个状态; 无 ImGui, 界面见 agent/ui/TaskAgent)
// =============================================================================
//   Phase    等谁    完成信号          转移
//   None     人      open              发 first (自检会话发 test) → Think
//   Think    agent   chat End          合格 (一串调用) → Act; 不合格 → 发 retry, 留 Think
//   Act      系统    调用做完          按序执行 (agent/Acts): 当场的立即做完; 回测 / 观察等 lab End; ask_human → Choose;
//                                      全部做完: 有 finish → Report, 否则发 round (各调用的结果) → Think
//   Choose   人      start             答复作 ask_human 的结果 → Act (接着执行余下的调用)
//   Report   人      open              开下一段
//   一问一答对仗: Think (agent 发调用) ↔ Act (系统给结果); 工具表见 agent/Tools.hpp, 执行见 agent/Acts.hpp
//   两个 worker 对仗: chat (ask → Think / Write / End) ↔ lab (eval → Search / Run / Settle / End; look → Look / End); update 每帧同样 poll, 各步时刻记进时间线的 Span
//   结束: 回复失败 / agent 调 finish / cancel 立即; stop = 在做的调用做完即结束 (转移唯一入口 go 处兑现; Choose 中立即)
//   GUI 线程: open / start / stop / cancel = 人的操作, update 每帧推进 (含会话日志)
//   Desk = 会话状态 (调用经它读写); Mine = Desk + 转移
//   timeline = 全会话唯一真相 (每条发出的消息一轮: 原文 / 回复 / 回复里每个调用的执行); UI 只读, 除 Choose 中在问那个调用的 Asked (picks / note)
//   total = timeline 的累计: 计数随事件即加 (调用开始即计其工具); 用时 / token 一段收尾才加 (在走那段由其 Span 现算)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Chat.hpp"
#include "agent/Journal.hpp"
#include "agent/Notes.hpp"
#include "agent/Prompt.hpp"
#include "agent/Tools.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace agent {

struct Desk {
  // 一段会话不变的设定 (New 定)
  struct Setup {
    Scope sc;
    Gate gate;
    bool gpu = false;
    bool ask = true;   // agent 可用 ask_human 向人提一次问
    bool test = false; // 自检: 首条换成 test (每个工具调一遍); 候选 / 日志 / 全局记录进 global/test 目录, submit_factor 只查门槛不拷
    llm::Options llm;  // system 由 open 填
  };
  // 计数 (+= 累加): calls = 各工具开始执行的次数; 其余是结局
  struct Tally {
    std::array<int, kToolCount> calls{};
    int unstaged = 0, hits = 0, adopted = 0; // eval_factor 没落文件 / 评出有参数组 / submit_factor 入库
    Tally &operator+=(const Tally &o);
    static Tally of(Tool t); // 开始一个 t
    int n(Tool t) const { return calls[static_cast<size_t>(t)]; }
    int n(Run r) const; // 该执行方的工具之和
    int all() const;
    int staged() const { return n(Tool::EvalFactor) - unstaged; }
    int noted() const; // 四个提交全局记录的工具之和
  };
  // 一轮 = 一条发出的消息. span = 发出 → 收齐, 其内 think / write = 首个思考增量 / 首个正文增量起 (无思考则 think 未开); acts = 回复里的调用, 按序执行
  struct Round {
    Msg kind = Msg::First;
    std::string user;
    Span span, think, write;
    llm::Reply reply; // 收齐后有效
    std::string err;  // 回复失败 / 不合协议的原因
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
  Notes notes; // 全局记录 (open 按 setup.test 读回; 无在跑会话时界面按候选目录重读)
  std::vector<Round> timeline;
  Total total;
  std::set<std::string> adopted; // 本会话入库的英文名
  std::set<std::string> seen;    // 本会话 search_leaves 列出过的叶 (eval_factor 的式子只能用它们)
  bool done = false;             // agent 调了 finish
  bool asked = false;            // 已用过 ask_human
  std::string reason, report;    // 结束原因 / 结束报告

  double now() const;                                             // 会话起算秒 (结束后停在结束时刻)
  int round() const { return static_cast<int>(timeline.size()); } // 在走那轮 (1 起)
  void add(const Tally &d);                                       // 末轮与 total 同加

protected:
  std::chrono::steady_clock::time_point t0;
  double t_end = -1; // 结束时刻 (−1 = 未结束)
};

class Mine : public Desk {
public:
  enum class Phase : uint8_t { None,
                               Think,
                               Act,
                               Choose,
                               Report };

  void open(const SharedData &data, Setup s); // 须无在跑的会话, 对话 / 评估都不忙; 作用域须 ready
  void start();                               // 须 Choose
  void stop();
  void cancel();
  void update();

  Phase phase() const { return phase_; }
  bool live() const { return phase_ != Phase::None && phase_ != Phase::Report; }
  bool stopping() const { return stop_ && live(); }
  size_t acting() const { return cur_; } // Act / Choose 中在做的调用 = timeline.back().acts[acting()]

private:
  bool go(Phase next); // 转移的唯一入口: 按过 Stop → 结束报告, false; 否则进 next
  void ask(Message m);
  void finish(std::string why);
  void on(Chat::Tick t);
  void on(Lab::Tick t);
  void replied(llm::Reply r);
  void step();        // 从 cur_ 起执行, 遇到要等的停下; 全部做完 → 发 round / 结束
  void ended(Act &a); // 一个调用做完: 截断结果, 记流水, 前进
  std::string report_of(const std::string &why) const;

  Phase phase_ = Phase::None;
  bool stop_ = false;
  size_t cur_ = 0;
  size_t folded_ = 0; // timeline 前 folded_ 条已过折叠 (结果消息换成了折叠版)
};

} // namespace agent
