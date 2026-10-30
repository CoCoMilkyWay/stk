#pragma once

// =============================================================================
// Mine — 挖掘会话状态机 (AGENT.md 状态图, 一个 Phase 一个状态; 无 ImGui, 界面见 agent/ui/TaskAgent)
// =============================================================================
//   Phase    等谁    完成信号          转移
//   None     人      open              发 first → Think
//   Think    agent   chat End          合格 (一串调用) → Act; 不合格 → 发 retry, 留 Think
//   Act      系统    调用做完          按序执行: 当场的立即做完; eval_factor 等 lab End; 观察等 lab Look + End; ask_human → Choose;
//                                      全部做完: 有 finish → Report, 否则发 round (各调用的结果) → Think
//   Choose   人      start             答复作 ask_human 的结果 → Act (接着执行余下的调用)
//   Report   人      open              开下一段
//   一问一答对仗: Think (agent 发调用) ↔ Act (系统给结果); 工具表见 agent/Tools.hpp, 加工具只动表与 Act 的分支
//   两个 worker 对仗: chat (ask → Think / Write / End) ↔ lab (eval → Search / Run / Settle / End; look → Look / End); update 每帧同样 poll, 各步时刻记进时间线的 Span
//   结束: 回复失败 / agent 调 finish / cancel 立即; stop = 在做的调用做完即结束 (转移唯一入口 go 处兑现; Choose 中立即)
//   GUI 线程: open / start / stop / cancel = 人的操作, update 每帧推进 (含会话日志)
//   timeline = 全会话唯一真相 (每条发出的消息一轮: 原文 / 回复 / 回复里每个调用的执行); UI 只读, 除 Choose 中在问那个调用的 Asked (picks / note)
//   total = timeline 的累计: 计数随事件即加; 用时 / token 一段收尾才加 (在走那段由其 Span 现算)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Chat.hpp"
#include "agent/Journal.hpp"
#include "agent/Prompt.hpp"
#include "agent/Tools.hpp"

#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace agent {

class Mine {
public:
  enum class Phase : uint8_t { None,
                               Think,
                               Act,
                               Choose,
                               Report };
  static constexpr const char *kPhaseNames[] = {"none", "think", "act", "choose", "report"}; // 流水里的阶段名

  // 一段会话不变的设定 (New 定)
  struct Setup {
    Scope sc;
    Gate gate;
    bool gpu = false;
    bool ask = true;  // agent 可用 ask_human 向人提一次问
    llm::Options llm; // system 由 open 填
  };

  void open(const SharedData &data, Setup s); // 须无在跑的会话, 对话 / 评估都不忙; 作用域须 ready
  void start();                               // 须 Choose
  void stop();
  void cancel();
  void update();

  // 计数 (+= 累加)
  struct Tally {
    int calls = 0, looks = 0, staged = 0, unstaged = 0, settled = 0, hits = 0, submits = 0, adopted = 0, issues = 0;
    Tally &operator+=(const Tally &o);
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

  Phase phase() const { return phase_; }
  bool live() const { return phase_ != Phase::None && phase_ != Phase::Report; }
  bool stopping() const { return stop_ && live(); }
  bool asked() const { return asked_; }
  size_t acting() const { return cur_; } // Act / Choose 中在做的调用 = timeline.back().acts[acting()]
  const std::string &log_dir() const { return log_.dir(); }
  double now() const; // 会话起算秒 (结束后停在结束时刻)

  Chat chat;
  Lab lab; // 上下文 = lab.ctx()
  Setup setup;
  std::vector<Round> timeline;
  Total total;
  std::set<std::string> adopted; // 本会话入库的英文名
  std::string reason, report;    // 结束原因 / 结束报告

private:
  bool go(Phase next); // 转移的唯一入口: 按过 Stop → 结束报告, false; 否则进 next
  void ask(Message m);
  void finish(std::string why);
  void add(const Tally &d); // 末轮与 total 同加
  void on(Chat::Tick t);
  void on(Lab::Tick t);
  void replied(llm::Reply r);
  void step();          // 从 cur_ 起执行, 遇到要等的停下; 全部做完 → 发 round / 结束
  bool begin(Act &a);   // 开始一个调用; true = 当场做完
  void here(Act &a);    // Run::Now 的调用
  void ended(Act &a);   // 一个调用做完: 截断结果, 记流水, 前进
  std::string report_of(const std::string &why) const;

  Journal log_;
  Phase phase_ = Phase::None;
  bool stop_ = false;
  bool done_ = false;  // agent 调了 finish
  bool asked_ = false; // 已用过 ask_human
  size_t cur_ = 0;
  std::chrono::steady_clock::time_point t0_;
  double t_end_ = -1; // 结束时刻 (−1 = 未结束)
};
inline const char *name(Mine::Phase p) { return Mine::kPhaseNames[static_cast<int>(p)]; }

} // namespace agent
