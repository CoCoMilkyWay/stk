#pragma once

// =============================================================================
// Mine — 挖掘会话状态机 (AGENT.md 状态图, 一个 Phase 一个状态; 无 ImGui, 界面见 agent/ui/TaskAgent)
// =============================================================================
//   Phase    等谁    完成信号     转移
//   Digest   agent   chat End     合格 → Choose; 不合格 → 发 retry, 留 Digest
//   Choose   人      start        发 answer → Propose
//   Propose  agent   chat End     合格 → 记 issues, 处理 submit; done → Report, 否则落候选 → Eval; 不合格 → 发 retry, 留 Propose
//   Eval     系统    lab End      发 round → Propose
//   Report   人      open         开下一段
//   两阶段对仗: Digest (agent 问) → Choose (人答) ↔ Propose (agent 交) → Eval (系统评)
//   两个 worker 对仗: chat (ask → Think / Write / End) ↔ lab (eval → Search / Run / Settle / End); update 每帧同样 poll, 各步时刻记进时间线的 Span
//   结束: 回复失败 / agent 宣布挖完 / cancel 立即; stop = 本步做完即结束 (转移唯一入口 go 处兑现; Choose 中立即)
//   GUI 线程: open / start / stop / cancel = 人的操作, update 每帧推进 (含会话日志)
//   timeline = 全会话唯一真相 (每条发出的消息一轮: 原文 / 回复 / 回复的处理); UI 只读, 除 Choose 中末轮的 Asked (picks / note)
//   total = timeline 的累计: 计数随事件即加; 用时 / token 一段收尾才加 (在走那段由其 Span 现算)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Chat.hpp"
#include "agent/Journal.hpp"
#include "agent/Prompt.hpp"

#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace agent {

class Mine {
public:
  enum class Phase : uint8_t { None,
                               Digest,
                               Choose,
                               Propose,
                               Eval,
                               Report };
  static constexpr const char *kPhaseNames[] = {"none", "digest", "choose", "propose", "eval", "report"}; // 流水里的阶段名

  // 一段会话不变的设定 (New 定)
  struct Setup {
    Scope sc;
    Gate gate;
    bool gpu = false;
    llm::Options llm; // system 由 open 填
  };

  void open(const SharedData &data, Setup s); // 须无在跑的会话, 对话 / 评估都不忙; 作用域须 ready
  void start();                               // 须 Choose
  void stop();
  void cancel();
  void update();

  // 计数 (+= 累加)
  struct Tally {
    int staged = 0, unstaged = 0, settled = 0, hits = 0, submits = 0, adopted = 0, issues = 0;
    Tally &operator+=(const Tally &o);
  };
  // 一轮 = 一条发出的消息. span = 发出 → 收齐, 其内 think / write = 首个思考增量 / 首个正文增量起 (无思考则 think 未开)
  struct Round {
    Msg kind = Msg::First;
    std::string user;
    Span span, think, write;
    llm::Reply reply; // 收齐后有效
    std::string err;  // 回复失败 / 不合协议的原因
    std::variant<std::monostate, Asked, Mined> got;
    Tally tally;
  };
  struct Total {
    Tally tally;
    double llm_s = 0, eval_s = 0; // 已收尾的 Round::span / Mined::span 之和
    llm::Usage usage;
  };

  Phase phase() const { return phase_; }
  bool live() const { return phase_ != Phase::None && phase_ != Phase::Report; }
  bool stopping() const { return stop_ && live(); }
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
  void evaluated();
  template <class T>
  void parse(int round);
  void on(Questions qs, int round);
  void on(Proposal p, int round);
  std::string report_of(const std::string &why) const;

  Journal log_;
  Phase phase_ = Phase::None;
  bool stop_ = false;
  std::chrono::steady_clock::time_point t0_;
  double t_end_ = -1; // 结束时刻 (−1 = 未结束)
};
inline const char *name(Mine::Phase p) { return Mine::kPhaseNames[static_cast<int>(p)]; }

} // namespace agent
