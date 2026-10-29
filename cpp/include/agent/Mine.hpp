#pragma once

// =============================================================================
// Mine — 挖掘会话状态机 (AGENT.md 状态图, 一个 Phase 一个状态; 无 ImGui, 界面见 agent/ui/TaskAgent)
// =============================================================================
//   Phase    等谁    完成信号     转移
//   Digest   agent   收齐一轮     合格 → Choose; 不合格 → 发 retry, 留 Digest
//   Choose   人      start        发 answer → Asking
//   Asking   agent   收齐一轮     合格 → 记 issues, 处理 submit; done → Report, 否则落候选 → Eval; 不合格 → 发 retry, 留 Asking
//   Eval     系统    Lab 不忙     发 round → Asking
//   Report   人      open         开下一段
//   结束: 回复失败 / agent 宣布挖完 / cancel 立即; stop = 本步做完即结束 (转移唯一入口 go 处兑现; Choose 中立即)
//   GUI 线程: open / start / stop / cancel = 人的操作, update 每帧推进 (含 Lab 评估队列与会话日志);
//   成员 UI 只读, 除 picks / note (Choose 中人改)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Chat.hpp"
#include "agent/Journal.hpp"
#include "agent/Prompt.hpp"

#include <cstdint>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace agent {

class Mine {
public:
  enum class Phase : uint8_t { None,
                               Digest,
                               Choose,
                               Asking,
                               Eval,
                               Report };
  static constexpr const char *kPhaseNames[] = {"无会话", "澄清", "选答案", "作答", "回测", "已结束"};

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

  Phase phase() const { return phase_; }
  bool live() const { return phase_ != Phase::None && phase_ != Phase::Report; }
  bool stopping() const { return stop_ && live(); }
  int rounds() const { return rounds_; }
  const std::string &log_dir() const { return log_.dir(); }

  Chat chat;
  Lab lab;
  Setup setup;
  Context ctx;
  // 澄清
  Questions questions;
  std::vector<Pick> picks; // 初值 = 推荐
  std::string note;
  // 挖掘 / 结束
  std::set<std::string> adopted; // 本会话入库的英文名
  std::string report;

private:
  struct Issue {
    int round = 0;
    std::string text;
  };
  struct Submit {
    int round = 0;
    Submitted s;
  };

  bool go(Phase next); // 转移的唯一入口: 按过 Stop → 结束报告, false; 否则进 next
  void ask(Message m);
  void finish(std::string reason);
  template <class T>
  void reply(std::string_view text, int round);
  void on(Questions qs, int round);
  void on(MineReply m, int round);
  void evaluated();
  std::string report_of(const std::string &reason) const;

  Journal log_;
  Phase phase_ = Phase::None;
  bool stop_ = false;
  uint64_t seen_ = 0; // 已处理到的 chat.done()
  int rounds_ = 0;    // 本会话收齐的轮数
  std::vector<Staged> batch_;
  std::vector<Submitted> submitted_; // 上一条回复的提交结局, 进下一条反馈
  std::vector<Submit> submits_;      // 全会话
  std::vector<Issue> issues_;
};

} // namespace agent
