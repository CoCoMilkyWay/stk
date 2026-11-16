#pragma once

// =============================================================================
// Journal — 一段会话的日志目录 (只 GUI 线程写)
// =============================================================================
//   open    system.md                     系统提示 (reopen = 载入的会话接着写它的目录)
//   context NN.context.md                 逐轮发出时 agent 看到的整份上下文 (task_agent/Tree, 摊平; system 另见 system.md)
//   turn    NN.{user,reply,thinking}.md   逐轮原文: 本条消息 (+ 插话) / 回复 / 思考 (drop = 回退时删掉丢弃的轮)
//   event   events.jsonl                  交互流水一行: 毫秒时间 + 事件名 (事件表 kEvents) + 字段 (原生类型; 字段表见 AGENT.md)
//   report  report.md                     停下报告
//   save    session.json + brief.json     会话的全部状态 (载入 / 回退 / 续接照它恢复, task_agent/Save) + 一览用的摘要; 原子替换
// 工具结果样本 (不随会话, output/run/agent/<main|test>/tools/): KeepToolOut 每个工具留最近一份 <工具名>.md; PruneToolOut 删掉 known 以外的
// =============================================================================

#include "gui/task_agent/Llm.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

namespace GUI::Agent {

// 交互流水的事件 (下标 = Event): id = 流水里的 event
enum class Event : uint8_t { Open,      // 按 New
                             Load,      // 载入一段存过的会话
                             Resume,    // 按继续
                             Ask,       // 发出一条消息
                             Memo,      // 插话随这条发出
                             Reply,     // 收齐一条回复
                             ParseFail, // 回复不合协议
                             Calls,     // 回复合协议
                             Call,      // 开始一个调用
                             Result,    // 一个调用做完
                             Stage,     // eval_factor 落文件
                             Eval,      // 候选评完
                             Submit,    // submit_factor
                             Note,      // 记一条全局记录 (request_feature / request_tool / note_insight / report_issue)
                             Hyp,       // hypothesize 记上假设板
                             Judge,     // judge 判定 (证据成立的)
                             Questions, // ask_human 开始
                             Pick,      // 交答案, 每题一行
                             Answer,    // 交答案
                             Stop,      // 按 Stop
                             Cancel,    // 按 Cancel
                             Rewind,    // 回退到某轮
                             Pause };   // 停下 (写停下报告)
struct EventDef {
  const char *id;
};
inline constexpr EventDef kEvents[] = {{"open"}, {"load"}, {"resume"}, {"ask"}, {"memo"}, {"reply"}, {"parse_fail"}, {"calls"}, {"call"}, {"result"}, {"stage"}, {"eval"}, {"submit"}, {"note"}, {"hyp"}, {"judge"}, {"questions"}, {"pick"}, {"answer"}, {"stop"}, {"cancel"}, {"rewind"}, {"pause"}};
static_assert(std::size(kEvents) == static_cast<size_t>(Event::Pause) + 1);
inline const EventDef &def(Event e) { return kEvents[static_cast<int>(e)]; }

class Journal {
public:
  void open(std::string dir, const std::string &system);
  void reopen(std::string dir);
  const std::string &dir() const { return dir_; }
  std::string system() const;

  void context(int n, const std::string &text) const;
  void turn(int n, const std::string &user, const llm::Reply &r) const;
  void drop(int from, int to) const; // 删第 from..to 轮的原文
  void event(Event e, const nlohmann::ordered_json &fields = nlohmann::ordered_json::object()) const;
  void report(const std::string &text) const;
  void save(const nlohmann::ordered_json &session, const nlohmann::ordered_json &brief) const;

private:
  std::string dir_;
};

inline constexpr const char *kSessionFile = "session.json";
inline constexpr const char *kBriefFile = "brief.json";

// 文件 = "# 调用本身" + 出处一行 (会话目录 · 轮 · 序) + 交给 agent 的结果 (截断后)
void KeepToolOut(bool test, const std::string &tool, const std::string &call, const std::string &from, const std::string &out);
void PruneToolOut(bool test, const std::vector<std::string> &known); // known = 工具表里的工具名

} // namespace GUI::Agent
