#pragma once

// =============================================================================
// Journal — 一段会话的日志目录 (只 GUI 线程写)
// =============================================================================
//   open    system.md                     系统提示
//   turn    NN.{user,reply,thinking}.md   逐轮原文
//   event   events.jsonl                  交互流水一行: 毫秒时间 + 事件名 (事件表 kEvents) + 字段 (原生类型; 字段表见 AGENT.md)
//   report  report.md                     结束报告
// 工具结果样本 (不随会话, kToolOutDir): KeepToolOut 每个工具留最近一份 <工具名>.md; PruneToolOut 删掉 known 以外的
// =============================================================================

#include "agent/Llm.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

namespace agent {

// 交互流水的事件 (下标 = Event): id = 流水里的 event
enum class Event : uint8_t { Open,      // 按 New
                             Ask,       // 发出一条消息
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
                             Pick,      // 按 Start, 每题一行
                             Start,     // 按 Start
                             Stop,      // 按 Stop
                             Cancel,    // 按 Cancel
                             Finish };  // 进结束报告
struct EventDef {
  const char *id;
};
inline constexpr EventDef kEvents[] = {{"open"}, {"ask"}, {"reply"}, {"parse_fail"}, {"calls"}, {"call"}, {"result"}, {"stage"}, {"eval"}, {"submit"}, {"note"}, {"hyp"}, {"judge"}, {"questions"}, {"pick"}, {"start"}, {"stop"}, {"cancel"}, {"finish"}};
static_assert(std::size(kEvents) == static_cast<size_t>(Event::Finish) + 1);
inline const EventDef &def(Event e) { return kEvents[static_cast<int>(e)]; }

class Journal {
public:
  void open(std::string dir, const std::string &system);
  const std::string &dir() const { return dir_; }

  void turn(int n, const std::string &user, const llm::Reply &r) const;
  void event(Event e, const nlohmann::ordered_json &fields = nlohmann::ordered_json::object()) const;
  void report(const std::string &text) const;

private:
  std::string dir_;
};

// 文件 = "# 调用本身" + 出处一行 (会话目录 · 轮 · 序) + 交给 agent 的结果 (截断后)
void KeepToolOut(const std::string &tool, const std::string &call, const std::string &from, const std::string &out);
void PruneToolOut(const std::vector<std::string> &known); // known = 工具表里的工具名

} // namespace agent
