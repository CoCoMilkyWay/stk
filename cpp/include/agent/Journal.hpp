#pragma once

// =============================================================================
// Journal — 一段会话的日志目录 (只 GUI 线程写)
// =============================================================================
//   open    system.md                     系统提示
//   turn    NN.{user,reply,thinking}.md   逐轮原文 + rounds.jsonl 一行 (后端 / 模型 / 结局 / 用量 / 耗时)
//   event   events.jsonl                  交互流水一行: 毫秒时间 + 事件名 + 字段 (原生类型; 事件表见 AGENT.md)
//   report  report.md                     结束报告
// =============================================================================

#include "agent/Chat.hpp"
#include "nlohmann/json.hpp"

#include <string>
#include <string_view>

namespace agent {

class Journal {
public:
  void open(std::string dir, const llm::Options &o);
  const std::string &dir() const { return dir_; }

  void turn(int n, const Turn &t) const;
  void event(std::string_view name, const nlohmann::ordered_json &fields = nlohmann::ordered_json::object()) const;
  void report(const std::string &text) const;

private:
  std::string dir_, backend_, model_;
};

} // namespace agent
