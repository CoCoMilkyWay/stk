#pragma once

// =============================================================================
// 会话 (build_context / check_call / probe_brain) 的结果 (仅工作台 debug, 不进 agent 工具表):
// 计算与原文在 tools/session/<工具>.cpp, 展示在 ui/session/<工具>.cpp
// =============================================================================
//   build_context  当场 (SyncFn): 按工作台快照拼出 system / 首条 / 本条增量 / 完整上下文 TurnsOf (远处折成结论)
//   check_call     当场 (SyncFn): 跑 ParseCall + eval_factor 落盘核验 (vet)
//   probe_brain    后台三拍 (StagedLab): 拿快照上下文新开一段对话调 llm::send 试跑一步并自动过 ParseCall
//   快照的下一条 (PendingOf): 工作台的步没有消息与回复, 照真会话补齐 (首条 / 上一个调用的结果; 回复 = 调用的协议形), 末个调用做完即接上它的结果
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <optional>
#include <string>
#include <vector>

namespace GUI::Agent {

std::vector<Step> PendingOf(const Seat &s, bool ask, const std::string &memo, const std::string &remark = {});

struct BuildContextRes final : Res {
  std::string kind, view;
  std::string body; // 选定 view 的最终原文
  std::string system, first, prev_reply, current, next_step;
  int step = 0;
  size_t turns = 0, folded = 0; // FoldOf
  bool done = false;
  std::string text() const override;
  void draw() const override;
};

struct CheckCallRes final : Res {
  bool ask = true;
  int step = 0;
  std::string error, retry_text;
  Call call;
  std::string pre_err; // eval_factor 落盘核验 (lab.vet) 原因; 空 = 可执行
  std::string text() const override;
  void draw() const override;
};

struct ProbeBrainRes final : Res {
  llm::Options opt;
  bool ask = true;
  size_t system_chars = 0, turns = 0;
  std::string prompt_text, error; // error 非空 = 前置校验或传输失败
  llm::Reply reply;
  CallCheck check;
  std::string text() const override;
  void draw() const override;
};

ToolOut BuildContext(const Call &c, Seat &s);
ToolOut CheckCall(const Call &c, Seat &s);

std::optional<LabTask> OpenProbeBrain(Act &a, Seat &s);
void CloseProbeBrain(Act &a, ToolOut out, Seat &s);
inline constexpr StagedLab ProbeBrain{OpenProbeBrain, CloseProbeBrain};

nlohmann::ordered_json SampleBuildContext(Feed &p);
nlohmann::ordered_json SampleCheckCall(Feed &p);
nlohmann::ordered_json SampleProbeBrain(Feed &p);

} // namespace GUI::Agent
