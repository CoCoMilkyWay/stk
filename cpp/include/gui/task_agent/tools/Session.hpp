#pragma once

// =============================================================================
// 会话 (build_context / check_batch / step_state / probe_brain) 的结果 (仅工作台 debug, 不进 agent 工具表):
// 计算与原文在 tools/session/<工具>.cpp, 展示在 ui/session/<工具>.cpp
// =============================================================================
//   build_context  当场 (SyncFn): 按工作台快照拼出 system / 首条 / 研究树 / 本条增量 / 完整上下文 TurnsOf
//   check_batch    当场 (SyncFn): 跑 VetBatch (ParseCalls) + eval_factor 落盘核验 (vet)
//   step_state     当场 (SyncFn): 透视账本 State、续跑判定 Cont、转移表、预算核对、回退损失与停下报告
//   probe_brain    后台三拍 (StagedLab): 拿快照上下文直接调 llm::send 单轮试跑并自动过 VetBatch
// =============================================================================

#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/tools/Kit.hpp"

#include <optional>
#include <string>
#include <vector>

namespace GUI::Agent {

struct BuildContextRes final : Res {
  std::string kind, view;
  std::string body; // 选定 view 的最终原文
  std::string system, first, tree, prev_reply, current, next_step, board_text;
  int round = 0;
  size_t turns = 0;
  Flags flags;
  std::string text() const override;
  void draw() const override;
};

struct CheckBatchRes final : Res {
  struct Item {
    Ref at;
    Call call;
    std::string pre_err; // eval_factor 落盘核验 (lab.vet) 原因; 空 = 可执行
  };
  bool ask = true;
  std::string error, retry_text;
  std::vector<Item> items;
  std::string text() const override;
  void draw() const override;
};

struct StepStateRes final : Res {
  struct Edge {
    std::string from, event, to, cond;
    bool active = false;
  };
  std::string action, cont_id, cont_cn, hint, budget_miss, report;
  int round = 0, rewind_k = 0;
  int loss_rounds = 0, loss_files = 0, loss_notes = 0;
  size_t cur = 0, acts = 0, hyps = 0;
  Flags flags;
  Tally tally;
  Budget budget;
  std::vector<Edge> edges;
  std::string text() const override;
  void draw() const override;
};

struct ProbeBrainRes final : Res {
  llm::Options opt;
  bool ask = true;
  size_t system_chars = 0, turns = 0;
  std::string prompt_text, error; // error 非空 = 前置校验或传输失败
  llm::Reply reply;
  BatchCheck check;
  std::string text() const override;
  void draw() const override;
};

ToolOut BuildContext(const Call &c, Seat &s);
ToolOut CheckBatch(const Call &c, Seat &s);
ToolOut StepState(const Call &c, Seat &s);

std::optional<LabTask> OpenProbeBrain(Act &a, Seat &s);
void CloseProbeBrain(Act &a, ToolOut out, Seat &s);
inline constexpr StagedLab ProbeBrain{OpenProbeBrain, CloseProbeBrain};

nlohmann::ordered_json SampleBuildContext(Feed &p);
nlohmann::ordered_json SampleCheckBatch(Feed &p);
nlohmann::ordered_json SampleStepState(Feed &p);
nlohmann::ordered_json SampleProbeBrain(Feed &p);

} // namespace GUI::Agent
