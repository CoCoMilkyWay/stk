#pragma once

// =============================================================================
// 回测 (eval_factor / submit_factor) 的结果: 计算与原文在 tools/trial/<工具>.cpp, 展示在 ui/trial/<工具>.cpp
// =============================================================================
//   eval_factor   后台三段: StageEvalFactor 核假设并落候选文件 (GUI) → EvalFactor 跑 Search + Run + 代表点分层 (Lab) → SettleEvalFactor 收结局 (GUI)
//   submit_factor 当场: 查门槛并拷进正式库 (自检会话 / 工作台只查门槛)
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <optional>
#include <string>

namespace GUI::Agent {

struct TrialRes : Res {
  Trial k;
  Gate gate;
  std::string text() const override;
  void draw() const override;
};

struct SubmitRes : Res {
  std::string name, why; // why 非空 = 未入库
  bool test = false, dry = false;
  std::optional<Result> cand;
  std::string text() const override;
  void draw() const override;
};

bool StageEvalFactor(Act &a, Seat &s);
ToolOut EvalFactor(const Call &c, Bay &bay);
void SettleEvalFactor(Act &a, ToolOut out, Seat &s);
ToolOut SubmitFactor(const Call &c, Seat &s);

nlohmann::ordered_json SampleEvalFactor(Feed &p);
nlohmann::ordered_json SampleSubmitFactor(Feed &p);

} // namespace GUI::Agent
