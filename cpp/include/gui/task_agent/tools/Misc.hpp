#pragma once

// =============================================================================
// 其他 (finish / ask_human) 的结果: 计算与原文在 tools/misc/<工具>.cpp, 展示在 ui/misc/<工具>.cpp
// =============================================================================
//   finish     当场 (SyncFn): 宣布挖完 (本条做完即结束会话)
//   ask_human  等人 (HumanFn): 研究员逐题点选 + 附言, 交答案时成文
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>

namespace GUI::Agent {

struct FinishRes : Res {
  std::string text() const override;
  void draw() const override;
};

struct AskRes : Res {
  mutable Asked a; // 展示只读 (控件禁用)
  std::string text() const override;
  void draw() const override;
};

ToolOut Finish(const Call &c, Seat &s);
ToolOut AskHuman(const Asked &a);

nlohmann::ordered_json SampleFinish(Feed &p);
nlohmann::ordered_json SampleAskHuman(Feed &p);

} // namespace GUI::Agent
