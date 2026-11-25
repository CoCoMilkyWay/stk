#pragma once

// =============================================================================
// 其他 (finish / ask_human) 的结果: 计算与原文在 tools/misc/<工具>.cpp, 展示在 ui/misc/<工具>.cpp
// =============================================================================
//   finish     Sync 形: 宣布挖完 (账本作用 = 结束; 本条做完即结束会话)
//   ask_human  Human 形: 交出问卷 (已按推荐预选) → 研究员逐题点选 + 附言, 交答案时成文; 存档复原
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>
#include <utility>

namespace GUI::Agent {

struct FinishRes final : Res {
  std::string text() const override;
  Effect effect() const override;
  void draw() const override;
};

struct AskRes final : Res {
  Asked asked;
  AskRes() = default;
  explicit AskRes(Asked a) : asked(std::move(a)) {}
  explicit AskRes(Questions q) : asked(std::move(q)) {}
  std::string text() const override;
  std::string title() const override; // 几个问题
  nlohmann::ordered_json save() const override;
  static ResPtr load(const Call &c, const nlohmann::ordered_json &j, Seat &s);
  void draw() const override;
};

} // namespace GUI::Agent
