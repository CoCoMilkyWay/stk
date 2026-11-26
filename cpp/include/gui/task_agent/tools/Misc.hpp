#pragma once

// =============================================================================
// 其他 (finish / ask_human) 的结果: 计算与原文在 tools/misc/<工具>.cpp, 展示在 ui/misc/<工具>.cpp
// =============================================================================
//   finish     Sync 形: 收尾 = 提交一个已评候选 (查门槛并拷进正式库; 自检会话 / 工作台只查门槛) + 结束会话;
//              账本作用 = 提交 / 入库的英文名 + 结束; name 对不上已评候选 = 不提交不结束 (ok=false)
//   ask_human  Human 形: 交出问卷 (已按推荐预选) → 研究员逐题点选 + 附言, 交答案时成文; 存档复原
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <optional>
#include <string>
#include <utility>

namespace GUI::Agent {

struct FinishRes final : Res {
  std::string name, why; // name 空 = 不提交; why 非空 = 未入库的原因 (或 name 对不上: 没结束)
  bool ended = true;
  bool test = false, dry = false;
  std::optional<Result> cand;
  std::string text() const override;
  std::string title() const override;
  Effect effect() const override;
  void draw() const override;
};

struct AskRes final : Res {
  Asked asked;
  AskRes() = default;
  explicit AskRes(Asked a) : asked(std::move(a)) {}
  explicit AskRes(Questions q) : asked(std::move(q)) {}
  std::string text() const override;
  std::string title() const override; // 几个问题 + 首个问题
  nlohmann::ordered_json save() const override;
  static ResPtr load(const Call &c, const nlohmann::ordered_json &j, Seat &s);
  void draw() const override;
};

} // namespace GUI::Agent
