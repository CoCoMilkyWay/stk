#pragma once

// =============================================================================
// 假设 (hypothesize / judge) 的结果: 计算与原文在 tools/hyp/<工具>.cpp, 展示在 ui/hyp/<工具>.cpp
// =============================================================================
//   两者都当场做 (RunSync), 只读会话台面; 假设板的改动由时间线折叠 (Apply) 落下
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>
#include <vector>

namespace GUI::Agent {

struct HypRes : Res {
  std::string id;
  nlohmann::ordered_json fields; // hypothesize 参数
  std::string text() const override;
  void draw() const override;
};

struct JudgeRes : Res {
  std::string hyp, title, miss; // miss 非空 = 证据不成, 判定没改
  Stance stance = Stance::Open;
  int evals = 0;
  nlohmann::ordered_json fields; // judge 参数
  struct Ev {
    std::string ref, call, gist; // call 空 = 编号对不上做过的调用
    bool ok = false;
    bool look = false; // 观察调用
  };
  std::vector<Ev> evidence;
  std::string text() const override;
  void draw() const override;
};

ToolOut Hypothesize(const Call &c, Seat &s);
ToolOut Judge(const Call &c, Seat &s);

nlohmann::ordered_json SampleHypothesize(Feed &p);
nlohmann::ordered_json SampleJudge(Feed &p);

} // namespace GUI::Agent
