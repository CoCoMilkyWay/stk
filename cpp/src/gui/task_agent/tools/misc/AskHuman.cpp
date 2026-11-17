// ask_human — 问研究员: 输入 questions → 研究员逐题点选 + 附言 → AskRes → 原文; 展示见 ui/misc/AskHuman.cpp
#include "gui/task_agent/tools/Misc.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string AskRes::text() const { return AnswerText(a); }

ToolOut AskOut(const Asked &a) {
  auto r = std::make_shared<AskRes>();
  r->a = a;
  return Done(std::move(r));
}

json SampleAskHuman(Feed &p) {
  return {{"questions",
           json::array({{{"prompt", "从哪个方向起步?"}, {"options", json::array({"接着做已试最好的候选", "新起一个 " + p.leaf() + " 方向", "先查研报再定"})}, {"recommended", json::array({0})}},
                        {{"prompt", "更看重哪一面?"}, {"options", json::array({"分层单调", "多空幅度", "低换手"})}, {"multiple", true}, {"recommended", json::array({0, 2})}}})}};
}

} // namespace GUI::Agent
