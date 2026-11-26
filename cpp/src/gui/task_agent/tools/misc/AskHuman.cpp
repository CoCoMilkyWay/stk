// ask_human — 问研究员: 输入 questions → 研究员逐题点选 + 附言 → AskRes → 原文; 展示见 ui/misc/AskHuman.cpp
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/Misc.hpp"

#include <cassert>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

constexpr size_t kAskChars = 40; // 一行结论里首个问题的字数

namespace {
std::string or_none(std::string s, const char *none) { return s.empty() ? none : s; }
} // namespace

std::string AskRes::text() const {
  assert(asked.picks.size() == asked.qs.size());
  std::string s;
  for (size_t i = 0; i < asked.qs.size(); ++i) {
    const Question &q = asked.qs[i];
    const Pick &p = asked.picks[i];
    s += std::to_string(i + 1) + ". " + q.prompt + "\n- " + or_none(Answered(q, p), "(未选, 你自定)") + "\n";
    if (p.on != q.recommended || (p.other && !p.text.empty()))
      s += "- (未照你的推荐: " + Picked(q, q.recommended) + ")\n";
  }
  return s + "附言: " + or_none(asked.note, "(无)") + "\n";
}

std::string AskRes::title() const { return std::to_string(asked.qs.size()) + " 个问题 · " + text::head(asked.qs.front().prompt, kAskChars); }

json AskRes::save() const { return {{"picks", asked.picks}, {"note", asked.note}}; }

ResPtr AskRes::load(const Call &c, const json &j, Seat &) {
  auto q = std::make_shared<AskRes>(QuestionsOf(c));
  j.at("picks").get_to(q->asked.picks);
  j.at("note").get_to(q->asked.note);
  assert(q->asked.picks.size() == q->asked.qs.size());
  return q;
}

ToolOut AskHuman(const Call &c, Seat &) { return {.res = std::make_shared<AskRes>(QuestionsOf(c))}; }

json SampleAskHuman(Feed &p) {
  return {{"questions",
           json::array({{{"prompt", "从哪个方向起步?"}, {"options", json::array({"接着做已试最好的候选", "新起一个 " + p.leaf() + " 方向", "先查研报再定"})}, {"recommended", json::array({0})}},
                        {{"prompt", "更看重哪一面?"}, {"options", json::array({"分层单调", "多空幅度", "低换手"})}, {"multiple", true}, {"recommended", json::array({0, 2})}}})}};
}

} // namespace GUI::Agent
