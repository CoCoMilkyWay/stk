// step_state — 看状态机: 透视账本 State、续跑判定 Cont、预算核对、回退损失与停下报告; 展示见 ui/session/StepState.cpp
#include "gui/task_agent/tools/Session.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include <algorithm>
#include <utility>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string StepStateRes::text() const {
  if (action == "report")
    return report;
  if (action == "rewind") {
    return "回退到第 " + std::to_string(rewind_k) + " 轮发出前 (当前共 " + std::to_string(round) + " 轮):\n"
                                                                                                   "- 丢弃轮数: " +
           std::to_string(loss_rounds) + "\n"
                                         "- 丢弃候选文件: " +
           std::to_string(loss_files) + " 个\n"
                                        "- 丢弃全局记录: " +
           std::to_string(loss_notes) + " 条 (仅已落盘会话可计)\n"
                                        "- 已入正式库的不动\n";
  }
  std::string s = "状态机透视 (第 " + std::to_string(round) + " 轮 · 调用 " + std::to_string(cur) + "/" + std::to_string(acts) + ")\n- 续跑判定 Cont: " + cont_id + " (" + cont_cn +
                  ")\n- 下一步提示: " + hint + "\n- 流程旗 done: " + std::string(flags.done ? "true" : "false") + "\n- 假设板: " + std::to_string(hyps) + " 条 · 候选 " +
                  std::to_string(tally.staged()) + " · 入库 " + std::to_string(tally.adopted) + "\n- 预算核对: " + (budget_miss.empty() ? std::string("未超限") : "已超限 — " + budget_miss) +
                  "\n";
  return s;
}

ToolOut StepState(const Call &c, Seat &s) {
  auto r = std::make_shared<StepStateRes>();
  r->action = c.str("action");
  r->round = static_cast<int>(s.timeline.size());
  const Round &rd = s.timeline.back();
  r->acts = rd.acts.size();
  size_t ended = 0;
  for (const Act &a : rd.acts)
    ended += a.span.ended();
  const int cur_arg = c.num("cur");
  r->cur = cur_arg < 0 ? ended : std::min(static_cast<size_t>(cur_arg), r->acts);

  const Cont ct = ContOf(s.timeline, r->cur);
  r->cont_id = def(ct).id, r->cont_cn = def(ct).cn;
  r->flags = s.st.flags;
  r->hint = HintOf(s.timeline, r->cur, r->flags.done);
  r->tally = TallyAt(s.st, s.timeline, 0.0);
  r->hyps = s.st.board.all().size();

  r->budget = {c.num("max_rounds"), c.num("max_minutes"), c.num("max_ktokens")};
  const int tok = r->tally.usage.prompt + r->tally.usage.completion;
  r->budget_miss = BudgetMiss(r->budget, r->round, r->tally.llm_s + r->tally.act_s, tok);

  r->rewind_k = std::clamp(c.num("round"), 1, r->round);
  r->loss_rounds = r->round - r->rewind_k + 1;
  for (size_t i = static_cast<size_t>(r->rewind_k) - 1; i < s.timeline.size(); ++i)
    for (const Act &a : s.timeline[i].acts)
      if (const TrialRes *t = AsTrial(a); t && t->st.ok())
        ++r->loss_files;
  if (!s.session.empty())
    r->loss_notes = static_cast<int>(s.notes.filed(s.session, r->rewind_k).size());

  const std::string why = c.str("why").empty() ? std::string("调试预览") : c.str("why");
  r->report = ReportOf(s.timeline, s.st, s.notes, s.session, why, {}, s.lab.test(), 0.0);
  return Done(std::move(r));
}

json SampleStepState(Feed &p) {
  return {{"action", "status"},
          {"round", std::max(1, static_cast<int>(p.timeline.size()))},
          {"cur", -1},
          {"max_rounds", 0},
          {"max_minutes", 0},
          {"max_ktokens", 0},
          {"why", ""}};
}

} // namespace GUI::Agent
