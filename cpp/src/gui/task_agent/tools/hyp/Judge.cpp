// judge — 判假设: 输入 hyp / verdict / result (+ 可选 evidence / why / insight / better_feature / next) → JudgeRes → 原文; 展示见 ui/hyp/Judge.cpp
#include "gui/task_agent/tools/Hyp.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

// 证据不成的原因 (空 = 成): 若给了证据, 编号须存在、在本调用之前且已做完
std::string evidence_miss(const Call &c, Ref me, Rounds timeline) {
  for (const std::string &s : c.strs("evidence")) {
    Ref r;
    if (!RefOf(s, r))
      return "证据 " + s + " 不是调用编号 (写结果标题里的 R<轮>#<序>, 如 R12#3)";
    if (!(r < me))
      return "证据 " + s + " 不在本调用之前 (同批里只能引用排在 judge 前面的调用)";
    if (static_cast<size_t>(r.call) > timeline[static_cast<size_t>(r.round) - 1].acts.size())
      return "证据 " + s + " 不存在: 第 " + std::to_string(r.round) + " 轮没有第 " + std::to_string(r.call) + " 个调用";
    const Act &x = timeline[static_cast<size_t>(r.round) - 1].acts[static_cast<size_t>(r.call) - 1];
    if (!x.span.ended() || !x.out.ok)
      return "证据 " + s + " (" + def(x.call.tool).id + ") 没做成";
  }
  return {};
}

} // namespace

std::string JudgeRes::text() const {
  if (!miss.empty())
    return miss + "; 判定没改";
  return "已判 " + hyp + ": " + def(stance).cn + " (变体 " + std::to_string(evals) + ")";
}

ToolOut Judge(const Call &c, Seat &s) {
  auto r = std::make_shared<JudgeRes>();
  r->hyp = c.str("hyp"), r->stance = IdOf<Stance>(c.str("verdict")), r->fields = c.args;
  for (const std::string &e : c.strs("evidence")) {
    JudgeRes::Ev ev{.ref = e};
    Ref at;
    if (RefOf(e, at) && at.round >= 1 && static_cast<size_t>(at.round) <= s.timeline.size() && at.call >= 1 &&
        static_cast<size_t>(at.call) <= s.timeline[static_cast<size_t>(at.round) - 1].acts.size()) {
      const Act &x = s.timeline[static_cast<size_t>(at.round) - 1].acts[static_cast<size_t>(at.call) - 1];
      if (x.span.ended())
        ev.call = x.call.text(60), ev.gist = x.out.gist, ev.ok = x.out.ok, ev.look = def(x.call.tool).cat == Cat::Look;
    }
    r->evidence.push_back(std::move(ev));
  }
  const Hyp *h = s.st.board.find(r->hyp);
  if (h)
    r->title = h->title(), r->evals = h->evals;
  r->miss = h ? evidence_miss(c, s.me, s.timeline) : "假设板上没有 " + r->hyp;
  const bool ok = r->miss.empty();
  return Done(std::move(r), ok);
}

json SampleJudge(Feed &p) {
  static const char *kVerdicts[] = {"supported", "refuted", "unclear"};
  json ev = json::array();
  for (const std::string &x : p.refs(2))
    ev.push_back(x);
  if (ev.empty())
    ev.push_back("R1#1");
  const std::string h = p.hyp();
  return {{"hyp", h.empty() ? "H1" : h},
          {"verdict", kVerdicts[p.below(3)]},
          {"evidence", ev},
          {"result", "待补: 对照 expect / refute 写看到的数"},
          {"why", "待补: 机制上怎么解释"},
          {"insight", "待补"},
          {"better_feature", "待补"},
          {"next", "待补: 下一步用哪个工具查什么"}};
}

} // namespace GUI::Agent
