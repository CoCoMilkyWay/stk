// judge — 判假设: 输入 hyp / verdict / evidence / result / why / insight / better_feature / next → JudgeRes (证据核对 + 判定) → 原文; 展示见 ui/hyp/Judge.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/tools/Hyp.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

// 证据不成的原因 (空 = 成): 编号存在、在假设提出之后与本调用之前、做成了; 判支持须有观察; 再判须有上次判定之后的
std::string evidence_miss(const Call &c, const Hyp &h, Ref me, Rounds timeline) {
  bool look = false, fresh = false;
  for (const std::string &s : c.strs("evidence")) {
    Ref r;
    if (!RefOf(s, r))
      return "证据 " + s + " 不是调用编号 (写结果标题里的 R<轮>#<序>, 如 R12#3)";
    if (!(r < me))
      return "证据 " + s + " 不在本调用之前 (同一条回复里只能引用排在 judge 前面的调用)";
    if (static_cast<size_t>(r.call) > timeline[static_cast<size_t>(r.round) - 1].acts.size())
      return "证据 " + s + " 不存在: 第 " + std::to_string(r.round) + " 轮没有第 " + std::to_string(r.call) + " 个调用";
    if (!(h.at < r))
      return "证据 " + s + " 在 " + h.id + " 提出 (" + RefText(h.at) + ") 之前: 检验它的调用须在提出之后";
    const Act &x = timeline[static_cast<size_t>(r.round) - 1].acts[static_cast<size_t>(r.call) - 1];
    if (!x.span.ended() || !x.ok)
      return "证据 " + s + " (" + def(x.call.tool).name + ") 没做成";
    look = look || def(x.call.tool).cat == Cat::Look;
    fresh = fresh || h.judged < r;
  }
  if (IdOf<Stance>(c.str("verdict")) == Stance::Supported && !look) {
    std::string near;
    const std::vector<Act> &acts = timeline.back().acts;
    for (int i = 1; i < me.call; ++i)
      if (const Act &x = acts[static_cast<size_t>(i) - 1]; def(x.call.tool).cat == Cat::Look && x.span.ended() && x.ok && h.at < Ref{me.round, i})
        near += (near.empty() ? "" : ", ") + RefText({me.round, i});
    return "判支持须有观察调用 (view_*) 作证据: 回测只说好不好, 观察才说机制是否成立. 同一条回复里把观察排在 judge 前面, 就能引用它们" +
           (near.empty() ? std::string() : "; 本条排在前面做成了的观察: " + near + ", 补进 evidence 即可");
  }
  if (h.judged.round && !fresh)
    return "再判 " + h.id + " 须至少一个证据在上次判定 (" + RefText(h.judged) + ") 之后";
  return {};
}

} // namespace

std::string JudgeRes::text() const {
  if (!miss.empty())
    return miss + "; 判定没改";
  return "已判 " + hyp + ": " + def(stance).cn + " (变体 " + std::to_string(evals) + "/" + std::to_string(kHypEvals) + ")" +
         (stance == Stance::Supported ? (evals < kHypEvals ? "; 可以挂变体" : "; 变体已用完, 接着做须提新假设") : "; 不能再挂变体, 接着做须提新假设或带新证据再判");
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
        ev.call = x.call.text(60), ev.gist = x.gist, ev.ok = x.ok, ev.look = def(x.call.tool).cat == Cat::Look;
    }
    r->evidence.push_back(std::move(ev));
  }
  const Hyp *h = s.st.board.find(r->hyp);
  if (h)
    r->title = h->title(), r->evals = h->evals;
  r->miss = h ? evidence_miss(c, *h, s.me, s.timeline) : "假设板上没有 " + r->hyp;
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
  return {{"hyp", p.hyp(false)},
          {"verdict", kVerdicts[p.below(3)]},
          {"evidence", ev},
          {"result", "待补: 对照 expect / refute 写看到的数"},
          {"why", "待补: 机制上怎么解释"},
          {"insight", "待补"},
          {"better_feature", "待补"},
          {"next", "待补: 下一步用哪个工具查什么"}};
}

} // namespace GUI::Agent
