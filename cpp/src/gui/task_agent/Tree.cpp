// Tree — 见头文件
#include "gui/task_agent/Tree.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <initializer_list>
#include <set>
#include <string_view>
#include <vector>

namespace GUI::Agent {

namespace {

using Rounds = std::span<const Desk::Round>;

constexpr size_t kWhyChars = 80; // 没做成的调用在树里留原因首行的前几字

// 挂在列表项下的续行: 每行缩进两格 (空行略去)
std::string under(std::string_view s) {
  std::string o;
  for (size_t i = 0; i < s.size();) {
    const size_t e = std::min(s.find('\n', i), s.size());
    if (e > i)
      o += "  " + std::string(s.substr(i, e - i)) + "\n";
    i = e + 1;
  }
  return o;
}

// 调用参数里 keys 的文本字段, 一行一个 "键: 值" (按参数表序), 挂在列表项下
std::string fields(const nlohmann::ordered_json &args, std::initializer_list<std::string_view> keys) {
  std::string s;
  for (const auto &[k, v] : args.items())
    if (v.is_string() && std::find(keys.begin(), keys.end(), k) != keys.end())
      s += "  - " + k + ": " + v.get<std::string>() + "\n";
  return s;
}

std::string why_of(const Act &a) { return text::head(std::string_view(a.out).substr(0, a.out.find('\n')), kWhyChars); }

struct Step {
  Ref at;
  const Act *a;
};

// 已收起的各条消息 msgs (第 1..m 条) 的研究树: 收前 m−1 轮的执行 (第 m 轮的调用在它的回复原文里) 与第 2..m 条随附的插话; 都没有 = 空.
// 假设的判定 / 配额取第 m 条发出时的板 (= 前 m−1 轮执行完)
std::string tree_of(Rounds msgs) {
  const Rounds done = msgs.first(msgs.size() - 1);
  const Board &board = msgs.back().before.board;
  std::vector<Step> steps;
  for (size_t i = 0; i < done.size(); ++i)
    for (size_t k = 0; k < done[i].acts.size(); ++k)
      if (const Act &a = done[i].acts[k]; a.span.ended())
        steps.push_back({{static_cast<int>(i) + 1, static_cast<int>(k) + 1}, &a});
  const auto act_at = [&](Ref r) -> const Act * {
    if (r.round < 1 || static_cast<size_t>(r.round) > done.size())
      return nullptr;
    const std::vector<Act> &as = done[static_cast<size_t>(r.round) - 1].acts;
    return r.call >= 1 && static_cast<size_t>(r.call) <= as.size() && as[static_cast<size_t>(r.call) - 1].span.ended() ? &as[static_cast<size_t>(r.call) - 1] : nullptr;
  };
  const auto on = [](const Act &a, Tool t) { return a.call.tool == t && a.ok; };
  const auto staged = [](const Act &a) { return a.call.tool == Tool::EvalFactor && std::get<Trial>(a.got).st.ok(); };

  std::set<Ref> cited; // 被判定引作证据的 (挂在那次判定下)
  for (const Step &s : steps)
    if (on(*s.a, Tool::Judge))
      for (const std::string &e : s.a->call.strs("evidence"))
        if (Ref r; RefOf(e, r))
          cited.insert(r);

  std::string hyps;
  for (const Hyp &h : board.all()) {
    const bool closed = h.stance == Stance::Refuted || (h.evals >= kHypEvals && !h.pending);
    hyps += "\n### " + h.id + " " + h.title() + " — " + def(h.stance).cn + " · 变体 " + std::to_string(h.evals) + "/" + std::to_string(kHypEvals) + " (旋钮 " + std::to_string(h.knobs) + "/" +
            std::to_string(kHypKnobs) + ") · 提出 " + RefText(h.at) + (closed ? " · 结案" : "") + "\n\n";
    if (!closed)
      hyps += "- 预测\n" + fields(h.fields, {"actor", "why", "expect", "refute"});
    for (const Step &s : steps) {
      const Act &a = *s.a;
      if (on(a, Tool::Judge) && a.call.str("hyp") == h.id) {
        const bool last = s.at == h.judged;
        std::string ev;
        for (const std::string &e : a.call.strs("evidence")) {
          Ref r;
          const Act *x = RefOf(e, r) ? act_at(r) : nullptr;
          ev += (ev.empty() ? "" : "; ") + (x ? CallTitle(*x, r) : e);
        }
        hyps += "- [" + RefText(s.at) + "] 判 " + def(StanceOf(a.call.str("verdict"))).cn + " · 证据 " + ev + "\n";
        if (!closed || last)
          hyps += fields(a.call.args, {"result", "insight"});
        if (!closed && last)
          hyps += fields(a.call.args, {"why", "better_feature", "next"});
      } else if (staged(a) && a.call.str("hyp") == h.id) {
        const Trial &k = std::get<Trial>(a.got);
        hyps += "- [" + RefText(s.at) + "] 变体 " + k.st.cand.name + " `" + k.st.cand.expr + "` · " + k.change + " → " + Brief(k.st.res) + "\n";
      }
    }
  }

  std::string rest, notes;
  size_t next = 0; // steps 里下一个要归入记录 / 其余的
  for (size_t i = 0; i < msgs.size(); ++i) {
    if (i > 0 && !msgs[i].memo.empty())
      notes += "- 研究员插话 (随第 " + std::to_string(i + 1) + " 条消息)\n" + under(msgs[i].memo);
    for (; next < steps.size() && static_cast<size_t>(steps[next].at.round) == i + 1; ++next) {
      const Step &s = steps[next];
      const Act &a = *s.a;
      if (on(a, Tool::Hypothesize) || on(a, Tool::Judge) || staged(a))
        continue;
      if (!a.ok)
        rest += "- " + CallTitle(a, s.at) + ": " + why_of(a) + "\n";
      else if (def(a.call.tool).fold == Fold::Keep)
        notes += "- " + CallTitle(a, s.at) + "\n" + under(a.out);
      else if (!cited.contains(s.at))
        rest += "- " + CallTitle(a, s.at) + "\n";
    }
  }
  if (hyps.empty() && rest.empty() && notes.empty())
    return {};
  std::string s = "# 研究树 (第 1–" + std::to_string(done.size()) +
                  " 轮的各步与结论)\n\n"
                  "各调用的原文只在交回它的那条消息里给一次, 此后收在这里: 假设留预测、各次判定的结论与变体的一行结局, 被引作证据的调用写在那次判定的证据里, "
                  "其余观察与查询只留调用一行; 被退回的回复与重发要求不留. 调用编号照旧可作 judge 的证据; 要细节就重调.\n";
  if (!hyps.empty())
    s += "\n## 假设\n" + hyps;
  if (!rest.empty())
    s += "\n## 其余各步 (只留调用)\n\n" + rest;
  if (!notes.empty())
    s += "\n## 记录 (提交 / 全局记录 / 问答 / 插话)\n\n" + notes;
  return s;
}

} // namespace

std::string TextOf(const Desk::Round &rd) { return rd.memo.empty() ? rd.user : rd.user + MemoText(rd.memo); }

llm::Turns TurnsOf(Rounds rs) {
  assert(!rs.empty());
  size_t j = rs.size() - 1; // 本条: 末条往前跳过重发要求
  while (rs[j].kind == Msg::Retry) {
    assert(j > 0 && "重发要求前面有被退回的那条");
    --j;
  }
  llm::Turns t;
  if (j == 0) {
    t.push_back({true, TextOf(rs[0])});
  } else {
    const Desk::Round &p = rs[j - 1];
    assert(p.reply.ok() && !p.acts.empty() && "本条前一轮的回复合格且执行了");
    const std::string tree = tree_of(rs.first(j));
    t.push_back({true, TextOf(rs[0]) + (tree.empty() ? "" : "\n---\n\n" + tree)});
    t.push_back({false, p.reply.text});
    t.push_back({true, TextOf(rs[j])});
  }
  for (size_t i = j; i + 1 < rs.size(); ++i) {
    assert(rs[i].reply.ok() && rs[i].acts.empty() && !rs[i].error.empty() && "重发之前的回复被退回");
    t.push_back({false, rs[i].reply.text});
    t.push_back({true, TextOf(rs[i + 1])});
  }
  return t;
}

} // namespace GUI::Agent
