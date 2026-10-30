// Acts — 见头文件
#include "agent/Acts.hpp"
#include "agent/Docs.hpp"
#include "agent/Mine.hpp"
#include "agent/Prompt.hpp"
#include "agent/Views.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <iterator>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace agent {

namespace {

Wait now(Act &a, Desk &d) {
  const Call &c = a.call;
  switch (c.tool) {
  case Tool::SearchLeaves: {
    std::vector<std::string> shown;
    a.out = LeavesText(d.lab.ctx().leaves, c.str("query"), c.num("n"), shown);
    d.seen.insert(shown.begin(), shown.end());
    break;
  }
  case Tool::ListTried:
    a.out = TriedText(d.lab.results(), c.num("top"));
    break;
  case Tool::ListLibrary:
    a.out = LibraryText(d.lab.ctx().library, c.num("top"));
    break;
  case Tool::SubmitFactor: {
    const Adopted x = d.lab.adopt(c.str("name"), d.setup.gate);
    a.ok = x.ok();
    if (a.ok && !d.setup.test)
      d.adopted.insert(x.name);
    a.out = !a.ok ? "未入库: " + x.err : d.setup.test ? "达门槛 (自检会话, 不拷进正式库)"
                                                      : "已入库";
    d.log.event(Event::Submit, {{"round", d.round()}, {"name", x.name}, {"result", a.ok ? std::string("入库") : x.err}});
    d.add({.adopted = a.ok});
    break;
  }
  case Tool::NoteIssue:
    a.out = "已记下, 会话结束后交给研究员";
    d.log.event(Event::Issue, {{"round", d.round()}, {"text", c.str("text")}});
    break;
  case Tool::Finish:
    d.done = true;
    a.out = "本条回复的调用做完后会话结束";
    break;
  default:
    assert(false && "不是当场做的工具");
  }
  return Wait::None;
}

Wait human(Act &a, Desk &d) {
  const Asked &q = a.got.emplace<Asked>(QuestionsOf(a.call));
  d.asked = true;
  d.log.event(Event::Questions, {{"round", d.round()}, {"n", q.qs.size()}});
  return Wait::Human;
}

// 式子里本会话没经 search_leaves 查过的叶 (逗号隔开; 全查过 = 空)
std::string unseen(const std::string &expr, const Desk &d) {
  const std::vector<Leaf> &leaves = d.lab.ctx().leaves;
  const auto word = [](char ch) { return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '.'; };
  std::set<std::string> miss;
  std::string out;
  for (size_t a = 0, b = 0; a < expr.size(); a = b + 1) {
    b = a;
    while (b < expr.size() && word(expr[b]))
      ++b;
    const std::string w = expr.substr(a, b - a);
    if (w.empty() || d.seen.contains(w) || miss.contains(w) || std::none_of(leaves.begin(), leaves.end(), [&](const Leaf &l) { return l.code == w; }))
      continue;
    miss.insert(w);
    out += (out.empty() ? "" : ", ") + w;
  }
  return out;
}

Wait eval(Act &a, Desk &d) {
  const Call &c = a.call;
  Trial &k = a.got.emplace<Trial>();
  const Cand x{c.str("name"), c.str("name_cn"), c.str("expr"), c.str("note")};
  if (const std::string un = unseen(x.expr, d); !un.empty()) {
    k.st.cand = x;
    k.st.res.error = "用了本会话没查过定义的叶 " + un + ": 先 search_leaves 查清它记的是谁的什么行为 (主动方还是被动方、侧是谁的方向、大小按什么分), 再写进式子";
    k.st.res.verdict = Verdict::Unstaged;
  } else {
    k.st = d.lab.stage(x);
  }
  d.log.event(Event::Stage, {{"round", d.round()},
                             {"name", k.st.cand.name},
                             {"expr", k.st.cand.expr},
                             {"note", k.st.cand.note},
                             {"result", k.st.ok() ? "落文件 " + k.st.file : Brief(k.st.res)}});
  if (!k.st.ok()) {
    a.out = TrialText(k.st), a.ok = false;
    d.add({.unstaged = 1});
    return Wait::None;
  }
  d.lab.eval(k.st.file, d.setup.gpu);
  return Wait::Lab;
}

Wait look(Act &a, Desk &d) {
  if (a.call.tool == Tool::ViewBook) {
    BookSpec bs;
    if (!BookOf(a.call, d.lab, bs, a.out)) {
      a.ok = false;
      return Wait::None;
    }
    d.lab.book(std::move(bs));
    return Wait::Lab;
  }
  LookSpec sp;
  if (!LookOf(a.call, d.lab, sp, a.out)) {
    a.ok = false;
    return Wait::None;
  }
  d.lab.look(std::move(sp));
  return Wait::Lab;
}

Wait docs(Act &a, Desk &) {
  a.ok = Browse(a.call, a.out);
  return Wait::None;
}

constexpr Wait (*kBegin[])(Act &, Desk &) = {now, human, eval, look, docs}; // 下标 = Run
static_assert(std::size(kBegin) == kRunCount);

} // namespace

Wait begin(Act &a, Desk &d) { return kBegin[static_cast<size_t>(def(a.call.tool).run)](a, d); }

// 回测记 Search / Run 时刻, 评完换上定论 (无显著另接代表点的分层摘要); 观察成文
void take(Act &a, Lab::Tick &&t, Desk &d) {
  const double now_s = d.now();
  switch (t.step) {
  case Lab::Step::Search:
    std::get<Trial>(a.got).search.begin(now_s);
    break;
  case Lab::Step::Run: {
    Trial &k = std::get<Trial>(a.got);
    k.search.end(now_s), k.run.begin(now_s);
    break;
  }
  case Lab::Step::Settle: {
    Trial &k = std::get<Trial>(a.got);
    k.search.end(now_s), k.run.end(now_s);
    k.st.res = std::get<Result>(std::move(t.got));
    assert(k.st.res.file == k.st.file);
    a.out = TrialText(k.st);
    a.ok = k.st.res.verdict != Verdict::Broken;
    d.log.event(Event::Eval, {{"name", k.st.cand.name}, {"result", Brief(k.st.res)}});
    d.add({.hits = k.st.res.verdict == Verdict::Weights});
    break;
  }
  case Lab::Step::Look:
    if (a.call.tool == Tool::EvalFactor)
      a.out = TrialText(std::get<Trial>(a.got).st, RepText(std::get<Look>(t.got)));
    else
      a.ok = Render(a.call, std::get<Look>(t.got), d.lab.ctx(), a.out);
    break;
  case Lab::Step::Book:
    a.ok = RenderBook(a.call, std::get<Book>(t.got), a.out);
    break;
  case Lab::Step::End:
    assert(false && "End 由 Mine 收");
  }
}

void answer(Act &a, Desk &d) {
  const Asked &q = std::get<Asked>(a.got);
  for (size_t i = 0; i < q.qs.size(); ++i) {
    const Question &x = q.qs[i];
    d.log.event(Event::Pick, {{"question", x.prompt}, {"picked", Answered(x, q.picks[i])}, {"recommended", Picked(x, x.recommended)}});
  }
  d.log.event(Event::Start, {{"note", q.note}});
  a.out = AnswerText(q);
}

} // namespace agent
