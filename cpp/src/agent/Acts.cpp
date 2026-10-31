// Acts — 见头文件
#include "agent/Acts.hpp"
#include "agent/Board.hpp"
#include "agent/Config.hpp"
#include "agent/Docs.hpp"
#include "agent/Mine.hpp"
#include "agent/Prompt.hpp"
#include "agent/Views.hpp"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

namespace agent {

namespace {

// 在做的调用的编号 (a 是末轮的一个调用)
Ref here(const Act &a, const Desk &d) {
  const std::vector<Act> &acts = d.timeline.back().acts;
  assert(&a >= acts.data() && &a < acts.data() + acts.size());
  return {d.round(), static_cast<int>(&a - acts.data()) + 1};
}

// judge 的证据: 编号存在、在 judge 之前且在假设提出之后、做成了; 判支持须有观察, 再判须有上次判定之后的; 成立 = 空
std::string evidence_miss(const Call &c, const Hyp &h, Ref me, const Desk &d) {
  bool look = false, fresh = false;
  for (const std::string &s : c.strs("evidence")) {
    Ref r;
    if (!RefOf(s, r))
      return "证据 " + s + " 不是调用编号 (写结果标题里的 R<轮>#<序>, 如 R12#3)";
    if (!(r < me))
      return "证据 " + s + " 不在本调用之前 (同一条回复里只能引用排在 judge 前面的调用)";
    if (static_cast<size_t>(r.call) > d.timeline[static_cast<size_t>(r.round) - 1].acts.size())
      return "证据 " + s + " 不存在: 第 " + std::to_string(r.round) + " 轮没有第 " + std::to_string(r.call) + " 个调用";
    if (!(h.at < r))
      return "证据 " + s + " 在 " + h.id + " 提出 (" + RefText(h.at) + ") 之前: 检验它的调用须在提出之后";
    const Act &x = d.timeline[static_cast<size_t>(r.round) - 1].acts[static_cast<size_t>(r.call) - 1];
    if (!x.span.ended() || !x.ok)
      return "证据 " + s + " (" + def(x.call.tool).name + ") 没做成";
    look = look || def(x.call.tool).cat == Cat::Look;
    fresh = fresh || h.judged < r;
  }
  if (StanceOf(c.str("verdict")) == Stance::Supported && !look)
    return "判支持须有观察调用 (view_*) 作证据: 回测只说好不好, 观察才说机制是否成立. 同一条回复里把观察排在 judge 前面, 就能引用它们";
  if (h.judged.round && !fresh)
    return "再判 " + h.id + " 须至少一个证据在上次判定 (" + RefText(h.judged) + ") 之后";
  return {};
}

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
    a.out = TriedText(d.lab.results(), c.num("skip"), c.num("top"), d.setup.gate);
    break;
  case Tool::ListLibrary:
    a.out = LibraryText(d.lab.ctx().library, c.num("skip"), c.num("top"));
    break;
  case Tool::SubmitFactor: {
    const Adopted x = d.lab.adopt(c.str("name"), d.setup.gate);
    a.ok = x.ok();
    if (a.ok && !d.setup.test)
      d.adopted.insert(x.name);
    a.out = !a.ok ? "未入库: " + x.error : d.setup.test ? "达门槛 (自检会话, 不拷进正式库)"
                                                        : "已入库";
    d.log.event(Event::Submit, {{"round", d.round()}, {"name", x.name}, {"result", a.ok ? std::string("入库") : x.error}});
    d.add({.adopted = a.ok});
    break;
  }
  case Tool::ListNotes:
    a.out = NotesText(d.notes, c.str("kind"), c.str("scope") == "here" ? catalog::label(d.setup.sc) : "", c.str("query"), c.num("n"));
    break;
  case Tool::RequestFeature:
  case Tool::RequestTool:
  case Tool::NoteInsight:
  case Tool::ReportIssue: {
    const NoteKind k = NoteOf(c.tool);
    if (const std::string ref = c.str("ref"); !ref.empty() && !d.notes.find(k, ref)) {
      a.ok = false;
      a.out = "没有" + std::string(def(k).cn) + " " + ref + ": ref 写 list_notes 里同类已有一条的编号 (编号首字母 " + def(k).prefix + "), 新提的不写";
      break;
    }
    const Note &x = d.notes.add(c, catalog::label(d.setup.sc), d.log.dir(), d.round());
    a.out = "已记为 " + x.id + (x.ref.empty() ? "" : " (附议 " + x.ref + ")") + "; 全局可见: 研究员在界面里看, 后续会话用 list_notes 查得到";
    d.log.event(Event::Note, {{"round", d.round()}, {"id", x.id}, {"ref", x.ref}, {"title", x.title()}});
    break;
  }
  case Tool::Hypothesize: {
    const Hyp &h = d.board.add(c, here(a, d));
    a.out = "已记为 " + h.id + " (待检验): 用观察按 expect / refute 检验它, 再 judge; 判为支持才能挂 eval_factor 的变体";
    d.log.event(Event::Hyp, {{"round", d.round()}, {"id", h.id}, {"title", h.title()}});
    break;
  }
  case Tool::Judge: {
    Hyp *h = d.board.find(c.str("hyp"));
    const Ref me = here(a, d);
    const std::string miss = h ? evidence_miss(c, *h, me, d) : "假设板上没有 " + c.str("hyp");
    if (!miss.empty()) {
      a.ok = false;
      a.out = miss + "; 判定没改";
      break;
    }
    h->stance = StanceOf(c.str("verdict")), h->judged = me, h->verdict = c.args, h->pending = 0;
    a.out = "已判 " + h->id + ": " + def(h->stance).cn + " (变体 " + std::to_string(h->evals) + "/" + std::to_string(kHypEvals) + ")" +
            (h->stance == Stance::Supported ? (h->evals < kHypEvals ? "; 可以挂变体" : "; 变体已用完, 接着做须提新假设") : "; 不能再挂变体, 接着做须提新假设或带新证据再判");
    d.log.event(Event::Judge, {{"round", d.round()}, {"hyp", h->id}, {"verdict", def(h->stance).id}, {"evidence", c.args.at("evidence")}});
    break;
  }
  case Tool::Finish:
    if (d.setup.test || d.reviewed)
      d.done = true, a.out = "本条回复的调用做完后会话结束";
    else
      d.reviewed = true, a.out = "先复盘: 本条回复的调用做完后系统发复盘清单; 处理完再 finish 即结束";
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
  std::string out;
  for (const std::string &l : d.lab.leaves_of(expr))
    if (!d.seen.contains(l))
      out += (out.empty() ? "" : ", ") + l;
  return out;
}

// eval_factor 落文件前的会话检查: 假设 (判为支持 / 没有待判 / 有配额) → base (可见) → 改动 (不退回; 旋钮类须 base 达门槛且有配额) → 功能块; 成立 = 空.
// 新起 (base 空) 也比对全部已试候选 (Lab::delta), 比上的按它当 base 归类
std::string eval_miss(const Call &c, Desk &d, Delta &dl) {
  const Hyp *h = d.board.find(c.str("hyp"));
  if (!h)
    return "假设板上没有 " + c.str("hyp") + ": 先 hypothesize, 用观察检验并 judge 为支持, 再挂变体";
  if (h->stance != Stance::Supported)
    return h->id + " 的判定是 " + def(h->stance).cn + ": 只有判为支持的假设能挂变体";
  if (h->pending && h->pending != d.round())
    return h->id + " 有第 " + std::to_string(h->pending) + " 轮回测过的变体还没判: 先 judge 它";
  if (h->evals >= kHypEvals)
    return h->id + " 的 " + std::to_string(kHypEvals) + " 个变体已用完: 回到观察与研报找还没解释的现象, 提新假设";
  if (const std::string base = c.str("base"); !base.empty() && !d.lab.tried(base))
    return "base " + base + " 不是可见的已试候选 (写 list_tried 里的英文名; 新起写空)";
  dl = d.lab.delta(c.str("base"), c.str("expr"));
  const ChangeDef &cd = def(dl.kind);
  const std::string at = "相对 " + dl.base + " " + cd.cn + " (" + dl.what + ")";
  switch (dl.kind) {
  case Change::Same:
    return at + ": 回测结果必同";
  case Change::Params:
    return at + ": 参数交给 Search, 要搜的写裸键 d / k / k2";
  case Change::Multi:
    return at + ": 一个变体只改一处, 才看得出这一处的好坏; 拆成几个变体";
  default:
    break;
  }
  assert(!cd.bad);
  if (cd.knob && !GateMiss(d.setup.gate, *d.lab.tried(dl.base)).empty())
    return at + ", 而 base 未达入库门槛: 门槛前换平滑 / 压缩 / 口径多半只是在噪声里挑点, 回到观察找还没解释的现象";
  if (cd.knob && h->knobs >= kHypKnobs)
    return at + ": " + h->id + " 的旋钮类配额 (" + std::to_string(kHypKnobs) + " 个) 已用完";
  return d.lab.parts_miss(c.str("expr"), PartsOf(c));
}

// 改动一行: 新起 / 归类 (相对 base)
std::string change_of(const Delta &dl) { return dl.base.empty() ? std::string("新起") : std::string(def(dl.kind).cn) + " (相对 " + dl.base + ")"; }

// 候选文件的思路: 假设 · 改动 · 预期 · 功能块, 一行
std::string note_of(const Call &c, const Hyp &h, const Delta &dl) {
  std::string s = h.id + " " + h.title() + " · 改动: " + change_of(dl) + ", " + c.str("change") +
                  " · 预期: " + c.str("expect") + " · 功能块:";
  for (const Part &p : PartsOf(c))
    s += " `" + p.expr + "` " + p.means + ";";
  return s;
}

Wait eval(Act &a, Desk &d) {
  const Call &c = a.call;
  Trial &k = a.got.emplace<Trial>();
  Delta dl;
  std::string miss = unseen(c.str("expr"), d);
  if (!miss.empty())
    miss = "用了本会话没查过定义的叶 " + miss + ": 先 search_leaves 查清它记的是谁的什么行为 (主动方还是被动方、侧是谁的方向、大小按什么分), 再写进式子";
  else
    miss = eval_miss(c, d, dl);
  Hyp *h = d.board.find(c.str("hyp"));
  if (!miss.empty()) {
    k.st.cand = {c.str("name"), c.str("name_cn"), c.str("expr"), ""};
    k.st.res.error = std::move(miss);
    k.st.res.verdict = Verdict::Unstaged;
  } else {
    k.change = change_of(dl) + (dl.base.empty() ? "" : ": " + dl.what);
    k.st = d.lab.stage({c.str("name"), c.str("name_cn"), c.str("expr"), note_of(c, *h, dl)});
    if (k.st.ok())
      ++h->evals, h->knobs += def(dl.kind).knob, h->pending = d.round();
  }
  d.log.event(Event::Stage, {{"round", d.round()},
                             {"name", k.st.cand.name},
                             {"expr", k.st.cand.expr},
                             {"note", k.st.cand.note},
                             {"result", k.st.ok() ? "落文件 " + k.st.file : Brief(k.st.res)}});
  if (!k.st.ok()) {
    a.out = TrialText(k, d.setup.gate), a.ok = false;
    d.add(Desk::Tally::of(Verdict::Unstaged));
    return Wait::None;
  }
  d.lab.send(EvalSpec{k.st.file, d.setup.gpu});
  return Wait::Lab;
}

// 观察 / 重放: 取参不成当场做完
template <class S>
Wait view(Act &a, Desk &d) {
  S s;
  if (!Spec(a.call, d.lab, s, a.out)) {
    a.ok = false;
    return Wait::None;
  }
  d.lab.send(std::move(s));
  return Wait::Lab;
}

Wait docs(Act &a, Desk &) {
  a.ok = Browse(a.call, a.out);
  return Wait::None;
}

constexpr Wait (*kBegin[])(Act &, Desk &) = {now, human, eval, view<LookSpec>, view<BookSpec>, docs}; // 下标 = Run
static_assert(std::size(kBegin) == kRunCount);

// 回测换上定论, 无显著另接代表点的分层摘要
void settled(Act &a, Eval &&e, Desk &d) {
  Trial &k = std::get<Trial>(a.got);
  k.st.res = std::move(e.res);
  assert(k.st.res.file == k.st.file);
  a.out = TrialText(k, d.setup.gate, e.rep ? RepText(*e.rep) : "");
  a.ok = k.st.res.verdict != Verdict::Broken;
  d.log.event(Event::Eval, {{"name", k.st.cand.name}, {"result", Brief(k.st.res)}});
  d.add(Desk::Tally::of(k.st.res.verdict));
}
void settled(Act &a, Look &&l, Desk &d) { a.ok = Render(a.call, l, d.lab.ctx(), a.out); }
void settled(Act &a, Book &&b, Desk &d) { a.ok = Render(a.call, b, d.lab.ctx(), a.out); }
void settled(Act &, std::monostate, Desk &) { assert(false && "取消的不收"); }

} // namespace

Wait begin(Act &a, Desk &d) { return kBegin[static_cast<size_t>(def(a.call.tool).run)](a, d); }

// 回测进一道: 前一道收尾, Search / Run 开始 (代表点的观察算在 Run 之后, 不另记)
void enter(Act &a, Lab::Step s, Desk &d) {
  Trial *k = std::get_if<Trial>(&a.got);
  if (!k)
    return;
  const double t = d.now();
  k->search.end(t), k->run.end(t);
  if (s == Lab::Step::Search)
    k->search.begin(t);
  if (s == Lab::Step::Run)
    k->run.begin(t);
}

void settle(Act &a, Lab::Got &&got, Desk &d) {
  enter(a, Lab::Step::End, d);
  std::visit([&](auto &&x) { settled(a, std::move(x), d); }, std::move(got));
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

std::string RuleMiss(const std::vector<Call> &cs, const Desk &d) {
  if (d.evaled && std::any_of(cs.begin(), cs.end(), [](const Call &c) { return c.tool == Tool::EvalFactor; }))
    return "上一条回复有落了文件的回测, 这一条不能再回测: 先读本批结果 —— 用观察诊断变体为什么好 / 为什么差, 对挂了变体的假设 judge; 回测留到下一条";
  std::string owe;
  for (const Hyp &h : d.board.all())
    if (h.pending && std::none_of(cs.begin(), cs.end(), [&](const Call &c) { return c.tool == Tool::Judge && c.str("hyp") == h.id; }))
      owe += (owe.empty() ? "" : ", ") + h.id;
  if (!owe.empty())
    return "假设板上待判的 " + owe + " 本条没有 judge: 它们的变体回测完了, 每个发一个 judge —— 对照 expect 写测出来是什么、为什么、得到什么认识、有没有更好的特征、下一步用什么工具查什么";
  return {};
}

} // namespace agent
