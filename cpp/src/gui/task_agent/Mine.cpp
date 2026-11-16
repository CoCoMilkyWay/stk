// Mine — 见头文件 (落盘与载入在 task_agent/Save)
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Acts.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <numeric>
#include <utility>
#include <vector>

namespace GUI::Agent {

namespace {

using Clock = std::chrono::steady_clock;

std::string net_name(const llm::Net &n) { return n.proxy ? "proxy " + n.host + ":" + std::to_string(n.port) : std::string("direct"); }

// 结果超 kOutChars 字 → 截在字边界, 附说明
void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

// 一轮的计数 (与在走时随事件即加的同口径): 开始了的调用计其工具; 做完的: 没做成计失败, 回测计结局, 入库的提交计入库
Desk::Tally tally_of(const Desk::Round &rd) {
  Desk::Tally t;
  for (const Act &a : rd.acts) {
    if (!a.span.began())
      continue;
    t += Desk::Tally::of(a.call.tool);
    if (!a.span.ended())
      continue;
    if (!a.ok)
      t += Desk::Tally::fail(a.call.tool);
    if (const Trial *k = std::get_if<Trial>(&a.got))
      t += Desk::Tally::of(k->st.res.verdict);
    t.adopted += a.call.tool == Tool::SubmitFactor && a.ok;
  }
  return t;
}

int tokens(const llm::Usage &u) { return u.prompt + u.completion; }

} // namespace

// ---- 人的操作 ----
void Mine::open(const SharedData &data, Setup s) {
  assert(!live() && idle());
  assert(catalog::ready(data, s.sc, s.gpu));
  flush();
  setup = std::move(s);
  lab.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  const Context &ctx = lab.ctx();
  assert(!ctx.leaves.empty() && !ctx.ops.empty() && "须有开放的叶与算子");
  setup.llm.system = SystemMessage(ctx, setup.gate, setup.ask, llm::def(setup.llm.backend).local).text;
  chat.open(setup.llm, {});
  disk::Key key = catalog::key(setup.sc, setup.test);
  key.stamp = disk::stamp_now();
  log.open(disk::path(disk::AgentChat, key).string(), setup.llm.system);
  std::vector<std::string> names;
  for (const ToolDef &d : Tools())
    names.push_back(d.name);
  PruneToolOut(setup.test, names);
  timeline.clear();
  restore({});
  memo.clear(), draft.reset(), reason.clear(), report.clear();
  cur_ = 0, retries_ = 0, stop_ = false, stale_ = false, agent_.clear();
  t0 = Clock::now(), t_end = -1;
  retally();
  leg_ = {};
  phase_ = Phase::Think;
  const int tried = static_cast<int>(std::count_if(lab.results().begin(), lab.results().end(), [](const Result &r) { return r.visible; }));
  log.event(Event::Open, {{"pool", setup.sc.pool},
                          {"level", ctx.level},
                          {"hold", ctx.hold},
                          {"years", setup.sc.years},
                          {"srcs", ctx.srcs},
                          {"leaves", ctx.leaves.size()},
                          {"ops", ctx.ops.size()},
                          {"backend", llm::def(setup.llm.backend).name},
                          {"model", setup.llm.model},
                          {"net", net_name(setup.llm.net)},
                          {"eval", setup.gpu ? "GPU" : "CPU"},
                          {"gate", {{"mono", setup.gate.mono}, {"ric", setup.gate.ric}}},
                          {"ask", setup.ask},
                          {"test", setup.test},
                          {"tried", tried},
                          {"remark", setup.remark},
                          {"budget", {{"rounds", setup.budget.rounds}, {"minutes", setup.budget.minutes}, {"ktokens", setup.budget.ktokens}}}});
  ask(setup.test ? TestMessage(setup.ask, setup.remark) : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark));
}

// 接着停下处: 待发的消息 / 重发没收成回复的那条 / 重发要求 / 余下的调用 (等人时停的重问) / 下一条
void Mine::resume(const SharedData &data, llm::Options llm, bool gpu, Budget budget) {
  assert(phase_ == Phase::Pause && idle());
  flush();
  llm.system = setup.llm.system;
  if (llm.backend != setup.llm.backend || llm.model != setup.llm.model)
    agent_.clear(), stale_ = true;
  if (!(llm.net == setup.llm.net))
    stale_ = true;
  setup.llm = std::move(llm), setup.gpu = gpu, setup.budget = budget;
  assert(catalog::ready(data, setup.sc, setup.gpu));
  assert(draft || !timeline.empty());
  std::optional<Message> send;
  if (draft) {
    send = std::move(*draft), draft.reset();
  } else if (!timeline.back().reply.ok()) {
    send = Message{timeline.back().kind, std::move(timeline.back().user)};
    timeline.pop_back();
    log.drop(round() + 1, round() + 1);
    retally();
  } else if (timeline.back().acts.empty()) {
    send = RetryMessage(timeline.back().error);
  }
  if (stale_)
    reopen();
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1;
  stop_ = false, retries_ = 0;
  reason.clear();
  leg_ = {round(), now(), tokens(total.usage)};
  log.event(Event::Resume, {{"round", round()},
                            {"backend", llm::def(setup.llm.backend).name},
                            {"model", setup.llm.model},
                            {"net", net_name(setup.llm.net)},
                            {"eval", setup.gpu ? "GPU" : "CPU"},
                            {"budget", {{"rounds", budget.rounds}, {"minutes", budget.minutes}, {"ktokens", budget.ktokens}}}});
  if (send) {
    phase_ = Phase::Think;
    return ask(std::move(*send));
  }
  Round &rd = timeline.back();
  if (cur_ < rd.acts.size()) {
    if (Act &a = rd.acts[cur_]; a.span.began())
      a = Act{.call = a.call};
    retally();
    return step();
  }
  if (done) {
    done = false;
    return next(true);
  }
  next(false);
}

void Mine::start() {
  assert(phase_ == Phase::Choose);
  Act &a = timeline.back().acts[cur_];
  answer(a, *this);
  ended(a);
  step();
}

void Mine::stop() {
  assert(live() && !stop_);
  log.event(Event::Stop);
  stop_ = true;
  if (phase_ == Phase::Choose)
    pause("按 Stop");
}

// 立即停: 在收的回复作废; 在跑的调用作废 (等人中的不算, 继续时重问)
void Mine::cancel() {
  assert(live());
  log.event(Event::Cancel);
  Round &rd = timeline.back();
  if (phase_ == Phase::Think) {
    const double t = now();
    rd.think.end(t), rd.write.end(t), rd.span.end(t);
    rd.error = "按 Cancel 作废了在收的回复";
  }
  if (phase_ == Phase::Act && cur_ < rd.acts.size() && rd.acts[cur_].span.running())
    abort(rd.acts[cur_]);
  pause("按 Cancel");
}

void Mine::close() {
  assert(phase_ == Phase::Pause && idle());
  flush();
  phase_ = Phase::None;
}

void Mine::queue(std::string m) {
  assert(held() && !m.empty());
  memo += (memo.empty() ? "" : "\n\n") + m;
  save();
}

std::string Mine::unqueue() {
  assert(held());
  std::string m = std::move(memo);
  memo.clear();
  save();
  return m;
}

// ---- 回退 ----
Mine::Loss Mine::loss(int k, bool keep) const {
  assert(phase_ == Phase::Pause && k >= 1 && k <= round());
  Loss l{.rounds = round() - k + !keep};
  for (size_t i = static_cast<size_t>(k) - 1; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (const Trial *t = std::get_if<Trial>(&a.got); t && t->st.ok())
        ++l.files;
  l.notes = notes.count(log.dir(), k);
  return l;
}

void Mine::redo_send(int k, std::string user) {
  assert(phase_ == Phase::Pause && idle() && k >= 1 && k <= round());
  flush();
  discard(k);
  const size_t i = static_cast<size_t>(k) - 1;
  const Msg kind = timeline[i].kind;
  const Snap s = timeline[i].before;
  log.event(Event::Rewind, {{"round", k}, {"keep", false}, {"dropped", round() - k + 1}});
  log.drop(k, round());
  timeline.resize(i);
  restore(s);
  draft = Message{kind, std::move(user)};
  cur_ = timeline.empty() ? 0 : timeline.back().acts.size();
  retries_ = 0, stale_ = true, agent_.clear();
  retally();
  reason = "回退到第 " + std::to_string(k) + " 条发出前, 改了它待发";
  report = report_of(reason);
  log.report(report);
  save();
}

bool Mine::redo_exec(int k, std::string reply, std::string &error) {
  assert(phase_ == Phase::Pause && idle() && k >= 1 && k <= round());
  const size_t i = static_cast<size_t>(k) - 1;
  const Snap now_s = snap();
  restore(timeline[i].before);
  std::vector<Call> cs;
  if (!ParseCalls(reply, setup.ask && !asked, cs, error) || !(error = RuleMiss(cs, *this)).empty()) {
    restore(now_s);
    return false;
  }
  flush();
  discard(k);
  log.event(Event::Rewind, {{"round", k}, {"keep", true}, {"dropped", round() - k}});
  log.drop(k + 1, round());
  timeline.resize(i + 1);
  Round &rd = timeline.back();
  rd.reply.text = std::move(reply), rd.reply.thinking.clear();
  rd.reply.end = llm::End::Ok, rd.reply.error.clear();
  rd.error.clear(), rd.edited = true;
  rd.acts.clear();
  for (Call &c : cs)
    rd.acts.push_back({.call = std::move(c)});
  if (llm::def(setup.llm.backend).local)
    folded = folds(i, folded);
  log.turn(k, rd.user, rd.reply);
  cur_ = 0, retries_ = 0, stale_ = true, agent_.clear();
  retally();
  reason = "第 " + std::to_string(k) + " 轮的回复改写了, 待重新执行";
  report = report_of(reason);
  log.report(report);
  save();
  return true;
}

// 丢的轮里落了文件的候选 (连同 Search 日志) + 本会话这些轮提的全局记录
void Mine::discard(int k) {
  for (size_t i = static_cast<size_t>(k) - 1; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (const Trial *t = std::get_if<Trial>(&a.got); t && t->st.ok())
        lab.discard(t->st.file);
  notes.drop(log.dir(), k);
}

void Mine::flush() {
  assert(!lab.busy());
  for (const std::string &f : doomed_)
    lab.discard(f);
  doomed_.clear();
}

// ---- 会话状态 ----
double Desk::now() const { return t_end >= 0 ? t_end : std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); }

void Desk::add(const Tally &d) { timeline.back().tally += d, total.tally += d; }

Desk::Snap Desk::snap() const { return {board, seen, adopted, evaled, done, reviewed, asked, folded, lab.ctx().look_from}; }

void Desk::restore(const Snap &s) {
  board = s.board, seen = s.seen, adopted = s.adopted;
  evaled = s.evaled, done = s.done, reviewed = s.reviewed, asked = s.asked;
  folded = s.folded;
  lab.look_at(s.look_from);
}

void Desk::retally() {
  total = {};
  for (Round &rd : timeline) {
    rd.tally = tally_of(rd);
    total.tally += rd.tally;
    if (rd.span.ended())
      total.llm_s += rd.span.len(0.0), total.usage += rd.reply.usage;
    for (const Act &a : rd.acts)
      if (a.span.ended())
        total.act_s += a.span.len(0.0);
  }
}

Desk::Tally &Desk::Tally::operator+=(const Tally &o) {
  for (size_t i = 0; i < kToolCount; ++i)
    calls[i] += o.calls[i], fails[i] += o.fails[i];
  for (size_t i = 0; i < kVerdictCount; ++i)
    verdicts[i] += o.verdicts[i];
  adopted += o.adopted;
  return *this;
}

Desk::Tally Desk::Tally::of(Tool t) {
  Tally d;
  ++d.calls[static_cast<size_t>(t)];
  return d;
}

Desk::Tally Desk::Tally::fail(Tool t) {
  Tally d;
  ++d.fails[static_cast<size_t>(t)];
  return d;
}

Desk::Tally Desk::Tally::of(Verdict v) {
  Tally d;
  ++d.verdicts[static_cast<size_t>(v)];
  return d;
}

int Desk::Tally::n(Run r) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.run == r ? n(d.id) : 0;
  return s;
}

int Desk::Tally::n(Cat c) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.cat == c ? n(d.id) : 0;
  return s;
}

int Desk::Tally::failed(Cat c) const {
  int s = 0;
  for (const ToolDef &d : Tools())
    s += d.cat == c ? failed(d.id) : 0;
  return s;
}

int Desk::Tally::all() const { return std::accumulate(calls.begin(), calls.end(), 0); }
int Desk::Tally::failed() const { return std::accumulate(fails.begin(), fails.end(), 0); }

std::string Mine::hint() const {
  if (phase_ != Phase::Pause)
    return {};
  const std::string n = std::to_string(round());
  if (draft)
    return "发出待发的第 " + std::to_string(round() + 1) + " 条消息";
  const Round &rd = timeline.back();
  if (!rd.reply.ok())
    return "重发第 " + n + " 条消息 (它的回复没收成)";
  if (rd.acts.empty())
    return "发重发要求 (第 " + n + " 条回复不合协议)";
  if (cur_ < rd.acts.size())
    return "接着执行第 " + n + " 轮第 " + std::to_string(cur_ + 1) + " 个调用起 (共 " + std::to_string(rd.acts.size()) + " 个)";
  if (done)
    return "发第 " + n + " 轮的结果, 让 agent 接着挖 (它已宣布挖完)";
  return "发第 " + n + " 轮的结果";
}

// ---- 每帧: 两个 worker 同样 poll; 作废回测的候选文件等实验台停稳后删 ----
void Mine::update() {
  while (std::optional<Chat::Tick> t = chat.poll())
    on(std::move(*t));
  while (std::optional<Lab::Tick> t = lab.poll())
    on(std::move(*t));
  if (!doomed_.empty() && !lab.busy())
    flush();
}

// 对话一步 (只在等 agent 时收): 思考 / 正文开始各记时刻, 重来作废已记的; 收齐 → replied
void Mine::on(Chat::Tick t) {
  if (phase_ != Phase::Think)
    return;
  Round &rd = timeline.back();
  const double now_s = now();
  switch (t.step) {
  case Chat::Step::Think:
    rd.think.begin(now_s);
    break;
  case Chat::Step::Write:
    rd.think.end(now_s), rd.write.begin(now_s);
    break;
  case Chat::Step::Restart:
    rd.think = {}, rd.write = {};
    break;
  case Chat::Step::End:
    rd.think.end(now_s), rd.write.end(now_s), rd.span.end(now_s);
    replied(std::move(t.reply));
    break;
  }
}

// 实验台一步 (只在 Act, 属在做的调用): 进一道记时刻; End → settled
void Mine::on(Lab::Tick t) {
  if (phase_ != Phase::Act)
    return;
  if (t.step != Lab::Step::End)
    return enter(timeline.back().acts[cur_], t.step, *this);
  settled(std::move(t.got));
}

// 收齐一轮: 落盘, 累计; 解析成一串调用, 开始执行
void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  total.llm_s += rd.span.len(now()), total.usage += x.usage;
  log.turn(round(), rd.user, x);
  log.event(Event::Reply, {{"round", round()},
                           {"end", llm::def(x.end).id},
                           {"error", x.error},
                           {"id", x.id},
                           {"seconds", x.seconds},
                           {"think_seconds", (rd.write.began() ? rd.write.t0 : rd.span.t1) - rd.span.t0},
                           {"prompt_tokens", x.usage.prompt},
                           {"cached_tokens", x.usage.cached},
                           {"completion_tokens", x.usage.completion},
                           {"chars", x.text.size()},
                           {"thinking_chars", x.thinking.size()}});
  if (!x.ok()) {
    rd.error = "回复失败 (" + std::string(llm::def(x.end).id) + (x.error.empty() ? "" : ": " + x.error) + ")";
    return pause(rd.error);
  }
  agent_ = x.agent;
  std::vector<Call> cs;
  std::string error;
  if (!ParseCalls(x.text, setup.ask && !asked, cs, error) || !(error = RuleMiss(cs, *this)).empty()) {
    log.event(Event::ParseFail, {{"round", round()}, {"error", error}});
    rd.error = error;
    if (++retries_ >= kMaxRetries)
      return pause("连续 " + std::to_string(kMaxRetries) + " 条回复不合协议");
    if (go(Phase::Think))
      ask(RetryMessage(error));
    return;
  }
  retries_ = 0;
  nlohmann::ordered_json tools = nlohmann::ordered_json::array();
  for (Call &c : cs) {
    tools.push_back(def(c.tool).name);
    rd.acts.push_back({.call = std::move(c)});
  }
  log.event(Event::Calls, {{"round", round()}, {"tools", tools}});
  cur_ = 0;
  save();
  step();
}

// 实验台交回结果: 在做的调用成文, 做完, 接着执行
void Mine::settled(Lab::Got g) {
  Act &a = timeline.back().acts[cur_];
  settle(a, std::move(g), *this);
  ended(a);
  save();
  step();
}

// ---- 执行 ----
void Mine::step() {
  Round &rd = timeline.back();
  while (cur_ < rd.acts.size()) {
    if (!go(Phase::Act))
      return;
    Act &a = rd.acts[cur_];
    a.span.begin(now());
    add(Tally::of(a.call.tool));
    log.event(Event::Call, {{"round", round()}, {"i", cur_ + 1}, {"tool", def(a.call.tool).name}, {"args", a.call.args}});
    switch (begin(a, *this)) {
    case Wait::None:
      ended(a);
      continue;
    case Wait::Lab:
      save();
      return;
    case Wait::Human:
      go(Phase::Choose);
      save();
      return;
    }
  }
  if (done)
    return pause("agent 宣布挖完");
  next(false);
}

void Mine::next(bool more) {
  const Round &rd = timeline.back();
  evaled = std::any_of(rd.acts.begin(), rd.acts.end(), [](const Act &a) { return a.call.tool == Tool::EvalFactor && std::get<Trial>(a.got).st.ok(); });
  if (!go(Phase::Think))
    return;
  const bool fin = !more && std::any_of(rd.acts.begin(), rd.acts.end(), [](const Act &a) { return a.call.tool == Tool::Finish; });
  std::string budget;
  if (!fin && !(budget = over()).empty()) {
    if (reviewed || setup.test)
      return pause("本段预算用完 (" + budget + ")");
    reviewed = true;
  }
  if (!fin && budget.empty())
    return ask(RoundMessage(rd.acts, round(), board, evaled, setup.test));
  std::vector<std::pair<Ref, const Call *>> judges;
  for (size_t i = 0; i < timeline.size(); ++i)
    for (size_t j = 0; j < timeline[i].acts.size(); ++j)
      if (const Act &a = timeline[i].acts[j]; a.call.tool == Tool::Judge && a.ok)
        judges.push_back({{static_cast<int>(i) + 1, static_cast<int>(j) + 1}, &a.call});
  std::vector<const Note *> filed;
  for (size_t k = 0; k < kNoteKindCount; ++k)
    for (const Note &n : notes.of(static_cast<NoteKind>(k)))
      if (n.session == log.dir())
        filed.push_back(&n);
  ask(ReviewMessage(rd.acts, round(), board, judges, filed, budget));
}

void Mine::ended(Act &a) {
  const double now_s = now();
  a.span.end(now_s);
  total.act_s += a.span.len(now_s);
  if (!a.ok)
    add(Tally::fail(a.call.tool));
  clip(a.out);
  KeepToolOut(setup.test, def(a.call.tool).name, a.call.text() + (a.ok ? "" : " — 没做成"),
              "出处: " + log.dir() + " · 第 " + std::to_string(round()) + " 轮第 " + std::to_string(cur_ + 1) + " 个调用", a.out);
  log.event(Event::Result, {{"round", round()},
                            {"i", cur_ + 1},
                            {"tool", def(a.call.tool).name},
                            {"ok", a.ok},
                            {"chars", a.out.size()},
                            {"seconds", a.span.len(now_s)}});
  ++cur_;
}

// 结果写明取消; 落了文件的回测: 候选文件记下待删 (实验台还在写), 假设的配额退回 (本条没有它别的落了文件的变体就不再待判)
void Mine::abort(Act &a) {
  enter(a, Lab::Step::End, *this);
  if (Trial *k = std::get_if<Trial>(&a.got); k && k->st.ok()) {
    doomed_.push_back(k->st.file);
    Hyp *h = board.find(a.call.str("hyp"));
    assert(h && "落了文件的回测挂在假设上");
    --h->evals, h->knobs -= k->knob;
    const std::vector<Act> &acts = timeline.back().acts;
    if (std::none_of(acts.begin(), acts.end(), [&](const Act &x) {
          const Trial *t = std::get_if<Trial>(&x.got);
          return &x != &a && t && t->st.ok() && x.call.str("hyp") == h->id;
        }))
      h->pending = 0;
    k->st.file.clear();
    k->st.res = Result{.error = "研究员取消了这次回测", .verdict = Verdict::Unstaged};
  }
  a.ok = false;
  a.out = "研究员取消了这个调用, 没做完";
  ended(a);
}

// ---- 转移 ----
bool Mine::go(Phase next) {
  if (stop_) {
    pause("按 Stop");
    return false;
  }
  phase_ = next;
  return true;
}

size_t Mine::folds(size_t n, size_t f) {
  const size_t keep = static_cast<size_t>(kKeepRounds);
  return n - f >= 2 * keep ? n - keep + 1 : f;
}

// 发第 n + 1 条前, 未折叠的条数 (连同这条) 将超 2·kKeepRounds 时成批折叠到只剩最近 kKeepRounds 条: 其间的结果消息在会话历史里换成折叠版 (它的调用在前一轮);
// 成批折叠让历史前缀多数轮不变 (远端的前缀缓存接得上). 此前各轮的回复都收齐了, 条序 = 会话的轮序. 快照记在折叠前 (回退后重发时照样再折)
void Mine::ask(Message m) {
  const Snap before = snap();
  if (const size_t n = timeline.size(), f = folds(n, folded); llm::def(setup.llm.backend).local && f != folded) {
    for (size_t j = folded; j < f; ++j)
      if (timeline[j].kind == Msg::Round || timeline[j].kind == Msg::Review)
        chat.fold(j, FoldedText(timeline[j - 1].acts, static_cast<int>(j)));
    folded = f;
  }
  if (!memo.empty()) {
    m.text += MemoText(memo);
    log.event(Event::Memo, {{"round", round() + 1}, {"chars", memo.size()}});
    memo.clear();
  }
  log.event(Event::Ask, {{"round", round() + 1}, {"kind", def(m.kind).id}, {"chars", m.text.size()}});
  timeline.push_back({.kind = m.kind, .user = std::move(m.text), .before = before, .span = {.t0 = now()}});
  chat.ask(timeline.back().user);
  save();
}

// 智谱: 此前各轮照原文重发, 已折叠的换折叠版; Cursor: agent_ 在则接着它, 否则新建并回放 (最近 kKeepRounds 条之前的结果消息用折叠版)
void Mine::reopen() {
  const bool local = llm::def(setup.llm.backend).local;
  llm::History h{.agent = local ? std::string() : agent_};
  const size_t n = timeline.size();
  for (size_t j = 0; j < n; ++j) {
    const Round &rd = timeline[j];
    assert(rd.reply.ok() && "历史各轮都已收齐");
    const bool fold = (rd.kind == Msg::Round || rd.kind == Msg::Review) && (local ? j < folded : j + static_cast<size_t>(kKeepRounds) < n);
    h.turns.push_back({fold ? FoldedText(timeline[j - 1].acts, static_cast<int>(j)) : rd.user, rd.reply.text});
  }
  chat.open(setup.llm, std::move(h));
  stale_ = false;
}

std::string Mine::over() const {
  const Budget &b = setup.budget;
  const int rounds = round() - leg_.round, used = tokens(total.usage) - leg_.tokens;
  const double secs = now() - leg_.t;
  if (b.rounds > 0 && rounds >= b.rounds)
    return "本段 " + std::to_string(rounds) + " 轮, 上限 " + std::to_string(b.rounds) + " 轮";
  if (b.minutes > 0 && secs >= b.minutes * 60.0)
    return "本段 " + std::to_string(static_cast<int>(secs / 60.0)) + " 分钟, 上限 " + std::to_string(b.minutes) + " 分钟";
  if (b.ktokens > 0 && used >= b.ktokens * 1000)
    return "本段 " + std::to_string(used / 1000) + "k token, 上限 " + std::to_string(b.ktokens) + "k";
  return {};
}

void Mine::pause(std::string why) {
  assert(live());
  chat.cancel();
  lab.cancel();
  if (!timeline.empty() && !timeline.back().reply.ok())
    stale_ = true, agent_.clear();
  t_end = now();
  reason = std::move(why);
  retally();
  report = report_of(reason);
  log.event(Event::Pause, {{"reason", reason}, {"rounds", round()}, {"adopted", total.tally.adopted}, {"notes", total.tally.n(Cat::Note)}});
  log.report(report);
  phase_ = Phase::Pause;
  stop_ = false;
  save();
}

std::string Mine::report_of(const std::string &why) const {
  const Tally &c = total.tally;
  std::string r = std::string(setup.test ? "# 自检报告\n\n- 自检会话: 候选 / 日志 / 全局记录在 global/test 目录, 提交只查门槛不拷进正式库" : "# 停下报告\n") + "\n- 停下原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Cat::Look)) + ", 查研报 " + std::to_string(c.n(Run::Docs)) + ", 假设 " + std::to_string(c.n(Tool::Hypothesize)) + ", 判定 " + std::to_string(c.n(Tool::Judge)) +
                  ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n" + (setup.remark.empty() ? "" : "- 本会话要求: " + setup.remark + "\n") + "\n";
  std::string in, out;
  for (size_t i = 0; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (a.span.ended() && a.call.tool == Tool::SubmitFactor)
        (a.ok ? in : out) += "- 第 " + std::to_string(i + 1) + " 轮 " + a.call.str("name") + (a.ok ? "" : " — " + a.out) + "\n";
  r += std::string(setup.test ? "## 达门槛 (" : "## 入库 (") + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.n(Tool::SubmitFactor) - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out);
  r += "\n## 假设 (" + std::to_string(board.all().size()) + ")\n" + board.report();
  for (size_t i = 0; i < kNoteKindCount; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    std::string s;
    int n = 0;
    for (const Note &x : notes.of(k))
      if (x.session == log.dir()) {
        ++n;
        s += "\n### " + x.id + " " + x.title() + " (第 " + std::to_string(x.round) + " 轮" + (x.ref.empty() ? "" : ", 附议 " + x.ref) + ")\n\n" + x.body();
      }
    r += "\n## " + std::string(def(k).cn) + " (" + std::to_string(n) + ")\n" + (s.empty() ? "\n(无)\n" : s);
  }
  return r;
}

} // namespace GUI::Agent
