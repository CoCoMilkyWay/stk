// Mine — 见头文件 (落盘与载入在 task_agent/Save)
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Acts.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/Tree.hpp"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

namespace GUI::Agent {

namespace {

using Clock = std::chrono::steady_clock;

// 结果超 kOutChars 字 → 截在字边界, 附说明
void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
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
  setup.llm.system = SystemMessage(ctx, setup.gate, setup.ask).text;
  chat.open(setup.llm);
  disk::Key key = catalog::key(setup.sc, setup.test);
  key.stamp = disk::stamp_now();
  log.open(disk::path(disk::AgentChat, key).string(), setup.llm.system);
  std::vector<std::string> names;
  for (const ToolDef &d : Tools())
    names.push_back(d.name);
  PruneToolOut(setup.test, names);
  timeline.clear();
  restate();
  memo.clear(), reason.clear(), report.clear();
  cur_ = 0, retries_ = 0, stop_ = false;
  t0 = Clock::now(), t_end = -1;
  leg_ = {};
  phase_ = Phase::Think;
  const int tried = static_cast<int>(std::ranges::count_if(lab.results(), [](const Result &r) { return r.visible; }));
  nlohmann::ordered_json ev = {{"pool", setup.sc.pool},
                               {"level", ctx.level},
                               {"hold", ctx.hold},
                               {"years", setup.sc.years},
                               {"srcs", ctx.srcs},
                               {"leaves", ctx.leaves.size()},
                               {"ops", ctx.ops.size()},
                               {"gate", setup.gate},
                               {"ask", setup.ask},
                               {"test", setup.test},
                               {"tried", tried},
                               {"remark", setup.remark}};
  ev.update(leg_j());
  log.event(Event::Open, ev);
  post(setup.test ? TestMessage(setup.ask, setup.remark) : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark));
}

// 接着停下处: 重发末条 (回退后待发 / 回复没收成) / 重发要求 / 余下的调用 (等人时停的重问) / 下一条 (挖完后 = 照常发结果)
void Mine::resume(const SharedData &data, llm::Options llm, bool gpu, Budget budget) {
  assert(phase_ == Phase::Pause && idle() && !timeline.empty());
  flush();
  llm.system = setup.llm.system;
  setup.llm = std::move(llm), setup.gpu = gpu, setup.budget = budget;
  assert(catalog::ready(data, setup.sc, setup.gpu));
  chat.open(setup.llm);
  if (!timeline.back().reply.ok())
    unsend();
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1;
  stop_ = false, retries_ = 0;
  reason.clear();
  leg_ = {round(), now(), tokens(tally().usage)};
  nlohmann::ordered_json ev = {{"round", round()}};
  ev.update(leg_j());
  log.event(Event::Resume, ev);
  Round &rd = timeline.back();
  if (!rd.span.began()) {
    phase_ = Phase::Think;
    return send();
  }
  if (rd.acts.empty()) {
    phase_ = Phase::Think;
    return post(RetryMessage(rd.error));
  }
  if (cur_ < rd.acts.size()) {
    if (Act &a = rd.acts[cur_]; a.span.began())
      a = Act{.call = a.call};
    return step();
  }
  next(flags().done);
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
Mine::Loss Mine::loss(int k) const {
  assert(phase_ == Phase::Pause && k >= 1 && k <= round());
  Loss l{.rounds = round() - k + 1};
  for (size_t i = static_cast<size_t>(k) - 1; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (const Trial *t = std::get_if<Trial>(&a.got); t && t->st.ok())
        ++l.files;
  l.notes = static_cast<int>(notes.filed(log.dir(), k).size());
  return l;
}

// 丢第 k 轮起的产出, 时间线截到第 k 轮 (待发), 状态照剩下的时间线重算; 它随附的插话退回排队 (排在已排的前面), 继续时可撤回改写
void Mine::rewind(int k) {
  assert(phase_ == Phase::Pause && idle() && k >= 1 && k <= round());
  flush();
  discard(k);
  log.event(Event::Rewind, {{"round", k}, {"dropped", round() - k + 1}});
  log.drop(k, round());
  timeline.resize(static_cast<size_t>(k));
  Round &rd = timeline.back();
  if (!rd.memo.empty())
    memo = rd.memo + (memo.empty() ? "" : "\n\n" + memo), rd.memo.clear();
  unsend();
  restate();
  cur_ = 0, retries_ = 0;
  reason = "回退到第 " + std::to_string(k) + " 条发出前, 继续即重发";
  report = report_of(reason);
  log.report(report);
  save();
}

void Mine::unsend() {
  Round &rd = timeline.back();
  rd = Round{.kind = rd.kind, .user = std::move(rd.user), .memo = std::move(rd.memo)};
}

void Mine::restate() { st = StateOf(timeline); }

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

// ---- 会话 ----
double Desk::now() const { return t_end >= 0 ? t_end : std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); }

std::string Mine::hint() const {
  if (phase_ != Phase::Pause)
    return {};
  const std::string n = std::to_string(round());
  const Round &rd = timeline.back();
  if (!rd.span.began())
    return "发出待发的第 " + n + " 条消息";
  if (!rd.reply.ok())
    return "重发第 " + n + " 条消息 (它的回复没收成)";
  if (rd.acts.empty())
    return "发重发要求 (第 " + n + " 条回复不合协议)";
  if (cur_ < rd.acts.size())
    return "接着执行第 " + n + " 轮第 " + std::to_string(cur_ + 1) + " 个调用起 (共 " + std::to_string(rd.acts.size()) + " 个)";
  if (flags().done)
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

// 收齐一轮: 落盘; 解析成一串调用, 开始执行
void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  log.turn(round(), TextOf(rd), x);
  log.event(Event::Reply, {{"round", round()},
                           {"end", x.end},
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
    rd.error = "回复失败 (" + std::string(def(x.end).id) + (x.error.empty() ? "" : ": " + x.error) + ")";
    return pause(rd.error);
  }
  std::vector<Call> cs;
  std::string error;
  if (!ParseCalls(x.text, setup.ask && !flags().asked, cs, error) || !(error = RuleMiss(cs, *this)).empty()) {
    log.event(Event::ParseFail, {{"round", round()}, {"error", error}});
    rd.error = error;
    if (++retries_ >= kMaxRetries)
      return pause("连续 " + std::to_string(kMaxRetries) + " 条回复不合协议");
    if (go(Phase::Think))
      post(RetryMessage(error));
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
  if (flags().done)
    return pause("agent 宣布挖完");
  next(false);
}

void Mine::next(bool more) {
  if (!go(Phase::Think))
    return;
  const Round &rd = timeline.back();
  const Flags f = flags();
  const bool fin = !more && std::ranges::any_of(rd.acts, [](const Act &a) { return a.call.tool == Tool::Finish; });
  std::string budget;
  if (!fin && !(budget = over()).empty() && f.reviewed)
    return pause("本段预算用完 (" + budget + ")");
  if (!fin && budget.empty())
    return post(RoundMessage(rd.acts, round(), st.board, f.evaled, setup.test));
  std::vector<std::pair<Ref, const Call *>> judges;
  for (size_t i = 0; i < timeline.size(); ++i)
    for (size_t j = 0; j < timeline[i].acts.size(); ++j)
      if (const Act &a = timeline[i].acts[j]; a.call.tool == Tool::Judge && a.ok)
        judges.push_back({{static_cast<int>(i) + 1, static_cast<int>(j) + 1}, &a.call});
  post(ReviewMessage(rd.acts, round(), st.board, judges, notes.filed(log.dir()), budget));
}

void Mine::ended(Act &a) {
  const double now_s = now();
  a.span.end(now_s);
  Apply(st, a, {round(), static_cast<int>(cur_) + 1});
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

// 结果写明取消; 落了文件的回测: 候选文件记下待删 (实验台还在写), 结局改成没落文件 (不落进状态, 不占假设的配额)
void Mine::abort(Act &a) {
  enter(a, Lab::Step::End, *this);
  if (Trial *k = std::get_if<Trial>(&a.got); k && k->st.ok()) {
    doomed_.push_back(k->st.file);
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

void Mine::post(Message m) {
  timeline.push_back({.kind = m.kind, .user = std::move(m.text)});
  send();
}

void Mine::send() {
  Round &rd = timeline.back();
  assert(!rd.span.began());
  if (!memo.empty()) {
    log.event(Event::Memo, {{"round", round()}, {"chars", memo.size()}});
    rd.memo += (rd.memo.empty() ? "" : "\n\n") + memo;
    memo.clear();
  }
  rd.span.begin(now());
  llm::Turns t = TurnsOf(timeline);
  const std::string flat = llm::Flat(t);
  log.event(Event::Ask, {{"round", round()}, {"kind", rd.kind}, {"chars", t.back().text.size()}, {"turns", t.size()}, {"context_chars", flat.size()}});
  log.context(round(), flat);
  chat.ask(std::move(t));
  save();
}

std::string Mine::over() const {
  const Budget &b = setup.budget;
  const int rounds = round() - leg_.round, used = tokens(tally().usage) - leg_.tokens;
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
  t_end = now();
  reason = std::move(why);
  report = report_of(reason);
  const Tally c = tally();
  log.event(Event::Pause, {{"reason", reason}, {"rounds", round()}, {"adopted", c.adopted}, {"notes", c.n(Cat::Note)}});
  log.report(report);
  phase_ = Phase::Pause;
  stop_ = false;
  save();
}

nlohmann::ordered_json Mine::leg_j() const {
  const llm::Net &n = setup.llm.net;
  return {{"backend", def(setup.llm.backend).name},
          {"model", setup.llm.model},
          {"net", n.proxy ? "proxy " + n.host + ":" + std::to_string(n.port) : std::string("direct")},
          {"eval", setup.gpu ? "GPU" : "CPU"},
          {"budget", setup.budget}};
}

std::string Mine::report_of(const std::string &why) const {
  const Tally c = tally();
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
  r += "\n## 假设 (" + std::to_string(st.board.all().size()) + ")\n" + st.board.report();
  const std::vector<const Note *> filed = notes.filed(log.dir());
  for (size_t i = 0; i < kNum<NoteKind>; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    std::string s;
    int n = 0;
    for (const Note *x : filed)
      if (x->kind == k) {
        ++n;
        s += "\n### " + x->id + " " + x->title() + " (第 " + std::to_string(x->round) + " 轮" + (x->ref.empty() ? "" : ", 附议 " + x->ref) + ")\n\n" + x->body();
      }
    r += "\n## " + std::string(def(k).cn) + " (" + std::to_string(n) + ")\n" + (s.empty() ? "\n(无)\n" : s);
  }
  return r;
}

} // namespace GUI::Agent
