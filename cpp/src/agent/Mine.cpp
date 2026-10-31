// Mine — 见头文件
#include "agent/Mine.hpp"
#include "agent/Acts.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <numeric>
#include <utility>
#include <vector>

namespace agent {

namespace {

namespace fs = std::filesystem;

std::string net_name(const llm::Net &n) { return n.proxy ? "proxy " + n.host + ":" + std::to_string(n.port) : std::string("direct"); }

// 结果超 kOutChars 字 → 截在字边界, 附说明
void clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

} // namespace

// ---- 人的操作 ----
void Mine::open(const SharedData &data, Setup s) {
  assert(!live() && !chat.busy() && !lab.busy());
  assert(catalog::ready(data, s.sc, s.gpu));
  setup = std::move(s);
  lab.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  const Context &ctx = lab.ctx();
  setup.llm.system = SystemMessage(ctx, setup.gate, setup.ask, llm::def(setup.llm.backend).local).text;
  chat.open(setup.llm);
  log.open((fs::path(catalog::dir(setup.sc, setup.test)) / "chat" / text::local("%Y%m%d-%H%M%S")).string(), setup.llm.system);
  std::vector<std::string> names;
  for (const ToolDef &d : Tools())
    names.push_back(d.name);
  PruneToolOut(names);
  phase_ = Phase::Think;
  stop_ = done = reviewed = asked = evaled = false;
  cur_ = folded_ = 0;
  t0 = std::chrono::steady_clock::now(), t_end = -1;
  timeline.clear();
  total = {};
  adopted.clear(), seen.clear();
  board.clear();
  reason.clear(), report.clear();
  const int tried = static_cast<int>(std::count_if(lab.results().begin(), lab.results().end(), [](const Result &r) { return r.visible; }));
  log.event(Event::Open, {{"pool", setup.sc.pool},
                          {"level", ctx.level},
                          {"hold", ctx.hold},
                          {"backend", llm::def(setup.llm.backend).name},
                          {"model", setup.llm.model},
                          {"net", net_name(setup.llm.net)},
                          {"eval", setup.gpu ? "GPU" : "CPU"},
                          {"gate", {{"mono", setup.gate.mono}, {"ric", setup.gate.ric}}},
                          {"ask", setup.ask},
                          {"test", setup.test},
                          {"tried", tried}});
  ask(setup.test ? TestMessage(setup.ask) : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask));
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
    finish("按 Stop");
}

void Mine::cancel() {
  assert(live());
  log.event(Event::Cancel);
  finish("按 Cancel");
}

// ---- 会话状态 ----
double Desk::now() const { return t_end >= 0 ? t_end : std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); }

void Desk::add(const Tally &d) { timeline.back().tally += d, total.tally += d; }

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

// ---- 每帧: 两个 worker 同样 poll ----
void Mine::update() {
  while (std::optional<Chat::Tick> t = chat.poll())
    on(std::move(*t));
  while (std::optional<Lab::Tick> t = lab.poll())
    on(std::move(*t));
}

// 对话一步: 思考 / 正文开始各记时刻, 重来作废已记的; 收齐 (含会话结束后才收尾的那轮) → replied
void Mine::on(Chat::Tick t) {
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

// 收齐一轮: 落盘, 累计; 在等 agent 则解析成一串调用, 开始执行
void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  total.llm_s += rd.span.len(now()), total.usage += x.usage;
  if (!x.ok())
    rd.error = "回复失败 (" + std::string(llm::def(x.end).id) + (x.error.empty() ? "" : ": " + x.error) + ")";
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
  if (phase_ != Phase::Think)
    return;
  if (!x.ok())
    return finish(rd.error);
  std::vector<Call> cs;
  std::string error;
  if (!ParseCalls(x.text, setup.ask && !asked, cs, error) || !(error = RuleMiss(cs, *this)).empty()) {
    log.event(Event::ParseFail, {{"round", round()}, {"error", error}});
    rd.error = "不合协议: " + error;
    if (go(Phase::Think))
      ask(RetryMessage(error));
    return;
  }
  nlohmann::ordered_json tools = nlohmann::ordered_json::array();
  for (Call &c : cs) {
    tools.push_back(def(c.tool).name);
    rd.acts.push_back({.call = std::move(c)});
  }
  log.event(Event::Calls, {{"round", round()}, {"tools", tools}});
  cur_ = 0;
  step();
}

// 实验台交回结果: 在做的调用成文, 做完, 接着执行
void Mine::settled(Lab::Got g) {
  Act &a = timeline.back().acts[cur_];
  settle(a, std::move(g), *this);
  ended(a);
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
      return;
    case Wait::Human:
      go(Phase::Choose);
      return;
    }
  }
  if (done)
    return finish("agent 宣布挖完");
  evaled = std::any_of(rd.acts.begin(), rd.acts.end(), [](const Act &a) { return a.call.tool == Tool::EvalFactor && std::get<Trial>(a.got).st.ok(); });
  if (!go(Phase::Think))
    return;
  if (std::none_of(rd.acts.begin(), rd.acts.end(), [](const Act &a) { return a.call.tool == Tool::Finish; }))
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
  ask(ReviewMessage(rd.acts, round(), board, judges, filed));
}

void Mine::ended(Act &a) {
  const double now_s = now();
  a.span.end(now_s);
  total.act_s += a.span.len(now_s);
  if (!a.ok)
    add(Tally::fail(a.call.tool));
  clip(a.out);
  KeepToolOut(def(a.call.tool).name, a.call.text() + (a.ok ? "" : " — 没做成"),
              "出处: " + log.dir() + " · 第 " + std::to_string(round()) + " 轮第 " + std::to_string(cur_ + 1) + " 个调用", a.out);
  log.event(Event::Result, {{"round", round()},
                            {"i", cur_ + 1},
                            {"tool", def(a.call.tool).name},
                            {"ok", a.ok},
                            {"chars", a.out.size()},
                            {"seconds", a.span.len(now_s)}});
  ++cur_;
}

// ---- 转移 ----
bool Mine::go(Phase next) {
  if (stop_) {
    finish("按 Stop");
    return false;
  }
  phase_ = next;
  return true;
}

// 发第 n 条前, 未折叠的条数 (连同这条) 将超 2·kKeepRounds 时成批折叠到只剩最近 kKeepRounds 条: 其间的结果消息在会话历史里换成折叠版 (它的调用在前一轮);
// 成批折叠让历史前缀多数轮不变 (远端的前缀缓存接得上). 此前各轮的回复都收齐了, 条序 = 会话的轮序
void Mine::ask(Message m) {
  if (const size_t n = timeline.size(), keep = static_cast<size_t>(kKeepRounds); llm::def(setup.llm.backend).local && n - folded_ >= 2 * keep) {
    for (size_t j = folded_; j + keep <= n; ++j)
      if (timeline[j].kind == Msg::Round || timeline[j].kind == Msg::Review)
        chat.fold(j, FoldedText(timeline[j - 1].acts, static_cast<int>(j)));
    folded_ = n - keep + 1;
  }
  log.event(Event::Ask, {{"round", round() + 1}, {"kind", def(m.kind).id}, {"chars", m.text.size()}});
  timeline.push_back({.kind = m.kind, .user = m.text, .span = {.t0 = now()}});
  chat.ask(std::move(m.text));
}

void Mine::finish(std::string why) {
  assert(live());
  chat.cancel();
  lab.cancel();
  t_end = now();
  reason = std::move(why);
  report = report_of(reason);
  log.event(Event::Finish, {{"reason", reason}, {"rounds", round()}, {"adopted", total.tally.adopted}, {"notes", total.tally.n(Cat::Note)}});
  log.report(report);
  phase_ = Phase::Report;
}

std::string Mine::report_of(const std::string &why) const {
  const Tally &c = total.tally;
  std::string r = std::string(setup.test ? "# 自检报告\n\n- 自检会话: 候选 / 日志 / 全局记录在 global/test 目录, 提交只查门槛不拷进正式库" : "# 结束报告\n") + "\n- 结束原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Cat::Look)) + ", 查研报 " + std::to_string(c.n(Run::Docs)) + ", 假设 " + std::to_string(c.n(Tool::Hypothesize)) + ", 判定 " + std::to_string(c.n(Tool::Judge)) +
                  ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n\n";
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

} // namespace agent
