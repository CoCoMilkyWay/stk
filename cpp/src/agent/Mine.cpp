// Mine — 见头文件
#include "agent/Mine.hpp"
#include "agent/Acts.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <ctime>
#include <filesystem>
#include <numeric>
#include <utility>

namespace agent {

namespace {

namespace fs = std::filesystem;

std::string stamp() {
  const std::time_t now = std::time(nullptr);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", std::localtime(&now));
  return b;
}

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
  const Context &ctx = lab.ctx();
  setup.llm.system = SystemMessage(ctx, setup.gate, setup.ask).text;
  chat.open(setup.llm);
  log.open((fs::path(catalog::dir(setup.sc, setup.test)) / "chat" / stamp()).string(), setup.llm.system);
  phase_ = Phase::Think;
  stop_ = done = asked = false;
  cur_ = 0;
  t0 = std::chrono::steady_clock::now(), t_end = -1;
  timeline.clear();
  total = {};
  adopted.clear();
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
  ask(setup.test ? TestMessage(setup.ask) : FirstMessage(ctx, tried, setup.ask));
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
    calls[i] += o.calls[i];
  unstaged += o.unstaged, hits += o.hits, adopted += o.adopted;
  return *this;
}

Desk::Tally Desk::Tally::of(Tool t) {
  Tally d;
  ++d.calls[static_cast<size_t>(t)];
  return d;
}

int Desk::Tally::n(Run r) const {
  int s = 0;
  for (size_t i = 0; i < kToolCount; ++i)
    s += def(static_cast<Tool>(i)).run == r ? calls[i] : 0;
  return s;
}

int Desk::Tally::all() const { return std::accumulate(calls.begin(), calls.end(), 0); }

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

// 实验台一步 (只在 Act, 属在做的调用): End → 该调用做完, 接着执行; 其余交给它 (agent/Acts take)
void Mine::on(Lab::Tick t) {
  if (phase_ != Phase::Act)
    return;
  Act &a = timeline.back().acts[cur_];
  if (t.step != Lab::Step::End)
    return take(a, std::move(t), *this);
  ended(a);
  step();
}

// 收齐一轮: 落盘, 累计; 在等 agent 则解析成一串调用, 开始执行
void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  total.llm_s += rd.span.len(now()), total.usage += x.usage;
  if (!x.ok())
    rd.err = "回复失败 (" + std::string(llm::def(x.end).id) + (x.why.empty() ? "" : ": " + x.why) + ")";
  log.turn(round(), rd.user, x);
  log.event(Event::Reply, {{"round", round()},
                           {"end", llm::def(x.end).id},
                           {"why", x.why},
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
    return finish(rd.err);
  std::vector<Call> cs;
  std::string err;
  if (!ParseCalls(x.text, setup.ask && !asked, cs, err)) {
    log.event(Event::ParseFail, {{"round", round()}, {"error", err}});
    rd.err = "不合协议: " + err;
    if (go(Phase::Think))
      ask(RetryMessage(err));
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
  if (go(Phase::Think))
    ask(RoundMessage(rd.acts));
}

void Mine::ended(Act &a) {
  const double now_s = now();
  a.span.end(now_s);
  total.act_s += a.span.len(now_s);
  clip(a.out);
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

void Mine::ask(Message m) {
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
  log.event(Event::Finish, {{"reason", reason}, {"rounds", round()}, {"adopted", total.tally.adopted}, {"issues", total.tally.n(Tool::NoteIssue)}});
  log.report(report);
  phase_ = Phase::Report;
}

std::string Mine::report_of(const std::string &why) const {
  const Tally &c = total.tally;
  std::string r = std::string(setup.test ? "# 自检报告\n\n- 自检会话: 候选与日志在 test 目录, 提交只查门槛不拷进正式库" : "# 结束报告\n") + "\n- 结束原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Run::Look)) + ", 查研报 " + std::to_string(c.n(Run::Docs)) + ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n\n";
  std::string in, out, issues;
  for (size_t i = 0; i < timeline.size(); ++i) {
    const std::string at = "- 第 " + std::to_string(i + 1) + " 轮";
    for (const Act &a : timeline[i].acts) {
      if (!a.span.ended())
        continue;
      if (a.call.tool == Tool::SubmitFactor)
        (a.ok ? in : out) += at + " " + a.call.str("name") + (a.ok ? "" : " — " + a.out) + "\n";
      if (a.call.tool == Tool::NoteIssue)
        issues += at + ": " + a.call.str("text") + "\n";
    }
  }
  r += std::string(setup.test ? "## 达门槛 (" : "## 入库 (") + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.n(Tool::SubmitFactor) - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(c.n(Tool::NoteIssue)) + ")\n\n" + (issues.empty() ? "(无)\n" : issues);
  return r;
}

} // namespace agent
