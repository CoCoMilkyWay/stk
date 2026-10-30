// Mine — 见头文件
#include "agent/Mine.hpp"
#include "agent/Config.hpp"
#include "agent/Docs.hpp"
#include "agent/Views.hpp"

#include <algorithm>
#include <cassert>
#include <ctime>
#include <filesystem>
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
  size_t chars = 0, cut = std::string::npos;
  for (size_t i = 0; i < s.size(); ++i)
    if ((static_cast<unsigned char>(s[i]) & 0xC0) != 0x80 && chars++ == static_cast<size_t>(kOutChars))
      cut = i;
  if (cut == std::string::npos)
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(chars) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
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
  log_.open((fs::path(catalog::dir(setup.sc, setup.test)) / "chat" / stamp()).string(), setup.llm.system);
  phase_ = Phase::Think;
  stop_ = done_ = asked_ = false;
  cur_ = 0;
  t0_ = std::chrono::steady_clock::now(), t_end_ = -1;
  timeline.clear();
  total = {};
  adopted.clear();
  reason.clear(), report.clear();
  const int tried = static_cast<int>(std::count_if(lab.results().begin(), lab.results().end(), [](const Result &r) { return r.visible; }));
  log_.event("open", {{"pool", setup.sc.pool},
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
  const Asked &q = std::get<Asked>(a.got);
  for (size_t i = 0; i < q.qs.size(); ++i) {
    const Question &x = q.qs[i];
    log_.event("pick", {{"question", x.prompt}, {"picked", Answered(x, q.picks[i])}, {"recommended", Picked(x, x.recommended)}});
  }
  log_.event("start", {{"note", q.note}});
  a.out = AnswerText(q);
  ended(a);
  step();
}

void Mine::stop() {
  assert(live() && !stop_);
  log_.event("stop");
  stop_ = true;
  if (phase_ == Phase::Choose)
    finish("按 Stop");
}

void Mine::cancel() {
  assert(live());
  log_.event("cancel");
  finish("按 Cancel");
}

double Mine::now() const { return t_end_ >= 0 ? t_end_ : std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_).count(); }

Mine::Tally &Mine::Tally::operator+=(const Tally &o) {
  calls += o.calls, looks += o.looks, reads += o.reads, staged += o.staged, unstaged += o.unstaged, settled += o.settled, hits += o.hits;
  submits += o.submits, adopted += o.adopted, issues += o.issues;
  return *this;
}

void Mine::add(const Tally &d) { timeline.back().tally += d, total.tally += d; }

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

// 实验台一步 (只在 Act, 属在做的调用): 回测记 Search / Run 时刻, 评完换上定论; 观察成文; End → 该调用做完, 接着执行
void Mine::on(Lab::Tick t) {
  if (phase_ != Phase::Act)
    return;
  Act &a = timeline.back().acts[cur_];
  const double now_s = now();
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
    assert(t.file == k.st.file);
    k.search.end(now_s), k.run.end(now_s);
    k.st.res = std::move(t.res);
    a.out = TrialText(k.st);
    a.ok = k.st.res.verdict != Verdict::Broken;
    log_.event("eval", {{"name", k.st.cand.name}, {"result", Brief(k.st.res)}});
    add({.settled = 1, .hits = k.st.res.verdict == Verdict::Weights});
    break;
  }
  case Lab::Step::Look:
    a.ok = Render(a.call, t.look, lab.ctx(), a.out);
    break;
  case Lab::Step::End:
    ended(a);
    step();
    break;
  }
}

// 收齐一轮: 落盘, 累计; 在等 agent 则解析成一串调用, 开始执行
void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  const int round = static_cast<int>(timeline.size());
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  total.llm_s += rd.span.len(now()), total.usage += x.usage;
  if (!x.ok())
    rd.err = "回复失败 (" + std::string(llm::name(x.end)) + (x.why.empty() ? "" : ": " + x.why) + ")";
  log_.turn(round, rd.user, x);
  log_.event("reply", {{"round", round},
                       {"end", llm::name(x.end)},
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
  if (!ParseCalls(x.text, setup.ask && !asked_, cs, err)) {
    log_.event("parse_fail", {{"round", round}, {"error", err}});
    rd.err = "不合协议: " + err;
    if (go(Phase::Think))
      ask(RetryMessage(err));
    return;
  }
  nlohmann::ordered_json tools = nlohmann::ordered_json::array();
  Tally d{.calls = static_cast<int>(cs.size())};
  for (Call &c : cs) {
    tools.push_back(def(c.tool).name);
    d.looks += def(c.tool).run == Run::Look;
    d.reads += def(c.tool).run == Run::Docs;
    rd.acts.push_back({.call = std::move(c)});
  }
  log_.event("calls", {{"round", round}, {"tools", tools}});
  add(d);
  cur_ = 0;
  step();
}

// ---- 执行 ----
void Mine::step() {
  Round &rd = timeline.back();
  const int round = static_cast<int>(timeline.size());
  while (cur_ < rd.acts.size()) {
    if (!go(Phase::Act))
      return;
    Act &a = rd.acts[cur_];
    a.span.begin(now());
    log_.event("call", {{"round", round}, {"i", cur_ + 1}, {"tool", def(a.call.tool).name}, {"args", a.call.args}});
    if (!begin(a))
      return;
    ended(a);
  }
  if (done_)
    return finish("agent 宣布挖完");
  if (go(Phase::Think))
    ask(RoundMessage(rd.acts));
}

bool Mine::begin(Act &a) {
  const Call &c = a.call;
  switch (def(c.tool).run) {
  case Run::Now:
    here(a);
    return true;
  case Run::Human:
    a.got.emplace<Asked>(QuestionsOf(c));
    asked_ = true;
    log_.event("questions", {{"round", timeline.size()}, {"n", std::get<Asked>(a.got).qs.size()}});
    go(Phase::Choose);
    return false;
  case Run::Eval: {
    Trial &k = a.got.emplace<Trial>();
    k.st.cand = {c.str("name"), c.str("name_cn"), c.str("expr"), c.str("note")};
    lab.stage(k.st);
    const bool staged = !k.st.file.empty();
    log_.event("stage", {{"round", timeline.size()},
                         {"name", k.st.cand.name},
                         {"expr", k.st.cand.expr},
                         {"note", k.st.cand.note},
                         {"result", staged ? "落文件 " + k.st.file : Brief(k.st.res)}});
    add({.staged = staged, .unstaged = !staged});
    if (!staged) {
      a.out = TrialText(k.st), a.ok = false;
      return true;
    }
    lab.eval({k.st.file}, setup.gpu);
    return false;
  }
  case Run::Look: {
    LookSpec sp;
    if (!LookOf(c, lab, sp, a.out)) {
      a.ok = false;
      return true;
    }
    lab.look(std::move(sp));
    return false;
  }
  case Run::Docs:
    a.ok = Browse(c, a.out);
    return true;
  }
  return true;
}

void Mine::here(Act &a) {
  const Call &c = a.call;
  const int round = static_cast<int>(timeline.size());
  switch (c.tool) {
  case Tool::ListTried:
    a.out = TriedText(lab.results(), c.num("top"));
    break;
  case Tool::ListLibrary:
    a.out = LibraryText(lab.ctx().library, c.num("top"));
    break;
  case Tool::SubmitFactor: {
    Submitted x{c.str("name"), {}};
    lab.adopt(x, setup.gate);
    a.ok = x.err.empty();
    if (a.ok && !setup.test)
      adopted.insert(x.name);
    a.out = !a.ok ? "未入库: " + x.err : setup.test ? "达门槛 (自检会话, 不拷进正式库)" : "已入库";
    log_.event("submit", {{"round", round}, {"name", x.name}, {"result", a.ok ? std::string("入库") : x.err}});
    add({.submits = 1, .adopted = a.ok});
    break;
  }
  case Tool::NoteIssue:
    a.out = "已记下, 会话结束后交给研究员";
    log_.event("issue", {{"round", round}, {"text", c.str("text")}});
    add({.issues = 1});
    break;
  case Tool::Finish:
    done_ = true;
    a.out = "本条回复的调用做完后会话结束";
    break;
  default:
    assert(false && "不是当场做的工具");
  }
}

void Mine::ended(Act &a) {
  const double now_s = now();
  a.span.end(now_s);
  total.act_s += a.span.len(now_s);
  clip(a.out);
  log_.event("result", {{"round", timeline.size()},
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
  log_.event("ask", {{"round", timeline.size() + 1}, {"kind", def(m.kind).id}, {"chars", m.text.size()}});
  timeline.push_back({.kind = m.kind, .user = m.text, .span = {.t0 = now()}});
  chat.ask(std::move(m.text));
}

void Mine::finish(std::string why) {
  assert(live());
  chat.cancel();
  lab.cancel();
  t_end_ = now();
  reason = std::move(why);
  report = report_of(reason);
  log_.event("finish", {{"reason", reason}, {"rounds", timeline.size()}, {"adopted", total.tally.adopted}, {"issues", total.tally.issues}});
  log_.report(report);
  phase_ = Phase::Report;
}

std::string Mine::report_of(const std::string &why) const {
  const Tally &c = total.tally;
  std::string r = std::string(setup.test ? "# 自检报告\n\n- 自检会话: 候选与日志在 test 目录, 提交只查门槛不拷进正式库" : "# 结束报告\n") + "\n- 结束原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.calls) + " (观察 " +
                  std::to_string(c.looks) + ", 查研报 " + std::to_string(c.reads) + ", 回测 " + std::to_string(c.staged + c.unstaged) + ")\n\n";
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
  r += "## 未入库的提交 (" + std::to_string(c.submits - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(c.issues) + ")\n\n" + (issues.empty() ? "(无)\n" : issues);
  return r;
}

} // namespace agent
