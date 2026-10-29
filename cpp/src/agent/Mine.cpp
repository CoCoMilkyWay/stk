// Mine — 见头文件
#include "agent/Mine.hpp"

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

} // namespace

// ---- 人的操作 ----
void Mine::open(const SharedData &data, Setup s) {
  assert(!live() && !chat.busy() && !lab.busy());
  setup = std::move(s);
  ctx = catalog::context(data, setup.sc);
  const fs::path dir = catalog::dir(setup.sc);
  lab.bind(data, setup.sc, (dir / "factors").string(), setup.gpu);
  setup.llm.system = SystemMessage(ctx, setup.gate).text;
  chat.open(setup.llm);
  log_.open((dir / "chat" / stamp()).string(), setup.llm.system);
  phase_ = Phase::Digest;
  stop_ = false;
  t0_ = std::chrono::steady_clock::now(), t_end_ = -1;
  timeline.clear();
  adopted.clear();
  reason.clear(), report.clear();
  log_.event("open", {{"pool", setup.sc.pool},
                      {"level", ctx.level},
                      {"hold", ctx.hold},
                      {"backend", llm::def(setup.llm.backend).name},
                      {"model", setup.llm.model},
                      {"net", net_name(setup.llm.net)},
                      {"eval", setup.gpu ? "GPU" : "CPU"},
                      {"gate", {{"mono", setup.gate.mono}, {"ric", setup.gate.ric}}},
                      {"tried", lab.results().size()}});
  ask(FirstMessage(ctx, lab.results()));
}

void Mine::start() {
  assert(phase_ == Phase::Choose);
  const Asked &a = std::get<Asked>(timeline.back().got);
  for (size_t i = 0; i < a.qs.size(); ++i) {
    const Question &q = a.qs[i];
    log_.event("pick", {{"question", q.prompt}, {"picked", Answered(q, a.picks[i])}, {"recommended", Picked(q, q.recommended)}});
  }
  log_.event("start", {{"note", a.note}});
  if (go(Phase::Propose))
    ask(AnswerMessage(a));
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
  staged += o.staged, unstaged += o.unstaged, hits += o.hits;
  submits += o.submits, adopted += o.adopted, issues += o.issues;
  return *this;
}

Mine::Tally Mine::tally(const Round &rd) {
  Tally t;
  const Mined *m = std::get_if<Mined>(&rd.got);
  if (!m)
    return t;
  for (const Trial &k : m->batch) {
    ++(k.st.res.verdict == Verdict::Unstaged ? t.unstaged : t.staged);
    t.hits += k.st.res.verdict == Verdict::Weights;
  }
  t.submits = static_cast<int>(m->submitted.size());
  t.adopted = static_cast<int>(std::count_if(m->submitted.begin(), m->submitted.end(), [](const Submitted &x) { return x.err.empty(); }));
  t.issues = static_cast<int>(m->issues.size());
  return t;
}

// ---- 每帧: 两个 worker 同样 poll ----
void Mine::update() {
  while (std::optional<llm::Reply> r = chat.poll())
    on(std::move(*r));
  while (std::optional<Tick> t = lab.poll())
    on(std::move(*t));
  if (phase_ == Phase::Eval && !lab.busy())
    evaluated();
}

// 收齐一轮 (含会话结束后才收尾的那轮): 落盘; 等 agent 的阶段按它推进
void Mine::on(llm::Reply r) {
  Round &rd = timeline.back();
  const int round = static_cast<int>(timeline.size());
  rd.t_reply = now();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  if (!x.ok())
    rd.err = "回复失败 (" + std::string(llm::name(x.end)) + (x.why.empty() ? "" : ": " + x.why) + ")";
  log_.turn(round, rd.user, x);
  log_.event("reply", {{"round", round},
                       {"end", llm::name(x.end)},
                       {"why", x.why},
                       {"id", x.id},
                       {"seconds", x.seconds},
                       {"think_seconds", x.think_s},
                       {"prompt_tokens", x.usage.prompt},
                       {"cached_tokens", x.usage.cached},
                       {"completion_tokens", x.usage.completion},
                       {"chars", x.text.size()},
                       {"thinking_chars", x.thinking.size()}});
  if (phase_ != Phase::Digest && phase_ != Phase::Propose)
    return;
  if (!x.ok())
    return finish(rd.err);
  return phase_ == Phase::Digest ? reply<Questions>(round) : reply<Proposal>(round);
}

// 评估一步: 在评的候选记开评 / 转 Run / 评完 (评完换上定论)
void Mine::on(Tick t) {
  if (phase_ != Phase::Eval)
    return;
  Mined &m = std::get<Mined>(timeline.back().got);
  const auto it = std::find_if(m.batch.begin(), m.batch.end(), [&](const Trial &k) { return k.st.file == t.file; });
  assert(it != m.batch.end() && "评估的文件应在本批");
  Trial &k = *it;
  const double now_s = now();
  switch (t.step) {
  case Step::Search:
    k.t0 = now_s;
    break;
  case Step::Run:
    if (k.t0 < 0)
      k.t0 = now_s;
    k.t_run = now_s;
    break;
  case Step::Idle:
    k.t1 = now_s;
    k.st.res = std::move(t.res);
    log_.event("eval", {{"name", k.st.cand.name}, {"result", Brief(k.st.res)}});
    break;
  }
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
  timeline.push_back({.kind = m.kind, .user = m.text, .t_ask = now()});
  chat.ask(std::move(m.text));
}

void Mine::finish(std::string why) {
  assert(live());
  if (chat.busy())
    chat.cancel();
  lab.cancel();
  t_end_ = now();
  reason = std::move(why);
  report = report_of(reason);
  Tally all;
  for (const Round &rd : timeline)
    all += tally(rd);
  log_.event("finish", {{"reason", reason}, {"rounds", timeline.size()}, {"adopted", all.adopted}, {"issues", all.issues}});
  log_.report(report);
  phase_ = Phase::Report;
}

template <class T>
void Mine::reply(int round) {
  T x;
  std::string err;
  if (!Parse(timeline.back().reply.text, x, err)) {
    log_.event("parse_fail", {{"round", round}, {"phase", name(phase_)}, {"error", err}});
    timeline.back().err = "不合协议: " + err;
    if (go(phase_))
      ask(RetryMessage(err));
    return;
  }
  on(std::move(x), round);
}

void Mine::on(Questions qs, int round) {
  log_.event("questions", {{"round", round}, {"n", qs.size()}});
  Asked a;
  for (const Question &q : qs)
    a.picks.push_back({q.recommended, false, {}});
  a.qs = std::move(qs);
  timeline.back().got = std::move(a);
  go(Phase::Choose);
}

void Mine::on(Proposal p, int round) {
  log_.event("mine_reply", {{"round", round}, {"factors", p.factors.size()}, {"submit", p.submit.size()}, {"issues", p.issues.size()}, {"done", p.done}});
  Mined &m = timeline.back().got.emplace<Mined>();
  m.issues = std::move(p.issues), m.done = p.done;
  for (const std::string &x : m.issues)
    log_.event("issue", {{"round", round}, {"text", x}});
  for (std::string &s : p.submit) {
    Submitted x{std::move(s), {}};
    lab.adopt(x, setup.gate);
    if (x.err.empty())
      adopted.insert(x.name);
    log_.event("submit", {{"round", round}, {"name", x.name}, {"result", x.err.empty() ? std::string("入库") : x.err}});
    m.submitted.push_back(std::move(x));
  }
  if (m.done)
    return finish("agent 宣布挖完");
  if (!go(Phase::Eval))
    return;
  std::vector<std::string> files;
  for (Cand &c : p.factors) {
    Trial k{.st = {std::move(c), {}, {}, {}}};
    lab.stage(k.st);
    if (!k.st.file.empty())
      files.push_back(k.st.file);
    log_.event("stage", {{"round", round},
                         {"name", k.st.cand.name},
                         {"expr", k.st.cand.expr},
                         {"note", k.st.cand.note},
                         {"result", k.st.file.empty() ? "未落文件: " + k.st.err : "落文件 " + k.st.file}});
    m.batch.push_back(std::move(k));
  }
  m.t_eval0 = now();
  log_.event("eval_begin", {{"round", round}, {"files", files.size()}});
  lab.eval(std::move(files));
}

void Mine::evaluated() {
  Mined &m = std::get<Mined>(timeline.back().got);
  m.t_eval1 = now();
  log_.event("eval_end");
  Message next = RoundMessage(ctx, m);
  if (go(Phase::Propose))
    ask(std::move(next));
}

std::string Mine::report_of(const std::string &why) const {
  std::string r = "# 结束报告\n\n- 结束原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n\n";
  std::string in, out, issues;
  int n_in = 0, n_out = 0, n_issue = 0;
  for (size_t i = 0; i < timeline.size(); ++i) {
    const Mined *m = std::get_if<Mined>(&timeline[i].got);
    if (!m)
      continue;
    const std::string at = "- 第 " + std::to_string(i + 1) + " 轮";
    for (const Submitted &x : m->submitted) {
      (x.err.empty() ? in : out) += at + " " + x.name + (x.err.empty() ? "" : ": " + x.err) + "\n";
      ++(x.err.empty() ? n_in : n_out);
    }
    for (const std::string &x : m->issues)
      issues += at + ": " + x + "\n", ++n_issue;
  }
  r += "## 入库 (" + std::to_string(n_in) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(n_out) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(n_issue) + ")\n\n" + (issues.empty() ? "(无)\n" : issues);
  return r;
}

} // namespace agent
