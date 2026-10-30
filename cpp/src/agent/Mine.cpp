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
  assert(catalog::ready(data, s.sc, s.gpu));
  setup = std::move(s);
  lab.bind(data, setup.sc);
  const Context &ctx = lab.ctx();
  setup.llm.system = SystemMessage(ctx, setup.gate).text;
  chat.open(setup.llm);
  log_.open((fs::path(catalog::dir(setup.sc)) / "chat" / stamp()).string(), setup.llm.system);
  phase_ = setup.ask ? Phase::Digest : Phase::Propose;
  stop_ = false;
  t0_ = std::chrono::steady_clock::now(), t_end_ = -1;
  timeline.clear();
  total = {};
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
                      {"ask", setup.ask},
                      {"tried", lab.results().size()}});
  ask(setup.ask ? FirstMessage(ctx, lab.results()) : GoMessage(ctx, lab.results()));
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
    ask(AnswerMessage(lab.ctx(), a));
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
  staged += o.staged, unstaged += o.unstaged, settled += o.settled, hits += o.hits;
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

// 评估一步 (只在 Eval): 在评的候选记 Search / Run 时刻, 评完换上定论; 本批评完 → evaluated
void Mine::on(Lab::Tick t) {
  if (phase_ != Phase::Eval)
    return;
  const double now_s = now();
  const auto trial = [&]() -> Trial & {
    std::vector<Trial> &b = std::get<Mined>(timeline.back().got).batch;
    const auto it = std::find_if(b.begin(), b.end(), [&](const Trial &k) { return k.st.file == t.file; });
    assert(it != b.end() && "评估的文件应在本批");
    it->span.begin(now_s);
    return *it;
  };
  switch (t.step) {
  case Lab::Step::Search:
    trial().search.begin(now_s);
    break;
  case Lab::Step::Run: {
    Trial &k = trial();
    k.search.end(now_s), k.run.begin(now_s);
    break;
  }
  case Lab::Step::Settle: {
    Trial &k = trial();
    k.search.end(now_s), k.run.end(now_s), k.span.end(now_s);
    k.st.res = std::move(t.res);
    log_.event("eval", {{"name", k.st.cand.name}, {"result", Brief(k.st.res)}});
    add({.settled = 1, .hits = k.st.res.verdict == Verdict::Weights});
    break;
  }
  case Lab::Step::End:
    evaluated();
    break;
  }
}

// 收齐一轮: 落盘, 累计; 等 agent 的阶段按它推进
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
  if (phase_ != Phase::Digest && phase_ != Phase::Propose)
    return;
  if (!x.ok())
    return finish(rd.err);
  return phase_ == Phase::Digest ? parse<Questions>(round) : parse<Proposal>(round);
}

// 评完一批: 累计, 发反馈
void Mine::evaluated() {
  Mined &m = std::get<Mined>(timeline.back().got);
  m.span.end(now());
  total.eval_s += m.span.len(now());
  log_.event("eval_end");
  if (go(Phase::Propose))
    ask(RoundMessage(lab.ctx(), m));
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

template <class T>
void Mine::parse(int round) {
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

// 澄清合格: 问题 → Asked (按推荐预选), 等人选
void Mine::on(Questions qs, int round) {
  log_.event("questions", {{"round", round}, {"n", qs.size()}});
  timeline.back().got.emplace<Asked>(std::move(qs));
  go(Phase::Choose);
}

// 挖掘合格: issues / submit → Mined; 未宣布挖完则落候选, 开评
void Mine::on(Proposal p, int round) {
  log_.event("mine_reply", {{"round", round}, {"factors", p.factors.size()}, {"submit", p.submit.size()}, {"issues", p.issues.size()}, {"done", p.done}});
  Mined &m = timeline.back().got.emplace<Mined>();
  m.issues = std::move(p.issues), m.done = p.done;
  for (const std::string &x : m.issues)
    log_.event("issue", {{"round", round}, {"text", x}});
  add({.issues = static_cast<int>(m.issues.size())});
  for (std::string &s : p.submit) {
    Submitted x{std::move(s), {}};
    lab.adopt(x, setup.gate);
    if (x.err.empty())
      adopted.insert(x.name);
    log_.event("submit", {{"round", round}, {"name", x.name}, {"result", x.err.empty() ? std::string("入库") : x.err}});
    add({.submits = 1, .adopted = x.err.empty()});
    m.submitted.push_back(std::move(x));
  }
  if (m.done)
    return finish("agent 宣布挖完");
  if (!go(Phase::Eval))
    return;
  std::vector<std::string> files;
  for (Cand &c : p.factors) {
    Trial k{.st = {std::move(c), {}, {}}};
    lab.stage(k.st);
    const bool staged = !k.st.file.empty();
    if (staged)
      files.push_back(k.st.file);
    log_.event("stage", {{"round", round},
                         {"name", k.st.cand.name},
                         {"expr", k.st.cand.expr},
                         {"note", k.st.cand.note},
                         {"result", staged ? "落文件 " + k.st.file : Brief(k.st.res)}});
    add({.staged = staged, .unstaged = !staged});
    m.batch.push_back(std::move(k));
  }
  m.span.begin(now());
  log_.event("eval_begin", {{"round", round}, {"files", files.size()}});
  lab.eval(std::move(files), setup.gpu);
}

std::string Mine::report_of(const std::string &why) const {
  const Tally &c = total.tally;
  std::string r = "# 结束报告\n\n- 结束原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n\n";
  std::string in, out, issues;
  for (size_t i = 0; i < timeline.size(); ++i) {
    const Mined *m = std::get_if<Mined>(&timeline[i].got);
    if (!m)
      continue;
    const std::string at = "- 第 " + std::to_string(i + 1) + " 轮";
    for (const Submitted &x : m->submitted)
      (x.err.empty() ? in : out) += at + " " + x.name + (x.err.empty() ? "" : ": " + x.err) + "\n";
    for (const std::string &x : m->issues)
      issues += at + ": " + x + "\n";
  }
  r += "## 入库 (" + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.submits - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(c.issues) + ")\n\n" + (issues.empty() ? "(无)\n" : issues);
  return r;
}

} // namespace agent
