// Mine — 见头文件
#include "agent/Mine.hpp"
#include "agent/Config.hpp"

#include <cassert>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <utility>

namespace agent {

namespace {

namespace fs = std::filesystem;

constexpr const char *kPhaseIds[] = {"none", "digest", "choose", "asking", "eval", "report"}; // 流水里的阶段名

std::string stamp() {
  const std::time_t now = std::time(nullptr);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", std::localtime(&now));
  return b;
}

std::string num(double v) {
  char b[32];
  std::snprintf(b, sizeof(b), "%.4g", v);
  return b;
}

std::string net_name(const llm::Net &n) { return n.proxy ? "proxy " + n.host + ":" + std::to_string(n.port) : std::string("direct"); }

// 一个候选文件的结局 (流水用)
std::string brief(const Result &r) {
  if (!r.error.empty())
    return "BROKEN: " + r.error;
  const Weight *w = r.best();
  if (!w)
    return r.ws.empty() && r.searched ? "无显著" : "未评估";
  return std::to_string(r.ws.size()) + " 个权重, 最佳 mono " + num(w->mono) + " ls " + num(w->ls * 1e4) + "bp rIC " + num(w->ric) + " score " + num(w->score) +
         " 有效率 " + num(w->valid);
}

} // namespace

// ---- 人的操作 ----
void Mine::open(const SharedData &data, Setup s) {
  assert(!live() && !chat.busy() && !lab.busy());
  setup = std::move(s);
  ctx = catalog::context(data, setup.sc);
  const fs::path dir = fs::path(kWorkDir) / setup.sc.pool / ctx.level / ctx.hold;
  lab.bind(data, setup.sc, (dir / "factors").string(), setup.gpu);
  setup.llm.system = SystemMessage(ctx, setup.gate).text;
  chat.open(setup.llm);
  log_.open((dir / "chat" / stamp()).string(), setup.llm);
  phase_ = Phase::Digest;
  stop_ = false;
  seen_ = chat.done(), rounds_ = 0;
  questions.clear(), picks.clear(), note.clear();
  batch_.clear(), submitted_.clear(), submits_.clear(), issues_.clear(), adopted.clear();
  report.clear();
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
  for (size_t i = 0; i < questions.size(); ++i) {
    const Question &q = questions[i];
    log_.event("pick", {{"question", q.prompt}, {"picked", Answered(q, picks[i])}, {"recommended", Picked(q, q.recommended)}});
  }
  log_.event("start", {{"note", note}});
  if (go(Phase::Asking))
    ask(AnswerMessage(questions, picks, note));
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

// ---- 每帧 ----
template <class T>
void Mine::reply(std::string_view text, int round) {
  T x;
  std::string err;
  if (!Parse(text, x, err)) {
    log_.event("parse_fail", {{"round", round}, {"phase", kPhaseIds[static_cast<int>(phase_)]}, {"error", err}});
    if (go(phase_))
      ask(RetryMessage(err));
    return;
  }
  on(std::move(x), round);
}

void Mine::update() {
  lab.update();
  if (phase_ == Phase::None)
    return;
  // 收齐一轮 (含会话结束后才收尾的那轮): 落盘; 等 agent 的阶段按它推进
  if (chat.done() != seen_) {
    seen_ = chat.done();
    Turn t;
    {
      std::lock_guard<std::mutex> lock(chat.mutex);
      assert(!chat.turns.empty());
      t = chat.turns.back();
    }
    const int round = ++rounds_;
    const llm::Reply &r = t.reply;
    log_.turn(round, t);
    log_.event("reply", {{"round", round},
                         {"end", llm::kEndNames[static_cast<int>(r.end)]},
                         {"why", r.why},
                         {"seconds", r.seconds},
                         {"prompt_tokens", r.usage.prompt},
                         {"cached_tokens", r.usage.cached},
                         {"completion_tokens", r.usage.completion},
                         {"chars", r.text.size()},
                         {"thinking_chars", r.thinking.size()}});
    if (phase_ != Phase::Digest && phase_ != Phase::Asking)
      return;
    if (!r.ok())
      return finish("回复失败 (" + std::string(llm::kEndNames[static_cast<int>(r.end)]) + (r.why.empty() ? "" : ": " + r.why) + ")");
    return phase_ == Phase::Digest ? reply<Questions>(r.text, round) : reply<MineReply>(r.text, round);
  }
  if (phase_ == Phase::Eval && !lab.busy())
    evaluated();
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
  log_.event("ask", {{"round", rounds_ + 1}, {"kind", kPrompts[static_cast<int>(m.kind)]}, {"chars", m.text.size()}});
  chat.ask(std::move(m.text));
}

void Mine::finish(std::string reason) {
  assert(live());
  if (chat.busy())
    chat.cancel();
  lab.cancel();
  report = report_of(reason);
  log_.event("finish", {{"reason", reason}, {"rounds", rounds_}, {"adopted", adopted.size()}, {"issues", issues_.size()}});
  log_.report(report);
  phase_ = Phase::Report;
}

void Mine::on(Questions qs, int round) {
  log_.event("questions", {{"round", round}, {"n", qs.size()}});
  questions = std::move(qs);
  picks.clear();
  for (const Question &q : questions)
    picks.push_back({q.recommended, false, {}});
  go(Phase::Choose);
}

void Mine::on(MineReply m, int round) {
  log_.event("mine_reply", {{"round", round}, {"factors", m.factors.size()}, {"submit", m.submit.size()}, {"issues", m.issues.size()}, {"done", m.done}});
  for (std::string &x : m.issues) {
    log_.event("issue", {{"round", round}, {"text", x}});
    issues_.push_back({round, std::move(x)});
  }
  submitted_.clear();
  for (std::string &name : m.submit) {
    Submitted x{std::move(name), {}};
    lab.adopt(x, setup.gate);
    if (x.err.empty())
      adopted.insert(x.name);
    log_.event("submit", {{"round", round}, {"name", x.name}, {"result", x.err.empty() ? std::string("入库") : x.err}});
    submits_.push_back({round, x});
    submitted_.push_back(std::move(x));
  }
  if (m.done)
    return finish("agent 宣布挖完");
  if (!go(Phase::Eval))
    return;
  batch_.clear();
  std::vector<std::string> files;
  for (Cand &c : m.factors) {
    Staged st{std::move(c), {}, {}, {}};
    lab.stage(st);
    if (!st.file.empty())
      files.push_back(st.file);
    log_.event("stage", {{"round", round},
                         {"name", st.cand.name},
                         {"expr", st.cand.expr},
                         {"note", st.cand.note},
                         {"result", st.file.empty() ? "未落文件: " + st.err : "落文件 " + st.file}});
    batch_.push_back(std::move(st));
  }
  log_.event("eval_begin", {{"round", round}, {"files", files.size()}});
  lab.eval(std::move(files));
}

void Mine::evaluated() {
  for (Staged &st : batch_) {
    if (st.file.empty())
      continue;
    st.res = lab.result(st.file);
    log_.event("eval", {{"name", st.cand.name}, {"result", brief(st.res)}});
  }
  log_.event("eval_end");
  if (go(Phase::Asking))
    ask(RoundMessage(ctx, submitted_, batch_));
}

std::string Mine::report_of(const std::string &reason) const {
  std::string r = "# 结束报告\n\n- 结束原因: " + reason + "\n- 轮数: " + std::to_string(rounds_) + "\n\n";
  std::string in, out;
  int n_in = 0, n_out = 0;
  for (const Submit &x : submits_) {
    const std::string line = "- 第 " + std::to_string(x.round) + " 轮 " + x.s.name + (x.s.err.empty() ? "" : ": " + x.s.err) + "\n";
    (x.s.err.empty() ? in : out) += line;
    ++(x.s.err.empty() ? n_in : n_out);
  }
  r += "## 入库 (" + std::to_string(n_in) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(n_out) + ")\n\n" + (out.empty() ? "(无)\n" : out) + "\n";
  r += "## issues (" + std::to_string(issues_.size()) + ")\n\n";
  for (const Issue &x : issues_)
    r += "- 第 " + std::to_string(x.round) + " 轮: " + x.text + "\n";
  if (issues_.empty())
    r += "(无)\n";
  return r;
}

} // namespace agent
