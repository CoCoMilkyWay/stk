// Mine — 见头文件
#include "gui/task_agent/Mine.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>
#include <vector>

namespace GUI::Agent {

namespace fs = std::filesystem;
using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

// =============================================================================
// JSON 存档契约 (读写严格成对)
// =============================================================================
namespace llm {
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Usage, prompt, cached, completion)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Reply, text, thinking, end, error, usage, id, seconds)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Net, proxy, host, port)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, backend, net, model)
} // namespace llm

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Span, t0, t1)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Cand, name, name_cn, expr, hyp, note)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Pick, on, other, text)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Desk::Setup, sc, gate, ask, strict, test, remark)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Desk::Leg, llm, mcp, gpu, budget)

void to_json(json &j, const Act &a) {
  j = {{"tool", a.call.tool}, {"args", a.call.args}, {"span", a.span}, {"out", a.out.text}, {"gist", a.out.gist}, {"ok", a.out.ok}};
  if (const TrialRes *k = AsTrial(a))
    j["trial"] = {{"cand", k->st.cand}, {"file", k->st.file}, {"error", k->st.res.error}, {"search", k->search}, {"run", k->run}};
  if (const AskRes *q = AsAsk(a))
    j["asked"] = {{"picks", q->asked.picks}, {"note", q->asked.note}};
}
void from_json(const json &j, Act &a) {
  a = Act{.call = {j.at("tool").get<Tool>(), j.at("args")},
          .span = j.at("span").get<Span>(),
          .out = {.ok = j.at("ok").get<bool>(), .text = j.at("out").get<std::string>(), .gist = j.value("gist", std::string())}};
  if (j.contains("trial")) {
    const json &t = j.at("trial");
    auto k = std::make_shared<TrialRes>();
    t.at("cand").get_to(k->st.cand);
    t.at("file").get_to(k->st.file);
    t.at("search").get_to(k->search);
    t.at("run").get_to(k->run);
    k->st.res = k->st.ok() ? Result{.error = "候选文件已不在", .verdict = Verdict::Pending}
                           : Result{.error = t.at("error").get<std::string>(), .verdict = Verdict::Unstaged};
    a.out.res = std::move(k);
  }
  if (j.contains("asked")) {
    auto q = std::make_shared<AskRes>(QuestionsOf(a.call));
    j.at("asked").at("picks").get_to(q->asked.picks);
    j.at("asked").at("note").get_to(q->asked.note);
    assert(q->asked.picks.size() == q->asked.qs.size());
    a.out.res = std::move(q);
  }
}

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Round, kind, user, memo, span, think, write, reply, error, acts)

// =============================================================================
// 可读研究日志 log.md 与会话目录 Journal
// =============================================================================
namespace {

void write_file(const fs::path &p, const std::string &s) {
  std::ofstream f(p, std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << s;
}

void replace_file(const fs::path &p, const std::string &s) {
  const fs::path tmp = p.string() + ".tmp";
  write_file(tmp, s);
  fs::rename(tmp, p);
}

void append_text(const fs::path &p, const std::string &s) {
  if (s.empty())
    return;
  std::ofstream f(p, std::ios::binary | std::ios::app);
  assert(f.is_open());
  f << s;
}

json read_json(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f.is_open());
  return json::parse(f);
}

std::string quote_md(const std::string &s) {
  std::string o;
  for (size_t i = 0; i < s.size();) {
    const size_t e = std::min(s.find('\n', i), s.size());
    o += "> " + s.substr(i, e - i) + "\n";
    i = e + 1;
  }
  return o;
}

std::string fmt_round_send(int n, const Round &rd) {
  std::string s = "\n---\n\n## 第 " + std::to_string(n) + " 轮 · " + def(rd.kind).cn + "\n\n";
  if (n == 1)
    s += rd.user + (rd.user.ends_with('\n') ? "\n" : "\n\n");
  if (!rd.memo.empty())
    s += "> **研究员插话**\n" + quote_md(rd.memo) + "\n";
  return s;
}

std::string fmt_round_reply(int n, const Round &rd) {
  if (!rd.span.ended())
    return {};
  const llm::Reply &r = rd.reply;
  std::string s = "### R" + std::to_string(n) + " Agent 回复 (" + text::fmt("%.1fs", r.seconds) + " · 输入 " + std::to_string(r.usage.prompt) + " [缓存 " +
                  std::to_string(r.usage.cached) + "] / 输出 " + std::to_string(r.usage.completion) + ")\n\n";
  if (!r.thinking.empty())
    s += "<details>\n<summary>思考过程 (" + std::to_string(text::chars(r.thinking)) + " 字)</summary>\n\n" + r.thinking +
         (r.thinking.ends_with('\n') ? "" : "\n") + "\n</details>\n\n";
  if (!r.ok() || !rd.error.empty()) {
    s += "> **未通过**: " + rd.error + "\n\n";
    if (!r.text.empty())
      s += "```json\n" + r.text + (r.text.ends_with('\n') ? "" : "\n") + "```\n\n";
    return s;
  }
  for (size_t i = 0; i < rd.acts.size(); ++i)
    s += std::to_string(i + 1) + ". `" + RefText({n, static_cast<int>(i) + 1}) + "` `" + rd.acts[i].call.text(100) + "`\n";
  return s + "\n";
}

std::string fmt_act_done(Ref at, const Act &a) {
  if (!a.span.ended())
    return {};
  std::string s = "#### [" + RefText(at) + "] `" + a.call.text(100) + "` (" + text::fmt("%.1fs", a.span.len(a.span.t1)) +
                  (a.out.ok ? "" : " · 没做成") + ")\n\n" + a.out.text;
  if (!s.ends_with('\n'))
    s += '\n';
  return s + "\n";
}

} // namespace

void Journal::open(std::string dir, const std::string &system, const std::string &head) {
  dir_ = std::move(dir), head_ = head;
  fs::create_directories(dir_);
  write_file(fs::path(dir_) / "system.md", system);
  write_file(fs::path(dir_) / kLogFile, head_);
}

void Journal::reopen(std::string dir, std::string head) {
  assert(fs::exists(fs::path(dir) / kSessionFile));
  dir_ = std::move(dir), head_ = std::move(head);
}

std::string Journal::system() const {
  std::ifstream f(fs::path(dir_) / "system.md", std::ios::binary);
  assert(f.is_open());
  std::ostringstream s;
  s << f.rdbuf();
  return s.str();
}

void Journal::append_log(const std::string &md) const {
  assert(!dir_.empty());
  append_text(fs::path(dir_) / kLogFile, md);
}

void Journal::rewrite_log(Rounds timeline, const std::string &stop_reason) const {
  assert(!dir_.empty());
  std::string md = head_;
  for (size_t i = 0; i < timeline.size(); ++i) {
    const int n = static_cast<int>(i) + 1;
    const Round &rd = timeline[i];
    if (!rd.span.began())
      continue;
    md += fmt_round_send(n, rd);
    md += fmt_round_reply(n, rd);
    for (size_t j = 0; j < rd.acts.size(); ++j)
      md += fmt_act_done({n, static_cast<int>(j) + 1}, rd.acts[j]);
  }
  if (!stop_reason.empty())
    md += "---\n\n**停下**: " + stop_reason + "\n\n";
  replace_file(fs::path(dir_) / kLogFile, md);
}

void Journal::report(const std::string &text) const {
  assert(!dir_.empty());
  replace_file(fs::path(dir_) / "report.md", text);
}

void Journal::save(const json &session, const json &brief) const {
  assert(!dir_.empty());
  replace_file(fs::path(dir_) / kSessionFile, session.dump());
  replace_file(fs::path(dir_) / kBriefFile, brief.dump(2));
}

// =============================================================================
// 工具校验与执行辅助
// =============================================================================
namespace {

int tokens(const llm::Usage &u) { return u.prompt + u.completion; }

} // namespace

// =============================================================================
// Desk & Mine 状态机实现
// =============================================================================
double Desk::now() const { return t_end >= 0.0 ? t_end : std::chrono::duration<double>(Clock::now() - t0).count(); }

std::string Mine::make_log_head() const {
  const Context &c = lab.ctx();
  std::string h = "# 会话日志 · " + catalog::label(setup.sc) + " (近 " + std::to_string(setup.sc.years) + " 年)\n\n";
  h += "- 模式: " + std::string(setup.test ? "自检" : "正式") + " · 大脑: " + brain() + " · 评估: " + (leg.gpu ? "GPU" : "CPU") + " · 门槛: 中位 rIC ≥ " +
       text::num(setup.gate.ric) + " · 会话约束: " + (setup.strict ? "守" : "不守") + " · 提问: " + (setup.ask ? "可" : "关") + "\n";
  h += "- 来源: " + c.srcs + " · 开放: 叶 " + std::to_string(c.leaves.size()) + " 个, 算子 " + std::to_string(c.ops.size()) + " 个 · 评估期: " + c.window + "\n";
  if (!setup.remark.empty())
    h += "- 本会话要求: " + text::flat(setup.remark) + "\n";
  return h;
}

void Mine::bind(const SharedData &data) {
  assert(!live() && idle());
  assert(catalog::ready(data, setup.sc, leg.gpu));
  flush();
  lab.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  system_ptr = std::make_shared<const std::string>(system);
  stop_ = false, retries_ = 0;
}

void Mine::depart() { from_ = {round(), now(), tokens(tally().usage)}; }

void Mine::open(const SharedData &data, Setup s, Leg l) {
  setup = std::move(s), leg = std::move(l);
  assert(!live() && idle() && catalog::ready(data, setup.sc, leg.gpu));
  flush();
  lab.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  const Context &ctx = lab.ctx();
  assert(!ctx.leaves.empty() && !ctx.ops.empty() && "须有开放的叶与算子");
  system = SystemMessage(ctx, setup.gate, setup.ask).text;
  system_ptr = std::make_shared<const std::string>(system);
  disk::Key key = catalog::key(setup.sc, setup.test);
  key.stamp = disk::stamp_now();
  log.open(disk::path(disk::AgentChat, key).string(), system, make_log_head());

  timeline.clear();
  restate();
  memo.clear(), reason.clear(), report.clear();
  cur_ = 0, stop_ = false, retries_ = 0;
  t0 = Clock::now(), t_end = -1.0;
  phase_ = Phase::Think;
  depart();
  const int tried = static_cast<int>(std::ranges::count_if(lab.results(), [](const Result &r) { return r.visible; }));
  post(setup.test ? TestMessage(setup.ask, setup.remark)
                  : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark));
}

void Mine::load(const SharedData &data, const std::string &dir) {
  const json j = read_json(fs::path(dir) / kSessionFile);
  j.at("setup").get_to(setup);
  j.at("leg").get_to(leg);
  leg.gpu = leg.gpu && catalog::gpu();
  log.reopen(dir, {});
  system = log.system();
  bind(data);
  log.reopen(dir, j.value("log_head", make_log_head()));
  j.at("timeline").get_to(timeline);
  assert(!timeline.empty() && "开会话即发首条");
  const std::vector<Result> &rs = lab.results();
  for (Round &rd : timeline)
    for (Act &a : rd.acts)
      if (TrialRes *k = AsTrial(a)) {
        k->gate = setup.gate;
        k->level = lab.ctx().level, k->hold = lab.ctx().hold;
        if (k->st.ok())
          if (const auto it = std::ranges::find(rs, k->st.file, &Result::file); it != rs.end())
            k->st.res = *it;
      }
  cur_ = j.at("cur").get<size_t>();
  memo = j.at("memo").get<std::string>();
  reason = j.at("reason").get<std::string>();
  t_end = j.at("elapsed").get<double>();
  const bool clean = j.at("clean").get<bool>();
  if (!clean) {
    Round &rd = timeline.back();
    if (!rd.span.ended()) {
      unsend();
    } else if (cur_ < rd.acts.size()) {
      discard(round());
      for (Act &a : rd.acts)
        a = Act{.call = a.call};
      cur_ = 0;
    }
    reason = "上次没停稳就退出了, 末轮作废重来";
  }
  restate();
  phase_ = Phase::Pause;
  report = report_of(reason);
  log.report(report);
  if (!clean || !fs::exists(fs::path(dir) / kLogFile))
    log.rewrite_log(timeline, reason);
  save();
}

void Mine::resume(const SharedData &data, Leg l) {
  assert(phase_ == Phase::Pause && idle() && !timeline.empty());
  flush();
  leg = std::move(l);
  assert(catalog::ready(data, setup.sc, leg.gpu));
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1.0;
  stop_ = false, retries_ = 0;
  reason.clear();
  depart();
  log.append_log("\n> **继续 (" + text::local("%H:%M:%S") + ")** · 大脑 " + brain() + "\n");
  switch (cont()) {
  case Cont::Resend:
    unsend();
    restate();
    [[fallthrough]];
  case Cont::Send:
    phase_ = Phase::Think;
    return send();
  case Cont::Retry:
    phase_ = Phase::Think;
    return post(RetryMessage(timeline.back().error));
  case Cont::Acts:
    if (Act &a = timeline.back().acts[cur_]; a.span.began())
      a = Act{.call = a.call};
    return step();
  case Cont::Results:
    return next();
  }
}

void Mine::start() {
  assert(phase_ == Phase::Choose);
  answer_act(cur_, now());
  log_act(cur_++);
  step();
}

void Mine::stop() {
  assert(live() && !stop_);
  stop_ = true;
  if (phase_ == Phase::Choose)
    pause("按 Stop");
}

void Mine::cancel() {
  assert(live());
  Round &rd = timeline.back();
  const double t = now();
  if (phase_ == Phase::Think) {
    rd.think.end(t), rd.write.end(t), rd.span.end(t);
    rd.error = "按 Cancel 作废了在收的回复";
    CloseReply(st, rd);
    log.append_log(fmt_round_reply(round(), rd));
  }
  if (phase_ == Phase::Act && cur_ < rd.acts.size() && rd.acts[cur_].span.running()) {
    abort_act(cur_, t);
    log_act(cur_++);
  }
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

void Mine::save() const {
  const Tally c = tally();
  const json session = {{"setup", setup},
                        {"leg", leg},
                        {"log_head", log.head()},
                        {"elapsed", now()},
                        {"clean", phase_ == Phase::Pause || phase_ == Phase::Choose},
                        {"cur", cur_},
                        {"memo", memo},
                        {"reason", reason},
                        {"report", report},
                        {"timeline", timeline}};
  const json brief = {{"sc", setup.sc},
                      {"test", setup.test},
                      {"rounds", round()},
                      {"live", live()},
                      {"reason", reason},
                      {"adopted", st.adopted},
                      {"cands", c.staged()},
                      {"weights", c.n(Verdict::Weights)},
                      {"hyps", st.board.all().size()},
                      {"brain", brain()},
                      {"remark", setup.remark},
                      {"elapsed", now()}};
  log.save(session, brief);
}

// ---- 回退 ----
Mine::Loss Mine::loss(int k) const {
  assert(phase_ == Phase::Pause && k >= 1 && k <= round());
  return {.rounds = round() - k + 1, .files = static_cast<int>(staged_from(k).size()), .notes = static_cast<int>(notes.filed(log.dir(), k).size())};
}

void Mine::rewind(int k) {
  assert(phase_ == Phase::Pause && idle() && k >= 1 && k <= round());
  flush();
  discard(k);
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
  log.rewrite_log(timeline, reason);
  save();
}

void Mine::unsend() {
  Round &rd = timeline.back();
  rd = Round{.kind = rd.kind, .user = std::move(rd.user), .memo = std::move(rd.memo)};
}

std::vector<std::string> Mine::staged_from(int k) const {
  std::vector<std::string> out;
  for (size_t i = static_cast<size_t>(k) - 1; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (const TrialRes *t = AsTrial(a); t && t->st.ok())
        out.push_back(t->st.file);
  return out;
}

void Mine::discard(int k) {
  for (const std::string &f : staged_from(k))
    lab.discard(f);
  notes.drop(log.dir(), k);
}

Mine::Cont Mine::cont() const {
  const Round &rd = timeline.back();
  return !rd.span.began()        ? Cont::Send
         : !rd.reply.ok()        ? Cont::Resend
         : rd.acts.empty()       ? Cont::Retry
         : cur_ < rd.acts.size() ? Cont::Acts
                                 : Cont::Results;
}

std::string Mine::hint() const {
  if (phase_ != Phase::Pause)
    return {};
  const std::string n = std::to_string(round());
  switch (cont()) {
  case Cont::Send:
    return "发出待发的第 " + n + " 条消息";
  case Cont::Resend:
    return "重发第 " + n + " 条消息 (它的回复没收成)";
  case Cont::Retry:
    return "发重发要求 (第 " + n + " 条回复不合协议)";
  case Cont::Acts:
    return "接着执行第 " + n + " 轮第 " + std::to_string(cur_ + 1) + " 个调用起 (共 " + std::to_string(timeline.back().acts.size()) + " 个)";
  case Cont::Results:
    return "发第 " + n + " 轮的结果" + (flags().done ? ", 让 agent 接着挖 (它已宣布挖完)" : "");
  }
  return {};
}

// =============================================================================
// 双翼对仗轮询与状态转移
// =============================================================================
void Mine::update() {
  while (std::optional<Chat::Tick> t = chat.poll())
    on_chat(std::move(*t));
  while (std::optional<Lab::Tick> t = lab.poll())
    on_lab(std::move(*t));
  flush();
}

void Mine::on_chat(Chat::Tick t) {
  if (phase_ != Phase::Think)
    return;
  Round &rd = timeline.back();
  const double now_s = now();
  switch (t.step) {
  case ChatStep::Think:
    rd.think.begin(now_s);
    break;
  case ChatStep::Write:
    rd.think.end(now_s), rd.write.begin(now_s);
    break;
  case ChatStep::Restart:
    rd.think = {}, rd.write = {};
    break;
  case ChatStep::End:
    rd.think.end(now_s), rd.write.end(now_s), rd.span.end(now_s);
    replied(std::move(t.out));
    break;
  }
}

void Mine::on_lab(Lab::Tick t) {
  if (phase_ != Phase::Act)
    return;
  if (t.step != LabStep::End)
    return step_act(cur_, t.step, now());
  if (t.out.cancelled)
    return;
  Seat s = mine_seat();
  settle_act(cur_, std::move(t.out.out), s, now());
  log_act(cur_++);
  save();
  step();
}

void Mine::replied(llm::Reply r) {
  Round &rd = timeline.back();
  rd.reply = std::move(r);
  const llm::Reply &x = rd.reply;
  if (!x.ok()) {
    rd.error = "回复失败 (" + std::string(def(x.end).id) + (x.error.empty() ? "" : ": " + x.error) + ")";
    CloseReply(st, rd);
    log.append_log(fmt_round_reply(round(), rd));
    return pause(rd.error);
  }
  std::vector<Call> cs;
  std::string error;
  if (!ParseCalls(x.text, setup.ask && !flags().asked, cs, error) || (setup.strict && !(error = RuleMiss(st, cs)).empty())) {
    rd.error = error;
    CloseReply(st, rd);
    log.append_log(fmt_round_reply(round(), rd));
    if (++retries_ >= kMaxRetries)
      return pause("连续 " + std::to_string(kMaxRetries) + " 条回复不合协议");
    if (go(Phase::Think))
      post(RetryMessage(error));
    return;
  }
  retries_ = 0;
  for (Call &c : cs)
    rd.acts.push_back({.call = std::move(c)});
  CloseReply(st, rd);
  log.append_log(fmt_round_reply(round(), rd));
  cur_ = 0;
  save();
  step();
}

Seat Mine::mine_seat() { return seat(cur_, setup.gate, log.dir(), !setup.strict, false, leg.gpu); }

std::string Mine::brain() const { return leg.mcp ? std::string("外部 agent (MCP)") : std::string(def(leg.llm.backend).name) + " / " + leg.llm.model; }

void Mine::reply(std::string calls) {
  assert(leg.mcp && waiting());
  Round &rd = timeline.back();
  const double t = now();
  rd.span.end(t);
  replied({.text = std::move(calls), .end = llm::End::Ok, .seconds = rd.span.len(t)});
}

void Mine::drop() {
  assert(leg.mcp && (phase_ == Phase::Act || phase_ == Phase::Choose));
  Round &rd = timeline.back();
  if (cur_ < rd.acts.size() && rd.acts[cur_].span.running()) {
    abort_act(cur_, now(), "你");
    log_act(cur_++);
  }
  if (const size_t left = rd.acts.size() - cur_; left > 0)
    log.append_log("> 你作废了本批余下的 " + std::to_string(left) + " 个调用 (没执行)\n\n");
  rd.acts.resize(cur_);
  save();
  step();
}

void Mine::step() {
  Round &rd = timeline.back();
  while (cur_ < rd.acts.size()) {
    if (!go(Phase::Act))
      return;
    Seat s = mine_seat();
    const Exec at = open_act(cur_, s, now());
    if (at == Exec::Sync) {
      log_act(cur_++);
      continue;
    }
    if (at == Exec::Human)
      go(Phase::Choose);
    save();
    return;
  }
  if (flags().done)
    return pause("agent 宣布挖完");
  if (const std::string budget = over(); !budget.empty())
    return pause("本段预算用完 (" + budget + ")");
  next();
}

void Mine::next() {
  if (!go(Phase::Think))
    return;
  const Round &rd = timeline.back();
  post(RoundMessage(rd.acts, round(), st.board, flags().evaled, setup.test));
}

void Mine::log_act(size_t i) { log.append_log(fmt_act_done({round(), static_cast<int>(i) + 1}, timeline.back().acts[i])); }

bool Mine::go(Phase next_ph) {
  if (stop_) {
    pause("按 Stop");
    return false;
  }
  phase_ = next_ph;
  return true;
}

void Mine::post(Message m) {
  timeline.push_back({.kind = m.kind, .user = std::move(m.text)});
  OpenRound(st);
  send();
}

void Mine::send() {
  Round &rd = timeline.back();
  assert(!rd.span.began());
  if (!memo.empty()) {
    rd.memo += (rd.memo.empty() ? "" : "\n\n") + memo;
    memo.clear();
  }
  rd.span.begin(now());
  log.append_log(fmt_round_send(round(), rd));
  if (!leg.mcp)
    chat.send({leg.llm, system_ptr, TurnsOf(timeline)});
  save();
}

std::string Mine::over() const {
  const Budget &b = leg.budget;
  const int rounds = round() - from_.round, used = tokens(tally().usage) - from_.tokens;
  const double secs = now() - from_.t;
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
  log.append_log("---\n\n**停下 (" + text::local("%H:%M:%S") + ")**: " + reason + "\n\n");
  log.report(report);
  phase_ = Phase::Pause;
  stop_ = false;
  save();
}

std::string Mine::report_of(const std::string &why) const {
  const Tally c = tally();
  std::string r = std::string(setup.test ? "# 自检报告\n\n- 自检会话: 候选 / 日志 / 全局记录在 agent/test 目录, 提交只查门槛不拷进正式库"
                                         : "# 停下报告\n") +
                  "\n- 停下原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Cat::Look)) + ", 查研报 " + std::to_string(c.docs()) + ", 假设 " + std::to_string(c.n(Tool::Hypothesize)) + ", 判定 " +
                  std::to_string(c.n(Tool::Judge)) + ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n" +
                  (setup.remark.empty() ? "" : "- 本会话要求: " + setup.remark + "\n") + "\n";
  std::string in, out;
  for (size_t i = 0; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (a.span.ended() && a.call.tool == Tool::SubmitFactor)
        (a.out.ok ? in : out) += "- 第 " + std::to_string(i + 1) + " 轮 " + a.call.str("name") + (a.out.ok ? "" : " — " + a.out.text) + "\n";
  r += std::string(setup.test ? "## 达门槛 (" : "## 入库 (") + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.n(Tool::SubmitFactor) - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out);
  r += "\n## 假设 (" + std::to_string(st.board.all().size()) + ")\n" + st.board.report();
  const std::vector<const Note *> filed = notes.filed(log.dir());
  for (const NoteKind k : kVals<NoteKind>) {
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

std::vector<Round> TimelineOf(const std::string &dir) { return read_json(fs::path(dir) / kSessionFile).at("timeline").get<std::vector<Round>>(); }

std::vector<Seen> SeenCalls(const Scope &sc, Tool t) {
  std::vector<Seen> out;
  for (const Saved &s : SavedSessions(sc)) {
    const std::vector<Round> rs = TimelineOf(s.dir);
    for (size_t i = 0; i < rs.size(); ++i)
      for (size_t j = 0; j < rs[i].acts.size(); ++j)
        if (const Act &a = rs[i].acts[j]; a.call.tool == t && a.span.ended())
          out.push_back({s.dir, s.stamp, s.test, {static_cast<int>(i) + 1, static_cast<int>(j) + 1}, a.call.args, a.out.text, a.out.ok});
  }
  return out;
}

std::vector<Saved> SavedSessions(const Scope &sc) {
  std::vector<Saved> out;
  for (const bool test : {false, true})
    for (const disk::Key &k : disk::list(disk::AgentChat, catalog::key(sc, test))) {
      const fs::path dir = disk::path(disk::AgentChat, k);
      if (!fs::exists(dir / kSessionFile) || !fs::exists(dir / kBriefFile))
        continue;
      Saved s{.dir = dir.string(), .stamp = k.stamp, .test = test, .brief = read_json(dir / kBriefFile)};
      s.brief.at("sc").get_to(s.sc);
      out.push_back(std::move(s));
    }
  std::sort(out.begin(), out.end(), [](const Saved &a, const Saved &b) { return a.stamp > b.stamp; });
  return out;
}

} // namespace GUI::Agent
