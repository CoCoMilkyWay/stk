// Mine — 见头文件
#include "gui/task_agent/Mine.hpp"
#include "disk/Disk.hpp"
#include "factor/Stat/Contract.hpp" // hold_name
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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Mine::Setup, sc, gate, ask, test, remote, remark)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Mine::Leg, llm, gpu, budget)

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
  std::string s = "### R" + std::to_string(n) + " Agent 回复 (" + text::fmt("%.1fs", r.seconds);
  if (r.usage.prompt > 0)
    s += " · 输入 " + std::to_string(r.usage.prompt) + " [缓存 " + std::to_string(r.usage.cached) + "] / 输出 " + std::to_string(r.usage.completion);
  s += ")\n\n";
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
// Mine 状态机实现
// =============================================================================
double Mine::now() const { return t_end >= 0.0 ? t_end : std::chrono::duration<double>(Clock::now() - t0).count(); }

std::string Mine::make_log_head() const {
  const Context &c = lab.ctx();
  std::string h = "# 会话日志 · " + catalog::label(setup.sc) + " (近 " + std::to_string(setup.sc.years) + " 年)\n\n";
  h += "- 模式: " + std::string(setup.test ? "自检" : "正式") + " · 大脑: " + brain() + " · 评估: " + (leg.gpu ? "GPU" : "CPU") + " · 门槛: 中位 rIC ≥ " +
       text::num(setup.gate.ric) + " · 提问: " + (setup.ask ? "可" : "关") + "\n";
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
  retry_reply_.clear(), retry_error_.clear();
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
  retry_reply_.clear(), retry_error_.clear();
  ++rev_;
  t0 = Clock::now(), t_end = -1.0;
  phase_ = Phase::Think;
  depart();
  const int tried = static_cast<int>(std::ranges::count_if(lab.results(), [](const Result &r) { return r.visible; }));
  post(setup.test ? TestMessage(setup.ask, setup.remark)
                  : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark));
}

std::string Mine::try_open(const SharedData &data, Setup s, Leg l) {
  assert(!live() && idle());
  const std::vector<Choice> hs = catalog::holds(data, s.sc.level);
  if (std::none_of(hs.begin(), hs.end(), [&](const Choice &h) { return h.key == s.sc.hold; })) {
    std::string opts;
    for (const Choice &h : hs)
      opts += (opts.empty() ? "" : " / ") + h.name;
    return opts.empty() ? "该层无可搜持有期 (标签未建)"
                        : "持有期 " + factor::stat::hold_name(s.sc.hold) + " 在该层不可搜 (可选 " + opts + ")";
  }
  if (!catalog::ready(data, s.sc, l.gpu))
    return "作用域 " + catalog::label(s.sc) + " 的数据未就绪 (资产轴未扫或标签库未建)";
  if (std::string miss = catalog::open_miss(data, s.sc); !miss.empty())
    return miss;
  if (phase_ == Phase::Pause)
    close();
  open(data, std::move(s), std::move(l));
  return {};
}

void Mine::load(const SharedData &data, const std::string &dir) {
  const json j = read_json(fs::path(dir) / kSessionFile);
  j.at("setup").get_to(setup);
  j.at("leg").get_to(leg);
  leg.gpu = leg.gpu && catalog::gpu();
  log.reopen(dir, j.at("log_head").get<std::string>());
  system = log.system();
  bind(data);
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
  phase_ = Phase::Pause;
  if (!j.at("clean").get<bool>())
    return redo(round(), "上次没停稳就退出了, 末轮作废重来");
  ++rev_;
  restate();
  if (reason.empty())
    reason = "上次在等你答问题时退出了";
  report = report_of(reason);
  log.report(report);
  if (!fs::exists(fs::path(dir) / kLogFile))
    log.rewrite_log(timeline, reason);
  save();
}

void Mine::resume(const SharedData &data, Leg l) {
  assert(phase_ == Phase::Pause && idle() && !timeline.empty());
  flush();
  if (setup.remote)
    leg.gpu = l.gpu, leg.budget = l.budget;
  else
    leg = std::move(l);
  assert(catalog::ready(data, setup.sc, leg.gpu));
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1.0;
  stop_ = false, retries_ = 0;
  retry_reply_.clear(), retry_error_.clear();
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
  const bool receiving = chat.busy(); // 先取: busy 落下与 End 入队在同一把锁内, 此刻不忙则下面的 update 必收到 End
  update();                           // 已到的先收: 回复 / 调用收尾随即按 Stop 停下
  if (phase_ == Phase::Choose)
    return pause("按 Stop");
  if (phase_ == Phase::Think && !receiving) { // 没有在收的回复 (外部 agent 还没交批): 本条作废, 继续即重发
    drop("按 Stop 时本条还没收到回复, 继续即重发");
    pause("按 Stop");
  }
}

void Mine::cancel() {
  assert(live());
  Round &rd = timeline.back();
  if (phase_ == Phase::Think)
    drop("按 Cancel 作废了在收的回复");
  if (phase_ == Phase::Act && cur_ < rd.acts.size() && rd.acts[cur_].span.running()) {
    abort_act(cur_, now());
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

void Mine::rewind(int k) { redo(k, "回退到第 " + std::to_string(k) + " 条发出前, 继续即重发"); }

// 第 k 轮起作废 (落的候选文件与提的全局记录一并删), 停在第 k 条发出前
void Mine::redo(int k, std::string why) {
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
  retry_reply_.clear(), retry_error_.clear();
  ++rev_;
  reason = std::move(why);
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

Cont ContOf(Rounds timeline, size_t cur) {
  assert(!timeline.empty());
  const Round &rd = timeline.back();
  return !rd.span.began()       ? Cont::Send
         : !rd.reply.ok()       ? Cont::Resend
         : cur < rd.acts.size() ? Cont::Acts
                                : Cont::Results;
}

std::string HintOf(Rounds timeline, size_t cur, bool done) {
  const std::string n = std::to_string(timeline.size());
  switch (ContOf(timeline, cur)) {
  case Cont::Send:
    return "发出待发的第 " + n + " 条消息";
  case Cont::Resend:
    return "重发第 " + n + " 条消息 (它的回复没收成)";
  case Cont::Acts:
    return "接着执行第 " + n + " 轮第 " + std::to_string(cur + 1) + " 个调用起 (共 " + std::to_string(timeline.back().acts.size()) + " 个)";
  case Cont::Results:
    return "发第 " + n + " 轮的结果" + (done ? ", 让 agent 接着挖 (它已宣布挖完)" : "");
  }
  return {};
}

std::string BudgetMiss(const Budget &b, int d_rounds, double d_secs, int d_tokens) {
  if (b.rounds > 0 && d_rounds >= b.rounds)
    return "本段 " + std::to_string(d_rounds) + " 轮, 上限 " + std::to_string(b.rounds) + " 轮";
  if (b.minutes > 0 && d_secs >= b.minutes * 60.0)
    return "本段 " + std::to_string(static_cast<int>(d_secs / 60.0)) + " 分钟, 上限 " + std::to_string(b.minutes) + " 分钟";
  if (b.ktokens > 0 && d_tokens >= b.ktokens * 1000)
    return "本段 " + std::to_string(d_tokens / 1000) + "k token, 上限 " + std::to_string(b.ktokens) + "k";
  return {};
}

std::string ReportOf(Rounds timeline, const State &st, const Notes &notes, const std::string &dir, const std::string &why, const std::string &remark, bool test, double now) {
  const Tally c = TallyAt(st, timeline, now);
  std::string r = std::string(test ? "# 自检报告\n\n- 自检会话: 候选 / 日志 / 全局记录在 agent/test 目录, 提交只查门槛不拷进正式库"
                                   : "# 停下报告\n") +
                  "\n- 停下原因: " + why + "\n- 轮数: " + std::to_string(timeline.size()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Cat::Look)) + ", 查研报 " + std::to_string(c.docs()) + ", 假设 " + std::to_string(c.n(Tool::Hypothesize)) + ", 判定 " +
                  std::to_string(c.n(Tool::Judge)) + ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n" +
                  (remark.empty() ? "" : "- 本会话要求: " + remark + "\n") + "\n";
  std::string in, out;
  for (size_t i = 0; i < timeline.size(); ++i)
    for (const Act &a : timeline[i].acts)
      if (a.span.ended() && a.call.tool == Tool::SubmitFactor)
        (a.out.ok ? in : out) += "- 第 " + std::to_string(i + 1) + " 轮 " + a.call.str("name") + (a.out.ok ? "" : " — " + a.out.text) + "\n";
  r += std::string(test ? "## 达门槛 (" : "## 入库 (") + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.n(Tool::SubmitFactor) - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out);
  r += "\n## 假设 (" + std::to_string(st.board.all().size()) + ")\n" + st.board.report();
  const std::vector<const Note *> filed = notes.filed(dir);
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

Cont Mine::cont() const { return ContOf(timeline, cur_); }

std::string Mine::hint() const { return phase_ != Phase::Pause ? std::string() : HintOf(timeline, cur_, flags().done); }

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
  if (!r.ok()) {
    const std::string why = "回复失败 (" + std::string(def(r.end).id) + (r.error.empty() ? "" : ": " + r.error) + ")";
    drop(why, std::move(r));
    return pause(why);
  }
  std::string text = r.text;
  std::string err = take(std::move(r));
  if (err.empty() || phase_ == Phase::Pause)
    return;
  if (stop_) {
    drop("按 Stop (本条回复未通过校验, 继续即重发)");
    return pause("按 Stop");
  }
  retry_reply_ = std::move(text), retry_error_ = std::move(err);
  Round &rd = timeline.back();
  rd.span.t1 = -1.0, rd.think = {}, rd.write = {};
  chat.send({leg.llm, system_ptr, TurnsOf(timeline, retry_reply_, retry_error_)});
}

// 大脑交来一批 (内置模型的回复 / 外部 agent 的 calls): 合协议即入本轮并执行 (返回空); 不合返回原因, 连续 kMaxRetries 批不合即作废本条并停下
std::string Mine::take(llm::Reply r) {
  BatchCheck got = VetBatch(r.text, setup.ask);
  if (!got.ok()) {
    log.append_log("> **本批未通过校验 (" + std::to_string(retries_ + 1) + "/" + std::to_string(kMaxRetries) + ")**: " + got.error + "\n\n");
    if (++retries_ >= kMaxRetries) {
      r.end = llm::End::Error, r.error = got.error;
      drop(got.error, std::move(r));
      pause("连续 " + std::to_string(kMaxRetries) + " 批不合协议");
    }
    return got.error;
  }
  retries_ = 0;
  retry_reply_.clear(), retry_error_.clear();
  Round &rd = timeline.back();
  rd.span.end(now());
  rd.reply = std::move(r);
  for (Call &c : got.calls)
    rd.acts.push_back({.call = std::move(c)});
  CloseReply(st, rd);
  log.append_log(fmt_round_reply(round(), rd));
  cur_ = 0;
  save();
  step();
  return {};
}

Seat Mine::mine_seat() { return seat(cur_, setup.gate, log.dir(), false, leg.gpu); }

std::string Mine::brain() const {
  return setup.remote ? std::string("外部 agent") : std::string(def(leg.llm.backend).name) + " / " + leg.llm.model;
}

std::string Mine::reply(std::string calls) {
  assert(remote() && waiting());
  const std::string err = take({.text = std::move(calls), .end = llm::End::Ok, .seconds = timeline.back().span.len(now())});
  return err.empty() ? err : RetryText(err);
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
  post(RoundMessage(rd.acts, round(), st.board, setup.test));
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
  if (!setup.remote)
    chat.send({leg.llm, system_ptr, TurnsOf(timeline)});
  save();
}

// 本条的回复作废 (没收到 / 收到一半 / 失败 / 连续不合协议; r = 收到的那条, 须非 Ok): 记为没收成, 继续即重发
void Mine::drop(std::string why, llm::Reply r) {
  assert(!r.ok());
  Round &rd = timeline.back();
  const double t = now();
  rd.think.end(t), rd.write.end(t), rd.span.end(t);
  rd.reply = std::move(r);
  rd.error = std::move(why);
  CloseReply(st, rd);
  log.append_log(fmt_round_reply(round(), rd));
}

std::string Mine::over() const { return BudgetMiss(leg.budget, round() - from_.round, now() - from_.t, tokens(tally().usage) - from_.tokens); }

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
  return ReportOf(timeline, st, notes, log.dir(), why, setup.remark, setup.test, now());
}

std::vector<Round> TimelineOf(const std::string &dir) { return read_json(fs::path(dir) / kSessionFile).at("timeline").get<std::vector<Round>>(); }

std::vector<Seen> SeenCalls(const Scope &sc, Tool t) {
  std::vector<Seen> out;
  for (const Saved &s : SavedSessions(sc)) {
    const std::vector<Round> rs = TimelineOf(s.dir);
    if (IsAgentTool(t)) {
      for (size_t i = 0; i < rs.size(); ++i)
        for (size_t j = 0; j < rs[i].acts.size(); ++j)
          if (const Act &a = rs[i].acts[j]; a.call.tool == t && a.span.ended())
            out.push_back({s.dir, s.stamp, s.test, {static_cast<int>(i) + 1, static_cast<int>(j) + 1}, a.call.args, a.out.text, a.out.ok});
      continue;
    }
    for (size_t i = 0; i < rs.size(); ++i) {
      const Round &rd = rs[i];
      if (!rd.span.began())
        continue;
      const int n = static_cast<int>(i) + 1;
      const Rounds prefix(rs.data(), i + 1);
      if (t == Tool::BuildContext) {
        out.push_back({s.dir, s.stamp, s.test, {n, 0}, {{"kind", def(rd.kind).id}, {"view", "full"}, {"ask", "yes"}, {"memo", rd.memo}, {"remark", ""}}, llm::Flat(TurnsOf(prefix)), rd.error.empty()});
      } else if (t == Tool::CheckBatch) {
        if (rd.reply.text.empty())
          continue;
        out.push_back({s.dir, s.stamp, s.test, {n, 1}, {{"reply", rd.reply.text}, {"ask", "yes"}}, rd.error.empty() ? "通过 (" + std::to_string(rd.acts.size()) + " 个调用)" : RetryText(rd.error), rd.error.empty()});
      } else if (t == Tool::StepState) {
        out.push_back({s.dir, s.stamp, s.test, {n, 0}, {{"action", "status"}, {"round", n}, {"cur", -1}, {"max_rounds", 0}, {"max_minutes", 0}, {"max_ktokens", 0}, {"why", ""}}, HintOf(prefix, rd.acts.size(), false), rd.error.empty()});
      } else if (t == Tool::ProbeBrain) {
        out.push_back({s.dir, s.stamp, s.test, {n, 1}, {{"backend", ""}, {"model", ""}, {"ask", "yes"}, {"memo", rd.memo}, {"prompt", ""}}, rd.reply.text, rd.reply.ok() && rd.error.empty()});
      }
    }
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
