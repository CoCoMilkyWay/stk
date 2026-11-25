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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Cand, name, name_cn, expr, note)
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

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(LogMark, at, md)

void to_json(json &j, const Step &sp) {
  j = {{"kind", sp.kind}, {"user", sp.user}, {"memo", sp.memo}, {"span", sp.span}, {"think", sp.think}, {"write", sp.write}, {"reply", sp.reply}, {"error", sp.error}, {"act", sp.act ? json(*sp.act) : json(nullptr)}, {"marks", sp.marks}};
}
void from_json(const json &j, Step &sp) {
  sp = Step{.kind = j.at("kind").get<Msg>(),
            .user = j.at("user").get<std::string>(),
            .memo = j.at("memo").get<std::string>(),
            .span = j.at("span").get<Span>(),
            .think = j.at("think").get<Span>(),
            .write = j.at("write").get<Span>(),
            .reply = j.at("reply").get<llm::Reply>(),
            .error = j.at("error").get<std::string>(),
            .marks = j.at("marks").get<std::vector<LogMark>>()};
  if (const json &a = j.at("act"); !a.is_null())
    sp.act = a.get<Act>();
}

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

std::string fmt_step_send(int n, const Step &sp) {
  std::string s = "\n---\n\n## 第 " + std::to_string(n) + " 步 · " + def(sp.kind).cn + "\n\n";
  if (n == 1)
    s += sp.user + (sp.user.ends_with('\n') ? "\n" : "\n\n");
  if (!sp.memo.empty())
    s += "> **研究员插话**\n" + quote_md(sp.memo) + "\n";
  return s;
}

std::string fmt_step_reply(int n, const Step &sp) {
  if (!sp.span.ended())
    return {};
  const llm::Reply &r = sp.reply;
  std::string s = "### " + StepTag(n) + " Agent 回复 (" + text::fmt("%.1fs", r.seconds);
  if (r.usage.prompt > 0)
    s += " · 输入 " + std::to_string(r.usage.prompt) + " [缓存 " + std::to_string(r.usage.cached) + "] / 输出 " + std::to_string(r.usage.completion);
  s += ")\n\n";
  if (!r.thinking.empty())
    s += "<details>\n<summary>思考过程 (" + std::to_string(text::chars(r.thinking)) + " 字)</summary>\n\n" + r.thinking +
         (r.thinking.ends_with('\n') ? "" : "\n") + "\n</details>\n\n";
  if (!sp.act) {
    s += "> **未通过**: " + sp.error + "\n\n";
    if (!r.text.empty())
      s += "```json\n" + r.text + (r.text.ends_with('\n') ? "" : "\n") + "```\n\n";
  }
  return s;
}

std::string fmt_act_done(int n, const Act &a) {
  if (!a.span.ended())
    return {};
  std::string s = "#### [" + StepTag(n) + "] `" + a.call.text(100) + "` (" + text::fmt("%.1fs", a.span.len(a.span.t1)) + (a.out.ok ? "" : " · 没做成") + ")\n\n" + a.out.text;
  if (!s.ends_with('\n'))
    s += '\n';
  return s + "\n";
}

std::string fmt_marks(const Step &sp, LogAt at) {
  std::string s;
  for (const LogMark &m : sp.marks)
    if (m.at == at)
      s += m.md;
  return s;
}

// 整份 log.md: 每步 = 发出 · 其后的笔 · 回复 · 其后的笔 · 调用 · 其后的笔; 未发出的步有笔才写
std::string fmt_log(const std::string &head, Steps timeline) {
  std::string md = head;
  for (size_t i = 0; i < timeline.size(); ++i) {
    const int n = static_cast<int>(i) + 1;
    const Step &sp = timeline[i];
    if (!sp.span.began() && sp.marks.empty())
      continue;
    md += fmt_step_send(n, sp) + fmt_marks(sp, LogAt::Sent);
    if (!sp.reply.ok())
      continue;
    md += fmt_step_reply(n, sp) + fmt_marks(sp, LogAt::Replied);
    if (sp.act)
      md += fmt_act_done(n, *sp.act) + fmt_marks(sp, LogAt::Done);
  }
  return md;
}

} // namespace

void Journal::open(std::string dir, const std::string &system, const std::string &head) {
  dir_ = std::move(dir), head_ = head;
  fs::create_directories(dir_);
  write_file(fs::path(dir_) / "system.md", system);
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

void Journal::report(const std::string &text) const {
  assert(!dir_.empty());
  replace_file(fs::path(dir_) / "report.md", text);
}

void Journal::save(const json &session, const json &brief, Steps timeline) const {
  assert(!dir_.empty());
  replace_file(fs::path(dir_) / kSessionFile, session.dump());
  replace_file(fs::path(dir_) / kBriefFile, brief.dump(2));
  replace_file(fs::path(dir_) / kLogFile, fmt_log(head_, timeline));
}

// =============================================================================
// 主循环纯函数 (只认时间线)
// =============================================================================
namespace {

// 这一步在等研究员答 ask_human
bool asking(const Step &sp) { return sp.span.ended() && sp.act && def(sp.act->call.tool).exec == Exec::Human && sp.act->span.running(); }

// 停着时按继续会做什么
std::string hint_of(Steps timeline, bool done) {
  const Step &sp = timeline.back();
  const std::string n = std::to_string(timeline.size());
  if (!sp.span.began())
    return "发出第 " + n + " 条消息" + (done ? " (agent 已宣布挖完, 发出即让它接着挖)" : "");
  if (sp.act && !sp.act->span.ended())
    return "接着执行第 " + n + " 步的调用";
  return "发第 " + n + " 步的结果";
}

// 末尾 (待发的一条之前) 连着几步不合协议
int invalid_run(Steps timeline) {
  int n = 0;
  for (size_t i = timeline.size() - 1; i-- > 0 && !timeline[i].error.empty();)
    ++n;
  return n;
}

std::string budget_miss(const Budget &b, int d_steps, double d_secs) {
  if (b.steps > 0 && d_steps >= b.steps)
    return "本段 " + std::to_string(d_steps) + " 步, 上限 " + std::to_string(b.steps) + " 步";
  if (b.minutes > 0 && d_secs >= b.minutes * 60.0)
    return "本段 " + std::to_string(static_cast<int>(d_secs / 60.0)) + " 分钟, 上限 " + std::to_string(b.minutes) + " 分钟";
  return {};
}

} // namespace

// =============================================================================
// Mine 主循环实现
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
  told_ = -1;
}

void Mine::depart() { from_ = {sent(), now()}; }

int Mine::sent() const { return step() - static_cast<int>(!timeline.empty() && !timeline.back().span.began()); }

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
  if (held())
    close();

  setup = std::move(s), leg = std::move(l);
  bind(data);
  const Context &ctx = lab.ctx();
  assert(!ctx.leaves.empty() && !ctx.ops.empty() && "须有开放的叶与算子");
  system = std::make_shared<const std::string>(SystemMessage(ctx, setup.gate, setup.ask).text);
  disk::Key key = catalog::key(setup.sc, setup.test);
  key.stamp = disk::stamp_now();
  log.open(disk::path(disk::AgentChat, key).string(), *system, make_log_head());
  timeline.clear();
  memo.clear(), reason.clear(), report.clear();
  t0 = Clock::now(), t_end = -1.0;
  held_ = true;
  depart();
  const int tried = static_cast<int>(std::ranges::count_if(lab.results(), [](const Result &r) { return r.visible; }));
  post(setup.test ? TestMessage(setup.ask, setup.remark)
                  : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark));
  advance();
  return {};
}

void Mine::load(const SharedData &data, const std::string &dir) {
  const json j = read_json(fs::path(dir) / kSessionFile);
  j.at("setup").get_to(setup);
  j.at("leg").get_to(leg);
  leg.gpu = leg.gpu && catalog::gpu();
  log.reopen(dir, j.at("log_head").get<std::string>());
  system = std::make_shared<const std::string>(log.system());
  bind(data);
  j.at("timeline").get_to(timeline);
  assert(!timeline.empty() && "开会话即发首条");
  const std::vector<Result> &rs = lab.results();
  for (Step &sp : timeline)
    if (TrialRes *k = sp.act ? AsTrial(*sp.act) : nullptr) {
      k->gate = setup.gate;
      k->level = lab.ctx().level, k->hold = lab.ctx().hold;
      if (k->st.ok())
        if (const auto it = std::ranges::find(rs, k->st.file, &Result::file); it != rs.end())
          k->st.res = *it;
    }
  memo = j.at("memo").get<std::string>();
  reason = j.at("reason").get<std::string>();
  t_end = j.at("elapsed").get<double>();
  held_ = true;
  if (reason.empty()) { // 没停下就退出了: 在等答的 ask_human 原样接着等, 其余末步作废重来
    const bool ask = asking(timeline.back());
    reason = ask ? "上次在等你答问题时退出了" : "上次没停稳就退出了, 末步作废重来";
    if (!ask)
      return redo(step(), reason);
    mark("---\n\n**停下**: " + reason + "\n\n");
  }
  report = report_of(reason);
  log.report(report);
  save();
}

void Mine::resume(const SharedData &data, Leg l) {
  assert(phase() == Phase::Pause && idle() && !timeline.empty());
  flush();
  if (setup.remote)
    leg.gpu = l.gpu, leg.budget = l.budget;
  else
    leg = std::move(l);
  assert(catalog::ready(data, setup.sc, leg.gpu));
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1.0;
  depart();
  reason.clear();
  mark("\n> **继续 (" + text::local("%H:%M:%S") + ")** · 大脑 " + brain() + "\n");
  advance();
}

void Mine::answer() {
  assert(phase() == Phase::Choose);
  answer_act(now());
  advance();
}

// 在收的回复 / 在跑的调用 (含等研究员答的 ask_human) 作废, 停下
void Mine::cancel() {
  assert(live());
  const Step &sp = timeline.back();
  if (!sp.span.ended())
    drop("按 Cancel 作废了在收的回复");
  else if (sp.act && sp.act->span.running())
    abort_act(now());
  pause("按 Cancel");
}

void Mine::close() {
  assert(phase() == Phase::Pause && idle());
  flush();
  held_ = false;
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
                        {"memo", memo},
                        {"reason", reason},
                        {"timeline", timeline}};
  const json brief = {{"sc", setup.sc},
                      {"test", setup.test},
                      {"steps", sent()},
                      {"reason", reason},
                      {"adopted", AdoptedOf(timeline)},
                      {"cands", c.staged()},
                      {"weights", c.n(Verdict::Weights)},
                      {"brain", brain()},
                      {"remark", setup.remark},
                      {"elapsed", now()}};
  log.save(session, brief, timeline);
}

// ---- 回退 ----
Mine::Loss Mine::loss(int k) const {
  assert(phase() == Phase::Pause && k >= 1 && k <= step());
  return {.steps = step() - k + 1, .files = static_cast<int>(staged_from(k).size()), .notes = static_cast<int>(notes.filed(log.dir(), k).size())};
}

void Mine::rewind(int k) { redo(k, "回退到第 " + std::to_string(k) + " 条发出前, 继续即重发"); }

// 第 k 步起作废 (落的候选文件与提的全局记录一并删), 停在第 k 条发出前
void Mine::redo(int k, std::string why) {
  assert(phase() == Phase::Pause && idle() && k >= 1 && k <= step());
  flush();
  discard(k);
  timeline.resize(static_cast<size_t>(k));
  Step &sp = timeline.back();
  if (!sp.memo.empty())
    memo = sp.memo + (memo.empty() ? "" : "\n\n" + memo), sp.memo.clear();
  unsend();
  sp.marks.clear();
  told_ = -1;
  reason = std::move(why);
  report = report_of(reason);
  mark("---\n\n**停下**: " + reason + "\n\n");
  log.report(report);
  save();
}

// 末步退回待发 (插话与日志笔留着)
void Mine::unsend() {
  Step &sp = timeline.back();
  sp = Step{.kind = sp.kind, .user = std::move(sp.user), .memo = std::move(sp.memo), .marks = std::move(sp.marks)};
}

std::set<std::string> Mine::staged_from(int k) const { return StagedOf(Steps(timeline).subspan(static_cast<size_t>(k) - 1)); }

void Mine::discard(int k) {
  for (const std::string &f : staged_from(k))
    lab.discard(f);
  notes.drop(log.dir(), k);
}

Mine::Phase Mine::phase() const {
  if (!held_)
    return Phase::None;
  if (!reason.empty())
    return Phase::Pause;
  const Step &sp = timeline.back();
  return !sp.span.ended() ? Phase::Think : asking(sp) ? Phase::Choose
                                                      : Phase::Act;
}

std::string Mine::hint() const { return phase() != Phase::Pause ? std::string() : hint_of(timeline, Finished(timeline)); }

// =============================================================================
// 主循环与双翼轮询
// =============================================================================
void Mine::update() {
  while (std::optional<Chat::Tick> t = chat.poll())
    on_chat(std::move(*t));
  while (pump_act(now()))
    advance();
}

void Mine::on_chat(Chat::Tick t) {
  if (phase() != Phase::Think)
    return;
  Step &sp = timeline.back();
  const double now_s = now();
  switch (t.step) {
  case ChatStep::Think:
    sp.think.begin(now_s);
    break;
  case ChatStep::Write:
    sp.think.end(now_s), sp.write.begin(now_s);
    break;
  case ChatStep::Restart:
    sp.think = {}, sp.write = {};
    break;
  case ChatStep::End:
    sp.think.end(now_s), sp.write.end(now_s), sp.span.end(now_s);
    got(std::move(t.out));
    break;
  }
}

// 主循环: 推进到要等的地方 (大脑回复 / 后台调用 / 研究员答) 或停下
void Mine::advance() {
  for (;;) {
    const Step &sp = timeline.back();
    if (!sp.span.began())
      return send();
    if (!sp.span.ended())
      return;
    if (sp.act && !sp.act->span.ended()) {
      if (sp.act->span.running())
        return;
      Seat s = seat_of();
      if (!open_act(s, now()))
        return save();
      continue;
    }
    post(sp.act ? ResultMessage(*sp.act, step(), setup.test) : RetryMessage(sp.error));
    if (Finished(timeline))
      return pause("agent 宣布挖完");
    if (invalid_run(timeline) >= kMaxRetries)
      return pause("连续 " + std::to_string(kMaxRetries) + " 条回复不合协议");
    if (const std::string budget = over(); !budget.empty())
      return pause("本段预算用完 (" + budget + ")");
  }
}

// 大脑交来一条 (内置模型的回复 / 外部 agent 交的调用): 没收成即作废本条并停下; 收成的照收 (不合协议的无调用, error = 原因)
void Mine::got(llm::Reply r) {
  if (!r.ok()) {
    const std::string why = "回复失败 (" + std::string(def(r.end).id) + (r.error.empty() ? "" : ": " + r.error) + ")";
    drop(why, std::move(r));
    return pause(why);
  }
  CallCheck c = ParseCall(r.text, setup.ask);
  Step &sp = timeline.back();
  sp.span.end(now());
  sp.reply = std::move(r);
  if (c.ok())
    sp.act = Act{.call = std::move(c.call)};
  else
    sp.error = std::move(c.error);
  advance();
}

Seat Mine::seat_of() { return seat(setup.gate, log.dir(), false, leg.gpu); }

std::string Mine::brain() const {
  return setup.remote ? std::string("外部 agent") : std::string(def(leg.llm.backend).name) + " / " + leg.llm.model;
}

void Mine::reply(std::string call) {
  assert(remote() && phase() == Phase::Think);
  got({.text = std::move(call), .end = llm::End::Ok, .seconds = timeline.back().span.len(now())});
}

// 大脑这回是否新开一段: 它已连着收到上一步 (或本步, 外部 agent 重取) → 不新开 (有状态的大脑只取末条); 否则新开整份. 记下它已收到本步
bool Mine::fresh() {
  const bool f = told_ != step() - 1 && told_ != step();
  told_ = step();
  return f;
}

std::string Mine::deliver() {
  assert(remote() && phase() == Phase::Think);
  const bool f = fresh();
  const llm::Turns t = TurnsOf(timeline);
  return f ? llm::Flat(t) : t.back().text;
}

void Mine::post(Message m) { timeline.push_back({.kind = m.kind, .user = std::move(m.text)}); }

void Mine::send() {
  Step &sp = timeline.back();
  assert(!sp.span.began());
  if (!memo.empty()) {
    sp.memo += (sp.memo.empty() ? "" : "\n\n") + memo;
    memo.clear();
  }
  sp.span.begin(now());
  if (!setup.remote) {
    const bool f = fresh();
    chat.send({leg.llm, system, TurnsOf(timeline), f});
  }
  save();
}

// 本条的回复作废 (没收到 / 收到一半 / 回复失败; r = 收到的那条, 须非 Ok): 原文记进日志, 本条退回待发; 大脑那段对话不再算连着
void Mine::drop(std::string why, llm::Reply r) {
  assert(!r.ok());
  Step &sp = timeline.back();
  const double t = now();
  sp.think.end(t), sp.write.end(t), sp.span.end(t);
  sp.reply = std::move(r);
  sp.error = std::move(why);
  mark(fmt_step_reply(step(), sp));
  unsend();
  told_ = -1;
}

void Mine::mark(std::string md) {
  Step &sp = timeline.back();
  const LogAt at = !sp.reply.ok() ? LogAt::Sent : sp.act && sp.act->span.ended() ? LogAt::Done
                                                                                 : LogAt::Replied;
  sp.marks.push_back({at, std::move(md)});
}

std::string Mine::over() const { return budget_miss(leg.budget, sent() - from_.step, now() - from_.t); }

void Mine::pause(std::string why) {
  assert(live() && !why.empty());
  chat.cancel();
  lab.cancel();
  t_end = now();
  reason = std::move(why);
  report = report_of(reason);
  mark("---\n\n**停下 (" + text::local("%H:%M:%S") + ")**: " + reason + "\n\n");
  log.report(report);
  save();
}

std::string Mine::report_of(const std::string &why) const {
  const Tally c = tally();
  const bool test = setup.test;
  std::string r = std::string(test ? "# 自检报告\n\n- 自检会话: 候选 / 日志 / 全局记录在 agent/test 目录, 提交只查门槛不拷进正式库" : "# 停下报告\n") +
                  "\n- 停下原因: " + why + "\n- 步数: " + std::to_string(sent()) + "\n- 调用: " + std::to_string(c.all()) + " (观察 " +
                  std::to_string(c.n(Cat::Look)) + ", 查研报 " + std::to_string(c.docs()) + ", 回测 " + std::to_string(c.n(Tool::EvalFactor)) + ")\n" +
                  (setup.remark.empty() ? "" : "- 本会话要求: " + setup.remark + "\n") + "\n";
  std::string in, out;
  for (size_t i = 0; i < timeline.size(); ++i)
    if (const std::optional<Act> &a = timeline[i].act; a && a->span.ended() && a->call.tool == Tool::SubmitFactor)
      (a->out.ok ? in : out) += "- " + StepTag(static_cast<int>(i) + 1) + " " + a->call.str("name") + (a->out.ok ? "" : " — " + a->out.text) + "\n";
  r += std::string(test ? "## 达门槛 (" : "## 入库 (") + std::to_string(c.adopted) + ")\n\n" + (in.empty() ? "(无)\n" : in) + "\n";
  r += "## 未入库的提交 (" + std::to_string(c.n(Tool::SubmitFactor) - c.adopted) + ")\n\n" + (out.empty() ? "(无)\n" : out);
  const std::vector<const Note *> filed = notes.filed(log.dir());
  for (const NoteKind k : kVals<NoteKind>) {
    std::string s;
    int n = 0;
    for (const Note *x : filed)
      if (x->kind == k) {
        ++n;
        s += "\n### " + x->id + " " + x->title() + " (" + StepTag(x->step) + (x->ref.empty() ? "" : ", 附议 " + x->ref) + ")\n\n" + x->body();
      }
    r += "\n## " + std::string(def(k).cn) + " (" + std::to_string(n) + ")\n" + (s.empty() ? "\n(无)\n" : s);
  }
  return r;
}

std::vector<Step> TimelineOf(const std::string &dir) { return read_json(fs::path(dir) / kSessionFile).at("timeline").get<std::vector<Step>>(); }

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
