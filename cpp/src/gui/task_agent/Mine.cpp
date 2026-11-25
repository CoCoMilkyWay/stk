// Mine — 见头文件
#include "gui/task_agent/Mine.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

namespace GUI::Agent {

using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Mine::Setup, sc, gate, ask, test, remote, remark)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Mine::Leg, llm, gpu, budget)

// =============================================================================
// 主循环纯函数 (只认时间线)
// =============================================================================
namespace {

// 这一步在等研究员答 ask_human
bool asking(const Step &sp) { return sp.span.ended() && sp.act && def(sp.act->call.tool).form == Form::Human && sp.act->span.running(); }

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

// 本段预算还剩多少 (空 = 不限)
std::string budget_left(const Budget &b, int d_steps, double d_secs) {
  std::string s;
  if (b.steps > 0)
    s = std::to_string(std::max(0, b.steps - d_steps)) + " 步";
  if (b.minutes > 0)
    s += (s.empty() ? "" : " · ") + std::to_string(std::max(0, static_cast<int>(b.minutes - d_secs / 60.0))) + " 分钟";
  return s;
}

} // namespace

// =============================================================================
// Mine 主循环实现
// =============================================================================
double Mine::now() const { return t_end >= 0.0 ? t_end : std::chrono::duration<double>(Clock::now() - t0).count(); }

std::string Mine::make_log_head() const {
  const Context &c = desk.ctx();
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
  desk.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  told_ = -1;
}

void Mine::depart() { from_ = {sent(), now()}; }

int Mine::sent() const { return step() - static_cast<int>(!timeline.empty() && !timeline.back().span.began()); }

std::string Mine::try_open(const SharedData &data, Setup s, Leg l) {
  assert(!live() && idle());
  if (std::string miss = catalog::hold_miss(data, s.sc); !miss.empty())
    return miss;
  if (!catalog::ready(data, s.sc, l.gpu))
    return "作用域 " + catalog::label(s.sc) + " 的数据未就绪 (资产轴未扫或标签库未建)";
  if (std::string miss = catalog::open_miss(data, s.sc); !miss.empty())
    return miss;
  if (held())
    close();

  setup = std::move(s), leg = std::move(l);
  bind(data);
  const Context &ctx = desk.ctx();
  assert(!ctx.leaves.empty() && !ctx.ops.empty() && "须有开放的叶与算子");
  system = std::make_shared<const std::string>(SystemMessage(ctx, setup.gate, setup.ask));
  disk::Key key = catalog::key(setup.sc, setup.test);
  key.stamp = disk::stamp_now();
  log.open(std::move(key), *system, make_log_head());
  timeline.clear();
  memo.clear(), reason.clear(), report.clear();
  t0 = Clock::now(), t_end = -1.0;
  held_ = true;
  depart();
  const int tried = static_cast<int>(std::ranges::count_if(desk.results(), [](const Result &r) { return r.visible; }));
  post(setup.test ? TestMessage(setup.ask, setup.remark)
                  : FirstMessage(ctx, tried, NotesBrief(notes, catalog::label(setup.sc)), setup.ask, setup.remark, budget_left(leg.budget, 0, 0.0)));
  advance();
  return {};
}

void Mine::load(const SharedData &data, const disk::Key &key) {
  const json j = SessionOf(key);
  j.at("setup").get_to(setup);
  j.at("leg").get_to(leg);
  leg.gpu = leg.gpu && catalog::gpu();
  log.reopen(key, j.at("log_head").get<std::string>());
  system = std::make_shared<const std::string>(log.system());
  bind(data);
  j.at("timeline").get_to(timeline);
  assert(!timeline.empty() && "开会话即发首条");
  Seat s = seat_of();
  restore(j.at("timeline"), s);
  memo = j.at("memo").get<std::string>();
  reason = j.at("reason").get<std::string>();
  t_end = j.at("elapsed").get<double>();
  held_ = true;
  if (reason.empty()) { // 没停下就退出了: 在等答的 ask_human 原样接着等, 其余末步作废重来
    const bool ask = asking(timeline.back());
    reason = ask ? "上次在等你答问题时退出了" : "上次没停稳就退出了, 末步作废重来";
    if (!ask)
      return redo(step(), reason);
    mark(Beat::Pause, reason, "---\n\n**停下**: " + reason + "\n\n");
  }
  report = report_of(reason);
  log.report(report);
  save();
}

void Mine::resume(const SharedData &data, Leg l) {
  assert(phase() == Phase::Pause && idle() && !timeline.empty());
  flush();
  leg = std::move(l);
  if (setup.remote)
    leg.llm = {};
  assert(catalog::ready(data, setup.sc, leg.gpu));
  t0 = Clock::now() - std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(t_end)), t_end = -1.0;
  depart();
  reason.clear();
  mark(Beat::Resume, brain(), "\n> **继续 (" + text::local("%H:%M:%S") + ")** · 大脑 " + brain() + "\n");
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
  mark(Beat::Pause, reason, "---\n\n**停下**: " + reason + "\n\n");
  log.report(report);
  save();
}

// 末步退回待发 (插话与日志笔留着)
void Mine::unsend() {
  Step &sp = timeline.back();
  sp = Step{.user = std::move(sp.user), .memo = std::move(sp.memo), .marks = std::move(sp.marks)};
}

std::set<std::string> Mine::staged_from(int k) const { return StagedOf(Steps(timeline).subspan(static_cast<size_t>(k) - 1)); }

void Mine::discard(int k) {
  for (const std::string &f : staged_from(k))
    desk.discard(f);
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
  while (std::optional<Chat::Tick> t = mind([](auto &b) { return b.poll(); }))
    heard(std::move(*t));
  while (pump_act(now()))
    advance();
}

// 大脑的一拍 (两形同一种 Tick): 思考 / 正文开始 / 重来 / 收齐
void Mine::heard(Chat::Tick t) {
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
    post(sp.act ? ResultMessage(*sp.act, step(), setup.test, budget_left(leg.budget, sent() - from_.step, now() - from_.t)) : RetryMessage(sp.error));
    if (Finished(timeline))
      return pause("agent 宣布挖完");
    if (invalid_run(timeline) >= kMaxRetries)
      return pause("连续 " + std::to_string(kMaxRetries) + " 条回复不合协议");
    if (const std::string budget = over(); !budget.empty())
      return pause("本段预算用完 (" + budget + ")");
  }
}

// 大脑交来一条 (内置模型的回复 / 外部 agent 交的调用, 同一种 Reply): 没收成即作废本条并停下; 收成的照收 (不合协议的无调用, error = 原因)
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

// 大脑这回是否新开一段: 它已连着收到上一步 → 不新开 (有状态的大脑只取末条); 否则新开整份. 记下它已收到本步
bool Mine::fresh() {
  const bool f = told_ != step() - 1;
  told_ = step();
  return f;
}

void Mine::post(std::string user) { timeline.push_back({.user = std::move(user)}); }

void Mine::send() {
  Step &sp = timeline.back();
  assert(!sp.span.began());
  if (!memo.empty()) {
    sp.memo += (sp.memo.empty() ? "" : "\n\n") + memo;
    memo.clear();
  }
  sp.span.begin(now());
  const bool f = fresh();
  mind([&](auto &b) { b.send({leg.llm, system, TurnsOf(timeline), f}); });
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
  mark(Beat::Drop, sp.error, ReplyLog(step(), sp));
  unsend();
  told_ = -1;
}

void Mine::mark(Beat k, std::string line, std::string md) {
  Step &sp = timeline.back();
  const LogAt at = !sp.reply.ok() ? LogAt::Sent : sp.act && sp.act->span.ended() ? LogAt::Done
                                                                                 : LogAt::Replied;
  sp.marks.push_back({at, k, std::move(line), std::move(md)});
}

std::string Mine::over() const { return budget_miss(leg.budget, sent() - from_.step, now() - from_.t); }

void Mine::pause(std::string why) {
  assert(live() && !why.empty());
  mind([](auto &b) { b.cancel(); });
  lab.cancel();
  t_end = now();
  reason = std::move(why);
  report = report_of(reason);
  mark(Beat::Pause, reason, "---\n\n**停下 (" + text::local("%H:%M:%S") + ")**: " + reason + "\n\n");
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

} // namespace GUI::Agent
