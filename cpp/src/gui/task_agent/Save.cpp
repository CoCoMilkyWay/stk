// Mine 的落盘与载入 (会话目录的 session.json / brief.json) — 见 Mine.hpp
//   session.json = 设定 + 时间线 (每轮: 消息 / 发出时的快照 / 时段 / 回复 / 各调用的参数 · 结果 · 回测或问答) + 此刻的状态 / 插话 / 待发 / 停下原因;
//   回测的统计不存, 载入时按候选文件名从候选目录重扫; 问答只存人的选择 (题目从调用参数重解析)
#include "disk/Disk.hpp"
#include "gui/task_agent/Json.hpp"
#include "gui/task_agent/Mine.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <string_view>

namespace GUI::Agent {

namespace {

using json = nlohmann::ordered_json;
namespace fs = std::filesystem;

json read_json(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f.is_open());
  return json::parse(f);
}

// 表里 id 的下标 (须在表里)
template <class Def, size_t N>
size_t index_of(const Def (&table)[N], std::string_view id) {
  for (size_t i = 0; i < N; ++i)
    if (id == table[i].id)
      return i;
  assert(false && "表里没有这个 id");
  return 0;
}

json span_j(const Span &s) { return json::array({s.t0, s.t1}); }
Span span_of(const json &j) { return {j.at(0).get<double>(), j.at(1).get<double>()}; }
json ref_j(Ref r) { return json::array({r.round, r.call}); }
Ref ref_of(const json &j) { return {j.at(0).get<int>(), j.at(1).get<int>()}; }

json snap_j(const Desk::Snap &s) {
  json hs = json::array();
  for (const Hyp &h : s.board.all())
    hs.push_back({{"id", h.id},
                  {"fields", h.fields},
                  {"at", ref_j(h.at)},
                  {"stance", def(h.stance).id},
                  {"judged", ref_j(h.judged)},
                  {"verdict", h.verdict},
                  {"evals", h.evals},
                  {"knobs", h.knobs},
                  {"pending", h.pending}});
  return {{"board", hs}, {"seen", s.seen}, {"adopted", s.adopted}, {"evaled", s.evaled}, {"done", s.done}, {"reviewed", s.reviewed}, {"asked", s.asked}, {"folded", s.folded}, {"look_from", s.look_from}};
}

Desk::Snap snap_of(const json &j) {
  Desk::Snap s;
  std::vector<Hyp> hs;
  for (const json &h : j.at("board"))
    hs.push_back({.id = h.at("id").get<std::string>(),
                  .fields = h.at("fields"),
                  .at = ref_of(h.at("at")),
                  .stance = static_cast<Stance>(index_of(kStances, h.at("stance").get<std::string>())),
                  .judged = ref_of(h.at("judged")),
                  .verdict = h.at("verdict"),
                  .evals = h.at("evals").get<int>(),
                  .knobs = h.at("knobs").get<int>(),
                  .pending = h.at("pending").get<int>()});
  s.board.assign(std::move(hs));
  s.seen = j.at("seen").get<std::set<std::string>>();
  s.adopted = j.at("adopted").get<std::set<std::string>>();
  s.evaled = j.at("evaled").get<bool>();
  s.done = j.at("done").get<bool>();
  s.reviewed = j.at("reviewed").get<bool>();
  s.asked = j.at("asked").get<bool>();
  s.folded = j.at("folded").get<size_t>();
  s.look_from = j.at("look_from").get<int>();
  return s;
}

json reply_j(const llm::Reply &r) {
  return {{"text", r.text},
          {"thinking", r.thinking},
          {"end", llm::def(r.end).id},
          {"error", r.error},
          {"usage", {{"prompt", r.usage.prompt}, {"cached", r.usage.cached}, {"completion", r.usage.completion}}},
          {"id", r.id},
          {"agent", r.agent},
          {"seconds", r.seconds}};
}

llm::Reply reply_of(const json &j) {
  const json &u = j.at("usage");
  return {.text = j.at("text").get<std::string>(),
          .thinking = j.at("thinking").get<std::string>(),
          .end = static_cast<llm::End>(index_of(llm::kEnds, j.at("end").get<std::string>())),
          .error = j.at("error").get<std::string>(),
          .usage = {u.at("prompt").get<int>(), u.at("cached").get<int>(), u.at("completion").get<int>()},
          .id = j.at("id").get<std::string>(),
          .agent = j.at("agent").get<std::string>(),
          .seconds = j.at("seconds").get<double>()};
}

json act_j(const Act &a) {
  json j = {{"tool", def(a.call.tool).name}, {"args", a.call.args}, {"span", span_j(a.span)}, {"out", a.out}, {"ok", a.ok}};
  if (const Trial *k = std::get_if<Trial>(&a.got)) {
    const Cand &c = k->st.cand;
    j["trial"] = {{"cand", {{"name", c.name}, {"name_cn", c.name_cn}, {"expr", c.expr}, {"note", c.note}}},
                  {"file", k->st.file},
                  {"error", k->st.res.error},
                  {"verdict", static_cast<int>(k->st.res.verdict)},
                  {"change", k->change},
                  {"knob", k->knob},
                  {"search", span_j(k->search)},
                  {"run", span_j(k->run)}};
  }
  if (const Asked *q = std::get_if<Asked>(&a.got)) {
    json ps = json::array();
    for (const Pick &p : q->picks)
      ps.push_back({{"on", p.on}, {"other", p.other}, {"text", p.text}});
    j["asked"] = {{"picks", ps}, {"note", q->note}};
  }
  return j;
}

// 回测的统计按文件名从候选目录取 (文件已不在 = 未评估); 没落文件的照存的原因
Act act_of(const json &j, const Lab &lab) {
  Act a{.call = {ToolOf(j.at("tool").get<std::string>()), j.at("args")}, .span = span_of(j.at("span")), .out = j.at("out").get<std::string>(), .ok = j.at("ok").get<bool>()};
  if (j.contains("trial")) {
    const json &t = j.at("trial"), &c = t.at("cand");
    Trial &k = a.got.emplace<Trial>();
    k.st.cand = {c.at("name").get<std::string>(), c.at("name_cn").get<std::string>(), c.at("expr").get<std::string>(), c.at("note").get<std::string>()};
    k.st.file = t.at("file").get<std::string>();
    k.change = t.at("change").get<std::string>();
    k.knob = t.at("knob").get<bool>();
    k.search = span_of(t.at("search")), k.run = span_of(t.at("run"));
    const std::vector<Result> &rs = lab.results();
    const auto it = std::find_if(rs.begin(), rs.end(), [&](const Result &r) { return !k.st.file.empty() && r.file == k.st.file; });
    if (it != rs.end())
      k.st.res = *it;
    else if (k.st.ok())
      k.st.res = Result{.error = "候选文件已不在", .verdict = Verdict::Pending};
    else
      k.st.res = Result{.error = t.at("error").get<std::string>(), .verdict = static_cast<Verdict>(t.at("verdict").get<int>())};
  }
  if (j.contains("asked")) {
    const json &x = j.at("asked");
    Asked &q = a.got.emplace<Asked>(QuestionsOf(a.call));
    q.picks.clear();
    for (const json &p : x.at("picks"))
      q.picks.push_back({p.at("on").get<std::vector<bool>>(), p.at("other").get<bool>(), p.at("text").get<std::string>()});
    assert(q.picks.size() == q.qs.size());
    q.note = x.at("note").get<std::string>();
  }
  return a;
}

json round_j(const Desk::Round &rd) {
  json acts = json::array();
  for (const Act &a : rd.acts)
    acts.push_back(act_j(a));
  return {{"kind", def(rd.kind).id}, {"user", rd.user}, {"before", snap_j(rd.before)}, {"span", span_j(rd.span)}, {"think", span_j(rd.think)}, {"write", span_j(rd.write)}, {"reply", reply_j(rd.reply)}, {"error", rd.error}, {"edited", rd.edited}, {"acts", acts}};
}

Desk::Round round_of(const json &j, const Lab &lab) {
  Desk::Round rd{.kind = static_cast<Msg>(index_of(kMsgs, j.at("kind").get<std::string>())),
                 .user = j.at("user").get<std::string>(),
                 .before = snap_of(j.at("before")),
                 .span = span_of(j.at("span")),
                 .think = span_of(j.at("think")),
                 .write = span_of(j.at("write")),
                 .reply = reply_of(j.at("reply")),
                 .error = j.at("error").get<std::string>(),
                 .edited = j.at("edited").get<bool>()};
  for (const json &a : j.at("acts"))
    rd.acts.push_back(act_of(a, lab));
  return rd;
}

json setup_j(const Desk::Setup &s) {
  const llm::Options &o = s.llm;
  return {{"sc", s.sc},
          {"gate", s.gate},
          {"gpu", s.gpu},
          {"ask", s.ask},
          {"test", s.test},
          {"remark", s.remark},
          {"budget", s.budget},
          {"llm", {{"backend", static_cast<int>(o.backend)}, {"model", o.model}, {"net", {{"proxy", o.net.proxy}, {"host", o.net.host}, {"port", o.net.port}}}}}};
}

Desk::Setup setup_of(const json &j) {
  Desk::Setup s;
  j.at("sc").get_to(s.sc);
  j.at("gate").get_to(s.gate);
  j.at("budget").get_to(s.budget);
  s.gpu = j.at("gpu").get<bool>();
  s.ask = j.at("ask").get<bool>();
  s.test = j.at("test").get<bool>();
  s.remark = j.at("remark").get<std::string>();
  const json &l = j.at("llm"), &n = l.at("net");
  const int b = l.at("backend").get<int>();
  assert(b >= 0 && b < llm::kBackendCount);
  s.llm.backend = static_cast<llm::Backend>(b);
  s.llm.model = l.at("model").get<std::string>();
  s.llm.net = {n.at("proxy").get<bool>(), n.at("host").get<std::string>(), n.at("port").get<int>()};
  return s;
}

} // namespace

void Mine::save() const {
  json rounds = json::array();
  for (const Round &rd : timeline)
    rounds.push_back(round_j(rd));
  const json session = {{"setup", setup_j(setup)},
                        {"elapsed", now()},
                        {"clean", phase_ == Phase::Pause || phase_ == Phase::Choose},
                        {"cur", cur_},
                        {"state", snap_j(snap())},
                        {"memo", memo},
                        {"draft", draft ? json{{"kind", def(draft->kind).id}, {"text", draft->text}} : json()},
                        {"agent", agent_},
                        {"reason", reason},
                        {"report", report},
                        {"timeline", std::move(rounds)}};
  const json brief = {{"sc", setup.sc},
                      {"test", setup.test},
                      {"rounds", round()},
                      {"live", live()},
                      {"reason", reason},
                      {"adopted", adopted},
                      {"cands", total.tally.staged()},
                      {"weights", total.tally.n(Verdict::Weights)},
                      {"hyps", board.all().size()},
                      {"backend", llm::def(setup.llm.backend).name},
                      {"model", setup.llm.model},
                      {"remark", setup.remark},
                      {"elapsed", now()}};
  log.save(session, brief);
}

// 干净的 (停着 / 等人时存的) 照存的续; 中途的 (没停稳就退了): 在走的回复作废, 末轮的执行整轮作废 (删它落的候选与提的记录), 回到它发出时
void Mine::load(const SharedData &data, const std::string &dir) {
  assert(!live() && idle());
  flush();
  log.reopen(dir);
  const json j = read_json(fs::path(dir) / kSessionFile);
  setup = setup_of(j.at("setup"));
  setup.llm.system = log.system();
  setup.gpu = setup.gpu && catalog::gpu();
  assert(catalog::ready(data, setup.sc, setup.gpu));
  lab.bind(data, setup.sc, setup.test);
  notes.load(setup.test);
  timeline.clear();
  for (const json &r : j.at("timeline"))
    timeline.push_back(round_of(r, lab));
  restore(snap_of(j.at("state")));
  cur_ = j.at("cur").get<size_t>();
  memo = j.at("memo").get<std::string>();
  draft.reset();
  if (const json &d = j.at("draft"); !d.is_null())
    draft = Message{static_cast<Msg>(index_of(kMsgs, d.at("kind").get<std::string>())), d.at("text").get<std::string>()};
  agent_ = j.at("agent").get<std::string>();
  reason = j.at("reason").get<std::string>();
  t_end = j.at("elapsed").get<double>();
  stop_ = false, retries_ = 0, stale_ = true;
  const bool clean = j.at("clean").get<bool>();
  if (!clean && !draft && !timeline.empty()) {
    Round &rd = timeline.back();
    if (!rd.span.ended()) {
      rd.think.end(t_end), rd.write.end(t_end), rd.span.end(t_end);
      rd.error = "没收齐就退出了";
      agent_.clear();
    } else if (cur_ < rd.acts.size()) {
      const int k = round();
      discard(k);
      restore(rd.before);
      if (llm::def(setup.llm.backend).local)
        folded = folds(timeline.size() - 1, folded);
      for (Act &a : rd.acts)
        a = Act{.call = a.call};
      cur_ = 0;
    }
    reason = "上次没停稳就退出了, 末轮作废重来";
  }
  retally();
  phase_ = Phase::Pause;
  log.event(Event::Load, {{"rounds", round()}, {"clean", clean}});
  report = report_of(reason);
  log.report(report);
  save();
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
