// Mine 的落盘与载入 (会话目录的 session.json / brief.json) — 见 Mine.hpp
//   session.json = 设定 + 时间线 (每轮: 消息 / 随附的插话 / 时段 (未开 = 待发) / 回复 / 各调用的参数 · 结果 · 回测 / 问答的选择 / 列出的叶)
//                  + 此刻在做的调用 / 排队的插话 / 停下原因; 写 (to_json) 与读 (from_json) 成对, 字段清单只写一遍, 枚举一律写 id (task_agent/Enum)
//   不存: 上下文与会话状态 (照时间线重算); 回测的统计 (载入时按候选文件名从候选目录重扫); 问答的题目 (从调用参数重解析)
#include "disk/Disk.hpp"
#include "gui/task_agent/Mine.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace llm {
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Usage, prompt, cached, completion)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Reply, text, thinking, end, error, usage, id, seconds)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Net, proxy, host, port)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Options, backend, net, model)
} // namespace llm

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Span, t0, t1)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Cand, name, name_cn, expr, note)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Pick, on, other, text)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Desk::Setup, sc, gate, ask, test, remark)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Desk::Leg, llm, gpu, budget) // system 不存: 载入取会话目录的 system.md

// 回测: 候选 / 文件 / 改动 / 两段时段 + 没落文件的原因 (落了文件的定论载入时重扫)
void to_json(json &j, const Trial &k) {
  j = {{"cand", k.st.cand}, {"file", k.st.file}, {"error", k.st.res.error}, {"change", k.change}, {"knob", k.knob}, {"search", k.search}, {"run", k.run}};
}
void from_json(const json &j, Trial &k) {
  j.at("cand").get_to(k.st.cand);
  j.at("file").get_to(k.st.file);
  j.at("change").get_to(k.change);
  j.at("knob").get_to(k.knob);
  j.at("search").get_to(k.search);
  j.at("run").get_to(k.run);
  k.st.res = k.st.ok() ? Result{.error = "候选文件已不在", .verdict = Verdict::Pending} : Result{.error = j.at("error").get<std::string>(), .verdict = Verdict::Unstaged};
}

// 调用: 工具名 / 参数 / 时段 / 结果 + 各工具落进状态的那份
void to_json(json &j, const Act &a) {
  j = {{"tool", def(a.call.tool).name}, {"args", a.call.args}, {"span", a.span}, {"out", a.out}, {"ok", a.ok}};
  if (const Trial *k = std::get_if<Trial>(&a.got))
    j["trial"] = *k;
  if (const Asked *q = std::get_if<Asked>(&a.got))
    j["asked"] = {{"picks", q->picks}, {"note", q->note}};
  if (const Shown *x = std::get_if<Shown>(&a.got))
    j["shown"] = x->leaves;
  if (const Seen *x = std::get_if<Seen>(&a.got))
    j["seen"] = x->gist;
}
void from_json(const json &j, Act &a) {
  a = Act{.call = {ToolOf(j.at("tool").get<std::string>()), j.at("args")},
          .span = j.at("span").get<Span>(),
          .out = j.at("out").get<std::string>(),
          .ok = j.at("ok").get<bool>()};
  if (j.contains("trial"))
    a.got = j.at("trial").get<Trial>();
  if (j.contains("asked")) {
    Asked &q = a.got.emplace<Asked>(QuestionsOf(a.call));
    j.at("asked").at("picks").get_to(q.picks);
    j.at("asked").at("note").get_to(q.note);
    assert(q.picks.size() == q.qs.size());
  }
  if (j.contains("shown"))
    a.got = Shown{j.at("shown").get<std::vector<std::string>>()};
  if (j.contains("seen"))
    a.got = Seen{j.at("seen").get<std::string>()};
}

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Round, kind, user, memo, span, think, write, reply, error, acts)

namespace {

namespace fs = std::filesystem;

json read_json(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f.is_open());
  return json::parse(f);
}

} // namespace

void Mine::save() const {
  const Tally c = tally();
  const json session = {{"setup", setup},
                        {"leg", leg},
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
                      {"backend", def(leg.llm.backend).name},
                      {"model", leg.llm.model},
                      {"remark", setup.remark},
                      {"elapsed", now()}};
  log.save(session, brief);
}

// 干净的 (停着 / 等人时存的) 照存的续; 中途的 (没停稳就退了): 在收的回复作废 (末条待发), 或末轮的执行整轮作废 (删它落的候选与提的记录); 状态照时间线重算
void Mine::load(const SharedData &data, const std::string &dir) {
  log.reopen(dir);
  const json j = read_json(fs::path(dir) / kSessionFile);
  j.at("setup").get_to(setup);
  j.at("leg").get_to(leg);
  leg.gpu = leg.gpu && catalog::gpu();
  system = log.system();
  bind(data);
  j.at("timeline").get_to(timeline);
  assert(!timeline.empty() && "开会话即发首条");
  const std::vector<Result> &rs = lab.results();
  for (Round &rd : timeline)
    for (Act &a : rd.acts)
      if (Trial *k = std::get_if<Trial>(&a.got); k && k->st.ok())
        if (const auto it = std::ranges::find(rs, k->st.file, &Result::file); it != rs.end())
          k->st.res = *it;
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
