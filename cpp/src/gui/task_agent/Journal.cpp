// Journal — 见头文件
#include "gui/task_agent/Journal.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

// =============================================================================
// 时间线 JSON 契约 (读写严格成对)
// =============================================================================
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(LogMark, at, kind, line, md)

void to_json(json &j, const Act &a) {
  j = {{"tool", a.call.tool}, {"args", a.call.args}, {"span", a.span}, {"laps", a.laps}, {"out", a.out.text}, {"gist", a.out.gist}, {"title", a.out.title}, {"ok", a.out.ok}, {"fx", a.out.fx}};
  if (json r = a.out.res ? a.out.res->save() : json(); !r.is_null())
    j["res"] = std::move(r);
}
void from_json(const json &j, Act &a) {
  a = Act{.call = {j.at("tool").get<Tool>(), j.at("args")},
          .span = j.at("span").get<Span>(),
          .laps = j.at("laps").get<std::vector<Lap>>(),
          .out = {.ok = j.at("ok").get<bool>(), .text = j.at("out").get<std::string>(), .gist = j.at("gist").get<std::string>(), .title = j.at("title").get<std::string>(), .fx = j.at("fx").get<Effect>()}};
}

void to_json(json &j, const Step &sp) {
  j = {{"user", sp.user}, {"memo", sp.memo}, {"span", sp.span}, {"think", sp.think}, {"write", sp.write}, {"reply", sp.reply}, {"error", sp.error}, {"act", sp.act ? json(*sp.act) : json(nullptr)}, {"marks", sp.marks}};
}
void from_json(const json &j, Step &sp) {
  sp = Step{.user = j.at("user").get<std::string>(),
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
// 可读研究日志 log.md
// =============================================================================
namespace {

std::string quote_md(const std::string &s) {
  std::string o;
  for (size_t i = 0; i < s.size();) {
    const size_t e = std::min(s.find('\n', i), s.size());
    o += "> " + s.substr(i, e - i) + "\n";
    i = e + 1;
  }
  return o;
}

std::string fmt_step_send(Steps ss, size_t i) {
  const Step &sp = ss[i];
  const int n = static_cast<int>(i) + 1;
  std::string s = "\n---\n\n## 第 " + std::to_string(n) + " 步 · " + def(KindOf(ss, i)).cn + "\n\n";
  if (n == 1)
    s += sp.user + (sp.user.ends_with('\n') ? "\n" : "\n\n");
  if (!sp.memo.empty())
    s += "> **研究员插话**\n" + quote_md(sp.memo) + "\n";
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
    md += fmt_step_send(timeline, i) + fmt_marks(sp, LogAt::Sent);
    if (!sp.reply.ok())
      continue;
    md += ReplyLog(n, sp) + fmt_marks(sp, LogAt::Replied);
    if (sp.act)
      md += fmt_act_done(n, *sp.act) + fmt_marks(sp, LogAt::Done);
  }
  return md;
}

} // namespace

std::string ReplyLog(int n, const Step &sp) {
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

// =============================================================================
// 会话目录 Journal 与存过的会话
// =============================================================================
void Journal::open(disk::Key key, const std::string &system, const std::string &head) {
  key_ = std::move(key), dir_ = disk::path(disk::AgentChat, key_).string(), head_ = head;
  disk::write(disk::AgentChatSystem, key_, system);
}

void Journal::reopen(disk::Key key, std::string head) {
  assert(disk::exists(disk::AgentChatSession, key));
  key_ = std::move(key), dir_ = disk::path(disk::AgentChat, key_).string(), head_ = std::move(head);
}

std::string Journal::system() const { return disk::read(disk::AgentChatSystem, key_); }

void Journal::report(const std::string &text) const { disk::write(disk::AgentChatReport, key_, text); }

void Journal::save(const json &session, const json &brief, Steps timeline) const {
  disk::write(disk::AgentChatSession, key_, session.dump());
  disk::write(disk::AgentChatBrief, key_, brief.dump(2));
  disk::write(disk::AgentChatLog, key_, fmt_log(head_, timeline));
}

json SessionOf(const disk::Key &key) { return json::parse(disk::read(disk::AgentChatSession, key)); }

std::vector<Saved> SavedSessions(const Scope &sc) {
  std::vector<Saved> out;
  for (const bool test : {false, true})
    for (const disk::Key &k : disk::list(disk::AgentChat, catalog::key(sc, test))) {
      if (!disk::exists(disk::AgentChatSession, k) || !disk::exists(disk::AgentChatBrief, k))
        continue;
      Saved s{.key = k, .dir = disk::path(disk::AgentChat, k).string(), .brief = json::parse(disk::read(disk::AgentChatBrief, k))};
      s.brief.at("sc").get_to(s.sc);
      out.push_back(std::move(s));
    }
  std::sort(out.begin(), out.end(), [](const Saved &a, const Saved &b) { return a.key.stamp > b.key.stamp; });
  return out;
}

} // namespace GUI::Agent
