// list_notes — 查全局记录: 输入 kind / scope / query / n → NotesRes (各类条数与标题, 或一类逐条全文) → 原文; 展示见 ui/query/ListNotes.cpp
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Query.hpp"

#include <algorithm>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

bool matches(const Note &x, const std::vector<std::string> &ws) {
  std::string hay;
  for (const auto &[k, v] : x.fields.items())
    hay += v.get<std::string>() + '\n';
  hay = text::lower(hay);
  return std::all_of(ws.begin(), ws.end(), [&](const std::string &w) { return text::has_word(hay, w); });
}

std::string relation(const Notes &notes, const Note &x) {
  std::string s;
  if (!x.ref.empty())
    s += " · 附议 " + x.ref;
  if (const int b = notes.backers(x); b > 0)
    s += " · 有 " + std::to_string(b) + " 条附议";
  return s;
}

std::string handled(const Note &x) {
  if (x.state == NoteState::Open && x.reply.empty())
    return "";
  return " · 研究员: " + std::string(def(x.state).cn) + (x.reply.empty() ? "" : " (" + x.reply + ")");
}

} // namespace

std::string NotesRes::text() const {
  const std::string filter = (here ? std::string(" · 只看本作用域") : std::string()) + (query ? std::string(" · query 命中") : std::string());
  const auto head = [&](const Group &g) {
    return "## " + std::string(def(g.kind).cn) + " (" + std::to_string(g.hit) + " 条" + filter + (g.hit > static_cast<size_t>(n) ? ", 列最近 " + std::to_string(n) + " 条" : "") + ")\n";
  };
  std::string s;
  if (kind.empty()) {
    for (const Group &g : groups) {
      s += head(g);
      for (const Item &x : g.items)
        s += "- " + x.id + " · " + x.time.substr(0, 10) + " · " + x.scope + " · " + x.title + x.relation + x.handled + "\n";
      s += g.items.empty() ? "(无)\n\n" : "\n";
    }
    return s + "要看全文就给 kind\n";
  }
  const Group &g = groups.front();
  s += head(g);
  if (g.items.empty())
    return s + "(无)\n";
  for (const Item &x : g.items) {
    s += "\n### " + x.id + " " + x.title + "\n- " + x.time + " · " + x.scope + x.relation + x.handled + "\n";
    for (const std::string &b : x.body)
      s += "- " + b + "\n";
  }
  return s;
}

ToolOut ListNotes(const Call &c, Seat &s) {
  auto res = std::make_shared<NotesRes>();
  NotesRes &R = *res;
  R.kind = c.str("kind"), R.n = c.num("n");
  const std::string scope = c.str("scope") == "here" ? s.scope : std::string();
  const std::vector<std::string> ws = text::words(c.str("query"));
  R.here = !scope.empty(), R.query = !ws.empty();
  const auto group = [&](NoteKind k) {
    NotesRes::Group g{.kind = k};
    const std::vector<Note> &v = s.notes.of(k);
    for (auto it = v.rbegin(); it != v.rend(); ++it) {
      if ((!scope.empty() && it->scope != scope) || !matches(*it, ws))
        continue;
      if (g.hit++ >= static_cast<size_t>(R.n))
        continue;
      NotesRes::Item x{it->id, it->time, it->scope, it->title(), relation(s.notes, *it), handled(*it), {}};
      const std::string b = it->body();
      for (size_t a = 0, e; a < b.size(); a = e + 1) {
        e = b.find('\n', a);
        x.body.push_back(b.substr(a, e - a));
      }
      g.items.push_back(std::move(x));
    }
    R.groups.push_back(std::move(g));
  };
  if (R.kind.empty())
    for (size_t i = 0; i < kNum<NoteKind>; ++i)
      group(static_cast<NoteKind>(i));
  else
    group(IdOf<NoteKind>(R.kind));
  return Done(std::move(res));
}

json SampleListNotes(Feed &p) {
  static const char *kKinds[] = {"", "feature", "tool", "insight", "issue"};
  return {{"kind", kKinds[p.below(5)]}, {"scope", p.coin(0.3) ? "here" : "all"}, {"query", ""}, {"n", 10 + p.below(10)}};
}

} // namespace GUI::Agent
