// Notes — 见头文件
#include "gui/task_agent/Notes.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <sstream>
#include <string_view>
#include <utility>

namespace GUI::Agent {

namespace {

using json = nlohmann::ordered_json;

disk::Key key_of(bool test, NoteKind k) { return {.test = test, .name = def(k).id}; }

bool matches(const Note &x, const std::vector<std::string> &ws) {
  std::string hay;
  for (const auto &[k, v] : x.fields.items())
    hay += v.get<std::string>() + '\n';
  hay = text::lower(hay);
  return std::all_of(ws.begin(), ws.end(), [&](const std::string &w) { return text::has_word(hay, w); });
}

// 附议关系: 附议了哪条 / 有几条附议它
std::string relation(const Notes &notes, const Note &x) {
  std::string s;
  if (!x.ref.empty())
    s += " · 附议 " + x.ref;
  if (const int b = notes.backers(x); b > 0)
    s += " · 有 " + std::to_string(b) + " 条附议";
  return s;
}

// 研究员的处理 (没回复的待办不写): " · 研究员: 已做 (回复)"
std::string handled(const Note &x) {
  if (x.state == NoteState::Open && x.reply.empty())
    return "";
  return " · 研究员: " + std::string(def(x.state).cn) + (x.reply.empty() ? "" : " (" + x.reply + ")");
}

json line_of(const Note &x) {
  return {{"id", x.id}, {"time", x.time}, {"scope", x.scope}, {"session", x.session}, {"round", x.round}, {"ref", x.ref}, {"state", x.state}, {"reply", x.reply}, {"fields", x.fields}};
}

} // namespace

NoteKind NoteOf(Tool t) {
  for (size_t i = 0; i < kNum<NoteKind>; ++i)
    if (kNoteKinds[i].tool == t)
      return static_cast<NoteKind>(i);
  assert(false && "不是提交记录的工具");
  return NoteKind::Feature;
}

std::string Note::body() const { return FieldsText(def(kind).tool, fields, {"title"}); }

void Notes::load(bool test) {
  test_ = static_cast<int>(test);
  for (size_t i = 0; i < kNum<NoteKind>; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    std::vector<Note> &v = notes_[i];
    v.clear();
    if (!disk::exists(disk::AgentNoteFile, key_of(test, k)))
      continue;
    std::istringstream f(disk::read(disk::AgentNoteFile, key_of(test, k)));
    for (std::string line; std::getline(f, line);) {
      if (line.empty())
        continue;
      const json j = json::parse(line);
      v.push_back({.kind = k,
                   .id = j.at("id").get<std::string>(),
                   .time = j.at("time").get<std::string>(),
                   .scope = j.at("scope").get<std::string>(),
                   .session = j.at("session").get<std::string>(),
                   .ref = j.at("ref").get<std::string>(),
                   .round = j.at("round").get<int>(),
                   .state = j.at("state").get<NoteState>(),
                   .reply = j.at("reply").get<std::string>(),
                   .fields = j.at("fields")});
      assert(v.back().fields.contains("title"));
    }
  }
}

const Note *Notes::find(NoteKind k, const std::string &id) const {
  const std::vector<Note> &v = of(k);
  const auto it = std::find_if(v.begin(), v.end(), [&](const Note &x) { return x.id == id; });
  return it == v.end() ? nullptr : &*it;
}

int Notes::backers(const Note &n) const {
  const std::vector<Note> &v = of(n.kind);
  return static_cast<int>(std::count_if(v.begin(), v.end(), [&](const Note &x) { return x.ref == n.id; }));
}

int Notes::open(NoteKind k) const {
  const std::vector<Note> &v = of(k);
  return static_cast<int>(std::count_if(v.begin(), v.end(), [](const Note &x) { return x.state == NoteState::Open; }));
}

const Note &Notes::add(const Call &c, std::string scope, std::string session, int round) {
  assert(test_ >= 0 && "先 load");
  const NoteKind k = NoteOf(c.tool);
  std::vector<Note> &v = notes_[static_cast<size_t>(k)];
  int no = 0;
  for (const Note &x : v)
    no = std::max(no, std::stoi(x.id.substr(1)));
  Note x{.kind = k,
         .id = def(k).prefix + std::to_string(no + 1),
         .time = text::local("%Y-%m-%d %H:%M"),
         .scope = std::move(scope),
         .session = std::move(session),
         .ref = c.str("ref"),
         .round = round,
         .fields = c.args};
  x.fields.erase("ref");
  assert(x.ref.empty() || find(k, x.ref));
  disk::append(disk::AgentNoteFile, key_of(test_, k), line_of(x).dump());
  v.push_back(std::move(x));
  return v.back();
}

void Notes::rewrite(NoteKind k) const {
  std::string s;
  for (const Note &x : of(k))
    s += line_of(x).dump() + "\n";
  disk::write(disk::AgentNoteFile, key_of(test_, k), s);
}

void Notes::set(NoteKind k, const std::string &id, NoteState s, std::string reply) {
  assert(test_ >= 0 && "先 load");
  std::vector<Note> &v = notes_[static_cast<size_t>(k)];
  const auto it = std::find_if(v.begin(), v.end(), [&](const Note &x) { return x.id == id; });
  assert(it != v.end());
  it->state = s, it->reply = std::move(reply);
  rewrite(k);
}

std::vector<const Note *> Notes::filed(const std::string &session, int from) const {
  std::vector<const Note *> out;
  for (const std::vector<Note> &v : notes_)
    for (const Note &x : v)
      if (x.session == session && x.round >= from)
        out.push_back(&x);
  return out;
}

void Notes::drop(const std::string &session, int from) {
  assert(test_ >= 0 && "先 load");
  for (size_t i = 0; i < kNum<NoteKind>; ++i)
    if (std::erase_if(notes_[i], [&](const Note &x) { return x.session == session && x.round >= from; }) > 0)
      rewrite(static_cast<NoteKind>(i));
}

std::string NotesText(const Notes &notes, const std::string &kind, const std::string &scope, const std::string &query, int n) {
  const std::vector<std::string> ws = text::words(query);
  // 命中的, 新到旧
  const auto hits = [&](NoteKind k) {
    std::vector<const Note *> h;
    const std::vector<Note> &v = notes.of(k);
    for (auto it = v.rbegin(); it != v.rend(); ++it)
      if ((scope.empty() || it->scope == scope) && matches(*it, ws))
        h.push_back(&*it);
    return h;
  };
  const std::string filter = (scope.empty() ? std::string() : " · 只看本作用域") + (ws.empty() ? std::string() : " · query 命中");
  const auto head = [&](NoteKind k, size_t hit) {
    return "## " + std::string(def(k).cn) + " (" + std::to_string(hit) + " 条" + filter + (hit > static_cast<size_t>(n) ? ", 列最近 " + std::to_string(n) + " 条" : "") + ")\n";
  };
  std::string s;
  if (kind.empty()) {
    for (size_t i = 0; i < kNum<NoteKind>; ++i) {
      const NoteKind k = static_cast<NoteKind>(i);
      const std::vector<const Note *> h = hits(k);
      s += head(k, h.size());
      for (size_t j = 0; j < std::min(h.size(), static_cast<size_t>(n)); ++j)
        s += "- " + h[j]->id + " · " + h[j]->time.substr(0, 10) + " · " + h[j]->scope + " · " + h[j]->title() + relation(notes, *h[j]) + handled(*h[j]) + "\n";
      s += h.empty() ? "(无)\n\n" : "\n";
    }
    return s + "要看全文就给 kind\n";
  }
  const NoteKind k = IdOf<NoteKind>(kind);
  const std::vector<const Note *> h = hits(k);
  s += head(k, h.size());
  if (h.empty())
    return s + "(无)\n";
  for (size_t j = 0; j < std::min(h.size(), static_cast<size_t>(n)); ++j) {
    const Note &x = *h[j];
    s += "\n### " + x.id + " " + x.title() + "\n- " + x.time + " · " + x.scope + relation(notes, x) + handled(x) + "\n";
    const std::string b = x.body();
    for (size_t a = 0, e; a < b.size(); a = e + 1) {
      e = b.find('\n', a);
      s += "- " + b.substr(a, e - a) + "\n";
    }
  }
  return s;
}

std::string NotesBrief(const Notes &notes, const std::string &scope) {
  std::string s;
  for (size_t i = 0; i < kNum<NoteKind>; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    const std::vector<Note> &v = notes.of(k);
    s += (i ? " · " : "") + std::string(def(k).cn) + " " + std::to_string(v.size()) + " (待办 " + std::to_string(notes.open(k));
    if (k == NoteKind::Insight)
      s += ", 本作用域 " + std::to_string(std::count_if(v.begin(), v.end(), [&](const Note &x) { return x.scope == scope; }));
    s += ")";
  }
  return s;
}

} // namespace GUI::Agent
