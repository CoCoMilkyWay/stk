// Notes — 见头文件
#include "gui/task_agent/Notes.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Note.hpp"

#include <algorithm>
#include <cassert>
#include <sstream>
#include <utility>

namespace GUI::Agent {

// =============================================================================
// 跨会话全局记录板 Notes
// =============================================================================
namespace {

using json = nlohmann::ordered_json;

disk::Key note_key(bool test, NoteKind k) { return {.test = test, .name = def(k).id}; }

json line_of(const Note &x) {
  return {{"id", x.id}, {"time", x.time}, {"scope", x.scope}, {"session", x.session}, {"step", x.step}, {"ref", x.ref}, {"state", x.state}, {"reply", x.reply}, {"fields", x.fields}};
}

} // namespace

NoteKind NoteOf(Tool t) {
  for (const NoteKind k : kVals<NoteKind>)
    if (def(k).tool == t)
      return k;
  assert(false && "不是提交记录的工具");
  return NoteKind::Feature;
}

std::string Note::body() const { return FieldsText(def(kind).tool, fields, {"title"}); }

void Notes::load(bool test) {
  test_ = static_cast<int>(test);
  for (const NoteKind k : kVals<NoteKind>) {
    std::vector<Note> &v = notes_[k];
    v.clear();
    if (!disk::exists(disk::AgentNoteFile, note_key(test, k)))
      continue;
    std::istringstream f(disk::read(disk::AgentNoteFile, note_key(test, k)));
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
                   .step = j.at("step").get<int>(),
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

std::string Notes::next(NoteKind k) const {
  int no = 0;
  for (const Note &x : of(k))
    no = std::max(no, std::stoi(x.id.substr(1)));
  return def(k).prefix + std::to_string(no + 1);
}

const Note &Notes::add(const Call &c, std::string scope, std::string session, int step) {
  assert(test_ >= 0 && "先 load");
  const NoteKind k = NoteOf(c.tool);
  std::vector<Note> &v = notes_[k];
  Note x{.kind = k,
         .id = next(k),
         .time = text::local("%Y-%m-%d %H:%M"),
         .scope = std::move(scope),
         .session = std::move(session),
         .ref = c.str("ref"),
         .step = step,
         .fields = c.args};
  x.fields.erase("ref");
  assert(x.ref.empty() || find(k, x.ref));
  disk::append(disk::AgentNoteFile, note_key(test_, k), line_of(x).dump());
  v.push_back(std::move(x));
  return v.back();
}

void Notes::rewrite(NoteKind k) const {
  std::string s;
  for (const Note &x : of(k))
    s += line_of(x).dump() + "\n";
  disk::write(disk::AgentNoteFile, note_key(test_, k), s);
}

void Notes::set(NoteKind k, const std::string &id, NoteState s, std::string reply) {
  assert(test_ >= 0 && "先 load");
  std::vector<Note> &v = notes_[k];
  const auto it = std::find_if(v.begin(), v.end(), [&](const Note &x) { return x.id == id; });
  assert(it != v.end());
  it->state = s, it->reply = std::move(reply);
  rewrite(k);
}

std::vector<const Note *> Notes::filed(const std::string &session, int from) const {
  std::vector<const Note *> out;
  for (const std::vector<Note> &v : notes_)
    for (const Note &x : v)
      if (x.session == session && x.step >= from)
        out.push_back(&x);
  return out;
}

void Notes::drop(const std::string &session, int from) {
  assert(test_ >= 0 && "先 load");
  for (const NoteKind k : kVals<NoteKind>)
    if (std::erase_if(notes_[k], [&](const Note &x) { return x.session == session && x.step >= from; }) > 0)
      rewrite(k);
}

// ---- 记录工具 (request_feature / request_tool / note_insight / report_issue) 的落记录与原文 ----
std::string NoteRes::text() const {
  if (!miss.empty())
    return miss;
  return "已记为 " + id + (ref.empty() ? "" : " (附议 " + ref + ")") + "; 全局可见: 研究员在界面里看, 后续会话用 list_notes 查得到";
}

std::string NoteRes::title() const { return miss.empty() ? id + " · " + fields.at("title").get<std::string>() : miss; }

ToolOut FileNote(const Call &c, Seat &s) {
  auto r = std::make_shared<NoteRes>();
  const NoteKind k = NoteOf(c.tool);
  r->kind = k, r->ref = c.str("ref"), r->fields = c.args, r->dry = s.dry;
  r->fields.erase("ref");
  if (!r->ref.empty() && !s.notes.find(k, r->ref)) {
    r->miss = "没有" + std::string(def(k).cn) + " " + r->ref + ": ref 写 list_notes 里同类已有一条的编号 (编号首字母 " + def(k).prefix + "), 新提的不写";
    return Done(std::move(r), false);
  }
  r->id = s.dry ? s.notes.next(k) : s.notes.add(c, s.scope, s.session, s.me).id;
  return Done(std::move(r));
}

std::string NotesBrief(const Notes &notes, const std::string &scope) {
  std::string s;
  bool first = true;
  for (const NoteKind k : kVals<NoteKind>) {
    const std::vector<Note> &v = notes.of(k);
    s += (first ? "" : " · ") + std::string(def(k).cn) + " " + std::to_string(v.size()) + " (待办 " + std::to_string(notes.open(k));
    first = false;
    if (k == NoteKind::Insight)
      s += ", 本作用域 " + std::to_string(std::count_if(v.begin(), v.end(), [&](const Note &x) { return x.scope == scope; }));
    s += ")";
  }
  return s;
}

} // namespace GUI::Agent
