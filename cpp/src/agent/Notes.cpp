// Notes — 见头文件
#include "agent/Notes.hpp"
#include "agent/Config.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <utility>

namespace agent {

namespace {

namespace fs = std::filesystem;
using json = nlohmann::ordered_json;

// 字段键 → 界面 / 结果里的标签 (四个提交工具的参数键, ref 除外)
constexpr std::pair<std::string_view, const char *> kLabels[] = {
    {"title", "标题"}, {"define", "定义"}, {"mechanism", "机制"}, {"gap", "缺口"}, {"want", "想看"}, {"purpose", "用途"}, {"evidence", "证据"}, {"reach", "适用范围"}, {"use", "含义"}, {"type", "类型"}, {"detail", "详情"}};

const char *label(std::string_view key) {
  const auto it = std::find_if(std::begin(kLabels), std::end(kLabels), [&](const auto &p) { return p.first == key; });
  assert(it != std::end(kLabels) && "字段键没有标签");
  return it->second;
}

fs::path dir_of(bool test) { return test ? fs::path(kGlobalDir) / "test" : fs::path(kGlobalDir); }
fs::path file_of(bool test, NoteKind k) { return dir_of(test) / (std::string(def(k).id) + ".jsonl"); }

// 本地时间 "YYYY-MM-DD HH:MM"
std::string now() {
  const std::time_t t = std::time(nullptr);
  char b[32];
  std::strftime(b, sizeof(b), "%Y-%m-%d %H:%M", std::localtime(&t));
  return b;
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return s;
}

std::vector<std::string> words(const std::string &query) {
  std::vector<std::string> ws;
  for (size_t a = 0, b; a < query.size(); a = b + 1) {
    b = std::min(query.find(' ', a), query.size());
    if (b > a)
      ws.push_back(lower(query.substr(a, b - a)));
  }
  return ws;
}

bool matches(const Note &x, const std::vector<std::string> &ws) {
  std::string hay;
  for (const auto &[k, v] : x.fields.items())
    hay += v.get<std::string>() + '\n';
  hay = lower(hay);
  return std::all_of(ws.begin(), ws.end(), [&](const std::string &w) { return hay.find(w) != std::string::npos; });
}

NoteKind kind_of(const std::string &id) {
  for (size_t i = 0; i < kNoteKindCount; ++i)
    if (id == kNoteKinds[i].id)
      return static_cast<NoteKind>(i);
  assert(false && "没有这类记录");
  return NoteKind::Feature;
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

} // namespace

NoteKind NoteOf(Tool t) {
  for (size_t i = 0; i < kNoteKindCount; ++i)
    if (kNoteKinds[i].tool == t)
      return static_cast<NoteKind>(i);
  assert(false && "不是提交记录的工具");
  return NoteKind::Feature;
}

std::string Note::body() const {
  std::string s;
  for (const auto &[k, v] : fields.items())
    if (k != "title")
      s += std::string(label(k)) + ": " + v.get<std::string>() + "\n";
  return s;
}

void Notes::load(bool test) {
  test_ = static_cast<int>(test);
  for (size_t i = 0; i < kNoteKindCount; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    std::vector<Note> &v = notes_[i];
    v.clear();
    const fs::path p = file_of(test, k);
    if (!fs::exists(p))
      continue;
    std::ifstream f(p);
    assert(f.is_open());
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

const Note &Notes::add(const Call &c, std::string scope, std::string session, int round) {
  assert(test_ >= 0 && "先 load");
  const NoteKind k = NoteOf(c.tool);
  std::vector<Note> &v = notes_[static_cast<size_t>(k)];
  Note x{.kind = k,
         .id = def(k).prefix + std::to_string(v.size() + 1),
         .time = now(),
         .scope = std::move(scope),
         .session = std::move(session),
         .ref = c.str("ref"),
         .round = round,
         .fields = c.args};
  x.fields.erase("ref");
  assert(x.ref.empty() || find(k, x.ref));
  const json j = {{"id", x.id}, {"time", x.time}, {"scope", x.scope}, {"session", x.session}, {"round", x.round}, {"ref", x.ref}, {"fields", x.fields}};
  fs::create_directories(dir_of(test_));
  std::ofstream f(file_of(test_, k), std::ios::app);
  assert(f.is_open());
  f << j.dump() << '\n';
  v.push_back(std::move(x));
  return v.back();
}

std::string NotesText(const Notes &notes, const std::string &kind, const std::string &scope, const std::string &query, int n) {
  const std::vector<std::string> ws = words(query);
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
    for (size_t i = 0; i < kNoteKindCount; ++i) {
      const NoteKind k = static_cast<NoteKind>(i);
      const std::vector<const Note *> h = hits(k);
      s += head(k, h.size());
      for (size_t j = 0; j < std::min(h.size(), static_cast<size_t>(n)); ++j)
        s += "- " + h[j]->id + " · " + h[j]->time.substr(0, 10) + " · " + h[j]->scope + " · " + h[j]->title() + relation(notes, *h[j]) + "\n";
      s += h.empty() ? "(无)\n\n" : "\n";
    }
    return s + "要看全文就给 kind\n";
  }
  const NoteKind k = kind_of(kind);
  const std::vector<const Note *> h = hits(k);
  s += head(k, h.size());
  if (h.empty())
    return s + "(无)\n";
  for (size_t j = 0; j < std::min(h.size(), static_cast<size_t>(n)); ++j) {
    const Note &x = *h[j];
    s += "\n### " + x.id + " " + x.title() + "\n- " + x.time + " · " + x.scope + relation(notes, x) + "\n";
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
  for (size_t i = 0; i < kNoteKindCount; ++i) {
    const NoteKind k = static_cast<NoteKind>(i);
    const std::vector<Note> &v = notes.of(k);
    s += (i ? " · " : "") + std::string(def(k).cn) + " " + std::to_string(v.size());
    if (k == NoteKind::Insight)
      s += " (本作用域 " + std::to_string(std::count_if(v.begin(), v.end(), [&](const Note &x) { return x.scope == scope; })) + ")";
  }
  return s;
}

} // namespace agent
