#include "gui/task_agent/Journal.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace GUI::Agent {

namespace {

namespace fs = std::filesystem;

void write_file(const fs::path &p, const std::string &s) {
  std::ofstream f(p, std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << s;
}

// <名>.tmp 写完再换名 (崩溃不留半截)
void replace_file(const fs::path &p, const std::string &s) {
  const fs::path tmp = p.string() + ".tmp";
  write_file(tmp, s);
  fs::rename(tmp, p);
}

void append_line(const fs::path &p, const nlohmann::ordered_json &j) {
  std::ofstream f(p, std::ios::app);
  assert(f.is_open());
  f << j.dump() << '\n';
}

std::string stem_of(int n) {
  char stem[16];
  std::snprintf(stem, sizeof(stem), "%02d", n);
  return stem;
}

} // namespace

void Journal::open(std::string dir, const std::string &system) {
  dir_ = std::move(dir);
  fs::create_directories(dir_);
  write_file(fs::path(dir_) / "system.md", system);
}

void Journal::reopen(std::string dir) {
  assert(fs::exists(fs::path(dir) / kSessionFile));
  dir_ = std::move(dir);
}

std::string Journal::system() const {
  std::ifstream f(fs::path(dir_) / "system.md", std::ios::binary);
  assert(f.is_open());
  std::ostringstream s;
  s << f.rdbuf();
  return s.str();
}

void Journal::context(int n, const std::string &text) const {
  assert(!dir_.empty());
  write_file((fs::path(dir_) / stem_of(n)).string() + ".context.md", text);
}

void Journal::turn(int n, const std::string &user, const llm::Reply &r) const {
  assert(!dir_.empty());
  const fs::path base = fs::path(dir_) / stem_of(n);
  write_file(base.string() + ".user.md", user);
  write_file(base.string() + ".reply.md", r.text);
  if (!r.thinking.empty())
    write_file(base.string() + ".thinking.md", r.thinking);
  else
    fs::remove(base.string() + ".thinking.md");
}

void Journal::drop(int from, int to) const {
  assert(!dir_.empty());
  for (int n = from; n <= to; ++n)
    for (const char *ext : {".context.md", ".user.md", ".reply.md", ".thinking.md"})
      fs::remove((fs::path(dir_) / stem_of(n)).string() + ext);
}

void Journal::event(Event e, const nlohmann::ordered_json &fields) const {
  assert(!dir_.empty() && fields.is_object());
  nlohmann::ordered_json j;
  j["time"] = text::local("%Y-%m-%d %H:%M:%S", true);
  j["event"] = def(e).id;
  for (auto it = fields.begin(); it != fields.end(); ++it)
    j[it.key()] = it.value();
  append_line(fs::path(dir_) / "events.jsonl", j);
}

void Journal::report(const std::string &text) const {
  assert(!dir_.empty());
  write_file(fs::path(dir_) / "report.md", text);
}

void Journal::save(const nlohmann::ordered_json &session, const nlohmann::ordered_json &brief) const {
  assert(!dir_.empty());
  replace_file(fs::path(dir_) / kSessionFile, session.dump());
  replace_file(fs::path(dir_) / kBriefFile, brief.dump(2));
}

void KeepToolOut(bool test, const std::string &tool, const std::string &call, const std::string &from, const std::string &out) {
  disk::write(disk::RunAgentToolFile, {.test = test, .name = tool}, "# " + call + "\n\n" + from + "\n\n" + out);
}

void PruneToolOut(bool test, const std::vector<std::string> &known) {
  for (const disk::Key &k : disk::list(disk::RunAgentToolFile, {.test = test}))
    if (std::find(known.begin(), known.end(), k.name) == known.end())
      disk::remove(disk::RunAgentToolFile, k);
}

} // namespace GUI::Agent
