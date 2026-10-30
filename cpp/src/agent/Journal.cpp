#include "agent/Journal.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>

namespace agent {

namespace {

namespace fs = std::filesystem;

void write_file(const fs::path &p, const std::string &s) {
  std::ofstream f(p, std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << s;
}

void append_line(const fs::path &p, const nlohmann::ordered_json &j) {
  std::ofstream f(p, std::ios::app);
  assert(f.is_open());
  f << j.dump() << '\n';
}

} // namespace

void Journal::open(std::string dir, const std::string &system) {
  dir_ = std::move(dir);
  fs::create_directories(dir_);
  write_file(fs::path(dir_) / "system.md", system);
}

void Journal::turn(int n, const std::string &user, const llm::Reply &r) const {
  assert(!dir_.empty());
  char stem[16];
  std::snprintf(stem, sizeof(stem), "%02d", n);
  const fs::path base = fs::path(dir_) / stem;
  write_file(base.string() + ".user.md", user);
  write_file(base.string() + ".reply.md", r.text);
  if (!r.thinking.empty())
    write_file(base.string() + ".thinking.md", r.thinking);
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

void KeepToolOut(const std::string &tool, const std::string &call, const std::string &from, const std::string &out) {
  fs::create_directories(kToolOutDir);
  write_file(fs::path(kToolOutDir) / (tool + ".md"), "# " + call + "\n\n" + from + "\n\n" + out);
}

void PruneToolOut(const std::vector<std::string> &known) {
  if (!fs::is_directory(kToolOutDir))
    return;
  for (const fs::directory_entry &e : fs::directory_iterator(kToolOutDir)) {
    const fs::path &p = e.path();
    if (!e.is_regular_file() || p.extension() != ".md" || std::find(known.begin(), known.end(), p.stem().string()) == known.end())
      fs::remove_all(p);
  }
}

} // namespace agent
