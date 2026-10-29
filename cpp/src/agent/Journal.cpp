#include "agent/Journal.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
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

// 本地时间 "YYYY-MM-DD HH:MM:SS.mmm"
std::string now() {
  const auto t = std::chrono::system_clock::now();
  const std::time_t sec = std::chrono::system_clock::to_time_t(t);
  const int ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count() % 1000);
  char b[48];
  const size_t n = std::strftime(b, sizeof(b), "%Y-%m-%d %H:%M:%S", std::localtime(&sec));
  std::snprintf(b + n, sizeof(b) - n, ".%03d", ms);
  return b;
}

} // namespace

void Journal::open(std::string dir, const llm::Options &o) {
  dir_ = std::move(dir);
  backend_ = llm::def(o.backend).name;
  model_ = o.model;
  fs::create_directories(dir_);
  write_file(fs::path(dir_) / "system.md", o.system);
}

void Journal::turn(int n, const Turn &t) const {
  assert(!dir_.empty());
  const fs::path dir(dir_);
  char stem[16];
  std::snprintf(stem, sizeof(stem), "%02d", n);
  write_file(dir / (std::string(stem) + ".user.md"), t.user);
  write_file(dir / (std::string(stem) + ".reply.md"), t.reply.text);
  if (!t.reply.thinking.empty())
    write_file(dir / (std::string(stem) + ".thinking.md"), t.reply.thinking);
  const llm::Reply &r = t.reply;
  append_line(dir / "rounds.jsonl", {{"round", n},
                                     {"time", now()},
                                     {"backend", backend_},
                                     {"model", model_},
                                     {"end", llm::kEndNames[static_cast<int>(r.end)]},
                                     {"why", r.why},
                                     {"id", r.id},
                                     {"prompt_tokens", r.usage.prompt},
                                     {"cached_tokens", r.usage.cached},
                                     {"completion_tokens", r.usage.completion},
                                     {"seconds", r.seconds}});
}

void Journal::event(std::string_view name, const nlohmann::ordered_json &fields) const {
  assert(!dir_.empty() && fields.is_object());
  nlohmann::ordered_json j;
  j["time"] = now();
  j["event"] = name;
  for (auto it = fields.begin(); it != fields.end(); ++it)
    j[it.key()] = it.value();
  append_line(fs::path(dir_) / "events.jsonl", j);
}

void Journal::report(const std::string &text) const {
  assert(!dir_.empty());
  write_file(fs::path(dir_) / "report.md", text);
}

} // namespace agent
