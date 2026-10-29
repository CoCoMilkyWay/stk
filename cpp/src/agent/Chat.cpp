#include "agent/Chat.hpp"
#include "nlohmann/json.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>

namespace agent {

namespace {

void write_file(const std::filesystem::path &p, const std::string &s) {
  std::ofstream f(p, std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << s;
}

} // namespace

Chat::Chat() { thread_ = std::thread(&Chat::loop, this); } // 成员全部就位后再起

Chat::~Chat() {
  {
    std::lock_guard<std::mutex> lock(qmutex_);
    stop_ = true;
  }
  cancel_.store(true, std::memory_order_relaxed);
  cv_.notify_all();
  thread_.join();
}

void Chat::open(llm::Options o, std::string log_dir) {
  assert(!busy() && "对话在收, 先取消");
  opts_ = std::move(o);
  log_dir_ = std::move(log_dir);
  sess_ = std::make_unique<llm::Session>(opts_);
  {
    std::lock_guard<std::mutex> lock(mutex);
    turns.clear();
    live_user.clear(), live_text.clear(), live_thinking.clear();
  }
  std::filesystem::create_directories(log_dir_);
  write_file(std::filesystem::path(log_dir_) / "system.md", opts_.system);
  opened_ = true;
}

void Chat::ask(std::string user) {
  assert(opened_ && !busy());
  busy_.store(true, std::memory_order_release);
  cancel_.store(false, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(mutex);
    live_user = user;
    live_text.clear(), live_thinking.clear();
  }
  {
    std::lock_guard<std::mutex> lock(qmutex_);
    pending_ = std::move(user);
  }
  cv_.notify_all();
}

void Chat::cancel() { cancel_.store(true, std::memory_order_relaxed); }

void Chat::event(std::string_view name, const Fields &f) const {
  assert(opened_);
  const auto now = std::chrono::system_clock::now();
  const std::time_t sec = std::chrono::system_clock::to_time_t(now);
  const int ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
  char ts[48];
  const size_t n = std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&sec));
  std::snprintf(ts + n, sizeof(ts) - n, ".%03d", ms);
  nlohmann::ordered_json j;
  j["time"] = ts;
  j["event"] = name;
  for (const auto &[k, v] : f)
    j[k] = v;
  std::ofstream o(std::filesystem::path(log_dir_) / "events.jsonl", std::ios::app);
  assert(o.is_open());
  o << j.dump() << '\n';
}

void Chat::loop() {
  for (;;) {
    std::string user;
    {
      std::unique_lock<std::mutex> lock(qmutex_);
      cv_.wait(lock, [&] { return stop_ || pending_.has_value(); });
      if (stop_)
        return;
      user = std::move(*pending_);
      pending_.reset();
    }
    const auto on_delta = [&](llm::Part part, std::string_view chunk) {
      std::lock_guard<std::mutex> lock(mutex);
      if (part == llm::Part::Restart)
        live_text.clear(), live_thinking.clear();
      else
        (part == llm::Part::Text ? live_text : live_thinking).append(chunk);
    };
    Turn t{user, sess_->send(user, on_delta, cancel_)};
    int n = 0;
    {
      std::lock_guard<std::mutex> lock(mutex);
      turns.push_back(t);
      n = static_cast<int>(turns.size());
      live_user.clear(), live_text.clear(), live_thinking.clear();
    }
    log_turn(t, n);
    busy_.store(false, std::memory_order_release);
    done_.fetch_add(1, std::memory_order_release);
  }
}

void Chat::log_turn(const Turn &t, int n) const {
  const std::filesystem::path dir(log_dir_);
  char stem[16];
  std::snprintf(stem, sizeof(stem), "%02d", n);
  write_file(dir / (std::string(stem) + ".user.md"), t.user);
  write_file(dir / (std::string(stem) + ".reply.md"), t.reply.text);
  if (!t.reply.thinking.empty())
    write_file(dir / (std::string(stem) + ".thinking.md"), t.reply.thinking);
  const std::time_t now = std::time(nullptr);
  char ts[32];
  std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
  nlohmann::ordered_json j;
  j["round"] = n;
  j["time"] = ts;
  j["backend"] = llm::kBackendNames[static_cast<int>(opts_.backend)];
  j["model"] = opts_.model;
  j["finish"] = t.reply.finish;
  if (!t.reply.error.empty())
    j["error"] = t.reply.error;
  j["id"] = t.reply.id;
  j["prompt_tokens"] = t.reply.usage.prompt;
  j["cached_tokens"] = t.reply.usage.cached;
  j["completion_tokens"] = t.reply.usage.completion;
  j["seconds"] = t.reply.seconds;
  std::ofstream f(dir / "rounds.jsonl", std::ios::app);
  assert(f.is_open());
  f << j.dump() << '\n';
}

} // namespace agent
