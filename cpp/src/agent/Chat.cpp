#include "agent/Chat.hpp"

#include <cassert>
#include <chrono>
#include <string_view>
#include <utility>

namespace agent {

Chat::Chat() { thread_ = std::thread(&Chat::loop, this); } // 成员全部就位后再起

Chat::~Chat() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cancel_.store(true, std::memory_order_relaxed);
  cv_.notify_all();
  thread_.join();
}

void Chat::open(llm::Options o) {
  assert(!busy() && "对话在收, 先取消");
  sess_ = std::make_unique<llm::Session>(std::move(o));
  std::lock_guard<std::mutex> lock(mutex_);
  live_ = {};
  got_.reset();
}

void Chat::ask(std::string user) {
  assert(sess_ && !busy());
  busy_.store(true, std::memory_order_release);
  cancel_.store(false, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    live_ = {};
    pending_ = std::move(user);
  }
  cv_.notify_all();
}

void Chat::cancel() { cancel_.store(true, std::memory_order_relaxed); }

std::optional<llm::Reply> Chat::poll() {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::exchange(got_, std::nullopt);
}

llm::Reply Chat::live() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return live_;
}

void Chat::loop() {
  for (;;) {
    std::string user;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&] { return stop_ || pending_.has_value(); });
      if (stop_)
        return;
      user = std::move(*pending_);
      pending_.reset();
    }
    const auto t0 = std::chrono::steady_clock::now();
    const auto on_delta = [&](llm::Part part, std::string_view chunk) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (part == llm::Part::Restart) {
        live_ = {};
        return;
      }
      if (part == llm::Part::Text && live_.think_s < 0.0)
        live_.think_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
      (part == llm::Part::Text ? live_.text : live_.thinking).append(chunk);
    };
    llm::Reply r = sess_->send(user, on_delta, cancel_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      r.think_s = live_.think_s < 0.0 ? r.seconds : live_.think_s;
      live_ = {};
      got_ = std::move(r);
    }
    busy_.store(false, std::memory_order_release);
  }
}

} // namespace agent
