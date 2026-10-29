#include "agent/Chat.hpp"

#include <cassert>
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
  live_ = {}, ++gen_;
  ticks_.clear();
}

void Chat::ask(std::string user) {
  assert(sess_ && !busy());
  busy_.store(true, std::memory_order_release);
  cancel_.store(false, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    live_ = {}, ++gen_;
    pending_ = std::move(user);
  }
  cv_.notify_all();
}

void Chat::cancel() { cancel_.store(true, std::memory_order_relaxed); }

std::optional<Chat::Tick> Chat::poll() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (ticks_.empty())
    return std::nullopt;
  Tick t = std::move(ticks_.front());
  ticks_.pop_front();
  return t;
}

void Chat::live(Live &l) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (l.gen != gen_) {
    l.gen = gen_, l.reply = live_;
    return;
  }
  l.reply.text.append(live_.text, l.reply.text.size());
  l.reply.thinking.append(live_.thinking, l.reply.thinking.size());
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
    const auto on_delta = [&](llm::Part part, std::string_view chunk) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (part == llm::Part::Restart) {
        live_ = {}, ++gen_;
        ticks_.push_back({Step::Restart, {}});
        return;
      }
      std::string &to = part == llm::Part::Text ? live_.text : live_.thinking;
      if (to.empty() && live_.text.empty())
        ticks_.push_back({part == llm::Part::Text ? Step::Write : Step::Think, {}});
      to.append(chunk);
    };
    llm::Reply r = sess_->send(user, on_delta, cancel_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      live_ = {}, ++gen_;
      ticks_.push_back({Step::End, std::move(r)});
    }
    busy_.store(false, std::memory_order_release);
  }
}

} // namespace agent
