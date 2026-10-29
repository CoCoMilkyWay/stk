#include "agent/Chat.hpp"

#include <cassert>
#include <string_view>

namespace agent {

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

void Chat::open(llm::Options o) {
  assert(!busy() && "对话在收, 先取消");
  sess_ = std::make_unique<llm::Session>(std::move(o));
  std::lock_guard<std::mutex> lock(mutex);
  turns.clear();
  live_user.clear(), live_text.clear(), live_thinking.clear();
}

void Chat::ask(std::string user) {
  assert(sess_ && !busy());
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
    {
      std::lock_guard<std::mutex> lock(mutex);
      turns.push_back(std::move(t));
      live_user.clear(), live_text.clear(), live_thinking.clear();
    }
    busy_.store(false, std::memory_order_release);
    done_.fetch_add(1, std::memory_order_release);
  }
}

} // namespace agent
