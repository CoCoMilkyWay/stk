#pragma once

// =============================================================================
// Chat — 一段 LLM 对话的后台单线程 (agent 自带; 不落盘, 会话日志见 agent/Journal.hpp)
// =============================================================================
//   GUI 线程: open 开新对话 (换后端 / 模型 / 系统提示) → ask 投一条 user → worker 里 Session::send 阻塞收齐 → poll 取走 (一轮一次).
//   不留历史: 收齐的轮归调用方; 在收的一轮 live 拷出 (与收齐同形). 与 agent/Bridge.hpp 的 Lab 同一套 ask / poll / live / busy / cancel.
// =============================================================================

#include "agent/Llm.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace agent {

class Chat {
public:
  Chat();
  ~Chat();
  Chat(const Chat &) = delete;
  Chat &operator=(const Chat &) = delete;

  void open(llm::Options o);  // 忙时 assert; 未取走的一轮作废
  void ask(std::string user); // 须已 open 且不忙
  void cancel();
  bool busy() const { return busy_.load(std::memory_order_acquire); }
  std::optional<llm::Reply> poll(); // 收齐的一轮 (含取消 / 失败的), 取走即空
  llm::Reply live() const;          // 在收的一轮 (增量)

private:
  void loop();

  std::unique_ptr<llm::Session> sess_; // 只 worker 碰 (open 在不忙时换)

  std::thread thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::optional<std::string> pending_;
  llm::Reply live_;
  std::optional<llm::Reply> got_;
  bool stop_ = false;
  std::atomic<bool> busy_{false}, cancel_{false};
};

} // namespace agent
