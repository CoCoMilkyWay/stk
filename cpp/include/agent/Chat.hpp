#pragma once

// =============================================================================
// Chat — 一段 LLM 对话的后台单线程 (agent 自带)
// =============================================================================
//   GUI 线程: open 开新对话 (换后端 / 模型 / 系统提示) → ask 投一条 user → worker 里 Session::send 阻塞收齐 → 整轮进 turns.
//   增量 (live_*) 与 turns UI 持 mutex 读; done() 计数变 = 有新一轮收齐 (含取消 / 失败的).
//   落盘: log_dir/system.md + 每轮 NN.user.md / NN.reply.md / NN.thinking.md + rounds.jsonl (一行一轮: 后端 / 模型 / 结局 / 用量 / 耗时)
//         + events.jsonl: 会话交互流水 (一行一事件: 毫秒时间 + 事件名 + 字段), 只由 GUI 线程经 event 写
// =============================================================================

#include "agent/Llm.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace agent {

struct Turn {
  std::string user;
  llm::Reply reply;
};

using Fields = std::vector<std::pair<std::string, std::string>>;

class Chat {
public:
  Chat();
  ~Chat();
  Chat(const Chat &) = delete;
  Chat &operator=(const Chat &) = delete;

  void open(llm::Options o, std::string log_dir); // 忙时 assert
  void ask(std::string user);                     // 须已 open 且不忙
  void cancel();
  void event(std::string_view name, const Fields &f = {}) const; // 追加一行 events.jsonl; 须已 open
  bool opened() const { return opened_; }
  bool busy() const { return busy_.load(std::memory_order_acquire); }
  uint64_t done() const { return done_.load(std::memory_order_acquire); }
  const llm::Options &options() const { return opts_; }
  const std::string &log_dir() const { return log_dir_; }

  // UI 持 mutex 读
  mutable std::mutex mutex;
  std::vector<Turn> turns;
  std::string live_user, live_text, live_thinking; // 在收的一轮

private:
  void loop();
  void log_turn(const Turn &t, int n) const;

  llm::Options opts_;
  std::string log_dir_;
  bool opened_ = false;
  std::unique_ptr<llm::Session> sess_; // 只 worker 碰 (open 在不忙时换)

  std::thread thread_;
  std::mutex qmutex_;
  std::condition_variable cv_;
  std::optional<std::string> pending_;
  bool stop_ = false;
  std::atomic<bool> busy_{false}, cancel_{false};
  std::atomic<uint64_t> done_{0};
};

} // namespace agent
