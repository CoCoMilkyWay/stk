#pragma once

// =============================================================================
// Chat — 一段 LLM 对话的后台单线程 (agent 自带; 不落盘, 会话日志见 agent/Journal.hpp)
// =============================================================================
//   GUI 线程: open 开新对话 (换后端 / 模型 / 系统提示) → ask 投一条 user → worker 里 Session::send 阻塞收齐 → poll 逐个取走 Tick.
//   worker 协议 (与 agent/Bridge.hpp 的 Lab 对仗): 交一件事 (ask ↔ eval) → poll 逐个取走 Tick, 末个 = End → live 看在收那轮 → busy / cancel
//   不留历史: 收齐的轮随 End 归调用方; 在收那轮 live 增量同步进调用方的镜像 (与收齐同形)
// =============================================================================

#include "agent/Llm.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace agent {

class Chat {
public:
  // 一轮的进展 (poll 逐个取走): 首个思考增量 / 首个正文增量 / 整轮重来 (已收的增量作废) / 收齐 (reply 有效, 含取消 / 失败的)
  enum class Step : uint8_t { Think,
                              Write,
                              Restart,
                              End };
  struct Tick {
    Step step;
    llm::Reply reply;
  };
  // 在收那轮的镜像 (调用方持有): live 只追加新增字节, 换了一轮 (gen) 才整份换
  struct Live {
    uint64_t gen = 0;
    llm::Reply reply;
  };

  Chat();
  ~Chat();
  Chat(const Chat &) = delete;
  Chat &operator=(const Chat &) = delete;

  void open(llm::Options o);                                          // 须不忙; 未取走的 Tick 作废
  void ask(std::string user);                                         // 须已 open 且不忙
  void cancel();                                                      // 在收那轮断流; 它收尾即 End (Cancelled)
  bool busy() const { return busy_.load(std::memory_order_acquire); } // ask 起, 到 End 入队
  std::optional<Tick> poll();
  void live(Live &l) const;

private:
  void loop();

  std::unique_ptr<llm::Session> sess_; // 只 worker 碰 (open 在不忙时换)

  std::thread thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::optional<std::string> pending_;
  llm::Reply live_;  // 在收那轮的增量
  uint64_t gen_ = 0; // live_ 换一轮 (ask / 重来 / 收齐) 加一
  std::deque<Tick> ticks_;
  bool stop_ = false;
  std::atomic<bool> busy_{false}, cancel_{false};
};

} // namespace agent
