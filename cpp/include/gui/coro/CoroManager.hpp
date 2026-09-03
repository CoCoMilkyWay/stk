#pragma once
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <memory>

namespace asio = boost::asio;

namespace Coro {

// 协作式让步: 把控制权交还 io_context 一次, 让主循环有机会渲染一帧.
//
// 协程与渲染共用主线程 (Poll() 每帧调一次), 所以协程一旦恢复就会一直占着这
// 帧直到下一个挂起点 —— 中间没有挂起点的大循环就是一帧卡死. 让步把它切成
// 批, 帧时间只由"一批"的耗时决定.
//
// delay 默认 1ms 而不是 0: post 式的零延时让步会在同一次 Poll() 里立刻被取
// 回来 (Poll 跑干队列), 等于没让.
inline asio::awaitable<void> Yield(asio::io_context &io,
                                   std::chrono::milliseconds delay = std::chrono::milliseconds(1)) {
  co_await asio::steady_timer(io, delay).async_wait(asio::use_awaitable);
}

} // namespace Coro

// RAII coroutine handle for automatic cancellation
struct CoroutineHandle {
private:
  std::shared_ptr<asio::cancellation_signal> cancel_signal_;

public:
  CoroutineHandle();
  ~CoroutineHandle();

  void Cancel();
  auto GetSlot() const { return cancel_signal_->slot(); }
};

// Coroutine manager - encapsulates all coroutine infrastructure
struct CoroManager {
private:
  asio::io_context io_ctx_;

public:
  CoroManager();
  ~CoroManager();

  // Process all ready coroutine events (non-blocking)
  // Should be called once per frame by GUI main loop
  void Poll();

  // Spawn a managed coroutine
  // Returns handle for manual lifetime control
  template <typename Awaitable>
  std::unique_ptr<CoroutineHandle> Spawn(Awaitable &&coro) {
    auto handle = std::make_unique<CoroutineHandle>();
    asio::co_spawn(
        io_ctx_,
        std::move(coro),
        asio::bind_cancellation_slot(handle->GetSlot(), asio::detached));
    return handle;
  }

  // Get io_context for advanced usage (e.g., creating timers)
  asio::io_context &GetIoContext();
};
