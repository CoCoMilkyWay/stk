#pragma once
#include <memory>
#include <boost/asio/io_context.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>

namespace asio = boost::asio;

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
  template<typename Awaitable>
  std::unique_ptr<CoroutineHandle> Spawn(Awaitable&& coro) {
    auto handle = std::make_unique<CoroutineHandle>();
    asio::co_spawn(
      io_ctx_,
      std::move(coro),
      asio::bind_cancellation_slot(handle->GetSlot(), asio::detached)
    );
    return handle;
  }
  
  // Get io_context for advanced usage (e.g., creating timers)
  asio::io_context& GetIoContext();
};

