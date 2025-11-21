#pragma once
#include <chrono>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>

namespace asio = boost::asio;
using namespace std::chrono_literals;

// Coroutine utility functions for common patterns

// Run a task periodically with a fixed interval
// Returns: awaitable that runs forever (until cancelled)
template<typename Func>
asio::awaitable<void> RunTimer(std::chrono::steady_clock::duration interval, Func&& func) {
  auto executor = co_await asio::this_coro::executor;
  asio::steady_timer timer(executor);
  
  while (true) {
    timer.expires_after(interval);
    co_await timer.async_wait(asio::use_awaitable);
    
    // Execute user function
    func();
  }
}

// Run a task once after a delay
template<typename Func>
asio::awaitable<void> RunDelayed(std::chrono::steady_clock::duration delay, Func&& func) {
  auto executor = co_await asio::this_coro::executor;
  asio::steady_timer timer(executor);
  
  timer.expires_after(delay);
  co_await timer.async_wait(asio::use_awaitable);
  
  func();
}

// Run a task immediately in a coroutine context
template<typename Func>
asio::awaitable<void> RunOnce(Func&& func) {
  func();
  co_return;
}

// Run a task in a loop with condition check
template<typename CondFunc, typename Func>
asio::awaitable<void> RunWhile(
  std::chrono::steady_clock::duration interval,
  CondFunc&& condition,
  Func&& func
) {
  auto executor = co_await asio::this_coro::executor;
  asio::steady_timer timer(executor);
  
  while (condition()) {
    timer.expires_after(interval);
    co_await timer.async_wait(asio::use_awaitable);
    
    if (condition()) {
      func();
    }
  }
}

// Helper: sleep in coroutine
inline asio::awaitable<void> CoroSleep(std::chrono::steady_clock::duration duration) {
  auto executor = co_await asio::this_coro::executor;
  asio::steady_timer timer(executor);
  timer.expires_after(duration);
  co_await timer.async_wait(asio::use_awaitable);
}

