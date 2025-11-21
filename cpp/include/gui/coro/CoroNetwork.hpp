#pragma once
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "gui/coro/CoroManager.hpp"  // Need complete type for unique_ptr member

namespace asio = boost::asio;

// Network monitoring coroutine (background ping)
// Generic async ping implementation using ASIO - never blocks GUI thread
class CoroNetwork {
public:
  struct PingTarget {
    std::string host;
    std::string name;
  };
  
  CoroNetwork() = default;
  ~CoroNetwork() = default;
  
  // Start monitoring with custom targets and interval
  void Start(CoroManager& coro_mgr, 
             const std::vector<PingTarget>& targets,
             std::chrono::seconds interval);
  
  // Stop monitoring (automatic on destruction)
  void Stop();
  
private:
  // Per-target ping coroutine (runs forever independently)
  asio::awaitable<void> PingLoop(size_t target_index);
  
  // Async ping using TCP connect to host:80
  // Returns latency in ms, or -1 on error
  asio::awaitable<int> DoPingAsync(const std::string& host);
  
  // Shared results (accessed atomically via pointer to avoid copy issues)
  std::vector<std::unique_ptr<std::atomic<int>>> results_;
  
  // Configuration
  std::vector<PingTarget> targets_;
  std::chrono::seconds interval_{5};
  
  // One coroutine per target (long-running, never destroyed)
  std::vector<std::unique_ptr<CoroutineHandle>> ping_handles_;
};

