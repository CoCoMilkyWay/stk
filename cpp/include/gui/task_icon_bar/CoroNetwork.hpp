#pragma once
#include "gui/coro/CoroManager.hpp"
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace asio = boost::asio;

// ============================================================================
// NetworkMonitor - Singleton for network status display
// ============================================================================

class NetworkMonitor {
public:
  enum class Status : int { Unknown,
                            Good,
                            Medium,
                            Bad,
                            Error };

private:
  std::atomic<Status> status_{Status::Unknown};
  std::atomic<int> ping_ms_{-1};
  std::vector<std::unique_ptr<std::atomic<int>>> target_pings_;
  mutable std::mutex mutex_;

public:
  static NetworkMonitor &Instance();

  void Initialize(size_t num_targets);
  void SetStatus(Status status, int best_ping);
  void SetTargetPing(size_t index, int ping_ms);

  Status GetStatus() const;
  int GetPingMs() const;
  std::vector<int> GetTargetPings() const;
};

// ============================================================================
// CoroNetwork - Network monitoring coroutine
// ============================================================================

class CoroNetwork {
public:
  enum class Status : int { Unknown,
                            Good,
                            Medium,
                            Bad,
                            Error };

  struct PingTarget {
    std::string host;
    std::string name;
  };

  CoroNetwork() = default;
  ~CoroNetwork() = default;

  void Start(CoroManager &coro_mgr,
             const std::vector<PingTarget> &targets,
             std::chrono::seconds interval);
  void Stop();

  Status GetStatus() const;
  int GetPingMs() const;
  std::vector<int> GetTargetPings() const;

private:
  asio::awaitable<void> PingLoop(size_t target_index);
  asio::awaitable<int> DoPingAsync(const std::string &host);

  void SetStatus(Status status, int best_ping);
  void SetTargetPing(size_t index, int ping_ms);

  std::vector<std::unique_ptr<std::atomic<int>>> results_;
  std::atomic<Status> status_{Status::Unknown};
  std::atomic<int> ping_ms_{-1};
  mutable std::mutex mutex_;

  std::vector<PingTarget> targets_;
  std::chrono::seconds interval_{5};
  std::vector<std::unique_ptr<CoroutineHandle>> ping_handles_;
};
