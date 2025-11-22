#include "gui/task_icon_bar/CoroNetwork.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/coro/CoroUtils.hpp"
#include <algorithm>
#include <atomic>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>

namespace {
// Thresholds for status classification
constexpr int NET_GREEN = 50;
constexpr int NET_YELLOW = 100;
} // anonymous namespace

// ============================================================================
// NetworkMonitor Implementation (Singleton)
// ============================================================================

NetworkMonitor &NetworkMonitor::Instance() {
  static NetworkMonitor instance;
  return instance;
}

void NetworkMonitor::Initialize(size_t num_targets) {
  std::lock_guard<std::mutex> lock(mutex_);
  target_pings_.clear();
  for (size_t i = 0; i < num_targets; ++i) {
    target_pings_.push_back(std::make_unique<std::atomic<int>>(-1));
  }
}

void NetworkMonitor::SetStatus(Status status, int best_ping) {
  status_.store(status);
  ping_ms_.store(best_ping);
}

void NetworkMonitor::SetTargetPing(size_t index, int ping_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (index < target_pings_.size()) {
    target_pings_[index]->store(ping_ms);
  }
}

NetworkMonitor::Status NetworkMonitor::GetStatus() const {
  return status_.load();
}

int NetworkMonitor::GetPingMs() const {
  return ping_ms_.load();
}

std::vector<int> NetworkMonitor::GetTargetPings() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<int> pings;
  for (const auto &p : target_pings_) {
    pings.push_back(p->load());
  }
  return pings;
}

// ============================================================================
// CoroNetwork Implementation
// ============================================================================

void CoroNetwork::Start(CoroManager &coro_mgr,
                        const std::vector<PingTarget> &targets,
                        std::chrono::seconds interval) {
  targets_ = targets;
  interval_ = interval;

  // Initialize results storage
  std::lock_guard<std::mutex> lock(mutex_);
  results_.clear();
  for (size_t i = 0; i < targets.size(); ++i) {
    results_.push_back(std::make_unique<std::atomic<int>>(-1));
  }

  // Spawn one long-running coroutine per target
  for (size_t i = 0; i < targets_.size(); ++i) {
    auto handle = coro_mgr.Spawn(PingLoop(i));
    ping_handles_.push_back(std::move(handle));
  }
}

void CoroNetwork::Stop() {
  for (auto &handle : ping_handles_) {
    if (handle) {
      handle->Cancel();
    }
  }
  ping_handles_.clear();
}

CoroNetwork::Status CoroNetwork::GetStatus() const {
  return status_.load();
}

int CoroNetwork::GetPingMs() const {
  return ping_ms_.load();
}

std::vector<int> CoroNetwork::GetTargetPings() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<int> pings;
  for (const auto &r : results_) {
    pings.push_back(r->load());
  }
  return pings;
}

void CoroNetwork::SetStatus(Status status, int best_ping) {
  status_.store(status);
  ping_ms_.store(best_ping);
  
  // Update NetworkMonitor singleton
  NetworkMonitor::Instance().SetStatus(static_cast<NetworkMonitor::Status>(status), best_ping);
}

void CoroNetwork::SetTargetPing(size_t index, int ping_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (index < results_.size()) {
    results_[index]->store(ping_ms);
  }
  
  // Update NetworkMonitor singleton
  NetworkMonitor::Instance().SetTargetPing(index, ping_ms);
}

asio::awaitable<void> CoroNetwork::PingLoop(size_t target_index) {
  const auto &target = targets_[target_index];

  // Each target has its own independent ping loop
  // Runs forever, never spawns new coroutines
  while (true) {
    // Execute ping for this target
    int ping = co_await DoPingAsync(target.host);

    // Update this target's result in shared storage
    SetTargetPing(target_index, ping);

    // After updating, recalculate overall status
    // (Only first target does this to avoid redundant updates)
    if (target_index == 0) {
      // Collect all current results
      auto pings = GetTargetPings();

      // Determine best ping (minimum of successful pings)
      int best_ping = -1;
      for (int p : pings) {
        if (p >= 0) {
          best_ping = (best_ping < 0) ? p : std::min(best_ping, p);
        }
      }

      // Update overall status based on thresholds
      Status status;
      if (best_ping < 0) {
        status = Status::Error;
      } else if (best_ping < NET_GREEN) {
        status = Status::Good;
      } else if (best_ping < NET_YELLOW) {
        status = Status::Medium;
      } else {
        status = Status::Bad;
      }

      SetStatus(status, best_ping);
    }

    // Wait before next ping
    co_await CoroSleep(interval_);
  }
}

asio::awaitable<int> CoroNetwork::DoPingAsync(const std::string &host) {
  using namespace std::chrono;
  namespace ip = boost::asio::ip;

  auto executor = co_await asio::this_coro::executor;
  ip::tcp::resolver resolver(executor);
  ip::tcp::socket socket(executor);

  try {
    // Measure start time
    auto start = steady_clock::now();

    // Resolve hostname to IP
    auto endpoints = co_await resolver.async_resolve(host, "80", asio::use_awaitable);

    // Try to connect (simulates ping by checking TCP reachability)
    co_await socket.async_connect(*endpoints.begin(), asio::use_awaitable);

    // Measure elapsed time
    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start);

    // Close socket
    try {
      socket.close();
    } catch (...) {
    }

    co_return static_cast<int>(elapsed.count());

  } catch (...) {
    // Connection failed - network unreachable
    co_return -1;
  }
}
