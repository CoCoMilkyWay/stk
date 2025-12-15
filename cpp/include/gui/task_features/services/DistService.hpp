// DistService - Distribution Analysis Service (KLL-based)
//
// Threading Model:
//   - Main thread: GUI rendering, polls status
//   - Coroutine: Manages computation lifecycle, yields to allow polling
//   - Thread pool: One thread per month for parallel KLL building
//
// Data Flow:
//   1. UI calls RequestCompute()
//   2. Coroutine dispatches build_all() to thread pool
//   3. Each worker builds one month's KLLs
//   4. Coroutine polls completion, then calls query() and build_trajectory()
//   5. UI renders results from Dist.result and Dist.trajectory
#pragma once

#include "gui/coro/CoroManager.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace asio = boost::asio;

struct SharedData;

namespace GUI::Features {

// ============================================================================
// Thread Pool (CPU count threads)
// ============================================================================

class SimpleThreadPool {
public:
  explicit SimpleThreadPool(size_t num_threads);
  ~SimpleThreadPool();

  template <typename Func> void submit(Func &&task) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      tasks_.emplace_back(std::forward<Func>(task));
    }
    cv_.notify_one();
  }

  void wait_all();
  size_t size() const { return threads_.size(); }

private:
  void worker();

  std::vector<std::thread> threads_;
  std::vector<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> stop_{false};
  std::atomic<size_t> active_{0};
};

// ============================================================================
// DistService
// ============================================================================

class DistService {
public:
  explicit DistService(const std::string &features_dir);
  ~DistService();

  // Coroutine loop (runs async)
  asio::awaitable<void> ComputeLoop(SharedData &data);

  // Lifecycle
  void StartCompute(CoroManager &coro, SharedData &data);
  void StopCompute(CoroManager &coro, SharedData &data);

  // UI requests (non-blocking)
  void RequestCompute();
  void RequestQuery();

  // Status
  bool is_running() const { return coro_running_.load(); }

private:
  // Features directory
  std::string features_dir_;

  // Thread pool (CPU count)
  std::unique_ptr<SimpleThreadPool> pool_;

  // Coroutine state
  std::unique_ptr<CoroutineHandle> coro_;
  std::atomic<bool> coro_running_{false};
  std::atomic<bool> coro_stop_{false};

  // Request flags
  std::atomic<bool> compute_requested_{false};
  std::atomic<bool> query_requested_{false};
};

} // namespace GUI::Features
