// DistService - Distribution Analysis Service
// Design:
//   - Main thread: GUI rendering
//   - Coroutine: Manages computation lifecycle
//   - Thread pool: Parallel statistics computation
//
// Lifecycle:
//   1. UI triggers computation request
//   2. Coroutine checks request flag
//   3. Spawns thread pool tasks for statistics
//   4. Updates progress atomically
//   5. GUI polls status and renders results
#pragma once

#include "shared/Dist.hpp"
#include "features/backend/FeatureReader.hpp"
#include "gui/coro/CoroManager.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace asio = boost::asio;

// Forward declarations
struct SharedData;
struct Feature;
struct Config;
struct Asset;

namespace GUI::Features {

// ============================================================================
// Simple Thread Pool for Parallel Computation
// ============================================================================

class SimpleThreadPool {
public:
  explicit SimpleThreadPool(size_t num_threads);
  ~SimpleThreadPool();

  // Submit a task to the pool
  template<typename Func>
  void submit(Func&& task) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      tasks_.emplace_back(std::forward<Func>(task));
    }
    condition_.notify_one();
  }

  // Wait for all pending tasks to complete
  void wait_all();

  // Get number of threads
  size_t size() const { return threads_.size(); }

private:
  void worker_thread();

  std::vector<std::thread> threads_;
  std::vector<std::function<void()>> tasks_;
  std::mutex queue_mutex_;
  std::condition_variable condition_;
  std::atomic<bool> stop_flag_{false};
  std::atomic<size_t> active_tasks_{0};
};

// ============================================================================
// Distribution Analysis Service
// ============================================================================

class DistService {
public:
  explicit DistService(const std::string &features_dir);
  ~DistService();

  // ========================================================================
  // Coroutine Loop
  // ========================================================================

  // Main computation loop (runs in coroutine)
  asio::awaitable<void> ComputeLoop(SharedData &data);

  // Start computation coroutine
  void StartCompute(CoroManager &coro_mgr, SharedData &data);

  // Stop computation coroutine (blocking)
  void StopCompute(CoroManager &coro_mgr, SharedData &data);

  // ========================================================================
  // UI Triggers
  // ========================================================================

  // Request full computation (non-blocking, coroutine will handle)
  void RequestCompute();

  // Request visualization cache rebuild only
  void RequestCacheRebuild();

  // Cancel current computation
  void CancelCompute();

  // ========================================================================
  // Query Status
  // ========================================================================

  bool is_running() const { return coro_running_.load(); }
  bool is_computing() const;
  float get_progress() const;
  std::string get_status_text() const;

private:
  // ========================================================================
  // Internal Computation Functions
  // ========================================================================

  // Load data from reader
  void load_data(Dist &dist, const Feature &feature, const Config &config,
                 const Asset &asset);

  // Apply time grouping
  void apply_grouping(Dist &dist);

  // Compute all statistics using thread pool
  void compute_statistics(Dist &dist);

  // Build visualization cache
  void build_cache(Dist &dist);

  // ========================================================================
  // Members
  // ========================================================================

  FeatureReader reader_;
  std::string features_dir_;
  std::unique_ptr<SimpleThreadPool> thread_pool_;

  // Coroutine control
  std::unique_ptr<CoroutineHandle> coro_;
  std::atomic<bool> coro_running_{false};
  std::atomic<bool> coro_should_stop_{false};

  // Computation requests
  std::atomic<bool> compute_requested_{false};
  std::atomic<bool> cache_rebuild_requested_{false};
};

} // namespace GUI::Features

