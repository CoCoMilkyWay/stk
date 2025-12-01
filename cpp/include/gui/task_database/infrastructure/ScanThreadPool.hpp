// Scan Thread Pool - Thread pool for heavy database scanning tasks
// High-performance compute engine: 1000+ days * 1000+ assets
#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Scan Task - Unit of work for thread pool
// ============================================================================

using ScanTask = std::function<void()>;

// ============================================================================
// Scan Thread Pool - Compute engine for database scanning
// ============================================================================

class ScanThreadPool {
private:
  std::vector<std::thread> workers_;
  std::queue<ScanTask> task_queue_;
  std::mutex queue_mutex_;
  std::condition_variable cv_;
  std::atomic<bool> shutdown_{false};
  std::atomic<size_t> active_tasks_{0};
  std::atomic<size_t> completed_tasks_{0};

public:
  explicit ScanThreadPool(size_t num_threads) {
    assert(num_threads > 0);
    workers_.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i) {
      workers_.emplace_back([this]() {
        while (true) {
          ScanTask task;

          {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this]() {
              return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
              return;
            }

            if (!task_queue_.empty()) {
              task = std::move(task_queue_.front());
              task_queue_.pop();
              active_tasks_.fetch_add(1);
            }
          }

          if (task) {
            task();
            active_tasks_.fetch_sub(1);
            completed_tasks_.fetch_add(1);
          }
        }
      });
    }
  }

  ~ScanThreadPool() {
    shutdown();
  }

  // Disable copy and move
  ScanThreadPool(const ScanThreadPool &) = delete;
  ScanThreadPool &operator=(const ScanThreadPool &) = delete;
  ScanThreadPool(ScanThreadPool &&) = delete;
  ScanThreadPool &operator=(ScanThreadPool &&) = delete;

  // Submit task (returns future for result synchronization)
  template <typename Func>
  auto submit(Func &&func) -> std::future<decltype(func())> {
    using ReturnType = decltype(func());
    auto task_ptr = std::make_shared<std::packaged_task<ReturnType()>>(std::forward<Func>(func));
    auto future = task_ptr->get_future();

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      assert(!shutdown_.load());
      task_queue_.emplace([task_ptr]() { (*task_ptr)(); });
    }

    cv_.notify_one();
    return future;
  }

  // Query state
  size_t get_pending_tasks() const {
    return task_queue_.size();
  }

  size_t get_active_tasks() const {
    return active_tasks_.load();
  }

  size_t get_completed_tasks() const {
    return completed_tasks_.load();
  }

  size_t get_num_workers() const {
    return workers_.size();
  }

  // Wait for all tasks to complete
  void wait_all() {
    while (true) {
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (task_queue_.empty() && active_tasks_.load() == 0) {
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Shutdown pool
  void shutdown() {
    if (!shutdown_.exchange(true)) {
      cv_.notify_all();
      for (auto &worker : workers_) {
        if (worker.joinable()) {
          worker.join();
        }
      }
    }
  }

  // Reset statistics (for reuse)
  void reset_stats() {
    completed_tasks_.store(0);
  }
};

} // namespace GUI::Database
