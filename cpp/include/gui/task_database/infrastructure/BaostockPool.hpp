// Data source: Baostock (证券宝) - http://baostock.com
// BaoStock Connection Pool and Task Consumer
// High-performance concurrent query executor with progress reporting

#pragma once

#include "BaostockClient.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Progress Callback Type (uses CrawlerProgress from SharedTypes.hpp)
// ============================================================================

using ProgressCallback = std::function<void(const CrawlerProgress &)>;

// ============================================================================
// Task Definition
// ============================================================================

struct BaostockTask {
  using ExecutorFunc = std::function<awaitable<QueryResult>(BaostockClient &)>;
  using CallbackFunc = std::function<void(QueryResult)>;

  ExecutorFunc executor;
  CallbackFunc callback;
  std::string description;
  std::string stock_code; // For progress reporting
};

// Alias for compatibility
using Task = BaostockTask;

// ============================================================================
// Connection Pool - Manages multiple client connections
// ============================================================================

class BaostockPool {
public:
  boost::asio::io_context &io_context_;
  size_t pool_size_;
  std::vector<std::shared_ptr<BaostockClient>> clients_;

private:
  std::queue<BaostockTask> task_queue_;
  std::mutex queue_mutex_;
  bool running_;

  // Statistics
  std::atomic<size_t> completed_tasks_{0};
  std::atomic<size_t> failed_tasks_{0};
  std::atomic<size_t> active_workers_{0};
  size_t total_tasks_{0};
  std::chrono::steady_clock::time_point start_time_;

  // Progress reporting
  ProgressCallback progress_callback_;
  std::mutex progress_mutex_;
  std::string current_stock_;

public:
  BaostockPool(boost::asio::io_context &io_context, size_t pool_size = 4)
      : io_context_(io_context), pool_size_(pool_size), running_(false) {
    clients_.reserve(pool_size);
  }

  // Set progress callback for GUI updates
  void set_progress_callback(ProgressCallback callback) {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    progress_callback_ = std::move(callback);
  }

  // Initialize pool - creates and logs in all clients
  awaitable<bool> initialize() {
    for (size_t i = 0; i < pool_size_; ++i) {
      auto client = std::make_shared<BaostockClient>(io_context_);
      bool success = co_await client->login();

      if (!success) {
        co_return false;
      }

      clients_.push_back(client);
    }

    running_ = true;
    start_time_ = std::chrono::steady_clock::now();
    co_return true;
  }

  // Shutdown pool - logs out all clients
  awaitable<void> shutdown() {
    running_ = false;

    for (auto &client : clients_) {
      co_await client->logout();
    }
    clients_.clear();
  }

  // Add task to queue
  void submit_task(BaostockTask task) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    task_queue_.push(std::move(task));
    total_tasks_ = task_queue_.size() + completed_tasks_ + failed_tasks_;
  }

  // Get next task from queue (thread-safe)
  std::optional<BaostockTask> get_next_task() {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (task_queue_.empty()) {
      return std::nullopt;
    }

    BaostockTask task = std::move(task_queue_.front());
    task_queue_.pop();

    // Update current stock for progress reporting
    {
      std::lock_guard<std::mutex> plock(progress_mutex_);
      current_stock_ = task.stock_code;
    }

    return task;
  }

  // Check if there are pending tasks
  bool has_pending_tasks() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(queue_mutex_));
    return !task_queue_.empty();
  }

  // Get pool size
  size_t size() const { return clients_.size(); }

  // Get client by index
  std::shared_ptr<BaostockClient> get_client(size_t index) {
    return index < clients_.size() ? clients_[index] : nullptr;
  }

  // Update statistics
  void record_success() {
    ++completed_tasks_;
    report_progress();
  }

  void record_failure() {
    ++failed_tasks_;
    report_progress();
  }

  void increment_active_workers() {
    ++active_workers_;
    report_progress();
  }

  void decrement_active_workers() {
    --active_workers_;
    report_progress();
  }

  // Report progress to GUI
  void report_progress() {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    if (!progress_callback_)
      return;

    CrawlerProgress progress;
    progress.active_workers = active_workers_;
    progress.total_workers = pool_size_;
    progress.completed_tasks = completed_tasks_;
    progress.total_tasks = total_tasks_;
    progress.current_item = current_stock_;

    // Calculate speed and ETA
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time_);
    double elapsed_seconds = elapsed.count() / 1000.0;

    if (elapsed_seconds > 0 && completed_tasks_ > 0) {
      progress.requests_per_second = completed_tasks_ / elapsed_seconds;
      size_t remaining = total_tasks_ - completed_tasks_;
      if (progress.requests_per_second > 0) {
        progress.eta_seconds = remaining / progress.requests_per_second;
      }
    }

    progress_callback_(progress);
  }

  // Get statistics summary
  size_t get_completed_count() const { return completed_tasks_; }
  size_t get_failed_count() const { return failed_tasks_; }
  size_t get_total_count() const { return total_tasks_; }
};

// ============================================================================
// Task Consumer - Worker coroutine that processes tasks
// ============================================================================

inline awaitable<void> task_consumer(std::shared_ptr<BaostockPool> pool,
                                     size_t worker_id) {
  auto client = pool->get_client(worker_id);
  if (!client) {
    co_return;
  }

  pool->increment_active_workers();

  while (pool->has_pending_tasks()) {
    auto task_opt = pool->get_next_task();
    if (!task_opt) {
      co_await boost::asio::post(boost::asio::use_awaitable);
      continue;
    }

    BaostockTask &task = *task_opt;

    try {
      auto result = co_await task.executor(*client);

      if (result.success()) {
        pool->record_success();
      } else {
        pool->record_failure();
      }

      if (task.callback) {
        task.callback(std::move(result));
      }

    } catch (const std::exception &e) {
      pool->record_failure();
    }
  }

  pool->decrement_active_workers();
}

// ============================================================================
// Batch Query Helpers
// ============================================================================

// Batch query adjustment factors for multiple stocks
inline awaitable<std::vector<QueryResult>>
batch_query_adjust_factor(std::shared_ptr<BaostockPool> pool,
                          const std::vector<std::string> &codes,
                          const std::string &start_date = "1990-01-01",
                          const std::string &end_date = "2100-01-01") {
  std::vector<QueryResult> results;
  results.resize(codes.size());
  std::mutex results_mutex;

  for (size_t i = 0; i < codes.size(); ++i) {
    BaostockTask task;
    task.description = "adjust_factor:" + codes[i];
    task.stock_code = codes[i];
    task.executor = [code = codes[i], start_date,
                     end_date](BaostockClient &client) -> awaitable<QueryResult> {
      co_return co_await client.query_adjust_factor(code, start_date, end_date);
    };
    task.callback = [&results, &results_mutex, i](QueryResult result) {
      std::lock_guard<std::mutex> lock(results_mutex);
      results[i] = std::move(result);
    };
    pool->submit_task(std::move(task));
  }

  // Start workers in parallel
  auto pending_workers = std::make_shared<std::atomic<int>>(pool->size());
  auto io_context = co_await boost::asio::this_coro::executor;

  for (size_t i = 0; i < pool->size(); ++i) {
    boost::asio::co_spawn(
        io_context,
        [pool, i, pending_workers]() -> awaitable<void> {
          co_await task_consumer(pool, i);
          pending_workers->fetch_sub(1);
        },
        boost::asio::detached);
  }

  // Wait for all workers to complete
  while (pending_workers->load() > 0) {
    co_await boost::asio::post(boost::asio::use_awaitable);
  }

  co_return results;
}

// Batch query K-line data for multiple stocks
inline awaitable<std::vector<QueryResult>> batch_query_k_data(
    std::shared_ptr<BaostockPool> pool, const std::vector<std::string> &codes,
    const std::string &fields, const std::string &start_date,
    const std::string &end_date, const std::string &frequency = "d",
    const std::string &adjustflag = "3") {
  std::vector<QueryResult> results;
  results.resize(codes.size());
  std::mutex results_mutex;

  for (size_t i = 0; i < codes.size(); ++i) {
    BaostockTask task;
    task.description = "k_data:" + codes[i];
    task.stock_code = codes[i];
    task.executor =
        [code = codes[i], fields, start_date, end_date, frequency,
         adjustflag](BaostockClient &client) -> awaitable<QueryResult> {
      co_return co_await client.query_history_k_data_plus(
          code, fields, start_date, end_date, frequency, adjustflag);
    };
    task.callback = [&results, &results_mutex, i](QueryResult result) {
      std::lock_guard<std::mutex> lock(results_mutex);
      results[i] = std::move(result);
    };
    pool->submit_task(std::move(task));
  }

  // Start workers in parallel
  auto pending_workers = std::make_shared<std::atomic<int>>(pool->size());
  auto io_context = co_await boost::asio::this_coro::executor;

  for (size_t i = 0; i < pool->size(); ++i) {
    boost::asio::co_spawn(
        io_context,
        [pool, i, pending_workers]() -> awaitable<void> {
          co_await task_consumer(pool, i);
          pending_workers->fetch_sub(1);
        },
        boost::asio::detached);
  }

  // Wait for all workers to complete
  while (pending_workers->load() > 0) {
    co_await boost::asio::post(boost::asio::use_awaitable);
  }

  co_return results;
}

} // namespace GUI::Database
