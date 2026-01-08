// TransformService - Transform Analysis Service
//
// Threading Model (对仗 DistService/TimeSeriesService):
//   - Main thread: GUI rendering, polls status
//   - Coroutine: Manages computation lifecycle, yields to allow polling
//   - Thread pool: Parallel processing per-asset
//
// Data Flow:
//   1. UI参数变化 → RequestCompute()
//   2. Coroutine检测request → 分发任务到线程池
//   3. 每个worker处理一批asset: 平稳化 → 归一化 → ADF/KPSS/FFT
//   4. 聚合结果，更新横截面PDF
//   5. UI轮询状态，实时渲染
//
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
// Thread Pool (复用 DistService 模式)
// ============================================================================

class TransformThreadPool {
public:
  explicit TransformThreadPool(size_t num_threads);
  ~TransformThreadPool();

  template <typename Func>
  void submit(Func &&task) {
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
// TransformService
// ============================================================================

class TransformService {
public:
  explicit TransformService(const std::string &features_dir);
  ~TransformService();

  // Coroutine loop (runs async)
  asio::awaitable<void> ComputeLoop(SharedData &data);

  // Lifecycle
  void StartCompute(CoroManager &coro, SharedData &data);
  void StopCompute(CoroManager &coro, SharedData &data);

  // UI requests (non-blocking)
  void RequestCompute();

  // Status
  bool is_running() const { return coro_running_.load(); }

private:
  // 内部计算方法
  void do_compute(SharedData &data);
  void process_asset(SharedData &data, size_t asset_idx);
  void finalize(SharedData &data);

  // Features directory
  std::string features_dir_;

  // Thread pool (CPU count)
  std::unique_ptr<TransformThreadPool> pool_;

  // Coroutine state
  std::unique_ptr<CoroutineHandle> coro_;
  std::atomic<bool> coro_running_{false};
  std::atomic<bool> coro_stop_{false};

  // Request flags
  std::atomic<bool> compute_requested_{false};
};

} // namespace GUI::Features
