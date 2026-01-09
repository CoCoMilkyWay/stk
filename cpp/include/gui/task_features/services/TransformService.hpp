// TransformService - Transform Analysis Service
//
// 设计目标:
//   1. 每个worker负责固定的asset集合 (预分配)
//   2. 高频拖动参数时能及时中断重算
//   3. 每个asset独立valid，算完一个画一个
//
// 计算流程:
//   1. UI 参数变化 → RequestCompute() → generation++
//   2. Worker 检测到 generation 变化
//   3. 中断当前计算，invalidate 所有 asset
//   4. 重新计算负责的 asset，完成一个 valid 一个
//   5. UI 实时渲染已 valid 的 asset
//
#pragma once

#include "features/backend/FeatureReader.hpp"
#include "gui/coro/CoroManager.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace asio = boost::asio;

struct SharedData;

namespace GUI::Features {

// ============================================================================
// Transform Worker (每个worker负责固定的asset集合)
// ============================================================================

class TransformWorkerPool {
public:
  explicit TransformWorkerPool(size_t num_workers);
  ~TransformWorkerPool();

  // 触发新一轮计算 (++generation, 唤醒所有worker)
  void trigger();

  // 当前generation
  uint64_t generation() const { return generation_.load(); }

  // 设置共享数据和回调
  void bind(SharedData *data,
            void (*compute_fn)(SharedData &, size_t, uint64_t),
            void (*on_all_done)(SharedData &));

  size_t num_workers() const { return workers_.size(); }

private:
  void worker_loop(size_t worker_id);

  std::vector<std::thread> workers_;
  std::atomic<uint64_t> generation_{0};
  std::atomic<bool> stop_{false};

  // 同步
  std::mutex mutex_;
  std::condition_variable cv_;
  uint64_t last_triggered_{0};

  // 回调
  SharedData *data_{nullptr};
  void (*compute_fn_)(SharedData &, size_t, uint64_t) = nullptr;
  void (*on_all_done_)(SharedData &) = nullptr;

  // 完成计数
  std::atomic<size_t> done_count_{0};
  size_t expected_count_{0};
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

  // UI requests (non-blocking) - 触发重算
  void RequestCompute();

  // Status
  bool is_running() const { return coro_running_.load(); }

private:
  // 内部方法
  void load_data(SharedData &data, int level, int feature_idx, int block_idx);
  void invalidate_all(SharedData &data);

  // 静态回调 (传给worker pool)
  static void compute_asset_static(SharedData &data, size_t asset_idx,
                                   uint64_t gen);
  static void on_all_done_static(SharedData &data);

  // 实例方法
  void compute_asset(SharedData &data, size_t asset_idx, uint64_t gen);
  void finalize(SharedData &data);

  // Features directory
  std::string features_dir_;

  // Feature reader
  FeatureReader reader_;
  FeatureReader::DayTensor day_tensor_;

  // Worker pool
  std::unique_ptr<TransformWorkerPool> pool_;

  // Coroutine state
  std::unique_ptr<CoroutineHandle> coro_;
  std::atomic<bool> coro_running_{false};
  std::atomic<bool> coro_stop_{false};

  // Request flags
  std::atomic<bool> compute_requested_{false};
  std::atomic<bool> reload_requested_{false};
};

} // namespace GUI::Features
