// TransformService - Transform Analysis Service
//
// 计算流程 (两阶段):
//   phase_ts: raw → stationary → ts_normed (每个 worker 独立)
//   barrier:  等待所有 worker 完成 ts 阶段
//   phase_cs: ts_normed[all] → cs_normed + 指标 (每个 worker 读共享)
//
#pragma once

#include "features/Backend/FeatureReader.hpp"
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

  // 触发新一轮计算
  void trigger(uint64_t gen);

  // 暂停/恢复 worker（在修改共享数据前调用）
  void pause();  // 暂停并等待所有 worker 进入空闲状态
  void resume(); // 恢复 worker

  void bind(SharedData *data,
            void (*compute_fn)(SharedData &, size_t, uint64_t, bool),
            void (*on_all_done)(SharedData &));

  size_t num_workers() const { return workers_.size(); }

private:
  void worker_loop(size_t worker_id);

  std::vector<std::thread> workers_;
  std::atomic<bool> stop_{false};
  std::atomic<size_t> n_waiting_{0}; // 当前在等待状态的 worker 数量

  std::mutex mutex_;
  std::condition_variable cv_;
  uint64_t triggered_gen_{0};

  SharedData *data_{nullptr};
  void (*compute_fn_)(SharedData &, size_t, uint64_t, bool) = nullptr;
  void (*on_all_done_)(SharedData &) = nullptr;
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
  void load_block(SharedData &data, int level, int feature_idx, int block_idx);
  void invalidate_all(SharedData &data);

  // 静态回调 (after_barrier: false=TS阶段, true=CS阶段)
  static void compute_asset_static(SharedData &data, size_t asset_idx, uint64_t gen, bool after_barrier);
  static void on_all_done_static(SharedData &data);

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

  // Request flag
  std::atomic<bool> compute_requested_{false};
};

} // namespace GUI::Features
