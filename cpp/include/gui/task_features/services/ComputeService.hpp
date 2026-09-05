// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: TS workers + CS worker + IO worker
#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <vector>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

namespace GUI::Features {

// ============================================================================
// Compute Status
// ============================================================================

enum class ComputeStatus {
  Idle,
  Running,
  Completed,
  Cancelled,
  Error
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService {
private:
  SharedData &data_;

  std::atomic<bool> cancel_flag_{false};
  std::shared_ptr<misc::ParallelProgress> progress_;
  std::vector<std::future<void>> workers_;
  ComputeStatus status_ = ComputeStatus::Idle;

  int num_workers_ = 0;
  size_t pool_slots_ = 4;
  std::chrono::steady_clock::time_point start_time_;

  std::future<void> compute_thread_; // Background compute thread

  // Feature store (allocated during compute)
  std::unique_ptr<GlobalFeatureStore> feature_store_;

public:
  ComputeService(SharedData &data);
  ~ComputeService();

  // Lifecycle (background thread, non-blocking)
  void start_compute(int num_workers, size_t pool_slots);
  void stop_compute();

  // Query
  ComputeStatus get_status() const { return status_; }
  bool is_running() const { return status_ == ComputeStatus::Running; }
  bool is_idle() const { return status_ == ComputeStatus::Idle || status_ == ComputeStatus::Completed || status_ == ComputeStatus::Cancelled; }
};

} // namespace GUI::Features
