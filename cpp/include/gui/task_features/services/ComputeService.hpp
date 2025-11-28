// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: TS workers + CS worker + IO worker
#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <vector>

// Forward declarations
struct SharedData;
class TaskTerminal;
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
// Compute Progress
// ============================================================================

struct ComputeProgress {
  size_t total_dates = 0;
  size_t completed_dates = 0;
  size_t total_assets = 0;

  double elapsed_seconds = 0.0;
  double compute_rate = 0.0; // dates/second

  // Worker breakdown
  int num_ts_workers = 0;
  int num_cs_workers = 0;
  int num_io_workers = 0;

  double progress_percent() const {
    return total_dates > 0 ? (100.0 * completed_dates / total_dates) : 0.0;
  }
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
  size_t compute_total_dates_ = 0; // Total dates in backtest period
  std::chrono::steady_clock::time_point start_time_;

  std::future<void> compute_thread_; // Background compute thread
  
  // Feature store (allocated during compute)
  std::unique_ptr<GlobalFeatureStore> feature_store_;

public:
  ComputeService(SharedData &data);
  ~ComputeService();

  // Lifecycle (background thread, non-blocking)
  void start_compute(int num_workers);
  void stop_compute();

  // Query
  ComputeStatus get_status() const { return status_; }
  ComputeProgress get_progress() const;
  bool is_running() const { return status_ == ComputeStatus::Running; }
  bool is_idle() const { return status_ == ComputeStatus::Idle || status_ == ComputeStatus::Completed || status_ == ComputeStatus::Cancelled; }
};

} // namespace GUI::Features
