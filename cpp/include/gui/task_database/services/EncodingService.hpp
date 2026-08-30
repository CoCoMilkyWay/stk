// Encoding Service - Manages L2 binary database encoding from CSV archives
#pragma once

#include "misc/file_check.hpp"
#include "misc/progress_parallel.hpp"
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <vector>

// Forward declarations
struct SharedData;
struct TaskTerminal;

namespace GUI::Database {

// ============================================================================
// Encoding Status
// ============================================================================

enum class EncodingStatus {
  Idle,

  // Encoding phases
  Running,

  // Final states
  Completed,
  Cancelled,
  Error
};

// ============================================================================
// Encoding Progress (real-time stats)
// ============================================================================

struct EncodingProgress {
  size_t total_assets = 0;
  size_t completed_assets = 0;
  size_t total_dates = 0;
  size_t encoded_dates = 0;
  size_t total_orders = 0;
  float elapsed_seconds = 0.0;
  float encoding_rate = 0.0; // Assets per second
};

// ============================================================================
// Encoding Service
// ============================================================================

class EncodingService {
private:
  SharedData &data_;
  TaskTerminal *terminal_;

  std::atomic<bool> cancel_flag_{false};
  std::shared_ptr<misc::ParallelProgress> progress_;
  std::vector<std::future<void>> workers_;
  EncodingStatus status_ = EncodingStatus::Idle;

  int num_workers_ = 0;
  bool skip_existing_ = true;
  std::chrono::steady_clock::time_point start_time_;

  FileCheck::FileCheckResult file_check_result_; // Cache file check result

  std::future<void> encoding_thread_;   // Background encoding thread
  std::future<void> file_check_thread_; // Background file check thread
  std::atomic<bool> file_check_running_{false};

  std::function<void()> scan_callback_; // Callback to trigger scan after encoding

public:
  EncodingService(SharedData &data, TaskTerminal *term);

  // Lifecycle (changed to non-coroutine, uses background threads)
  void start_encoding(int num_workers, bool skip_existing);
  void stop_encoding();

  // Query
  EncodingStatus get_status() const { return status_; }
  EncodingProgress get_progress() const;
  bool is_running() const { return status_ == EncodingStatus::Running; }
  bool is_idle() const {
    return status_ == EncodingStatus::Idle ||
           status_ == EncodingStatus::Completed ||
           status_ == EncodingStatus::Cancelled;
  }

  // Callback management
  void set_scan_callback(std::function<void()> cb) {
    scan_callback_ = cb;
  }

  // Status string helper (for GUI display)
  const char *get_status_string() const {
    switch (status_) {
    case EncodingStatus::Idle:
      return "Idle";
    case EncodingStatus::Running:
      return "Encoding...";
    case EncodingStatus::Completed:
      return "Completed";
    case EncodingStatus::Cancelled:
      return "Cancelled";
    case EncodingStatus::Error:
      return "Error";
    default:
      return "Unknown";
    }
  }

  // File check (archive validation)
  void run_file_check(const std::string &archive_base_dir);
  bool is_file_check_running() const { return file_check_running_.load(); }
  const FileCheck::FileCheckResult &get_file_check_result() const { return file_check_result_; }

private:
  void run_file_check_async(const std::string &archive_base_dir);
};

} // namespace GUI::Database
