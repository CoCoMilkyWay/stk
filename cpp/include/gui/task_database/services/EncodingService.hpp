// Encoding Service - Manages L2 binary database encoding from CSV archives
#pragma once

#include "misc/file_check.hpp"
#include "misc/progress_parallel.hpp"
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <future>
#include <memory>
#include <vector>

// Forward declarations
struct SharedData;
class TaskTerminal;

namespace GUI::Database {

using boost::asio::awaitable;
using boost::asio::io_context;

// ============================================================================
// Encoding Status
// ============================================================================

enum class EncodingStatus {
  Idle,

  // Database check phases (fine-grained)
  InitializingCheck,  // Starting database check coroutine
  CheckingFileSystem, // Checking if directories exist
  ScanningBinary,     // Scanning binary database
  ScanningArchive,    // Scanning archive database
  ComputingCoverage,  // Computing backtest coverage
  AnalyzingStatus,    // Determining database status

  // Encoding phases
  Running,

  // Final states
  Completed,
  Cancelled,
  Error
};

// ============================================================================
// Database Check Types
// ============================================================================

enum class DatabaseStatus {
  Unchecked,   // 尚未检查(初始状态)
  Pass,        // Binary完整覆盖backtest period,可以回测
  Incomplete,  // Binary不完整,但可以从archive encode补全
  NeedArchive, // Binary不完整,缺失日期无对应archive
  NotEncoded,  // Binary不存在,需要encode
  NoData,      // Binary和archive都不存在
  Error        // 配置错误或其他异常
};

struct BinaryDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  size_t coverage_in_backtest = 0;
  std::vector<std::string> available_dates;
};

struct ArchiveDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  size_t coverage_in_backtest = 0;
  std::vector<std::string> available_dates;
};

struct DatabaseCheckResult {
  DatabaseStatus status = DatabaseStatus::Unchecked; // 默认为未检查，不是 Error
  std::string error_message;

  // Binary database
  BinaryDatabaseInfo binary;

  // Archive database
  ArchiveDatabaseInfo archive;

  // Coverage analysis
  size_t required_dates = 0;   // Backtest period内需要的交易日数
  size_t binary_coverage = 0;  // Binary已覆盖的日期数
  size_t archive_coverage = 0; // Archive可提供的日期数

  // Missing details
  std::vector<std::string> missing_dates;      // 所有缺失日期
  std::vector<std::string> missing_can_encode; // 缺失但有archive可encode
  std::vector<std::string> missing_no_archive; // 缺失且无archive

  // Helper
  bool can_unlock_overview() const {
    return status == DatabaseStatus::Pass;
  }

  const char *get_status_string() const {
    switch (status) {
    case DatabaseStatus::Unchecked:
      return "Not checked";
    case DatabaseStatus::Pass:
      return "Pass";
    case DatabaseStatus::Incomplete:
      return "Incomplete";
    case DatabaseStatus::NeedArchive:
      return "NeedArchive";
    case DatabaseStatus::NotEncoded:
      return "NotEncoded";
    case DatabaseStatus::NoData:
      return "NoData";
    case DatabaseStatus::Error:
      return "ERROR";
    }
    return "UNKNOWN";
  }
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
  // StateManager needs direct access to spawn coroutine and update status inline
  friend class StateManager;

private:
  SharedData &data_;
  io_context &io_;
  TaskTerminal *terminal_;

  std::atomic<bool> cancel_flag_{false};
  std::shared_ptr<misc::ParallelProgress> progress_;
  std::vector<std::future<void>> workers_;
  EncodingStatus status_ = EncodingStatus::Idle;

  int num_workers_ = 0;
  bool skip_existing_ = true;
  std::chrono::steady_clock::time_point start_time_;

  DatabaseCheckResult last_check_;               // Cache last database check result
  FileCheck::FileCheckResult file_check_result_; // Cache file check result

  std::future<void> encoding_thread_; // Background encoding thread
  std::future<void> scan_thread_;     // Background scan thread

  // Scan operations now implemented directly in Asset class

public:
  EncodingService(SharedData &data, io_context &io, TaskTerminal *term);

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
  bool is_checking() const {
    return status_ == EncodingStatus::InitializingCheck ||
           status_ == EncodingStatus::CheckingFileSystem ||
           status_ == EncodingStatus::ScanningBinary ||
           status_ == EncodingStatus::ScanningArchive ||
           status_ == EncodingStatus::ComputingCoverage ||
           status_ == EncodingStatus::AnalyzingStatus;
  }

  // Status string helper (for GUI display)
  const char *get_status_string() const {
    switch (status_) {
    case EncodingStatus::Idle:
      return "Idle";
    case EncodingStatus::InitializingCheck:
      return "Initializing check...";
    case EncodingStatus::CheckingFileSystem:
      return "Checking filesystem...";
    case EncodingStatus::ScanningBinary:
      return "Scanning binary database...";
    case EncodingStatus::ScanningArchive:
      return "Scanning archive database...";
    case EncodingStatus::ComputingCoverage:
      return "Computing coverage...";
    case EncodingStatus::AnalyzingStatus:
      return "Analyzing status...";
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

  // Scan and check database coverage (async, using coroutines)
  awaitable<void> coro_database_check(); // Internal coroutine implementation

  // Get last check result
  const DatabaseCheckResult &get_last_check_result() const { return last_check_; }

  // File check (archive validation)
  void run_file_check(const std::string &archive_base_dir);
  const FileCheck::FileCheckResult &get_file_check_result() const { return file_check_result_; }
};

} // namespace GUI::Database
