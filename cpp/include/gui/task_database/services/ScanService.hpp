// Scan Service - Manages database scanning and coverage checking
#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <functional>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;
struct TaskTerminal;

namespace GUI::Database {

using boost::asio::awaitable;
using boost::asio::io_context;

// ============================================================================
// Scan Status
// ============================================================================

enum class ScanStatus {
  Idle,

  // Scan phases (fine-grained)
  InitializingCheck,  // Starting scan coroutine
  CheckingFileSystem, // Checking if directories exist
  ScanningBinary,     // Scanning binary database
  ScanningArchive,    // Scanning archive database
  ComputingCoverage,  // Computing backtest coverage
  AnalyzingStatus,    // Determining database status

  // Final states
  Completed,
  Error
};

enum class ScanMode {
  RecomputeCoverage, // 只用已有 binary/archive 扫描缓存重算 backtest coverage
  RescanStorage      // 底层 binary/archive 可能变了, 重新逐日扫盘
};

// ============================================================================
// Database Check Types
// ============================================================================

enum class DatabaseStatus {
  Unchecked,   // Not checked yet (initial state)
  Pass,        // Binary fully covers backtest period
  Incomplete,  // Binary incomplete but can encode from archive
  NeedArchive, // Binary incomplete, missing dates have no archive
  NotEncoded,  // Binary doesn't exist, needs encoding
  NoData,      // Both binary and archive don't exist
  Error        // Configuration error or other exception
};

struct BinaryDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  std::vector<std::string> available_dates;
};

struct ArchiveDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  std::vector<std::string> available_dates;
};

struct DatabaseCheckResult {
  DatabaseStatus status = DatabaseStatus::Unchecked;
  std::string error_message;

  // Binary database
  BinaryDatabaseInfo binary;

  // Archive database
  ArchiveDatabaseInfo archive;

  // Coverage analysis
  size_t required_dates = 0;  // Required trading days in backtest period
  size_t binary_coverage = 0; // Binary covered dates

  // Missing details
  std::vector<std::string> missing_dates;      // All missing dates
  std::vector<std::string> missing_can_encode; // Missing but has archive
  std::vector<std::string> missing_no_archive; // Missing without archive

  // Helper: L2 覆盖检查通过 (binary 覆盖回测区间内全部交易日)
  bool is_pass() const {
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
// Scan Service
// ============================================================================

class ScanService {
  // StateManager needs direct access to spawn coroutine
  friend class StateManager;

private:
  SharedData &data_;
  io_context &io_;
  TaskTerminal *terminal_;

  std::atomic<bool> is_scanning_{false};
  ScanStatus status_ = ScanStatus::Idle;
  DatabaseCheckResult last_check_;
  std::function<void()> on_complete_callback_;

public:
  ScanService(SharedData &data, io_context &io, TaskTerminal *term = nullptr);

  // ============================================================================
  // Lifecycle
  // ============================================================================

  // Unified trigger entry (atomic protection, ignores concurrent requests)
  void trigger_scan(ScanMode mode);

  // Set callback to be called when scan completes
  void set_on_complete(std::function<void()> callback) { on_complete_callback_ = std::move(callback); }

  // ============================================================================
  // Query
  // ============================================================================

  ScanStatus get_status() const { return status_; }
  bool is_scanning() const { return is_scanning_.load(); }
  bool is_idle() const {
    return status_ == ScanStatus::Idle ||
           status_ == ScanStatus::Completed ||
           status_ == ScanStatus::Error;
  }

  const DatabaseCheckResult &get_last_check_result() const { return last_check_; }
  const char *get_status_string() const;

private:
  // get_status_string 返回裸指针, 带进度的那两个阶段需要一块常驻缓冲
  mutable char status_buf_[64] = {};

  // Complete scan flow (coroutine)
  awaitable<void> coro_scan(ScanMode mode);
};

} // namespace GUI::Database
