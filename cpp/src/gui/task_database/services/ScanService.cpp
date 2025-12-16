// Scan Service Implementation
#include "gui/task_database/services/ScanService.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <filesystem>

namespace GUI::Database {

ScanService::ScanService(SharedData &data, io_context &io, TaskTerminal *term)
    : data_(data), io_(io), terminal_(term) {}

void ScanService::trigger_scan() {
  // Atomic check: if already scanning, ignore request
  bool expected = false;
  if (!is_scanning_.compare_exchange_strong(expected, true)) {
    return; // Ignore concurrent requests
  }

  // Reset scanned flags to force rescan
  data_.asset.binary.scanned = false;
  data_.asset.archive.scanned = false;

  // Clear old result, update status
  last_check_ = DatabaseCheckResult{};
  status_ = ScanStatus::InitializingCheck;

  // Launch coroutine
  boost::asio::co_spawn(io_, [this]() -> awaitable<void> {
    co_await coro_scan();
    is_scanning_.store(false); // Release lock after completion
  }(), boost::asio::detached);
}

awaitable<void> ScanService::coro_scan() {
  using namespace std::chrono;
  namespace fs = std::filesystem;

  DatabaseCheckResult result;

  // ========================================
  // Phase 0: Initialization - yield immediately, let GUI render
  // ========================================
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // ========================================
  // Phase 1: Validate config (before any FS operations)
  // ========================================

  // Input: backtest_start, backtest_end
  std::string backtest_start = data_.config.start_date;
  std::string backtest_end = data_.config.end_date;

  // Convert YYYY-MM-DD to YYYYMMDD
  backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
  backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());

  if (backtest_start.empty() || backtest_end.empty()) {
    result.status = DatabaseStatus::Error;
    result.error_message = "Backtest period not configured";
    last_check_ = result;
    status_ = ScanStatus::Error;
    co_return;
  }

  // ========================================
  // Phase 2: Check file system - update status, yield, then check
  // ========================================

  status_ = ScanStatus::CheckingFileSystem;
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  bool binary_exists = fs::exists(data_.config.database_dir) &&
                       !fs::is_empty(data_.config.database_dir);
  bool archive_exists = fs::exists(data_.config.archive_dir) &&
                        !fs::is_empty(data_.config.archive_dir);

  // Case 3: binary_exists == false && archive_exists == false -> NoData
  if (!binary_exists && !archive_exists) {
    result.status = DatabaseStatus::NoData;
    result.error_message = "No binary or archive database found";
    last_check_ = result;
    status_ = ScanStatus::Idle;
    co_return;
  }

  // ========================================
  // Phase 3: Scan binary database - update status, yield, then scan
  // ========================================

  if (!data_.asset.binary.scanned) {
    status_ = ScanStatus::ScanningBinary;
    co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

    auto scan_pool = std::make_shared<ScanThreadPool>(std::thread::hardware_concurrency());
    co_await data_.asset.coro_scan_binary_database(io_, data_.config.database_dir,
                                                   data_.config.binary_extension, scan_pool);
  }

  // ========================================
  // Phase 4: Scan archive database - update status, yield, then scan
  // ========================================

  if (!data_.asset.archive.scanned) {
    status_ = ScanStatus::ScanningArchive;
    co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

    auto scan_pool = std::make_shared<ScanThreadPool>(std::thread::hardware_concurrency());
    co_await data_.asset.coro_scan_archive_database(io_, data_.config.archive_dir,
                                                    data_.config.archive_extension, scan_pool);
  }

  // ========================================
  // Phase 5: Compute backtest coverage - update status, yield, then compute
  // ========================================

  status_ = ScanStatus::ComputingCoverage;
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  data_.asset.compute_backtest_coverage(backtest_start, backtest_end);

  // ========================================
  // Phase 6: Analyze and determine status - update status, yield, then analyze
  // ========================================

  status_ = ScanStatus::AnalyzingStatus;
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // Populate result from Asset data
  result.binary.exists = data_.asset.binary.exists;
  result.binary.path = data_.asset.binary.path;
  result.binary.total_dates = data_.asset.binary.dates.size();
  result.binary.available_dates.assign(data_.asset.binary.dates.begin(), data_.asset.binary.dates.end());

  result.archive.exists = data_.asset.archive.exists;
  result.archive.path = data_.asset.archive.path;
  result.archive.total_dates = data_.asset.archive.dates.size();
  result.archive.available_dates.assign(data_.asset.archive.dates.begin(), data_.asset.archive.dates.end());

  result.required_dates = data_.asset.backtest.required_dates.size();
  result.binary_coverage = data_.asset.backtest.covered_dates.size();
  result.missing_dates.assign(data_.asset.backtest.missing_dates.begin(),
                              data_.asset.backtest.missing_dates.end());
  result.missing_can_encode.assign(data_.asset.backtest.can_encode.begin(),
                                   data_.asset.backtest.can_encode.end());
  result.missing_no_archive.assign(data_.asset.backtest.need_download.begin(),
                                   data_.asset.backtest.need_download.end());

  // Decision Tree
  if (binary_exists) {
    // Case 1: binary exists
    if (data_.asset.binary.min_date.empty() || data_.asset.binary.min_date > backtest_start) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Binary starts too late (" + data_.asset.binary.min_date + " > " + backtest_start + ")";
    } else if (data_.asset.binary.max_date.empty() || data_.asset.binary.max_date < backtest_end) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Binary ends too early (" + data_.asset.binary.max_date + " < " + backtest_end + ")";
    } else {
      // Range OK
      if (archive_exists) {
        // Has archive: use archive as ground truth
        if (result.missing_dates.empty()) {
          result.status = DatabaseStatus::Pass;
        } else {
          result.status = DatabaseStatus::Incomplete;
        }
      } else {
        // No archive: binary is ground truth
        result.status = DatabaseStatus::Pass;
      }
    }
  } else {
    // Case 2: binary doesn't exist, but archive exists
    if (data_.asset.archive.min_date.empty() || data_.asset.archive.min_date > backtest_start) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Archive starts too late (" + data_.asset.archive.min_date + " > " + backtest_start + ")";
    } else if (data_.asset.archive.max_date.empty() || data_.asset.archive.max_date < backtest_end) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Archive ends too early (" + data_.asset.archive.max_date + " < " + backtest_end + ")";
    } else {
      // Range OK -> NotEncoded
      result.status = DatabaseStatus::NotEncoded;
    }
  }

  // Cache result and finalize
  last_check_ = result;
  status_ = ScanStatus::Completed;
  co_return;
}

const char* ScanService::get_status_string() const {
  switch (status_) {
    case ScanStatus::Idle: return "Idle";
    case ScanStatus::InitializingCheck: return "Initializing check...";
    case ScanStatus::CheckingFileSystem: return "Checking filesystem...";
    case ScanStatus::ScanningBinary: return "Scanning binary database...";
    case ScanStatus::ScanningArchive: return "Scanning archive database...";
    case ScanStatus::ComputingCoverage: return "Computing coverage...";
    case ScanStatus::AnalyzingStatus: return "Analyzing status...";
    case ScanStatus::Completed: return "Completed";
    case ScanStatus::Error: return "Error";
    default: return "Unknown";
  }
}

} // namespace GUI::Database

