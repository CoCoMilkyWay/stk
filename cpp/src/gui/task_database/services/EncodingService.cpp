// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/logging.hpp"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <iostream>
#include <mutex>

namespace GUI::Database {

EncodingService::EncodingService(SharedData &data, io_context &io, TaskTerminal *term)
    : data_(data), io_(io), terminal_(term) {
  // Scan operations now in Asset class
}

void EncodingService::start_encoding(int num_workers, bool skip_existing) {
  if (status_ == EncodingStatus::Running)
    return;

  status_ = EncodingStatus::Running;
  cancel_flag_.store(false);
  num_workers_ = num_workers;
  skip_existing_ = skip_existing;
  start_time_ = std::chrono::steady_clock::now();

  // Enable High Performance Mode: GUI sleeps, all CPU for encoding
  data_.gui.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI thread sleeping\n"
            << std::endl;

  // Launch encoding in background thread
  encoding_thread_ = std::async(std::launch::async, [this]() {
    std::cout << "\n=== Encoding Started ===\n"
              << "Workers: " << num_workers_ << " | Assets: " << data_.asset.items.size()
              << " | Dates: " << data_.asset.all_dates.size() << "\n"
              << std::endl;

    // Initialize date_info for all assets (if not already done)
    for (auto &asset : data_.asset.items) {
      for (const auto &date_str : data_.asset.all_dates) {
        if (date_str >= asset.start_date && date_str <= asset.end_date) {
          // Create DateInfo if not exists (may already exist from binary scan)
          if (asset.date_info.find(date_str) == asset.date_info.end()) {
            DateInfo &di = asset.date_info[date_str];
            di.database_dir = Utils::generate_temp_asset_dir(data_.config.database_dir, date_str,
                                                             asset.asset_code, asset.exchange);
            di.snapshots_encoded = 0;
            di.orders_encoded = 0;
          }
        }
      }
    }

    // Build asset queue (all assets, skip logic handled per-date in worker)
    std::vector<size_t> asset_queue;
    asset_queue.reserve(data_.asset.items.size());
    for (size_t i = 0; i < data_.asset.items.size(); ++i) {
      asset_queue.push_back(i);
    }

    std::cout << "Encoding: 二进制数据库创建中...\n"
              << std::endl;

    // Initialize logger for all encoding workers (shared log file)
    Logger::init(data_.config.log_dir);
    Logger::reg("encoding");

    // Launch worker threads
    progress_ = std::make_shared<misc::ParallelProgress>(num_workers_);
    std::mutex queue_mutex;
    workers_.clear();
    workers_.reserve(num_workers_);

    for (int i = 0; i < num_workers_; ++i) {
      workers_.push_back(std::async(std::launch::async, [this, i, &asset_queue, &queue_mutex]() {
        encoding_worker(data_, asset_queue, queue_mutex, &cancel_flag_, i, progress_->get_handle(i));
      }));
    }

    // Wait for completion
    for (auto &worker : workers_)
      worker.wait();
    progress_->stop();
    workers_.clear();

    // Finalize
    status_ = cancel_flag_.load() ? EncodingStatus::Cancelled : EncodingStatus::Completed;

    std::cout << "\n=== Encoding " << (status_ == EncodingStatus::Completed ? "Complete" : "Cancelled") << " ===\n"
              << "Encoded: " << data_.asset.binary.dates.size() << " dates" << std::endl;

    // Trigger database check coroutine to update coverage after encoding
    boost::asio::co_spawn(io_, [this]() -> boost::asio::awaitable<void> {
      co_await coro_database_check();
      std::cout << "\nDatabase: " << last_check_.get_status_string() << std::endl;
    }(), boost::asio::detached);

    // Disable High Performance Mode: GUI resumes
    data_.gui.DisableHighPerformanceMode();
    std::cout << "\n[High Performance Mode] Disabled - GUI thread resumed\n"
              << std::endl;
  });
}

void EncodingService::stop_encoding() {
  if (status_ != EncodingStatus::Running) {
    return;
  }

  cancel_flag_.store(true);
  std::cout << "[Encoding] Cancelling..." << std::endl;

  // Wait for encoding thread to finish
  if (encoding_thread_.valid()) {
    encoding_thread_.wait();
  }
}

EncodingProgress EncodingService::get_progress() const {
  EncodingProgress prog;
  prog.total_assets = data_.asset.items.size();
  prog.total_dates = data_.asset.all_dates.size();
  prog.encoded_dates = data_.asset.binary.dates.size();
  prog.completed_assets = 0;

  // Calculate total orders
  for (const auto &item : data_.asset.items) {
    prog.total_orders += item.get_total_order_count();
  }

  if (status_ == EncodingStatus::Running) {
    prog.elapsed_seconds = std::chrono::duration<double>(
                               std::chrono::steady_clock::now() - start_time_)
                               .count();
    prog.encoding_rate = prog.elapsed_seconds > 0 ? prog.completed_assets / prog.elapsed_seconds : 0;
  }

  return prog;
}

// ============================================================================
// Database Check - Coroutine Version (uses thread pool for heavy scan)
// ============================================================================

awaitable<void> EncodingService::coro_database_check() {
  using namespace std::chrono;
  namespace fs = std::filesystem;

  DatabaseCheckResult result;

  // ========================================
  // Phase 0: Initialization - yield immediately, let GUI render
  // ========================================
  // Status already set to InitializingCheck in start_database_check()
  // Yield immediately to let GUI render the status
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
    status_ = EncodingStatus::Error;
    co_return;
  }

  // ========================================
  // Phase 2: Check file system - update status, yield, then check
  // ========================================
  
  status_ = EncodingStatus::CheckingFileSystem;
  // Yield BEFORE doing any FS operations
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // Now do the actual FS checks (after GUI has rendered the status)
  bool binary_exists = fs::exists(data_.config.database_dir) &&
                       !fs::is_empty(data_.config.database_dir);
  bool archive_exists = fs::exists(data_.config.archive_dir) &&
                        !fs::is_empty(data_.config.archive_dir);

  // Case 3: binary_exists == false && archive_exists == false -> NoData
  if (!binary_exists && !archive_exists) {
    result.status = DatabaseStatus::NoData;
    result.error_message = "No binary or archive database found";
    last_check_ = result;
    status_ = EncodingStatus::Idle;
    co_return;
  }

  // ========================================
  // Phase 3: Scan binary database - update status, yield, then scan
  // ========================================

  if (!data_.asset.binary.scanned) {
    status_ = EncodingStatus::ScanningBinary;
    // Yield BEFORE creating thread pool and scanning
    co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);
    
    // Now do the actual scanning (after GUI has rendered the status)
    auto scan_pool = std::make_shared<ScanThreadPool>(std::thread::hardware_concurrency());
    co_await data_.asset.coro_scan_binary_database(io_, data_.config.database_dir,
                                                    data_.config.binary_extension, scan_pool);
  }

  // ========================================
  // Phase 4: Scan archive database - update status, yield, then scan
  // ========================================

  if (!data_.asset.archive.scanned) {
    status_ = EncodingStatus::ScanningArchive;
    // Yield BEFORE creating thread pool and scanning
    co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);
    
    // Now do the actual scanning (after GUI has rendered the status)
    auto scan_pool = std::make_shared<ScanThreadPool>(std::thread::hardware_concurrency());
    co_await data_.asset.coro_scan_archive_database(io_, data_.config.archive_dir,
                                                     data_.config.archive_extension, scan_pool);
  }

  // ========================================
  // Phase 5: Compute backtest coverage - update status, yield, then compute
  // ========================================

  status_ = EncodingStatus::ComputingCoverage;
  // Yield BEFORE computing coverage
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // Now do the actual computation (after GUI has rendered the status)
  data_.asset.compute_backtest_coverage(backtest_start, backtest_end);

  // ========================================
  // Phase 6: Analyze and determine status - update status, yield, then analyze
  // ========================================
  
  status_ = EncodingStatus::AnalyzingStatus;
  // Yield BEFORE analyzing
  co_await boost::asio::steady_timer(io_, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // Now do the actual analysis (after GUI has rendered the status)
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
  status_ = EncodingStatus::Idle;
  co_return;
}


void EncodingService::run_file_check(const std::string &archive_base_dir) {
  if (!terminal_)
    return;

  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Starting Archive Validation");
  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Archive path: " + archive_base_dir);
  terminal_->AddLine("");

  // Step 1: Check directory exists
  terminal_->AddLine("[File Check] Step 1: Checking archive directory...");
  file_check_result_ = FileCheck::check_src_archives(archive_base_dir);

  if (!file_check_result_.archive_dir_exists) {
    terminal_->AddLine("[File Check] ✗ Archive directory does not exist", Color::Yellow());
    terminal_->AddLine("[File Check] Will use built binaries instead");
    terminal_->AddLine("========================================");
    return;
  }

  terminal_->AddLine("[File Check] ✓ Archive directory exists", Color::Green());
  terminal_->AddLine("");

  // Step 2: Check required commands
  terminal_->AddLine("[File Check] Step 2: Checking required commands (unrar, 7z, rar, gdb)...");
  if (!file_check_result_.commands_available) {
    terminal_->AddLine("[File Check] ✗ Some required commands are missing", Color::Red());
    terminal_->AddLine("[File Check] Please install: unrar, 7z, rar, gdb");
    terminal_->AddLine("========================================");
    return;
  }
  terminal_->AddLine("[File Check] ✓ All required commands available", Color::Green());
  terminal_->AddLine("");

  // Step 3: Scan archives
  terminal_->AddLine("[File Check] Step 3: Scanning archive files...");
  terminal_->AddLine("[File Check] Total archives found: " + std::to_string(file_check_result_.total_archives), Color::Green());
  terminal_->AddLine("");

  // Step 4-7: Validate naming, format, structure, ZIP files
  auto print_errors = [this](const std::string &step, const std::string &desc, size_t count,
                             const std::vector<std::string> &files, const std::string &fix = "") {
    terminal_->AddLine("[File Check] " + step + ": " + desc + "...");
    if (count > 0) {
      terminal_->AddLine("[File Check] ✗ Found " + std::to_string(count) + " error(s)", Color::Red());
      if (!fix.empty())
        terminal_->AddLine("[File Check]   Fix: " + fix);
      for (const auto &file : files) {
        terminal_->AddLine("[File Check]   - " + file, Color::Yellow());
      }
    } else {
      terminal_->AddLine("[File Check] ✓ All correct", Color::Green());
    }
    terminal_->AddLine("");
  };

  print_errors("Step 4", "Checking archive naming (YYYY/YYYYMM/YYYYMMDD.rar)",
               file_check_result_.naming_errors, file_check_result_.naming_error_files);

  print_errors("Step 5", "Checking archive format (RAR non-solid)",
               file_check_result_.format_errors, file_check_result_.format_error_files,
               "Run py/app/FileRepair/fix_7z_to_rar.py or fix_solid_to_nonsolid.py");

  print_errors("Step 6", "Checking internal structure (YYYYMMDD/asset_code/*.csv)",
               file_check_result_.structure_errors, file_check_result_.structure_error_files,
               "Run py/app/FileRepair/fix_archive_structure.py");

  print_errors("Step 7", "Checking for ZIP files (should be RAR)",
               file_check_result_.zip_files, file_check_result_.zip_error_files,
               "Run py/app/FileRepair/fix_zip_to_rar.py");

  // Summary
  terminal_->AddLine("========================================");
  if (file_check_result_.passed) {
    terminal_->AddLine("[File Check] ✓ ALL CHECKS PASSED", Color::Green());
    terminal_->AddLine("[File Check] Valid archives: " + std::to_string(file_check_result_.valid_archives));
  } else {
    terminal_->AddLine("[File Check] ✗ SOME CHECKS FAILED", Color::Red());
    terminal_->AddLine("[File Check] Valid: " + std::to_string(file_check_result_.valid_archives) +
                       " / Total: " + std::to_string(file_check_result_.total_archives));
    terminal_->AddLine("[File Check] Total errors: " + std::to_string(
                                                           file_check_result_.naming_errors + file_check_result_.format_errors +
                                                           file_check_result_.structure_errors + file_check_result_.zip_files));
  }
  terminal_->AddLine("========================================");
}

} // namespace GUI::Database
