// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/logging.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <iostream>
#include <mutex>

namespace GUI::Database {

EncodingService::EncodingService(SharedData &data, TaskTerminal *term)
    : data_(data), terminal_(term) {
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
  data_.EnableHighPerformanceMode();
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

    // Trigger scan callback after encoding completion
    if (scan_callback_) {
      scan_callback_();
    }

    // Disable High Performance Mode: GUI resumes
    data_.DisableHighPerformanceMode();
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

  // Wait for encoding thread to finish (which will also wait for workers)
  if (encoding_thread_.valid()) {
    encoding_thread_.wait();
  }

  // Clear any remaining worker futures (though they should already be cleared in the encoding thread)
  workers_.clear();
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
    prog.elapsed_seconds = std::chrono::duration<float>(
                               std::chrono::steady_clock::now() - start_time_)
                               .count();
    prog.encoding_rate = prog.elapsed_seconds > 0 ? prog.completed_assets / prog.elapsed_seconds : 0;
  }

  return prog;
}

void EncodingService::run_file_check(const std::string &archive_base_dir) {
  if (!terminal_)
    return;

  if (file_check_running_.load()) {
    terminal_->AddLine("[File Check] Already running, please wait...", Color::Yellow());
    return;
  }

  file_check_running_.store(true);
  file_check_thread_ = std::async(std::launch::async, [this, archive_base_dir]() {
    run_file_check_async(archive_base_dir);
    file_check_running_.store(false);
  });
}

void EncodingService::run_file_check_async(const std::string &archive_base_dir) {
  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Starting Archive Validation");
  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Archive path: " + archive_base_dir);
  terminal_->AddLine("");

  // Step 1: Check directory exists
  terminal_->AddLine("[File Check] Step 1: Checking archive directory...");

  // Each probe (unrar lb) does O(entries) scattered read+lseek pairs across
  // the whole archive to walk its header chain (measured via strace: ~6500
  // read+lseek pairs for a 3270-entry / 3.9GB archive). The archive store is
  // a single-actuator spinning disk, so probes run sequentially -- running
  // several concurrently thrashes the disk head (measured 359x slowdown).
  auto progress = [this](size_t done, size_t total, const std::string &path) {
    terminal_->AddLine("[File Check]   (" + std::to_string(done) + "/" +
                       std::to_string(total) + ") " + path);
  };

  FileCheck::FileCheckResult local_result =
      FileCheck::check_src_archives(archive_base_dir, progress);

  if (!local_result.archive_dir_exists) {
    terminal_->AddLine("[File Check] ✗ Archive directory does not exist", Color::Yellow());
    terminal_->AddLine("[File Check] Will use built binaries instead");
    terminal_->AddLine("========================================");
    file_check_result_ = local_result;
    return;
  }

  terminal_->AddLine("[File Check] ✓ Archive directory exists", Color::Green());
  terminal_->AddLine("");

  // Step 2: Check required commands
  terminal_->AddLine("[File Check] Step 2: Checking required commands (unrar, 7z, rar, gdb)...");
  if (!local_result.commands_available) {
    terminal_->AddLine("[File Check] ✗ Some required commands are missing", Color::Red());
    terminal_->AddLine("[File Check] Please install: unrar, 7z, rar, gdb");
    terminal_->AddLine("========================================");
    file_check_result_ = local_result;
    return;
  }
  terminal_->AddLine("[File Check] ✓ All required commands available", Color::Green());
  terminal_->AddLine("");

  // Step 3: Scan archives
  terminal_->AddLine("[File Check] Step 3: Scanning archive files...");
  terminal_->AddLine("[File Check] Total archives found: " + std::to_string(local_result.total_archives), Color::Green());
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
               local_result.naming_errors, local_result.naming_error_files);

  print_errors("Step 5", "Checking archive format (RAR non-solid)",
               local_result.format_errors, local_result.format_error_files,
               "Run py/app/FileRepair/fix_7z_to_rar.py or fix_solid_to_nonsolid.py");

  print_errors("Step 6", "Checking internal structure (YYYYMMDD/asset_code/*.csv)",
               local_result.structure_errors, local_result.structure_error_files,
               "Run py/app/FileRepair/fix_archive_structure.py");

  print_errors("Step 6b", "Checking archive integrity (truncated / corrupt headers)",
               local_result.integrity_errors, local_result.integrity_error_files,
               "Re-download or re-create the archive");

  print_errors("Step 7", "Checking for ZIP files (should be RAR)",
               local_result.zip_files, local_result.zip_error_files,
               "Run py/app/FileRepair/fix_zip_to_rar.py");

  // Summary
  terminal_->AddLine("========================================");
  if (local_result.passed) {
    terminal_->AddLine("[File Check] ✓ ALL CHECKS PASSED", Color::Green());
    terminal_->AddLine("[File Check] Valid archives: " + std::to_string(local_result.valid_archives));
  } else {
    terminal_->AddLine("[File Check] ✗ SOME CHECKS FAILED", Color::Red());
    terminal_->AddLine("[File Check] Valid: " + std::to_string(local_result.valid_archives) +
                       " / Total: " + std::to_string(local_result.total_archives));
    terminal_->AddLine("[File Check] Total errors: " + std::to_string(
                                                           local_result.naming_errors + local_result.format_errors +
                                                           local_result.structure_errors + local_result.integrity_errors +
                                                           local_result.zip_files));
  }
  terminal_->AddLine("========================================");

  // Publish result once, atomically, at the end.
  file_check_result_ = local_result;
}

} // namespace GUI::Database
