// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/affinity.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <mutex>

namespace GUI::Database {

namespace asio = boost::asio;

EncodingService::EncodingService(SharedData &data, io_context & /*io*/, TaskTerminal *term)
    : data_(data), terminal_(term) {}

awaitable<void> EncodingService::start_encoding(int num_workers, bool skip_existing) {
  if (status_ == EncodingStatus::Running) {
    co_return;
  }

  // Note: Asset scanning is already done in StateManager::initialize()
  // No need to re-scan here, just use existing data

  co_await asio::this_coro::executor;

  status_ = EncodingStatus::Running;
  cancel_flag_.store(false);
  num_workers_ = num_workers;
  skip_existing_ = skip_existing;
  start_time_ = std::chrono::steady_clock::now();

  if (terminal_) {
    terminal_->Clear();
    terminal_->AddLine("[Encoding] Starting with " + std::to_string(num_workers) + " workers...");
    terminal_->AddLine("[Encoding] Total assets: " + std::to_string(data_.asset.items.size()));
    terminal_->AddLine("[Encoding] Total dates: " + std::to_string(data_.asset.all_dates.size()));
  }

  // Create asset queue
  std::vector<size_t> asset_queue;
  for (size_t i = 0; i < data_.asset.items.size(); ++i) {
    if (!skip_existing_ || data_.asset.items[i].get_missing_count() > 0) {
      asset_queue.push_back(i);
    }
  }

  if (asset_queue.empty()) {
    if (terminal_) {
      terminal_->AddLine("[Encoding] All assets already encoded. Nothing to do.");
    }
    status_ = EncodingStatus::Completed;

    // Check database coverage
    check_database_coverage();

    co_return;
  }

  if (terminal_) {
    terminal_->AddLine("[Encoding] Assets to encode: " + std::to_string(asset_queue.size()));
  }

  // Create progress tracker
  progress_ = std::make_shared<misc::ParallelProgress>(num_workers);

  // Launch worker threads
  std::mutex queue_mutex;
  workers_.clear();
  workers_.reserve(num_workers);

  for (int i = 0; i < num_workers; ++i) {
    workers_.push_back(std::async(std::launch::async, [this, i, &asset_queue, &queue_mutex]() {
      unsigned int core_id = misc::Affinity::core_count() > 0 ? i % misc::Affinity::core_count() : 0;
      encoding_worker(data_, asset_queue, queue_mutex, &cancel_flag_, core_id, progress_->get_handle(i));
    }));
  }

  // Wait for all workers to complete
  for (auto &worker : workers_) {
    worker.wait();
  }

  // Update status
  if (cancel_flag_.load()) {
    status_ = EncodingStatus::Cancelled;
    if (terminal_) {
      terminal_->AddLine("[Encoding] Cancelled by user.");
    }
  } else {
    status_ = EncodingStatus::Completed;
    if (terminal_) {
      terminal_->AddLine("[Encoding] Completed successfully.");
      terminal_->AddLine("[Encoding] Encoded: " + std::to_string(data_.asset.binary.dates.size()) + " dates");
    }
  }

  // Check database coverage after encoding
  auto check_result = check_database_coverage();
  if (terminal_) {
    terminal_->AddLine("[Check] Database status: " + std::string(check_result.get_status_string()));

    switch (check_result.status) {
    case DatabaseStatus::Pass:
      terminal_->AddLine("[Check] Binary database完整覆盖backtest period", Color::Green());
      break;

    case DatabaseStatus::Incomplete:
      terminal_->AddLine("[Check] Missing " + std::to_string(check_result.missing_dates.size()) +
                             " dates, " + std::to_string(check_result.missing_can_encode.size()) +
                             " can encode from archive",
                         Color::Yellow());
      break;

    case DatabaseStatus::NeedArchive:
      terminal_->AddLine("[Check] Missing " + std::to_string(check_result.missing_no_archive.size()) +
                             " dates without archive",
                         Color::Red());
      break;

    case DatabaseStatus::NotEncoded:
      terminal_->AddLine("[Check] Archive available, need to encode", Color::Yellow());
      break;

    case DatabaseStatus::NoData:
    case DatabaseStatus::Error:
      terminal_->AddLine("[Check] " + check_result.error_message, Color::Red());
      break;
    }
  }

  workers_.clear();
}

awaitable<void> EncodingService::stop_encoding() {
  if (status_ != EncodingStatus::Running) {
    co_return;
  }

  co_await asio::this_coro::executor;

  cancel_flag_.store(true);

  if (terminal_) {
    terminal_->AddLine("[Encoding] Cancelling...");
  }

  // Wait for workers to finish
  for (auto &worker : workers_) {
    if (worker.valid()) {
      worker.wait();
    }
  }

  status_ = EncodingStatus::Cancelled;
}

EncodingProgress EncodingService::get_progress() const {
  EncodingProgress prog;

  prog.total_assets = data_.asset.items.size();
  prog.total_dates = data_.asset.all_dates.size();
  prog.encoded_dates = data_.asset.binary.dates.size();

  // Calculate total orders
  size_t total_orders = 0;
  for (const auto &item : data_.asset.items) {
    total_orders += item.get_total_order_count();
  }
  prog.total_orders = total_orders;

  // Completed assets tracking would need additional state
  // For now, just use basic progress metrics
  prog.completed_assets = 0;

  if (status_ == EncodingStatus::Running) {
    auto now = std::chrono::steady_clock::now();
    prog.elapsed_seconds = std::chrono::duration<double>(now - start_time_).count();
    if (prog.elapsed_seconds > 0) {
      prog.encoding_rate = prog.completed_assets / prog.elapsed_seconds;
    }
  }

  return prog;
}

DatabaseCheckResult EncodingService::check_database_coverage() {
  namespace fs = std::filesystem;
  DatabaseCheckResult result;

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
    return result;
  }

  // Step 1: Lightweight check - folder existence
  bool binary_exists = fs::exists(data_.config.database_dir) &&
                       !fs::is_empty(data_.config.database_dir);
  bool archive_exists = fs::exists(data_.config.archive_dir) &&
                        !fs::is_empty(data_.config.archive_dir);

  // Case 3: binary_exists == false && archive_exists == false -> NoData
  if (!binary_exists && !archive_exists) {
    result.status = DatabaseStatus::NoData;
    result.error_message = "No binary or archive database found";
    last_check_ = result;
    return result;
  }

  // Scan databases (if not already scanned)
  if (!data_.asset.binary.scanned) {
    data_.asset.scan_binary_database(data_.config.database_dir, data_.config.binary_extension);
  }

  if (!data_.asset.archive.scanned) {
    data_.asset.scan_archive_database(data_.config.archive_dir, data_.config.archive_extension);
  }

  // Compute backtest coverage (also calculates backtest range statistics)
  data_.asset.compute_backtest_coverage(backtest_start, backtest_end);

  // Populate result
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

  // Decision Tree (from Untitled-1)
  if (binary_exists) {
    // Case 1: binary exists
    // Step: Scan binary database, collect all encoded dates
    // Check all assets have snapshot + order files for each date

    // Check range coverage
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
        // Extract required_dates (from backtest period)
        // Extract binary_coverage (binary dates in backtest period)
        // Calculate missing_dates = required_dates - binary_coverage
      if (result.missing_dates.empty()) {
        result.status = DatabaseStatus::Pass;
      } else {
          result.status = DatabaseStatus::Incomplete;
        }
      } else {
        // No archive: binary is ground truth
        // Directly pass (cannot judge completeness without reference)
        result.status = DatabaseStatus::Pass;
      }
    }
  } else {
    // Case 2: binary doesn't exist, but archive exists
    // Scan archive database, collect all archive dates

    // Check range coverage
    if (data_.asset.archive.min_date.empty() || data_.asset.archive.min_date > backtest_start) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Archive starts too late (" + data_.asset.archive.min_date + " > " + backtest_start + ")";
    } else if (data_.asset.archive.max_date.empty() || data_.asset.archive.max_date < backtest_end) {
      result.status = DatabaseStatus::NeedArchive;
      result.error_message = "Archive ends too early (" + data_.asset.archive.max_date + " < " + backtest_end + ")";
    } else {
      // Range OK -> NotEncoded (need to encode all, but data is sufficient)
      result.status = DatabaseStatus::NotEncoded;
    }
  }

  // Cache result
  last_check_ = result;
  return result;
}

} // namespace GUI::Database
