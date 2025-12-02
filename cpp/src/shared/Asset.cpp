#include "shared/Asset.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <filesystem>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

// ============================================================================
// AssetItem Implementation
// ============================================================================

AssetItem::AssetItem(size_t id, std::string code, std::string name, std::string exch, std::string start, std::string end)
    : asset_id(id),
      asset_code(std::move(code)),
      asset_name(std::move(name)),
      exchange(std::move(exch)),
      exchange_type(L2::infer_exchange_type(asset_code)),
      start_date(std::move(start)),
      end_date(std::move(end)) {}

size_t AssetItem::get_total_trading_days() const {
  return date_info.size();
}

size_t AssetItem::get_encoded_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info) {
    if (di.is_fully_encoded())
      count++;
  }
  return count;
}

size_t AssetItem::get_snapshots_encoded_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info)
    count += di.snapshots_encoded;
  return count;
}

size_t AssetItem::get_orders_encoded_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info)
    count += di.orders_encoded;
  return count;
}

size_t AssetItem::get_missing_count() const {
  return get_total_trading_days() - get_encoded_count();
}

size_t AssetItem::get_analyzed_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info)
    count += di.analyzed;
  return count;
}

size_t AssetItem::get_total_order_count() const {
  size_t total = 0;
  for (const auto &[_, di] : date_info)
    total += di.order_count;
  return total;
}

size_t AssetItem::get_total_snapshot_count() const {
  size_t total = 0;
  for (const auto &[_, di] : date_info)
    total += di.snapshot_count;
  return total;
}

std::vector<std::string> AssetItem::get_missing_dates() const {
  std::vector<std::string> missing;
  for (const auto &[date_str, di] : date_info) {
    if (!di.is_fully_encoded()) {
      missing.push_back(date_str);
    }
  }
  std::sort(missing.begin(), missing.end());
  return missing;
}

std::string AssetItem::get_display_name() const {
  return asset_code + " " + asset_name;
}

// ============================================================================
// Asset Implementation
// ============================================================================

// ============================================================================
// Binary Database Scan (Coroutine Version)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_scan_binary_database(
    boost::asio::io_context &io,
    const std::string &database_dir,
    const std::string &binary_extension,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {

  namespace fs = std::filesystem;
  using boost::asio::use_awaitable;

  // Clear browser statistics cache - will be recomputed on next Browser tab access
  date_stats.clear();

  binary.scanned = true;
  binary.path = database_dir;
  binary.exists = fs::exists(database_dir) && fs::is_directory(database_dir);

  if (!binary.exists) {
    binary.dates.clear();
    binary.min_date.clear();
    binary.max_date.clear();
    binary.total_assets = 0;
    binary.encoded_assets = 0;
    binary.complete_assets = 0;
    binary.total_snapshots = 0;
    binary.total_orders = 0;
    binary.snapshots_size_gb = 0.0;
    binary.orders_size_gb = 0.0;
    all_dates.clear();
    co_return;
  }

  // Month path structure
  struct MonthPath {
    std::string path;
    std::string year_str;
    std::string month_str;
  };

  // Collect all month paths
  std::vector<MonthPath> month_paths;
  for (const auto &year_entry : fs::directory_iterator(database_dir)) {
    if (!year_entry.is_directory())
      continue;
    std::string year_str = year_entry.path().filename().string();
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      month_paths.push_back({month_entry.path().string(), year_str,
                             month_entry.path().filename().string()});
    }
  }

  // Build asset lookup map
  std::unordered_map<std::string, size_t> asset_map;
  for (size_t i = 0; i < items.size(); ++i) {
    asset_map[items[i].asset_code + "." + items[i].exchange] = i;
  }

  // Shared result accumulator
  struct ScanResult {
    std::mutex mutex;
    std::set<std::string> all_dates;
    std::set<std::string> snap_dates;
    std::set<std::string> order_dates;
    std::map<std::string, size_t> date_coverage;
    size_t total_snapshots = 0;
    size_t total_orders = 0;
    float total_snapshots_size = 0.0;
    float total_orders_size = 0.0;
    std::unordered_map<size_t, std::unordered_map<std::string, DateInfo>> asset_date_info;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single month (runs in thread pool)
  auto scan_month = [&asset_map, &binary_extension, result](const MonthPath &month_path) {
    std::set<std::string> local_dates;
    std::set<std::string> local_snap_dates;
    std::set<std::string> local_order_dates;
    std::map<std::string, size_t> local_date_coverage;
    size_t local_total_snapshots = 0;
    size_t local_total_orders = 0;
    float local_snapshots_size = 0.0;
    float local_orders_size = 0.0;
    std::unordered_map<size_t, std::unordered_map<std::string, DateInfo>> local_date_info;

    for (const auto &day_entry : fs::directory_iterator(month_path.path)) {
      if (!day_entry.is_directory())
        continue;
      std::string day_str = day_entry.path().filename().string();
      std::string date_str = month_path.year_str + month_path.month_str + day_str;
      local_dates.insert(date_str);

      for (const auto &asset_entry : fs::directory_iterator(day_entry.path())) {
        if (!asset_entry.is_directory())
          continue;
        std::string asset_folder = asset_entry.path().filename().string();

        auto it = asset_map.find(asset_folder);
        if (it == asset_map.end())
          continue;

        size_t asset_idx = it->second;
        DateInfo di;
        di.database_dir = asset_entry.path().string();

        std::string snap_prefix = asset_folder + "_snapshots_";
        std::string order_prefix = asset_folder + "_orders_";

        for (const auto &file_entry : fs::directory_iterator(asset_entry.path())) {
          std::string filename = file_entry.path().filename().string();

          if (filename.find(snap_prefix) == 0 && filename.ends_with(binary_extension)) {
            di.snapshots_file = file_entry.path().string();
            di.snapshot_count = L2::BinaryDecoder_L2::extract_count_from_filename(di.snapshots_file);
            di.snapshots_encoded = 1;
            try {
              di.snapshots_file_size = fs::file_size(file_entry.path());
            } catch (...) {
              di.snapshots_file_size = 0;
            }
            local_total_snapshots += di.snapshot_count;
            local_snapshots_size += static_cast<float>(di.snapshots_file_size);
            local_snap_dates.insert(date_str);

          } else if (filename.find(order_prefix) == 0 && filename.ends_with(binary_extension)) {
            di.orders_file = file_entry.path().string();
            di.order_count = L2::BinaryDecoder_L2::extract_count_from_filename(di.orders_file);
            di.orders_encoded = 1;
            try {
              di.orders_file_size = fs::file_size(file_entry.path());
            } catch (...) {
              di.orders_file_size = 0;
            }
            local_total_orders += di.order_count;
            local_orders_size += static_cast<float>(di.orders_file_size);
            local_order_dates.insert(date_str);
          }
        }

        if (di.snapshots_encoded || di.orders_encoded) {
          local_date_info[asset_idx][date_str] = di;
          if (di.is_fully_encoded()) {
            local_date_coverage[date_str]++;
          }
        }
      }
    }

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      result->all_dates.insert(local_dates.begin(), local_dates.end());
      result->snap_dates.insert(local_snap_dates.begin(), local_snap_dates.end());
      result->order_dates.insert(local_order_dates.begin(), local_order_dates.end());
      for (const auto &[date, count] : local_date_coverage) {
        result->date_coverage[date] += count;
      }
      result->total_snapshots += local_total_snapshots;
      result->total_orders += local_total_orders;
      result->total_snapshots_size += local_snapshots_size;
      result->total_orders_size += local_orders_size;

      for (const auto &[asset_idx, date_map] : local_date_info) {
        for (const auto &[date, info] : date_map) {
          result->asset_date_info[asset_idx][date] = info;
        }
      }
    }
  };

  // Submit all month scan tasks to thread pool
  std::vector<std::future<void>> futures;
  futures.reserve(month_paths.size());
  for (const auto &month_path : month_paths) {
    futures.push_back(thread_pool->submit([scan_month, month_path]() { scan_month(month_path); }));
  }

  // Wait for all tasks, yielding to GUI periodically
  while (true) {
    bool all_done = true;
    for (auto &future : futures) {
      if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_done = false;
        break;
      }
    }
    if (all_done)
      break;

    boost::asio::steady_timer timer(io, std::chrono::milliseconds(50));
    co_await timer.async_wait(use_awaitable);
  }

  // Collect all futures
  for (auto &future : futures) {
    if (future.valid()) {
      future.get();
    }
  }

  // Merge results into Asset
  all_dates.assign(result->all_dates.begin(), result->all_dates.end());
  binary.total_assets = items.size();
  binary.total_snapshots = result->total_snapshots;
  binary.total_orders = result->total_orders;
  binary.snapshots_size_gb = result->total_snapshots_size / (1024.0 * 1024.0 * 1024.0);
  binary.orders_size_gb = result->total_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.database_snap_days = result->snap_dates.size();
  binary.database_order_days = result->order_dates.size();

  binary.dates.clear();
  for (const auto &[date, count] : result->date_coverage) {
    if (count > 0) {
      binary.dates.insert(date);
    }
  }

  for (const auto &[asset_idx, date_map] : result->asset_date_info) {
    for (const auto &[date, info] : date_map) {
      items[asset_idx].date_info[date] = info;
    }
  }

  if (!all_dates.empty()) {
    binary.min_date = all_dates.front();
    binary.max_date = all_dates.back();
    for (auto &item : items) {
      item.start_date = binary.min_date;
      item.end_date = binary.max_date;
    }
  } else {
    binary.min_date.clear();
    binary.max_date.clear();
  }

  binary.encoded_assets = 0;
  binary.complete_assets = 0;
  for (const auto &item : items) {
    if (!item.date_info.empty()) {
      binary.encoded_assets++;
      bool is_complete = true;
      for (const auto &[date, info] : item.date_info) {
        if (!info.is_fully_encoded()) {
          is_complete = false;
          break;
        }
      }
      if (is_complete) {
        binary.complete_assets++;
      }
    }
  }

  co_return;
}

// ============================================================================
// Archive Database Scan (Coroutine Version)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_scan_archive_database(
    boost::asio::io_context &io,
    const std::string &archive_dir,
    const std::string &archive_extension,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {

  namespace fs = std::filesystem;
  using boost::asio::use_awaitable;

  // Clear browser statistics cache - will be recomputed on next Browser tab access
  date_stats.clear();

  archive.scanned = true;
  archive.path = archive_dir;
  archive.exists = fs::exists(archive_dir) && fs::is_directory(archive_dir);

  if (!archive.exists) {
    archive.dates.clear();
    archive.min_date.clear();
    archive.max_date.clear();
    archive.total_files = 0;
    archive.total_size_gb = 0.0;
    if (all_dates.empty()) {
      all_dates.clear();
    }
    co_return;
  }

  // Year path structure
  struct YearPath {
    std::string path;
    std::string year_str;
  };

  // Collect all year paths
  std::vector<YearPath> year_paths;
  for (const auto &year_entry : fs::directory_iterator(archive_dir)) {
    if (!year_entry.is_directory())
      continue;
    year_paths.push_back({year_entry.path().string(), year_entry.path().filename().string()});
  }

  // Shared result accumulator
  struct ScanResult {
    std::mutex mutex;
    std::set<std::string> archive_dates;
    size_t total_files = 0;
    float total_size = 0.0;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single year (runs in thread pool)
  auto scan_year = [&archive_extension, result](const YearPath &year_path) {
    std::set<std::string> local_dates;
    size_t local_files = 0;
    float local_size = 0.0;

    try {
      for (const auto &month_entry : fs::directory_iterator(year_path.path)) {
        if (!month_entry.is_directory())
          continue;

        for (const auto &file_entry : fs::directory_iterator(month_entry.path())) {
          if (!file_entry.is_regular_file())
            continue;

          const std::string ext = file_entry.path().extension().string();
          if (ext == archive_extension) {
            const std::string filename = file_entry.path().stem().string();
            if (filename.size() == 8 && std::all_of(filename.begin(), filename.end(), ::isdigit)) {
              local_dates.insert(filename);
              local_files++;
              try {
                local_size += static_cast<float>(fs::file_size(file_entry.path()));
              } catch (...) {
              }
            }
          }
        }
      }
    } catch (...) {
    }

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      result->archive_dates.insert(local_dates.begin(), local_dates.end());
      result->total_files += local_files;
      result->total_size += local_size;
    }
  };

  // Submit all year scan tasks to thread pool
  std::vector<std::future<void>> futures;
  futures.reserve(year_paths.size());
  for (const auto &year_path : year_paths) {
    futures.push_back(thread_pool->submit([scan_year, year_path]() { scan_year(year_path); }));
  }

  // Wait for all tasks, yielding to GUI periodically
  while (true) {
    bool all_done = true;
    for (auto &future : futures) {
      if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_done = false;
        break;
      }
    }
    if (all_done)
      break;

    boost::asio::steady_timer timer(io, std::chrono::milliseconds(50));
    co_await timer.async_wait(use_awaitable);
  }

  // Collect all futures
  for (auto &future : futures) {
    if (future.valid()) {
      future.get();
    }
  }

  // Merge results into Asset
  archive.dates = result->archive_dates;
  archive.total_files = result->total_files;
  archive.total_size_gb = result->total_size / (1024.0 * 1024.0 * 1024.0);

  if (!archive.dates.empty()) {
    archive.min_date = *archive.dates.begin();
    archive.max_date = *archive.dates.rbegin();
  } else {
    archive.min_date.clear();
    archive.max_date.clear();
  }

  if (all_dates.empty()) {
    all_dates.assign(archive.dates.begin(), archive.dates.end());
    for (auto &item : items) {
      for (const auto &date_str : all_dates) {
        if (date_str >= item.start_date && date_str <= item.end_date) {
          // Date info will be created during encoding
        }
      }
    }
  }

  co_return;
}

// ============================================================================
// Backtest Coverage Analysis
// ============================================================================

void Asset::compute_backtest_coverage(const std::string &start, const std::string &end) {
  // Clear previous results
  backtest.start = start;
  backtest.end = end;
  backtest.required_dates.clear();
  backtest.covered_dates.clear();
  backtest.missing_dates.clear();
  backtest.can_encode.clear();
  backtest.need_download.clear();
  backtest.coverage_percent = 0.0;

  // Step 1: Determine ground truth (required dates in backtest period)
  // Priority: Archive > Binary
  if (archive.scanned && archive.exists && !archive.dates.empty()) {
    // Use archive as ground truth
    for (const auto &date : archive.dates) {
      if (date >= start && date <= end) {
        backtest.required_dates.insert(date);
      }
    }
  } else if (binary.scanned && binary.exists && !binary.dates.empty()) {
    // Fallback: use binary as ground truth (not ideal, but better than nothing)
    for (const auto &date : binary.dates) {
      if (date >= start && date <= end) {
        backtest.required_dates.insert(date);
      }
    }
  } else {
    // No data available, cannot determine required dates
    return;
  }

  // Step 2: Compute binary coverage
  for (const auto &date : backtest.required_dates) {
    if (binary.dates.count(date)) {
      backtest.covered_dates.insert(date);
    } else {
      backtest.missing_dates.insert(date);
    }
  }

  // Step 3: Check archive availability for missing dates
  if (archive.scanned && archive.exists) {
    for (const auto &date : backtest.missing_dates) {
      if (archive.dates.count(date)) {
        backtest.can_encode.insert(date);
      } else {
        backtest.need_download.insert(date);
      }
    }
  } else {
    // No archive, all missing dates need download
    backtest.need_download = backtest.missing_dates;
  }

  // Step 4: Compute coverage percentage
  if (!backtest.required_dates.empty()) {
    backtest.coverage_percent = 100.0 * static_cast<float>(backtest.covered_dates.size()) /
                                static_cast<float>(backtest.required_dates.size());
  } else {
    backtest.coverage_percent = 0.0;
  }

  // Step 5: Calculate backtest range statistics (single pass)
  binary.backtest_snapshots = 0;
  binary.backtest_orders = 0;
  float backtest_snapshots_size = 0.0;
  float backtest_orders_size = 0.0;

  std::set<std::string> snap_dates_in_backtest;
  std::set<std::string> order_dates_in_backtest;

  // Single pass through all assets and dates
  for (const auto &item : items) {
    for (const auto &[date, info] : item.date_info) {
      if (date >= start && date <= end) {
        if (info.snapshots_encoded) {
          binary.backtest_snapshots += info.snapshot_count;
          backtest_snapshots_size += static_cast<float>(info.snapshots_file_size);
          snap_dates_in_backtest.insert(date);
        }
        if (info.orders_encoded) {
          binary.backtest_orders += info.order_count;
          backtest_orders_size += static_cast<float>(info.orders_file_size);
          order_dates_in_backtest.insert(date);
        }
      }
    }
  }

  binary.backtest_snapshots_size_gb = backtest_snapshots_size / (1024.0 * 1024.0 * 1024.0);
  binary.backtest_orders_size_gb = backtest_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.backtest_snap_days = snap_dates_in_backtest.size();
  binary.backtest_order_days = order_dates_in_backtest.size();
}
