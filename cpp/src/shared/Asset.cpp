#include "shared/Asset.hpp"
#include "codec/binary_decoder_L2.hpp"
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

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
// Binary Database Scan and Statistics
// ============================================================================

void Asset::scan_binary_database(const std::string &database_dir, const std::string &binary_extension) {
  namespace fs = std::filesystem;

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
    return;
  }

  // EXTREME OPTIMIZATION: Parallel scan by month
  // Collect all year/month directories first
  struct MonthPath {
    std::string path;
    std::string year_str;
    std::string month_str;
  };
  std::vector<MonthPath> month_paths;

  for (const auto &year_entry : fs::directory_iterator(database_dir)) {
    if (!year_entry.is_directory())
      continue;
    std::string year_str = year_entry.path().filename().string();
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      month_paths.push_back({month_entry.path().string(),
                             year_str,
                             month_entry.path().filename().string()});
    }
  }

  // Build asset lookup map (shared, read-only)
  std::unordered_map<std::string, size_t> asset_map;
  for (size_t i = 0; i < items.size(); ++i) {
    asset_map[items[i].asset_code + "." + items[i].exchange] = i;
  }

  // Shared data structures (protected by mutex)
  std::mutex data_mutex;
  std::set<std::string> all_dates_set;
  std::set<std::string> snap_dates_set;
  std::set<std::string> order_dates_set;
  std::map<std::string, size_t> date_coverage;
  size_t total_snapshots = 0;
  size_t total_orders = 0;
  double total_snapshots_size = 0.0;
  double total_orders_size = 0.0;

  // Parallel scan function
  auto scan_month = [&](const MonthPath &month_path) {
    // Local accumulators (no locking needed)
    std::set<std::string> local_dates;
    std::set<std::string> local_snap_dates;
    std::set<std::string> local_order_dates;
    std::map<std::string, size_t> local_date_coverage;
    size_t local_total_snapshots = 0;
    size_t local_total_orders = 0;
    double local_snapshots_size = 0.0;
    double local_orders_size = 0.0;

    // Temporary storage for date_info updates (per asset)
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

        // Scan files
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
            local_snapshots_size += static_cast<double>(di.snapshots_file_size);
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
            local_orders_size += static_cast<double>(di.orders_file_size);
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

    // Merge local results into shared data (single lock per month)
    std::lock_guard<std::mutex> lock(data_mutex);
    all_dates_set.insert(local_dates.begin(), local_dates.end());
    snap_dates_set.insert(local_snap_dates.begin(), local_snap_dates.end());
    order_dates_set.insert(local_order_dates.begin(), local_order_dates.end());
    for (const auto &[date, count] : local_date_coverage) {
      date_coverage[date] += count;
    }
    total_snapshots += local_total_snapshots;
    total_orders += local_total_orders;
    total_snapshots_size += local_snapshots_size;
    total_orders_size += local_orders_size;

    // Update items date_info
    for (const auto &[asset_idx, date_map] : local_date_info) {
      for (const auto &[date, info] : date_map) {
        items[asset_idx].date_info[date] = info;
      }
    }
  };

  // Launch parallel scans
  unsigned int num_threads = std::min(std::thread::hardware_concurrency(),
                                      static_cast<unsigned int>(month_paths.size()));
  if (num_threads < 2)
    num_threads = 1;

  if (num_threads == 1 || month_paths.size() <= 1) {
    // Single-threaded fallback
    for (const auto &mp : month_paths) {
      scan_month(mp);
    }
  } else {
    // Parallel execution
    std::vector<std::future<void>> futures;
    size_t months_per_thread = (month_paths.size() + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
      size_t start = t * months_per_thread;
      size_t end = std::min(start + months_per_thread, month_paths.size());
      if (start >= end)
        break;

      futures.push_back(std::async(std::launch::async, [&, start, end]() {
        for (size_t i = start; i < end; ++i) {
          scan_month(month_paths[i]);
        }
      }));
    }

    // Wait for all threads
    for (auto &f : futures) {
      f.get();
    }
  }

  // Store results
  all_dates.assign(all_dates_set.begin(), all_dates_set.end());
  binary.total_assets = items.size();
  binary.total_snapshots = total_snapshots;
  binary.total_orders = total_orders;
  binary.snapshots_size_gb = total_snapshots_size / (1024.0 * 1024.0 * 1024.0);
  binary.orders_size_gb = total_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.database_snap_days = snap_dates_set.size();
  binary.database_order_days = order_dates_set.size();

  // Fully encoded dates
  binary.dates.clear();
  for (const auto &[date, count] : date_coverage) {
    if (count == binary.total_assets) {
      binary.dates.insert(date);
    }
  }

  // Date range
  if (!all_dates.empty()) {
    binary.min_date = all_dates.front();
    binary.max_date = all_dates.back();
    
    // Update all assets to use actual database date range
    for (auto &item : items) {
      item.start_date = binary.min_date;
      item.end_date = binary.max_date;
    }
  } else {
    binary.min_date.clear();
    binary.max_date.clear();
  }

  // Count encoded and complete assets
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
}

// ============================================================================
// Archive Database Scan and Statistics
// ============================================================================

void Asset::scan_archive_database(const std::string &archive_dir, const std::string &archive_extension) {
  namespace fs = std::filesystem;

  archive.scanned = true;
  archive.path = archive_dir;
  archive.exists = fs::exists(archive_dir) && fs::is_directory(archive_dir);

  if (!archive.exists) {
    // Clear all data if not exists
    archive.dates.clear();
    archive.min_date.clear();
    archive.max_date.clear();
    archive.total_files = 0;
    archive.total_size_gb = 0.0;

    // If all_dates is empty, archive scan failed
    if (all_dates.empty()) {
      all_dates.clear();
    }
    return;
  }

  // Scan archive structure: archive_dir/YYYY/YYYYMM/YYYYMMDD.ext
  std::set<std::string> archive_dates_set;
  size_t total_files = 0;
  double total_size = 0.0; // bytes

  try {
    // Iterate through year folders
    for (const auto &year_entry : fs::directory_iterator(archive_dir)) {
      if (!year_entry.is_directory())
        continue;

      // Iterate through month folders
      for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
        if (!month_entry.is_directory())
          continue;

        // Iterate through archive files
        for (const auto &file_entry : fs::directory_iterator(month_entry.path())) {
          if (!file_entry.is_regular_file())
            continue;

          const std::string ext = file_entry.path().extension().string();
          if (ext == archive_extension) {
            const std::string filename = file_entry.path().stem().string();

            // Validate date format: YYYYMMDD (8 digits)
            if (filename.size() == 8 && std::all_of(filename.begin(), filename.end(), ::isdigit)) {
              archive_dates_set.insert(filename);
              total_files++;

              // Get file size
              try {
                total_size += static_cast<double>(fs::file_size(file_entry.path()));
              } catch (const std::exception &e) {
                // Ignore file size errors
              }
            }
          }
        }
      }
    }
  } catch (const std::exception &e) {
    // If scan fails, mark as not exists
    archive.exists = false;
    archive.dates.clear();
    archive.min_date.clear();
    archive.max_date.clear();
    archive.total_files = 0;
    archive.total_size_gb = 0.0;
    if (all_dates.empty()) {
      all_dates.clear();
    }
    return;
  }

  // Store results
  archive.dates = archive_dates_set;
  archive.total_files = total_files;
  archive.total_size_gb = total_size / (1024.0 * 1024.0 * 1024.0);

  // Set date range
  if (!archive.dates.empty()) {
    archive.min_date = *archive.dates.begin();
    archive.max_date = *archive.dates.rbegin();
  } else {
    archive.min_date.clear();
    archive.max_date.clear();
  }

  // If all_dates is empty, use archive dates as ground truth
  if (all_dates.empty()) {
    all_dates.assign(archive.dates.begin(), archive.dates.end());

    // Initialize date_info for each asset (for future encoding)
    for (auto &item : items) {
      for (const auto &date_str : all_dates) {
        if (date_str >= item.start_date && date_str <= item.end_date) {
          // Don't create DateInfo yet, will be created during encoding
          // Just reserve space in the map is not necessary
        }
      }
    }
  }

  // Archive statistics already computed during scan above
}

// Deleted: update_binary_statistics() and update_archive_statistics()
// All statistics are now computed inline during scan and coverage analysis

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
    backtest.coverage_percent = 100.0 * static_cast<double>(backtest.covered_dates.size()) /
                                static_cast<double>(backtest.required_dates.size());
  } else {
    backtest.coverage_percent = 0.0;
  }

  // Step 5: Calculate backtest range statistics (single pass)
  binary.backtest_snapshots = 0;
  binary.backtest_orders = 0;
  double backtest_snapshots_size = 0.0;
  double backtest_orders_size = 0.0;

  std::set<std::string> snap_dates_in_backtest;
  std::set<std::string> order_dates_in_backtest;

  // Single pass through all assets and dates
  for (const auto &item : items) {
    for (const auto &[date, info] : item.date_info) {
      if (date >= start && date <= end) {
        if (info.snapshots_encoded) {
          binary.backtest_snapshots += info.snapshot_count;
          backtest_snapshots_size += static_cast<double>(info.snapshots_file_size);
          snap_dates_in_backtest.insert(date);
        }
        if (info.orders_encoded) {
          binary.backtest_orders += info.order_count;
          backtest_orders_size += static_cast<double>(info.orders_file_size);
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
