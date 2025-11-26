// L2 Database Service Implementation
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/infrastructure/CoroScanner.hpp"
#include <algorithm>
#include <boost/asio/this_coro.hpp>
#include <filesystem>
#include <set>

namespace GUI::Database {

namespace asio = boost::asio;

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<void> L2DatabaseService::scan_database() {
  // Skip if already scanned
  if (scanned_once_) {
    co_return;
  }
  
  scan_status_ = L2ScanStatus::Scanning;
  error_message_.clear();

  co_await asio::this_coro::executor;

  try {
    // Create temporary scan result
    ScanResult result;

    // Run scanner
    CoroScanner scanner(result);
    scanner.scan_binary_directory(database_dir_);

    // If no assets found in database, try fallback to targets.json
    if (result.assets.empty()) {
      scanner.load_targets_json("config/targets.json");
    }

    // Move results to service
    assets_ = std::move(result.assets);
    all_dates_ = std::move(result.all_dates);

    scan_status_ = L2ScanStatus::Scanned;
    scanned_once_ = true; // Mark as scanned to prevent repeated scans

  } catch (const std::exception &e) {
    scan_status_ = L2ScanStatus::Error;
    error_message_ = e.what();
  }
}

awaitable<void> L2DatabaseService::refresh_asset(size_t asset_idx) {
  if (asset_idx >= assets_.size()) {
    co_return;
  }

  co_await asio::this_coro::executor;

  // Rescan specific asset
  assets_[asset_idx].scan_existing_binaries();
}

// ============================================================================
// Statistics
// ============================================================================

L2Summary L2DatabaseService::get_summary(const std::string &backtest_start,
                                         const std::string &backtest_end,
                                         const std::vector<std::vector<std::string>> &trading_days) const {
  // Suppress unused variable warning - config_ is reserved for future use
  (void)config_;
  namespace fs = std::filesystem;
  L2Summary summary;

  // Database date range from all_dates_
  if (!all_dates_.empty()) {
    summary.database_range_start = all_dates_.front();
    summary.database_range_end = all_dates_.back();
    summary.database_trade_days = all_dates_.size();
  }

  // Backtest date range (from config)
  summary.backtest_range_start = backtest_start;
  summary.backtest_range_end = backtest_end;

  // Calculate backtest statistics
  if (!backtest_start.empty() && !backtest_end.empty()) {
    // Collect trade days from JSON (in backtest range)
    std::set<std::string> json_trade_days;
    if (!trading_days.empty()) {
      for (const auto &day : trading_days) {
        if (day.size() >= 2) {
          std::string date = day[0];       // First column is date (YYYY-MM-DD)
          std::string is_trading = day[1]; // Second column is is_trading_day ("0" or "1")

          // Remove dashes to get YYYYMMDD format
          date.erase(std::remove(date.begin(), date.end(), '-'), date.end());

          // Only count actual trading days in backtest range
          if (date >= backtest_start && date <= backtest_end && is_trading == "1") {
            json_trade_days.insert(date);
          }
        }
      }
    }
    summary.backtest_trade_days_in_json = json_trade_days.size();

    // Collect trade days with binaries (in backtest range)
    std::set<std::string> binary_trade_days;
    for (const auto &date : all_dates_) {
      if (date >= backtest_start && date <= backtest_end) {
        binary_trade_days.insert(date);
      }
    }
    summary.backtest_trade_days_with_binary = binary_trade_days.size();

    // Calculate non-intersection (error days)
    // Days in JSON but not in binary OR days in binary but not in JSON
    std::vector<std::string> json_only;
    std::vector<std::string> binary_only;

    std::set_difference(json_trade_days.begin(), json_trade_days.end(),
                        binary_trade_days.begin(), binary_trade_days.end(),
                        std::back_inserter(json_only));

    std::set_difference(binary_trade_days.begin(), binary_trade_days.end(),
                        json_trade_days.begin(), json_trade_days.end(),
                        std::back_inserter(binary_only));

    // Combine both differences into error_dates
    summary.backtest_error_dates = json_only;
    summary.backtest_error_dates.insert(summary.backtest_error_dates.end(),
                                        binary_only.begin(), binary_only.end());
    std::sort(summary.backtest_error_dates.begin(), summary.backtest_error_dates.end());

    summary.backtest_error_days = summary.backtest_error_dates.size();

    // Check if backtest is within database range
    summary.backtest_in_range = !summary.database_range_start.empty() &&
                                !summary.database_range_end.empty() &&
                                backtest_start >= summary.database_range_start &&
                                backtest_end <= summary.database_range_end;
  }

  summary.total_assets = assets_.size();
  summary.total_trading_days = 0;
  summary.total_encoded_days = 0;
  summary.total_missing_days = 0;

  size_t fully_encoded_assets = 0;
  size_t total_snapshots_size = 0; // in bytes
  size_t total_orders_size = 0;    // in bytes

  // Track unique dates with binaries in backtest range
  std::set<std::string> backtest_dates_with_snapshots;
  std::set<std::string> backtest_dates_with_orders;

  for (const auto &asset : assets_) {
    size_t total_days = asset.get_total_trading_days();
    size_t encoded_days = asset.get_encoded_count();
    size_t missing_days = asset.get_missing_count();

    summary.total_trading_days += total_days;
    summary.total_encoded_days += encoded_days;
    summary.total_missing_days += missing_days;

    if (missing_days == 0 && total_days > 0) {
      fully_encoded_assets++;
    }

    // Track missing dates for this asset
    std::vector<std::string> missing_snapshots_dates;
    std::vector<std::string> missing_orders_dates;
    bool has_missing_snapshots = false;
    bool has_missing_orders = false;

    for (const auto &[date, info] : asset.date_info) {
      // Check if this date is in backtest range
      bool in_backtest_range = !backtest_start.empty() && !backtest_end.empty() &&
                               date >= backtest_start && date <= backtest_end;

      // Count encoded files
      if (info.snapshots_encoded) {
        summary.snapshots_encoded_count++;

        // Track unique dates with snapshots in backtest range
        if (in_backtest_range) {
          backtest_dates_with_snapshots.insert(date);
        }

        // Get actual file size
        if (!info.snapshots_file.empty() && fs::exists(info.snapshots_file)) {
          try {
            total_snapshots_size += fs::file_size(info.snapshots_file);
          } catch (...) {
            // Ignore errors
          }
        }
      } else {
        has_missing_snapshots = true;
        if (in_backtest_range) {
          missing_snapshots_dates.push_back(date);
        }
      }

      if (info.orders_encoded) {
        summary.orders_encoded_count++;

        // Track unique dates with orders in backtest range
        if (in_backtest_range) {
          backtest_dates_with_orders.insert(date);
        }

        // Get actual file size
        if (!info.orders_file.empty() && fs::exists(info.orders_file)) {
          try {
            total_orders_size += fs::file_size(info.orders_file);
          } catch (...) {
            // Ignore errors
          }
        }
      } else {
        has_missing_orders = true;
        if (in_backtest_range) {
          missing_orders_dates.push_back(date);
        }
      }
    }

    // Count assets with missing data
    if (has_missing_snapshots) {
      summary.assets_missing_snapshots++;
    }
    if (has_missing_orders) {
      summary.assets_missing_orders++;
    }

    // Store missing dates for this asset (only if there are missing dates in backtest range)
    if (!missing_snapshots_dates.empty()) {
      std::string asset_name = asset.asset_code + "." + asset.exchange;
      summary.missing_snapshots_by_asset[asset_name] = missing_snapshots_dates;
    }
    if (!missing_orders_dates.empty()) {
      std::string asset_name = asset.asset_code + "." + asset.exchange;
      summary.missing_orders_by_asset[asset_name] = missing_orders_dates;
    }
  }

  // Set backtest encoded counts to unique date counts
  summary.backtest_snapshots_encoded = backtest_dates_with_snapshots.size();
  summary.backtest_orders_encoded = backtest_dates_with_orders.size();

  summary.encoded_assets = fully_encoded_assets;
  summary.missing_assets = summary.total_assets - fully_encoded_assets;

  if (summary.total_trading_days > 0) {
    summary.coverage_percent =
        (double)summary.total_encoded_days / summary.total_trading_days * 100.0;
  }

  // Convert sizes to GB
  summary.snapshots_size_gb = total_snapshots_size / (1024.0 * 1024.0 * 1024.0);
  summary.orders_size_gb = total_orders_size / (1024.0 * 1024.0 * 1024.0);
  summary.disk_usage_gb = summary.snapshots_size_gb + summary.orders_size_gb;

  return summary;
}

} // namespace GUI::Database
