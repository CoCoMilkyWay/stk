// L2 Database Service Implementation
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/infrastructure/CoroScanner.hpp"
#include <boost/asio/this_coro.hpp>
#include <algorithm>
#include <filesystem>

namespace GUI::Database {

namespace asio = boost::asio;

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<void> L2DatabaseService::scan_database() {
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
  if (!backtest_start.empty() && !backtest_end.empty() && !trading_days.empty()) {
    // Count trading days in backtest range (only days with is_trading_day == "1")
    for (const auto &day : trading_days) {
      if (day.size() >= 2) {
        std::string date = day[0]; // First column is date (YYYY-MM-DD)
        std::string is_trading = day[1]; // Second column is is_trading_day ("0" or "1")
        
        // Remove dashes to get YYYYMMDD format
        date.erase(std::remove(date.begin(), date.end(), '-'), date.end());
        
        // Only count actual trading days
        if (date >= backtest_start && date <= backtest_end && is_trading == "1") {
          summary.backtest_trade_days++;
        }
      }
    }
    
    // Calculate missing days: trade days in backtest range that don't have binaries
    // Count days in all_dates_ that fall within backtest range
    size_t backtest_encoded_days = 0;
    for (const auto &date : all_dates_) {
      if (date >= backtest_start && date <= backtest_end) {
        backtest_encoded_days++;
      }
    }
    summary.backtest_missing_days = summary.backtest_trade_days > backtest_encoded_days 
                                    ? summary.backtest_trade_days - backtest_encoded_days 
                                    : 0;
    
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

    // Count encoded snapshots and orders, and accumulate file sizes
    bool has_missing_snapshots = false;
    bool has_missing_orders = false;
    
    for (const auto &[date, info] : asset.date_info) {
      // Check if this date is in backtest range
      bool in_backtest_range = !backtest_start.empty() && !backtest_end.empty() &&
                               date >= backtest_start && date <= backtest_end;
      
      // Count encoded files
      if (info.snapshots_encoded) {
        summary.snapshots_encoded_count++;
        
        // Count snapshots in backtest range
        if (in_backtest_range) {
          summary.backtest_snapshots_encoded++;
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
      }
      
      if (info.orders_encoded) {
        summary.orders_encoded_count++;
        
        // Count orders in backtest range
        if (in_backtest_range) {
          summary.backtest_orders_encoded++;
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
      }
    }
    
    // Count assets with missing data
    if (has_missing_snapshots) {
      summary.assets_missing_snapshots++;
    }
    if (has_missing_orders) {
      summary.assets_missing_orders++;
    }
  }

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
