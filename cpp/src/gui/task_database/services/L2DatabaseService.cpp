// L2 Database Service Implementation
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/infrastructure/CoroScanner.hpp"
#include <boost/asio/this_coro.hpp>
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

L2Summary L2DatabaseService::get_summary() const {
  namespace fs = std::filesystem;
  L2Summary summary;

  // Date range from all_dates_
  if (!all_dates_.empty()) {
    summary.date_range_start = all_dates_.front();
    summary.date_range_end = all_dates_.back();
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
      // Count encoded files
      if (info.snapshots_encoded) {
        summary.snapshots_encoded_count++;
        
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
