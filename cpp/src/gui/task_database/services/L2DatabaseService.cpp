// L2 Database Service Implementation
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/infrastructure/CoroScanner.hpp"
#include <boost/asio/this_coro.hpp>

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
  L2Summary summary;

  summary.total_assets = assets_.size();
  summary.total_trading_days = 0;
  summary.total_encoded_days = 0;
  summary.total_missing_days = 0;

  size_t fully_encoded_assets = 0;

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
  }

  summary.encoded_assets = fully_encoded_assets;
  summary.missing_assets = summary.total_assets - fully_encoded_assets;

  if (summary.total_trading_days > 0) {
    summary.coverage_percent =
        (double)summary.total_encoded_days / summary.total_trading_days * 100.0;
  }

  // Estimate disk usage (rough calculation)
  // Assume average: 1 snapshot ~= 500 bytes, 1 order ~= 200 bytes
  size_t total_snapshots = 0;
  size_t total_orders = 0;
  for (const auto &asset : assets_) {
    for (const auto &[date, info] : asset.date_info) {
      total_snapshots += info.snapshot_count;
      total_orders += info.order_count;
    }
  }

  double bytes = total_snapshots * 500.0 + total_orders * 200.0;
  summary.disk_usage_gb = bytes / (1024.0 * 1024.0 * 1024.0);

  return summary;
}

} // namespace GUI::Database
