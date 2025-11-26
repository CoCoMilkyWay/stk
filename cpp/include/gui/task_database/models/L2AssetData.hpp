// L2 Database asset data structures (simplified, no exchange metadata)
#pragma once

#include "codec/L2_DataType.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Per-date database status
// ============================================================================

struct DateInfo {
  std::string database_dir;      // e.g. "database/2024/01/15/SH688001"
  std::string snapshots_file;    // Full path to *_snapshots_*.bin
  std::string orders_file;       // Full path to *_orders_*.bin
  size_t snapshot_count = 0;     // Extracted from filename
  size_t order_count = 0;        // Extracted from filename
  uint8_t snapshots_encoded = 0; // 0=no snapshots binary, 1=has snapshots
  uint8_t orders_encoded = 0;    // 0=no orders binary, 1=has orders
  uint8_t analyzed = 0;          // 0=not analyzed, 1=analyzed

  bool has_binaries() const {
    return !snapshots_file.empty() || !orders_file.empty();
  }

  bool is_fully_encoded() const {
    return snapshots_encoded && orders_encoded;
  }
};

// ============================================================================
// Complete asset data (simplified)
// ============================================================================

struct AssetInfo {
  // IDENTITY (immutable)
  size_t asset_id = 0;
  std::string asset_code; // "000001" (6 digits)
  std::string exchange;   // "SH"/"SZ"
  L2::ExchangeType exchange_type = L2::ExchangeType::UNKNOWN;

  // DATE RANGE
  std::string start_date; // YYYYMMDD
  std::string end_date;   // YYYYMMDD

  // PER-DATE STATUS
  std::unordered_map<std::string, DateInfo> date_info;

  // WORKER ASSIGNMENT
  int assigned_worker_id = -1;

  // CONSTRUCTOR
  AssetInfo() = default;
  AssetInfo(size_t id, std::string code, std::string exch,
            std::string start, std::string end);

  // FILESYSTEM OPS
  void init_paths(const std::string &db_dir,
                  const std::vector<std::string> &all_dates);
  void scan_existing_binaries();

  // STATISTICS
  size_t get_total_trading_days() const;
  size_t get_encoded_count() const; // Fully encoded (both snapshots and orders)
  size_t get_snapshots_encoded_count() const;
  size_t get_orders_encoded_count() const;
  size_t get_missing_count() const;
  size_t get_analyzed_count() const;
  size_t get_total_order_count() const;
  size_t get_total_snapshot_count() const; // Total snapshot count across all dates
  std::vector<std::string> get_missing_dates() const;
  std::string get_display_name() const; // Simple display name
};

// ============================================================================
// L2 Database Summary (detailed statistics)
// ============================================================================

struct L2Summary {
  // Database date range
  std::string database_range_start; // YYYYMMDD
  std::string database_range_end;   // YYYYMMDD
  size_t database_trade_days = 0;   // Number of trading days in database

  // Backtest date range (from config)
  std::string backtest_range_start;              // YYYYMMDD
  std::string backtest_range_end;                // YYYYMMDD
  size_t backtest_trade_days_in_json = 0;        // Trading days defined in stock_days.json
  size_t backtest_trade_days_with_binary = 0;    // Trading days that have binaries in L2 database
  size_t backtest_error_days = 0;                // Non-intersection count between json and binary
  std::vector<std::string> backtest_error_dates; // Dates in non-intersection (for tooltip)
  bool backtest_in_range = false;                // Is backtest range within database range?

  // Assets
  size_t total_assets = 0;
  size_t encoded_assets = 0;
  size_t missing_assets = 0;
  double coverage_percent = 0.0;

  // Snapshots (all database)
  size_t snapshots_encoded_count = 0;
  double snapshots_size_gb = 0.0;

  // Orders (all database)
  size_t orders_encoded_count = 0;
  double orders_size_gb = 0.0;

  // Snapshots/Orders in backtest range
  size_t backtest_snapshots_encoded = 0;
  size_t backtest_orders_encoded = 0;

  // Missing data per asset
  size_t assets_missing_snapshots = 0;
  size_t assets_missing_orders = 0;
  std::map<std::string, std::vector<std::string>> missing_snapshots_by_asset; // asset -> missing dates
  std::map<std::string, std::vector<std::string>> missing_orders_by_asset;    // asset -> missing dates

  // Legacy fields (optional)
  size_t total_trading_days = 0;
  size_t total_encoded_days = 0;
  size_t total_missing_days = 0;
  double disk_usage_gb = 0.0; // Total (snapshots + orders)
};

} // namespace GUI::Database
