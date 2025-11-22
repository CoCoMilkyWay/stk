#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "l2/L2.hpp"

namespace GUI::Database {

// Per-date database status
struct DateInfo {
  std::string database_dir;      // e.g. "database/2024/01/15/SH688001"
  std::string snapshots_file;    // Full path to *_snapshots_*.bin
  std::string orders_file;       // Full path to *_orders_*.bin
  size_t snapshot_count = 0;     // Extracted from filename
  size_t order_count = 0;        // Extracted from filename
  uint8_t encoded = 0;           // 0=no binary, 1=has binary
  uint8_t analyzed = 0;          // 0=not analyzed, 1=analyzed

  bool has_binaries() const {
    return !snapshots_file.empty() || !orders_file.empty();
  }
};

// Exchange metadata from crawler
struct ExchangeMetadata {
  // Names
  std::string name_cn;           // 简称 (primary)
  std::string name_cn_full;      // 公司全称
  std::string name_en;           // English name

  // Trading
  std::string listing_date;      // YYYY-MM-DD
  std::string delisting_date;    // Empty if active
  std::string status;            // "上市"/"退市"/"暂停"

  // Shares (actual units, not 万)
  uint64_t total_shares = 0;
  uint64_t circ_shares = 0;

  // Location
  std::string registered_addr;
  std::string province;

  // Classification
  std::string industry;
  std::string sector;            // 主板/科创板/创业板

  // Contact
  std::string website;
  std::string email;

  uint64_t last_updated_ts = 0;
  std::string data_source;       // "SH_API"/"SZ_EXCEL"
};

// Complete asset data
struct AssetInfo {
  // IDENTITY (immutable)
  size_t asset_id = 0;
  std::string asset_code;        // "000001" (6 digits)
  std::string exchange;          // "SH"/"SZ"
  L2::ExchangeType exchange_type = L2::ExchangeType::Unknown;

  // EXCHANGE METADATA
  ExchangeMetadata metadata;

  // DATE RANGE
  std::string start_date;        // YYYYMMDD
  std::string end_date;          // YYYYMMDD

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
  size_t get_encoded_count() const;
  size_t get_missing_count() const;
  size_t get_analyzed_count() const;
  size_t get_total_order_count() const;
  std::vector<std::string> get_missing_dates() const;
  std::string get_display_name() const;  // Name + UWV markers
};

// Task state
struct DatabaseState {
  // MAIN DATA
  std::vector<AssetInfo> assets;
  std::vector<std::string> all_dates;

  // GLOBAL STATS
  size_t total_assets() const { return assets.size(); }
  size_t total_trading_days() const;
  size_t total_encoded_dates() const;
  size_t total_missing_dates() const;
  size_t total_orders() const;
  double disk_usage_gb = 0.0;

  // UI STATE
  int selected_asset_idx = -1;
  bool filter_missing_only = false;
  bool filter_sh_only = false;
  bool filter_sz_only = false;
  std::string search_query;
  int sort_column = 0;
  bool sort_ascending = true;

  // ASYNC STATUS
  enum class ScanStatus { Idle, Scanning, Complete, Error };
  enum class CrawlStatus { Idle, FetchingSZ, FetchingSH, Complete, Error };
  ScanStatus scan_status = ScanStatus::Idle;
  CrawlStatus crawl_status = CrawlStatus::Idle;
  int crawled_count = 0;
  int total_to_crawl = 0;
  std::string status_message;
};

} // namespace GUI::Database

