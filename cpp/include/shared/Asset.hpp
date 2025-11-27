#pragma once

#include "codec/L2_DataType.hpp"

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations
namespace L2 {
class BinaryDecoder_L2;
}

// ============================================================================
// PER-DATE STATUS
// ============================================================================

struct DateInfo {
  // Paths
  std::string database_dir;   // e.g. "database/2024/01/15/SH688001"
  std::string snapshots_file; // Full path to *_snapshots_*.bin
  std::string orders_file;    // Full path to *_orders_*.bin

  // Counts
  size_t snapshot_count = 0; // Extracted from filename
  size_t order_count = 0;    // Extracted from filename

  // File sizes (bytes, cached during scan to avoid repeated I/O)
  size_t snapshots_file_size = 0;
  size_t orders_file_size = 0;

  // Status flags (fine-grained)
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
// SINGLE ASSET
// ============================================================================

struct AssetItem {
  // IDENTITY (immutable)
  size_t asset_id = 0;
  std::string asset_code; // "000001" (6 digits)
  std::string asset_name; // "平安银行"
  std::string exchange;   // "SH"/"SZ"
  L2::ExchangeType exchange_type = L2::ExchangeType::UNKNOWN;

  // DATE RANGE
  std::string start_date; // YYYYMMDD
  std::string end_date;   // YYYYMMDD

  // PER-DATE STATUS (sparse storage - only dates with actual data)
  std::unordered_map<std::string, DateInfo> date_info;

  // WORKER ASSIGNMENT
  int assigned_worker_id = -1;

  // CONSTRUCTORS
  AssetItem() = default;
  AssetItem(size_t id, std::string code, std::string name, std::string exch, std::string start, std::string end);

  // STATISTICS
  size_t get_total_trading_days() const;
  size_t get_encoded_count() const; // Fully encoded (both snapshots and orders)
  size_t get_snapshots_encoded_count() const;
  size_t get_orders_encoded_count() const;
  size_t get_missing_count() const;
  size_t get_analyzed_count() const;
  size_t get_total_order_count() const;
  size_t get_total_snapshot_count() const;
  std::vector<std::string> get_missing_dates() const;
  std::string get_display_name() const;
};

// ============================================================================
// ALL ASSETS WITH DATABASE METADATA
// ============================================================================

struct Asset {
  // ========================================
  // Core Data
  // ========================================
  std::vector<AssetItem> items;
  std::vector<std::string> all_dates; // All known trading days (from scan)
  
  // ========================================
  // Per-Date Statistics (for Browser, computed once after loading stock_info)
  // ========================================
  struct DateStats {
    size_t total_assets = 0;           // Total assets listed on this date (considering delist)
    size_t assets_with_snapshots = 0;  // Assets with snapshot data
    size_t assets_with_orders = 0;     // Assets with order data
    size_t assets_with_both = 0;       // Assets with both snapshot and order data
  };
  std::unordered_map<std::string, DateStats> date_stats; // date -> stats (computed once)
  
  // ========================================
  // Binary Database Metadata
  // ========================================
  struct {
    bool scanned = false;
    bool exists = false;
    std::string path;
    
    // Date coverage
    std::string min_date;        // YYYYMMDD
    std::string max_date;        // YYYYMMDD
    std::set<std::string> dates; // All fully encoded dates
    
    // Statistics (computed from items)
    size_t total_assets = 0;
    size_t encoded_assets = 0;  // Assets with any encoded data
    size_t complete_assets = 0; // Assets fully encoded
    
    // Whole database statistics
    size_t total_snapshots = 0;
    size_t total_orders = 0;
    double snapshots_size_gb = 0.0;
    double orders_size_gb = 0.0;
    
    // Whole database days (trading days with at least one asset having data)
    size_t database_snap_days = 0;  // Days with at least one snapshot in database
    size_t database_order_days = 0; // Days with at least one order in database

    // Backtest range statistics (only within backtest period)
    size_t backtest_snapshots = 0;
    size_t backtest_orders = 0;
    double backtest_snapshots_size_gb = 0.0;
    double backtest_orders_size_gb = 0.0;
    
    // Backtest range days (trading days with at least one asset having data)
    size_t backtest_snap_days = 0;  // Days with at least one snapshot in backtest
    size_t backtest_order_days = 0; // Days with at least one order in backtest
  } binary;
  
  // ========================================
  // Archive Database Metadata
  // ========================================
  struct {
    bool scanned = false;
    bool exists = false;
    std::string path;
    
    // Date coverage
    std::string min_date;        // YYYYMMDD
    std::string max_date;        // YYYYMMDD
    std::set<std::string> dates; // All available archive dates
    
    // Statistics (computed from file scan)
    size_t total_files = 0;
    double total_size_gb = 0.0;
  } archive;
  
  // ========================================
  // Backtest Coverage (computed on demand)
  // ========================================
  struct {
    std::string start; // From config
    std::string end;   // From config
    
    // Ground truth (from archive if exists, else from binary)
    std::set<std::string> required_dates;
    
    // Binary coverage
    std::set<std::string> covered_dates;
    std::set<std::string> missing_dates;
    
    // Archive availability for missing dates
    std::set<std::string> can_encode;
    std::set<std::string> need_download;
    
    // Statistics
    double coverage_percent = 0.0;
  } backtest;
  
  // ========================================
  // Methods
  // ========================================
  // Scan operations (heavy, call once)
  void scan_binary_database(const std::string &database_dir, const std::string &binary_extension);
  void scan_archive_database(const std::string &archive_dir, const std::string &archive_extension);
  
  // Coverage analysis (lightweight, call after config changes)
  // Also computes backtest range statistics using cached data
  void compute_backtest_coverage(const std::string &start, const std::string &end);
  
  // Compute per-date browser statistics (requires stock_info for delist dates)
  // Should be called once after loading stock_info and stock_days
  template<typename StockInfoMap, typename StockDaysVec>
  void compute_browser_statistics(const StockInfoMap &stock_info, const StockDaysVec &stock_days) {
    date_stats.clear();
    
    // Helper function to convert YYYY-MM-DD to YYYYMMDD
    auto date_to_dense = [](const std::string &date_dashed) -> std::string {
      if (date_dashed.size() == 10 && date_dashed[4] == '-' && date_dashed[7] == '-') {
        return date_dashed.substr(0, 4) + date_dashed.substr(5, 2) + date_dashed.substr(8, 2);
      }
      return "";
    };
    
    // For each date in stock_days, but only within database range, count listed assets and L2 data availability
    for (const auto &day_info : stock_days) {
      if (day_info.size() < 2)
        continue;
      
      const std::string &date_dashed = day_info[0]; // YYYY-MM-DD
      std::string date_dense = date_to_dense(date_dashed);
      if (date_dense.empty())
        continue;
      
      // Only process dates within database range
      if (!binary.min_date.empty() && !binary.max_date.empty()) {
        if (date_dense < binary.min_date || date_dense > binary.max_date)
          continue;
      }
      
      DateStats &stats = date_stats[date_dense];
      
      for (const auto &asset : items) {
        // Build full stock code (e.g., "sh.600128") - convert to lowercase
        std::string exchange_lower = asset.exchange;
        std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
        std::string full_code = exchange_lower + "." + asset.asset_code;
        
        // Get listing and delisting dates from stock_info
        std::string list_date_dense;
        std::string delist_date_dense;
        auto info_it = stock_info.find(full_code);
        if (info_it != stock_info.end()) {
          if (!info_it->second.ipoDate.empty()) {
            list_date_dense = date_to_dense(info_it->second.ipoDate);
          }
          if (!info_it->second.outDate.empty()) {
            delist_date_dense = date_to_dense(info_it->second.outDate);
          }
        }
        
        // Check if asset should be listed on this date
        // Listed if: date >= ipoDate AND (not delisted OR date <= outDate)
        if (!list_date_dense.empty() && date_dense < list_date_dense)
          continue;
        if (!delist_date_dense.empty() && date_dense > delist_date_dense)
          continue;
        
        // This asset should be listed on this date
        stats.total_assets++;
        
        // Check if we have L2 data for this asset on this date
        auto date_it = asset.date_info.find(date_dense);
        if (date_it != asset.date_info.end()) {
          if (date_it->second.snapshots_encoded) {
            stats.assets_with_snapshots++;
          }
          if (date_it->second.orders_encoded) {
            stats.assets_with_orders++;
          }
          if (date_it->second.is_fully_encoded()) {
            stats.assets_with_both++;
          }
        }
      }
    }
  }
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

namespace Utils {
// database/2023/01/03/000023.SZ/000023.SZ_orders_26536.bin
// database/2023/01/03/000023.SZ/000023.SZ_snapshots_2848.bin

  inline std::string generate_archive_path(const std::string &base_dir, const std::string &date_str, const std::string &extension) {
    return base_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(0, 6) + "/" + date_str + extension;
  }
  
inline std::string generate_temp_asset_dir(const std::string &database_dir, const std::string &date_str, const std::string &asset_code, const std::string &exchange) {
  return database_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code + "." + exchange;
  }
  
  inline std::set<std::string> collect_dates_from_archives(const std::string &l2_archive_base, const std::string &archive_extension) {
    std::set<std::string> dates;
    if (!std::filesystem::exists(l2_archive_base))
      return dates;
  
    // Archive structure: archive_base/YYYY/YYYYMM/YYYYMMDD.ext
    for (const auto &year_entry : std::filesystem::directory_iterator(l2_archive_base)) {
      if (!year_entry.is_directory())
        continue;
  
      for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
        if (!month_entry.is_directory())
          continue;
  
        for (const auto &file_entry : std::filesystem::directory_iterator(month_entry.path())) {
          const std::string ext = file_entry.path().extension().string();
          if (ext == archive_extension) {
            const std::string filename = file_entry.path().stem().string();
            if (filename.size() == 8 && std::all_of(filename.begin(), filename.end(), ::isdigit)) {
              dates.insert(filename);
            }
          }
        }
      }
    }
    return dates;
  }
  
  inline std::set<std::string> collect_dates_from_binaries(const std::string &temp_dir_base) {
    std::set<std::string> dates;
    if (!std::filesystem::exists(temp_dir_base))
      return dates;
  
    // Binary structure: database_dir/YYYY/MM/DD/asset_code/
    for (const auto &year_entry : std::filesystem::directory_iterator(temp_dir_base)) {
      if (!year_entry.is_directory())
        continue;
      const std::string year_str = year_entry.path().filename().string();
  
      for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
        if (!month_entry.is_directory())
          continue;
        const std::string month_str = month_entry.path().filename().string();
  
        for (const auto &day_entry : std::filesystem::directory_iterator(month_entry.path())) {
          if (!day_entry.is_directory())
            continue;
          const std::string day_str = day_entry.path().filename().string();
  
          const std::string date_str = year_str + month_str + day_str;
          dates.insert(date_str);
        }
      }
    }
    return dates;
  }
  } // namespace Utils
