#pragma once

#include "boost/asio/awaitable.hpp"
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
namespace GUI::Database {
class ScanThreadPool;
}
struct AssetInfo;

// ============================================================================
// PER-DATE STATUS
// ============================================================================

// 一个 (资产, 日期). 只有 orders 一种产物 —— 快照不再编码 (全项目无人读取,
// 特征计算只吃 orders 并靠 LimitOrderBook 重建盘口).
struct DateInfo {
  std::string orders_file; // orders/YYYY/MM/DD/<CODE>.<EX>.bin

  size_t order_count = 0;      // 由文件头 original_size 推出
  size_t orders_file_size = 0; // 扫描时缓存, 免得反复 stat

  uint8_t orders_encoded = 0; // 0=无二进制, 1=已编码
  uint8_t analyzed = 0;       // 0=未分析, 1=已分析

  bool has_binaries() const {
    return !orders_file.empty();
  }

  bool is_fully_encoded() const {
    return orders_encoded != 0;
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
  size_t get_encoded_count() const;
  size_t get_orders_encoded_count() const;
  size_t get_missing_count() const;
  size_t get_analyzed_count() const;
  size_t get_total_order_count() const;
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
    size_t total_assets = 0;       // Total assets listed on this date (considering delist)
    size_t assets_with_orders = 0; // Assets with order data
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
    size_t total_orders = 0;
    float orders_size_gb = 0.0;

    // Whole database days (trading days with at least one asset having data)
    size_t database_order_days = 0;

    // Backtest range statistics (only within backtest period)
    size_t backtest_orders = 0;
    float backtest_orders_size_gb = 0.0;
    size_t backtest_order_days = 0;
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
    float total_size_gb = 0.0;
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
    float coverage_percent = 0.0;
  } backtest;

  // ========================================
  // Methods
  // ========================================
  // Scan operations (asynchronous coroutine-based)
  boost::asio::awaitable<void> coro_scan_binary_database(
      boost::asio::io_context &io,
      const std::string &orders_dir,
      const std::string &binary_extension,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  boost::asio::awaitable<void> coro_scan_archive_database(
      boost::asio::io_context &io,
      const std::string &archive_dir,
      const std::string &archive_extension,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  // Coverage analysis (lightweight, call after config changes)
  // Also computes backtest range statistics using cached data
  // required_dates 的 ground truth = 基本面交易日历 (assetinfo.stock_days)
  void compute_backtest_coverage(const std::string &start, const std::string &end,
                                 const AssetInfo &assetinfo);

  // Sync AssetItem fields from AssetInfo (call after AssetInfo updates)
  template <typename StockInfoMap>
  void sync_from_asset_info(const StockInfoMap &stock_info) {
    for (auto &asset : items) {
      // Build stock key: "exchange.code" (lowercase exchange)
      std::string exchange_lower = asset.exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
      std::string stock_key = exchange_lower + "." + asset.asset_code;

      // Find stock info
      auto info_it = stock_info.find(stock_key);
      if (info_it != stock_info.end()) {
        const auto &info = info_it->second;

        // Update name
        asset.asset_name = info.name;

        // Update start_date (ipoDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.ipoDate.empty()) {
          std::string ipo_date = info.ipoDate;
          ipo_date.erase(std::remove(ipo_date.begin(), ipo_date.end(), '-'), ipo_date.end());
          asset.start_date = ipo_date;
        }

        // Update end_date (outDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.outDate.empty()) {
          std::string out_date = info.outDate;
          out_date.erase(std::remove(out_date.begin(), out_date.end(), '-'), out_date.end());
          asset.end_date = out_date;
        } else {
          asset.end_date = "20991231"; // Not delisted
        }
      }
    }
  }

  // Compute per-date browser statistics (requires stock_info for delist dates)
  // Should be called once after loading stock_info and stock_days
  //
  // 分母 = 当日"本该有逐笔"的标的: 已上市未退市, 且排除
  //   - 北交所 (L2 archive 从不覆盖 .BJ)
  //   - 当日全天停牌 (suspended, 无逐笔可编码)
  // 这两项不剔掉的话全市场完整性会被压到 ~94%, 掩盖真实缺口.
  template <typename StockInfoMap, typename StockDaysVec, typename SuspendedMap>
  void compute_browser_statistics(const StockInfoMap &stock_info, const StockDaysVec &stock_days,
                                  const SuspendedMap &suspended) {
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

      // 当日停牌名单 (无条目 = 该日无人停牌)
      auto susp_it = suspended.find(date_dense);
      const auto *susp_today = (susp_it != suspended.end()) ? &susp_it->second : nullptr;

      for (const auto &asset : items) {
        if (asset.exchange == "BJ")
          continue;

        // Build full stock code (e.g., "sh.600128") - convert to lowercase
        std::string exchange_lower = asset.exchange;
        std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
        std::string full_code = exchange_lower + "." + asset.asset_code;

        if (susp_today && susp_today->count(full_code))
          continue;

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
        if (date_it != asset.date_info.end() && date_it->second.orders_encoded) {
          stats.assets_with_orders++;
        }
      }
    }
  }
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

namespace Utils {
// orders/2023/01/03/000023.SZ.bin
//
// 一个 (资产, 日期) 就一个文件, 所以没有"每资产目录"这一层. 文件名里也不带
// 条数 —— 条数由文件头的 original_size 精确推出 (见 BinaryDecoder_L2), 写在
// 名字里纯属冗余, 还会让"这天编过了吗"退化成通配符匹配而不是一次 exists.

inline std::string generate_archive_path(const std::string &base_dir, const std::string &date_str, const std::string &extension) {
  return base_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(0, 6) + "/" + date_str + extension;
}

// orders/YYYY/MM/DD
inline std::string generate_date_dir(const std::string &orders_dir, const std::string &date_str) {
  return orders_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2);
}

// orders/YYYY/MM/DD/<CODE>.<EX>.bin
inline std::string generate_orders_path(const std::string &orders_dir, const std::string &date_str,
                                        const std::string &asset_code, const std::string &exchange,
                                        const std::string &binary_extension) {
  return generate_date_dir(orders_dir, date_str) + "/" + asset_code + "." + exchange + binary_extension;
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

  // Binary structure: orders_dir/YYYY/MM/DD/<CODE>.<EX>.bin
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
