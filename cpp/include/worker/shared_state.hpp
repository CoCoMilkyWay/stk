#pragma once

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// ============================================================================
// CONFIG EXTERNS
// ============================================================================
namespace Config {
extern const char *ARCHIVE_EXTENSION;
extern const char *BINARY_EXTENSION;
} // namespace Config

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

namespace Utils {
inline std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
  return base_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(0, 6) + "/" + date_str + Config::ARCHIVE_EXTENSION;
}

inline std::string generate_temp_asset_dir(const std::string &database_dir, const std::string &date_str, const std::string &asset_code) {
  return database_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
}

inline std::set<std::string> collect_dates_from_archives(const std::string &l2_archive_base) {
  std::set<std::string> dates;
  if (!std::filesystem::exists(l2_archive_base))
    return dates;

  // Archive structure: archive_base/YYYY/YYYYMM/YYYYMMDD.rar
  for (const auto &year_entry : std::filesystem::directory_iterator(l2_archive_base)) {
    if (!year_entry.is_directory())
      continue;

    for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;

      for (const auto &file_entry : std::filesystem::directory_iterator(month_entry.path())) {
        const std::string filename = file_entry.path().stem().string();
        if (filename.size() == 8 && std::all_of(filename.begin(), filename.end(), ::isdigit)) {
          dates.insert(filename);
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

// ============================================================================
// SHARED STATE - All assets and global synchronization
// ============================================================================

struct AssetInfo {
  // ===== Per-date information =====
  struct DateInfo {
    size_t order_count{0};      // Order count (written during encoding)
    uint8_t encoded{0};         // 0=not encoded, 1=encoded (has binary or csv)
    uint8_t analyzed{0};        // 0=not analyzed, 1=analyzed
    std::string database_dir;   // Cached temp directory path
    std::string snapshots_file; // Full path to snapshots binary file
    std::string orders_file;    // Full path to orders binary file

    bool has_binaries() const {
      return !snapshots_file.empty() || !orders_file.empty();
    }
  };

  // ===== Immutable metadata =====
  size_t asset_id;
  std::string asset_code;
  std::string asset_name;
  std::string start_date;
  std::string end_date;
  L2::ExchangeType exchange_type;

  // ===== Per-date data (only for dates in [start_date, end_date]) =====
  std::unordered_map<std::string, DateInfo> date_info;

  // ===== Analysis assignment =====
  int assigned_worker_id{-1};

  // Constructor
  inline AssetInfo(size_t id, std::string code, std::string name, std::string start, std::string end)
      : asset_id(id),
        asset_code(std::move(code)),
        asset_name(std::move(name)),
        start_date(std::move(start)),
        end_date(std::move(end)),
        exchange_type(L2::infer_exchange_type(asset_code)) {}

  // Initialize paths for this asset's date range (based on global all_dates)
  inline void init_paths(const std::string &temp_dir_base, const std::vector<std::string> &all_dates) {
    for (const auto &date_str : all_dates) {
      if (date_str >= start_date && date_str <= end_date) {
        date_info[date_str].database_dir = Utils::generate_temp_asset_dir(temp_dir_base, date_str, asset_code);
      }
    }
  }

  // Scan existing binary files (for resume support)
  inline void scan_existing_binaries() {
    for (auto &[date_str, di] : date_info) {
      if (!std::filesystem::exists(di.database_dir))
        continue;

      for (const auto &entry : std::filesystem::directory_iterator(di.database_dir)) {
        const std::string filename = entry.path().filename().string();
        if (filename.starts_with(asset_code + "_snapshots_") && filename.ends_with(Config::BINARY_EXTENSION)) {
          di.snapshots_file = entry.path().string();
        } else if (filename.starts_with(asset_code + "_orders_") && filename.ends_with(Config::BINARY_EXTENSION)) {
          di.orders_file = entry.path().string();
          di.order_count = L2::BinaryDecoder_L2::extract_count_from_filename(di.orders_file);
        }
      }

      if (di.has_binaries()) {
        di.encoded = 1;
      }
    }
  }

  // Statistics
  inline size_t get_total_order_count() const {
    size_t total = 0;
    for (const auto &[_, di] : date_info)
      total += di.order_count;
    return total;
  }

  inline size_t get_total_trading_days() const {
    return date_info.size();
  }

  inline size_t get_encoded_count() const {
    size_t count = 0;
    for (const auto &[_, di] : date_info)
      count += di.encoded;
    return count;
  }

  inline size_t get_missing_count() const {
    return get_total_trading_days() - get_encoded_count();
  }

  inline std::vector<std::string> get_missing_dates() const {
    std::vector<std::string> missing;
    for (const auto &[date_str, di] : date_info) {
      if (!di.encoded) {
        missing.push_back(date_str);
      }
    }
    std::sort(missing.begin(), missing.end());
    return missing;
  }

  inline size_t get_analyzed_count() const {
    size_t count = 0;
    for (const auto &[_, di] : date_info)
      count += di.analyzed;
    return count;
  }
};

struct SharedState {
  // ===== Asset-level information =====
  std::vector<AssetInfo> assets;

  // ===== Global date sequence =====
  std::vector<std::string> all_dates; // Sorted unique trading dates

  // ===== Future: Cross-sectional synchronization =====
  // std::vector<std::barrier<>> date_barriers;
  // std::vector<std::mutex> date_locks;
  // std::unordered_map<std::string, CrossSectionalData> cross_sectional_cache;

  SharedState() = default;

  // Populate all_dates from filesystem and filter by date range
  inline void init_dates(const std::string &l2_archive_base,
                         const std::string &database_dir,
                         const std::string &start_date_str,
                         const std::string &end_date_str) {
    // Collect all dates from archives or binaries
    std::set<std::string> global_dates = Utils::collect_dates_from_archives(l2_archive_base);
    if (global_dates.empty()) {
      global_dates = Utils::collect_dates_from_binaries(database_dir);
    }

    // Filter dates to match config date range
    std::set<std::string> filtered_dates;
    for (const auto &date_str : global_dates) {
      if (date_str >= start_date_str && date_str <= end_date_str) {
        filtered_dates.insert(date_str);
      }
    }
    all_dates.assign(filtered_dates.begin(), filtered_dates.end());
  }

  // Initialize all asset paths (must be called after all_dates is populated)
  inline void init_paths(const std::string &temp_dir_base) {
    for (auto &asset : assets) {
      asset.init_paths(temp_dir_base, all_dates);
    }
  }

  // Scan all existing binaries (for resume)
  inline void scan_all_existing_binaries() {
    for (auto &asset : assets) {
      asset.scan_existing_binaries();
    }
  }

  // Statistics
  inline size_t total_trading_days() const {
    size_t total = 0;
    for (const auto &asset : assets)
      total += asset.get_total_trading_days();
    return total;
  }

  inline size_t total_encoded_dates() const {
    size_t total = 0;
    for (const auto &asset : assets)
      total += asset.get_encoded_count();
    return total;
  }

  inline size_t total_missing_dates() const {
    size_t total = 0;
    for (const auto &asset : assets)
      total += asset.get_missing_count();
    return total;
  }

  inline size_t total_orders() const {
    size_t total = 0;
    for (const auto &asset : assets)
      total += asset.get_total_order_count();
    return total;
  }
};
