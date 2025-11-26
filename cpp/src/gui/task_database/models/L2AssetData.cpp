// L2 Asset Data Implementation (simplified, no exchange metadata)
#include "gui/task_database/models/L2AssetData.hpp"
#include <filesystem>

namespace GUI::Database {

namespace fs = std::filesystem;

// ============================================================================
// AssetInfo Constructor
// ============================================================================

AssetInfo::AssetInfo(size_t id, std::string code, std::string exch,
                     std::string start, std::string end)
    : asset_id(id), asset_code(std::move(code)), exchange(std::move(exch)),
      start_date(std::move(start)), end_date(std::move(end)) {
  // Set exchange type
  if (exchange == "SH") {
    exchange_type = L2::ExchangeType::SSE;
  } else if (exchange == "SZ") {
    exchange_type = L2::ExchangeType::SZSE;
  } else {
    exchange_type = L2::ExchangeType::UNKNOWN;
  }
}

// ============================================================================
// Filesystem Operations
// ============================================================================

void AssetInfo::init_paths(const std::string &db_dir,
                           const std::vector<std::string> &all_dates) {
  date_info.clear();

  for (const auto &date : all_dates) {
    // Skip dates outside range
    if (date < start_date || date > end_date)
      continue;

    // Build path: database/YYYY/MM/DD/CODE.EXCHANGE (e.g., 000785.SZ)
    std::string year = date.substr(0, 4);
    std::string month = date.substr(4, 2);
    std::string day = date.substr(6, 2);
    std::string asset_dir = asset_code + "." + exchange;

    std::string full_path = db_dir + "/" + year + "/" + month + "/" + day + "/" + asset_dir;

    DateInfo info;
    info.database_dir = full_path;
    date_info[date] = info;
  }
}

void AssetInfo::scan_existing_binaries() {
  for (auto &[date, info] : date_info) {
    if (!fs::exists(info.database_dir))
      continue;

    for (const auto &entry : fs::directory_iterator(info.database_dir)) {
      if (!entry.is_regular_file())
        continue;

      std::string filename = entry.path().filename().string();

      // Parse snapshot files: *_snapshots_*.bin
      if (filename.find("_snapshots_") != std::string::npos &&
          filename.ends_with(".bin")) {
        info.snapshots_file = entry.path().string();
        info.snapshots_encoded = 1;

        // Extract count from filename
        size_t pos = filename.find("_snapshots_");
        if (pos != std::string::npos) {
          pos += 11; // length of "_snapshots_"
          size_t end_pos = filename.find(".bin", pos);
          if (end_pos != std::string::npos) {
            std::string count_str = filename.substr(pos, end_pos - pos);
            info.snapshot_count = std::stoull(count_str);
          }
        }
      }

      // Parse order files: *_orders_*.bin
      if (filename.find("_orders_") != std::string::npos &&
          filename.ends_with(".bin")) {
        info.orders_file = entry.path().string();
        info.orders_encoded = 1;

        // Extract count from filename
        size_t pos = filename.find("_orders_");
        if (pos != std::string::npos) {
          pos += 8; // length of "_orders_"
          size_t end_pos = filename.find(".bin", pos);
          if (end_pos != std::string::npos) {
            std::string count_str = filename.substr(pos, end_pos - pos);
            info.order_count = std::stoull(count_str);
          }
        }
      }
    }
  }
}

// ============================================================================
// Statistics
// ============================================================================

size_t AssetInfo::get_total_trading_days() const {
  return date_info.size();
}

size_t AssetInfo::get_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.is_fully_encoded())
      ++count;
  }
  return count;
}

size_t AssetInfo::get_snapshots_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.snapshots_encoded)
      ++count;
  }
  return count;
}

size_t AssetInfo::get_orders_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.orders_encoded)
      ++count;
  }
  return count;
}

size_t AssetInfo::get_missing_count() const {
  return get_total_trading_days() - get_encoded_count();
}

size_t AssetInfo::get_analyzed_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.analyzed)
      ++count;
  }
  return count;
}

size_t AssetInfo::get_total_order_count() const {
  size_t total = 0;
  for (const auto &[date, info] : date_info) {
    total += info.order_count;
  }
  return total;
}

size_t AssetInfo::get_total_snapshot_count() const {
  size_t total = 0;
  for (const auto &[date, info] : date_info) {
    total += info.snapshot_count;
  }
  return total;
}

std::vector<std::string> AssetInfo::get_missing_dates() const {
  std::vector<std::string> missing;
  for (const auto &[date, info] : date_info) {
    if (!info.is_fully_encoded()) {
      missing.push_back(date);
    }
  }
  return missing;
}

std::string AssetInfo::get_display_name() const {
  return exchange + asset_code;
}

} // namespace GUI::Database
