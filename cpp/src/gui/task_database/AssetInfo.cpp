#include "gui/task_database/AssetInfo.hpp"
#include <filesystem>
#include <algorithm>

namespace GUI::Database {

AssetInfo::AssetInfo(size_t id, std::string code, std::string exch,
                     std::string start, std::string end)
    : asset_id(id), asset_code(std::move(code)), exchange(std::move(exch)),
      start_date(std::move(start)), end_date(std::move(end)) {
  if (exchange == "SH") {
    exchange_type = L2::ExchangeType::SSE;
  } else if (exchange == "SZ") {
    exchange_type = L2::ExchangeType::SZSE;
  }
}

void AssetInfo::init_paths(const std::string &db_dir,
                           const std::vector<std::string> &all_dates) {
  // Format: CODE.EXCHANGE (e.g., "000023.SZ")
  std::string full_code = asset_code + "." + exchange;
  
  for (const auto &date : all_dates) {
    if (date < start_date || date > end_date) continue;
    
    // Build directory path: database_dir/YYYY/MM/DD/CODE.EXCHANGE/
    std::string year = date.substr(0, 4);
    std::string month = date.substr(4, 2);
    std::string day = date.substr(6, 2);
    
    DateInfo info;
    info.database_dir = db_dir + "/" + year + "/" + month + "/" + day + "/" + full_code;
    date_info[date] = info;
  }
}

void AssetInfo::scan_existing_binaries() {
  namespace fs = std::filesystem;
  
  for (auto &[date, info] : date_info) {
    if (!fs::exists(info.database_dir)) continue;
    
    try {
      for (const auto &entry : fs::directory_iterator(info.database_dir)) {
        if (!entry.is_regular_file()) continue;
        
        std::string filename = entry.path().filename().string();
        
        if (filename.find("_snapshots_") != std::string::npos && filename.ends_with(".bin")) {
          info.snapshots_file = entry.path().string();
          
          // Extract count from filename
          size_t pos = filename.find("_snapshots_");
          if (pos != std::string::npos) {
            size_t end = filename.find(".bin");
            std::string count_str = filename.substr(pos + 11, end - pos - 11);
            info.snapshot_count = std::stoull(count_str);
          }
          info.snapshots_encoded = 1;
        } else if (filename.find("_orders_") != std::string::npos && filename.ends_with(".bin")) {
          info.orders_file = entry.path().string();
          
          // Extract count from filename
          size_t pos = filename.find("_orders_");
          if (pos != std::string::npos) {
            size_t end = filename.find(".bin");
            std::string count_str = filename.substr(pos + 8, end - pos - 8);
            info.order_count = std::stoull(count_str);
          }
          info.orders_encoded = 1;
        }
      }
    } catch (...) {
      // Skip problematic directories
    }
  }
}

size_t AssetInfo::get_total_trading_days() const {
  return date_info.size();
}

size_t AssetInfo::get_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.is_fully_encoded()) count++;
  }
  return count;
}

size_t AssetInfo::get_snapshots_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.snapshots_encoded > 0) count++;
  }
  return count;
}

size_t AssetInfo::get_orders_encoded_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.orders_encoded > 0) count++;
  }
  return count;
}

size_t AssetInfo::get_missing_count() const {
  return get_total_trading_days() - get_encoded_count();
}

size_t AssetInfo::get_analyzed_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    if (info.analyzed > 0) count++;
  }
  return count;
}

size_t AssetInfo::get_total_order_count() const {
  size_t count = 0;
  for (const auto &[date, info] : date_info) {
    count += info.order_count;
  }
  return count;
}

std::vector<std::string> AssetInfo::get_missing_dates() const {
  std::vector<std::string> missing;
  for (const auto &[date, info] : date_info) {
    if (!info.is_fully_encoded()) {
      missing.push_back(date);
    }
  }
  std::sort(missing.begin(), missing.end());
  return missing;
}

std::string AssetInfo::get_display_name() const {
  std::string name = metadata.name_cn;
  if (name.empty()) {
    name = exchange + asset_code;
  }
  
  // Add special markers
  // TODO: Add U/W/V markers based on metadata fields
  
  return name;
}

size_t DatabaseState::total_trading_days() const {
  size_t total = 0;
  for (const auto &asset : assets) {
    total += asset.get_total_trading_days();
  }
  return total;
}

size_t DatabaseState::total_encoded_dates() const {
  size_t total = 0;
  for (const auto &asset : assets) {
    total += asset.get_encoded_count();
  }
  return total;
}

size_t DatabaseState::total_missing_dates() const {
  size_t total = 0;
  for (const auto &asset : assets) {
    total += asset.get_missing_count();
  }
  return total;
}

size_t DatabaseState::total_orders() const {
  size_t total = 0;
  for (const auto &asset : assets) {
    total += asset.get_total_order_count();
  }
  return total;
}

} // namespace GUI::Database

