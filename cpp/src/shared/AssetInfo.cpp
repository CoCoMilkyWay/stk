// Asset Information Implementation
#include "shared/AssetInfo.hpp"

#include <string>

// ============================================================================
// Query Methods
// ============================================================================

const StockInfo *AssetInfo::find_stock_info(const std::string &code) const {
  auto it = stock_info_.find(code);
  if (it != stock_info_.end()) {
    return &it->second;
  }
  return nullptr;
}

float AssetInfo::calculate_market_cap(const std::string &code) const {
  const StockInfo *info = find_stock_info(code);
  if (!info || info->mcap.empty()) {
    return 0.0f;
  }
  return std::stof(info->mcap);
}

bool AssetInfo::is_trading_day(const std::string &date) const {
  // Support both YYYYMMDD and YYYY-MM-DD formats
  if (date.size() == 10 && date[4] == '-') {
    // YYYY-MM-DD format
    return trading_days_set_.find(date) != trading_days_set_.end();
  } else if (date.size() == 8) {
    // YYYYMMDD format - convert to YYYY-MM-DD
    std::string dashed = date.substr(0, 4) + "-" + date.substr(4, 2) + "-" + date.substr(6, 2);
    return trading_days_set_.find(dashed) != trading_days_set_.end();
  }
  return false;
}

void AssetInfo::rebuild_cache() {
  trading_days_set_.clear();
  for (const auto &day : stock_days_) {
    if (day.size() >= 2 && day[1] == "1") {
      trading_days_set_.insert(day[0]); // date in YYYY-MM-DD format
    }
  }
}
