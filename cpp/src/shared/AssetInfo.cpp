// Asset Information Implementation
#include "shared/AssetInfo.hpp"

#include <string>

const StockInfo *AssetInfo::find_stock_info(const std::string &code) const {
  auto it = stock_info_.find(code);
  if (it != stock_info_.end()) {
    return &it->second;
  }
  return nullptr;
}
