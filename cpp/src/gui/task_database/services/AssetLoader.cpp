// Asset Loader Implementation
#include "gui/task_database/services/AssetLoader.hpp"
#include "shared/SharedData.hpp"
#include "package/nlohmann/json.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>

namespace GUI::Database {

using json = nlohmann::json;

void AssetLoader::load_from_config(SharedData &data) {
  data.asset.items.clear();
  
  std::filesystem::path assets_path = std::filesystem::path(data.config.config_dir) / data.config.assets_file;
  
  if (!std::filesystem::exists(assets_path)) {
    return;
  }
  
  // Get stock_info from shared data
  const auto &stock_info_map = data.asset_info.get_stock_info();
  
  try {
    std::ifstream file(assets_path);
    json j;
    file >> j;
    
    if (!j.contains("assets") || !j["assets"].is_array()) {
      return;
    }
    
    const auto &assets_array = j["assets"];
    data.asset.items.reserve(assets_array.size());
    
    for (size_t i = 0; i < assets_array.size(); ++i) {
      if (!assets_array[i].is_string()) {
        continue;
      }
      
      std::string code = assets_array[i].get<std::string>();
      if (code.empty() || code == "000000") {
        continue;
      }
      
      std::string exchange = infer_exchange(code);
      
      // Build key for stock_info lookup: "exchange.code" (lowercase exchange)
      std::string exchange_lower = exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
      std::string stock_key = exchange_lower + "." + code;
      
      std::string asset_name = "";
      std::string start_date = "19900101";
      std::string end_date = "20991231";
      
      // Load from stock_info if available
      auto info_it = stock_info_map.find(stock_key);
      if (info_it != stock_info_map.end()) {
        const auto &info = info_it->second;
        asset_name = info.name;
        
        std::string ipo_date = info.ipoDate;
        if (!ipo_date.empty()) {
          ipo_date.erase(std::remove(ipo_date.begin(), ipo_date.end(), '-'), ipo_date.end());
          start_date = ipo_date;
        }
        
        std::string out_date = info.outDate;
        if (!out_date.empty()) {
          out_date.erase(std::remove(out_date.begin(), out_date.end(), '-'), out_date.end());
          end_date = out_date;
        }
      }
      
      AssetItem asset(i, code, asset_name, exchange, start_date, end_date);
      data.asset.items.push_back(std::move(asset));
    }
    
  } catch (const std::exception &e) {
    // Failed to load, leave assets empty
  }
}

std::string AssetLoader::infer_exchange(const std::string &code) {
  if (code.empty()) {
    return "SZ";
  }
  
  char first = code[0];
  
  // SZ: 0xxxxx (main board), 3xxxxx (ChiNext)
  if (first == '0' || first == '3') {
    return "SZ";
  }
  
  // SH: 6xxxxx (main board)
  if (first == '6') {
    return "SH";
  }
  
  // B-shares: 900xxx (SH), 200xxx (SZ)
  if (first == '9') {
    return "SH";
  }
  if (first == '2' && code.length() == 6) {
    return "SZ";
  }
  
  // Default to SZ
  return "SZ";
}

} // namespace GUI::Database

