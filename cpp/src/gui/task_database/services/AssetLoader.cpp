// Asset Loader Implementation
#include "gui/task_database/services/AssetLoader.hpp"
#include "shared/SharedData.hpp"
#include "package/nlohmann/json.hpp"

#include <fstream>
#include <filesystem>

namespace GUI::Database {

using json = nlohmann::json;

void AssetLoader::load_from_config(SharedData &data) {
  // Clear existing assets
  data.asset.items.clear();
  
  // Build full path from config
  std::filesystem::path assets_path = std::filesystem::path(data.config.config_dir) / data.config.assets_file;
  
  if (!std::filesystem::exists(assets_path)) {
    return;
  }
  
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
      
      // Use a very wide date range - actual listing dates will come from stock_info
      // This is just a fallback for the date_info map initialization
      std::string start_date = "19900101";  // Before any A-share listing
      std::string end_date = "20991231";    // Far future
      
      AssetItem asset(i, code, "", exchange, start_date, end_date);
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

