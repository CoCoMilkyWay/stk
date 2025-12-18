// Asset Information Implementation
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"
#include "package/nlohmann/json.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
using json = nlohmann::json;

// ============================================================================
// Query Methods
// ============================================================================

const StockInfo* AssetInfo::find_stock_info(const std::string& code) const {
  auto it = stock_info_.find(code);
  if (it != stock_info_.end()) {
    return &it->second;
  }
  return nullptr;
}

float AssetInfo::calculate_market_cap(const std::string& code) const {
  const StockInfo* info = find_stock_info(code);
  if (!info || info->amount.empty() || info->turn.empty()) {
    return 0.0f;
  }
  
  try {
    float amount = std::stof(info->amount);
    float turn = std::stof(info->turn);
    
    if (turn <= 0.0f) {
      return 0.0f;
    }
    
    // Market cap (billion yuan) = amount * 100 / turn / 1e8
    return amount * 100.0f / turn / 1e8f;
  } catch (...) {
    return 0.0f;
  }
}

bool AssetInfo::is_trading_day(const std::string& date) const {
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
  for (const auto& day : stock_days_) {
    if (day.size() >= 2 && day[1] == "1") {
      trading_days_set_.insert(day[0]); // date in YYYY-MM-DD format
    }
  }
}

// ============================================================================
// Persistence
// ============================================================================

void AssetInfo::load_from_json(const std::string& config_dir, const Config& config) {
  // Load stock_factor
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_factor_file;
    if (fs::exists(filepath)) {
      try {
        std::ifstream infile(filepath);
        json j;
        infile >> j;
        infile.close();
        
        stock_factor_.clear();
        for (auto& [code, obj] : j.items()) {
          StockFactorData factor_data;
          factor_data.last_update = obj.value("last_update", "");
          factor_data.data = obj.value("data", std::vector<std::vector<std::string>>{});
          stock_factor_[code] = std::move(factor_data);
        }
      } catch (const std::exception& e) {
        stock_factor_.clear();
        if (fs::exists(filepath)) {
          fs::remove(filepath);
        }
      }
    } else {
      stock_factor_.clear();
    }
  }
  
  // Load stock_info
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_info_file;
    if (fs::exists(filepath)) {
      try {
        std::ifstream infile(filepath);
        json j;
        infile >> j;
        infile.close();
        
        stock_info_.clear();
        for (auto& [code, info_json] : j.items()) {
          StockInfo info;
          info.name = info_json.value("name", "");
          info.ipoDate = info_json.value("ipoDate", "");
          info.outDate = info_json.value("outDate", "");
          info.ind_code = info_json.value("ind_code", "");
          info.ind_name = info_json.value("ind_name", "");
          info.update_date = info_json.value("update_date", "");
          info.volume = info_json.value("volume", "");
          info.amount = info_json.value("amount", "");
          info.turn = info_json.value("turn", "");
          info.tradestatus = info_json.value("tradestatus", "");
          info.isST = info_json.value("isST", "");
          info.peTTM = info_json.value("peTTM", "");
          info.pbMRQ = info_json.value("pbMRQ", "");
          info.psTTM = info_json.value("psTTM", "");
          info.pcfNcfTTM = info_json.value("pcfNcfTTM", "");
          stock_info_[code] = info;
        }
      } catch (const std::exception& e) {
        stock_info_.clear();
        if (fs::exists(filepath)) {
          fs::remove(filepath);
        }
      }
    } else {
      stock_info_.clear();
    }
  }
  
  // Load stock_days
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_days_file;
    if (fs::exists(filepath)) {
      try {
        std::ifstream infile(filepath);
        json j;
        infile >> j;
        infile.close();
        
        stock_days_ = j.get<std::vector<std::vector<std::string>>>();
      } catch (const std::exception& e) {
        stock_days_.clear();
        if (fs::exists(filepath)) {
          fs::remove(filepath);
        }
      }
    } else {
      stock_days_.clear();
    }
  }
  
  // Build trading days cache
  rebuild_cache();
}

void AssetInfo::load_stock_factor_from_json(const std::string& config_dir, const Config& config) {
  std::string filepath = config_dir + "/" + config.baostock_stock_factor_file;
  if (fs::exists(filepath)) {
    try {
      std::ifstream infile(filepath);
      json j;
      infile >> j;
      infile.close();
      
      stock_factor_.clear();
      for (auto& [code, obj] : j.items()) {
        StockFactorData factor_data;
        factor_data.last_update = obj.value("last_update", "");
        factor_data.data = obj.value("data", std::vector<std::vector<std::string>>{});
        stock_factor_[code] = std::move(factor_data);
      }
    } catch (const std::exception& e) {
      stock_factor_.clear();
      if (fs::exists(filepath)) {
        fs::remove(filepath);
      }
    }
  } else {
    stock_factor_.clear();
  }
}

void AssetInfo::load_stock_info_from_json(const std::string& config_dir, const Config& config) {
  std::string filepath = config_dir + "/" + config.baostock_stock_info_file;
  if (fs::exists(filepath)) {
    try {
      std::ifstream infile(filepath);
      json j;
      infile >> j;
      infile.close();
      
      stock_info_.clear();
      for (auto& [code, info_json] : j.items()) {
        StockInfo info;
        info.name = info_json.value("name", "");
        info.ipoDate = info_json.value("ipoDate", "");
        info.outDate = info_json.value("outDate", "");
        info.ind_code = info_json.value("ind_code", "");
        info.ind_name = info_json.value("ind_name", "");
        info.update_date = info_json.value("update_date", "");
        info.volume = info_json.value("volume", "");
        info.amount = info_json.value("amount", "");
        info.turn = info_json.value("turn", "");
        info.tradestatus = info_json.value("tradestatus", "");
        info.isST = info_json.value("isST", "");
        info.peTTM = info_json.value("peTTM", "");
        info.pbMRQ = info_json.value("pbMRQ", "");
        info.psTTM = info_json.value("psTTM", "");
        info.pcfNcfTTM = info_json.value("pcfNcfTTM", "");
        stock_info_[code] = info;
      }
    } catch (const std::exception& e) {
      stock_info_.clear();
      if (fs::exists(filepath)) {
        fs::remove(filepath);
      }
    }
  } else {
    stock_info_.clear();
  }
}

void AssetInfo::load_stock_days_from_json(const std::string& config_dir, const Config& config) {
  std::string filepath = config_dir + "/" + config.baostock_stock_days_file;
  if (fs::exists(filepath)) {
    try {
      std::ifstream infile(filepath);
      json j;
      infile >> j;
      infile.close();
      
      stock_days_ = j.get<std::vector<std::vector<std::string>>>();
    } catch (const std::exception& e) {
      stock_days_.clear();
      if (fs::exists(filepath)) {
        fs::remove(filepath);
      }
    }
  } else {
    stock_days_.clear();
  }
  
  // Rebuild trading days cache
  rebuild_cache();
}

void AssetInfo::save_to_json(const std::string& config_dir, const Config& config) const {
  // Save stock_factor
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_factor_file;
    std::string temp_filepath = filepath + ".tmp";
    
    std::ofstream outfile(temp_filepath);
    assert(outfile.is_open());
    
    outfile << "{\n";
    bool first_stock = true;
    for (const auto& [code, factor_data] : stock_factor_) {
      if (!first_stock)
        outfile << ",\n";
      first_stock = false;
      
      outfile << "  \"" << code << "\": {\n";
      outfile << "    \"last_update\": \"" << factor_data.last_update << "\",\n";
      outfile << "    \"data\": [\n";
      for (size_t i = 0; i < factor_data.data.size(); ++i) {
        outfile << "      [\"" << factor_data.data[i][0] << "\",\"" << factor_data.data[i][1] << "\"]";
        if (i < factor_data.data.size() - 1)
          outfile << ",";
        outfile << "\n";
      }
      outfile << "    ]\n";
      outfile << "  }";
    }
    outfile << "\n}\n";
    outfile.close();
    
    fs::rename(temp_filepath, filepath);
  }
  
  // Save stock_info
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_info_file;
    std::string temp_filepath = filepath + ".tmp";
    
    std::ofstream outfile(temp_filepath);
    assert(outfile.is_open());
    
    outfile << "{\n";
    bool first_stock = true;
    for (const auto& [code, info] : stock_info_) {
      if (!first_stock)
        outfile << ",\n";
      first_stock = false;
      
      outfile << "  \"" << code << "\": {";
      outfile << "\"name\":\"" << info.name << "\",";
      outfile << "\"ipoDate\":\"" << info.ipoDate << "\",";
      outfile << "\"outDate\":\"" << info.outDate << "\",";
      outfile << "\"ind_code\":\"" << info.ind_code << "\",";
      outfile << "\"ind_name\":\"" << info.ind_name << "\",";
      outfile << "\"update_date\":\"" << info.update_date << "\",";
      outfile << "\"volume\":\"" << info.volume << "\",";
      outfile << "\"amount\":\"" << info.amount << "\",";
      outfile << "\"turn\":\"" << info.turn << "\",";
      outfile << "\"tradestatus\":\"" << info.tradestatus << "\",";
      outfile << "\"isST\":\"" << info.isST << "\",";
      outfile << "\"peTTM\":\"" << info.peTTM << "\",";
      outfile << "\"pbMRQ\":\"" << info.pbMRQ << "\",";
      outfile << "\"psTTM\":\"" << info.psTTM << "\",";
      outfile << "\"pcfNcfTTM\":\"" << info.pcfNcfTTM << "\"}";
    }
    outfile << "\n}\n";
    outfile.close();
    
    fs::rename(temp_filepath, filepath);
  }
  
  // Save stock_days
  {
    std::string filepath = config_dir + "/" + config.baostock_stock_days_file;
    std::string temp_filepath = filepath + ".tmp";
    
    std::ofstream outfile(temp_filepath);
    assert(outfile.is_open());
    
    outfile << "[\n";
    for (size_t i = 0; i < stock_days_.size(); ++i) {
      outfile << "  [\"" << stock_days_[i][0] << "\",\"" << stock_days_[i][1] << "\"]";
      if (i < stock_days_.size() - 1)
        outfile << ",";
      outfile << "\n";
    }
    outfile << "]\n";
    outfile.close();
    
    fs::rename(temp_filepath, filepath);
  }
}

void AssetInfo::save_stock_factor_to_json(const std::string& config_dir, const Config& config) const {
  std::string filepath = config_dir + "/" + config.baostock_stock_factor_file;
  std::string temp_filepath = filepath + ".tmp";
  
  std::ofstream outfile(temp_filepath);
  assert(outfile.is_open());
  
  outfile << "{\n";
  bool first_stock = true;
  for (const auto& [code, factor_data] : stock_factor_) {
    if (!first_stock)
      outfile << ",\n";
    first_stock = false;
    
    outfile << "  \"" << code << "\": {\n";
    outfile << "    \"last_update\": \"" << factor_data.last_update << "\",\n";
    outfile << "    \"data\": [\n";
    for (size_t i = 0; i < factor_data.data.size(); ++i) {
      outfile << "      [\"" << factor_data.data[i][0] << "\",\"" << factor_data.data[i][1] << "\"]";
      if (i < factor_data.data.size() - 1)
        outfile << ",";
      outfile << "\n";
    }
    outfile << "    ]\n";
    outfile << "  }";
  }
  outfile << "\n}\n";
  outfile.close();
  
  fs::rename(temp_filepath, filepath);
}

void AssetInfo::save_stock_info_to_json(const std::string& config_dir, const Config& config) const {
  std::string filepath = config_dir + "/" + config.baostock_stock_info_file;
  std::string temp_filepath = filepath + ".tmp";
  
  std::ofstream outfile(temp_filepath);
  assert(outfile.is_open());
  
  outfile << "{\n";
  bool first_stock = true;
  for (const auto& [code, info] : stock_info_) {
    if (!first_stock)
      outfile << ",\n";
    first_stock = false;
    
    outfile << "  \"" << code << "\": {";
    outfile << "\"name\":\"" << info.name << "\",";
    outfile << "\"ipoDate\":\"" << info.ipoDate << "\",";
    outfile << "\"outDate\":\"" << info.outDate << "\",";
    outfile << "\"ind_code\":\"" << info.ind_code << "\",";
    outfile << "\"ind_name\":\"" << info.ind_name << "\",";
    outfile << "\"update_date\":\"" << info.update_date << "\",";
    outfile << "\"volume\":\"" << info.volume << "\",";
    outfile << "\"amount\":\"" << info.amount << "\",";
    outfile << "\"turn\":\"" << info.turn << "\",";
    outfile << "\"tradestatus\":\"" << info.tradestatus << "\",";
    outfile << "\"isST\":\"" << info.isST << "\",";
    outfile << "\"peTTM\":\"" << info.peTTM << "\",";
    outfile << "\"pbMRQ\":\"" << info.pbMRQ << "\",";
    outfile << "\"psTTM\":\"" << info.psTTM << "\",";
    outfile << "\"pcfNcfTTM\":\"" << info.pcfNcfTTM << "\"}";
  }
  outfile << "\n}\n";
  outfile.close();
  
  fs::rename(temp_filepath, filepath);
}

void AssetInfo::save_stock_days_to_json(const std::string& config_dir, const Config& config) const {
  std::string filepath = config_dir + "/" + config.baostock_stock_days_file;
  std::string temp_filepath = filepath + ".tmp";
  
  std::ofstream outfile(temp_filepath);
  assert(outfile.is_open());
  
  outfile << "[\n";
  for (size_t i = 0; i < stock_days_.size(); ++i) {
    outfile << "  [\"" << stock_days_[i][0] << "\",\"" << stock_days_[i][1] << "\"]";
    if (i < stock_days_.size() - 1)
      outfile << ",";
    outfile << "\n";
  }
  outfile << "]\n";
  outfile.close();
  
  fs::rename(temp_filepath, filepath);
}

