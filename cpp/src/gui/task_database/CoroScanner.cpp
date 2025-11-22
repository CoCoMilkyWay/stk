#include "gui/task_database/CoroScanner.hpp"
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include "package/nlohmann/json.hpp"
#include <filesystem>
#include <fstream>
#include <set>

namespace asio = boost::asio;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace GUI::Database {

CoroScanner::CoroScanner(DatabaseState &state) : state_(state) {}

asio::awaitable<void> CoroScanner::scan_database() {
  state_.scan_status = DatabaseState::ScanStatus::Scanning;
  state_.status_message = "Scanning database...";
  
  co_await asio::this_coro::executor;
  
  std::string db_dir = "output/database";
  
  // Check if binary directory exists and has data
  bool has_data = false;
  if (fs::exists(db_dir)) {
    try {
      for (const auto &entry : fs::directory_iterator(db_dir)) {
        if (entry.is_directory()) {
          has_data = true;
          break;
        }
      }
    } catch (...) {}
  }
  
  if (has_data) {
    scan_binary_directory(db_dir);
  } else {
    load_targets_json("config/targets.json");
  }
  
  state_.scan_status = DatabaseState::ScanStatus::Complete;
  state_.status_message = "Scan complete";
  
  co_return;
}

void CoroScanner::scan_binary_directory(const std::string &db_dir) {
  std::set<std::string> codes_found;
  std::set<std::string> dates_found;
  
  // Scan directory structure: database/YYYY/MM/DD/EXCHANGE_CODE/
  if (!fs::exists(db_dir)) return;
  
  try {
    for (const auto &year_entry : fs::directory_iterator(db_dir)) {
      if (!year_entry.is_directory()) continue;
      
      for (const auto &month_entry : fs::directory_iterator(year_entry)) {
        if (!month_entry.is_directory()) continue;
        
        for (const auto &day_entry : fs::directory_iterator(month_entry)) {
          if (!day_entry.is_directory()) continue;
          
          std::string date = year_entry.path().filename().string() +
                           month_entry.path().filename().string() +
                           day_entry.path().filename().string();
          dates_found.insert(date);
          
          for (const auto &asset_entry : fs::directory_iterator(day_entry)) {
            if (!asset_entry.is_directory()) continue;
            
            std::string full_code = asset_entry.path().filename().string();
            codes_found.insert(full_code);
          }
        }
      }
    }
  } catch (...) {}
  
  // Convert codes to AssetInfo
  state_.all_dates.assign(dates_found.begin(), dates_found.end());
  std::sort(state_.all_dates.begin(), state_.all_dates.end());
  
  size_t asset_id = 0;
  for (const auto &full_code : codes_found) {
    // Parse format: CODE.EXCHANGE (e.g., "000023.SZ" or "600000.SH")
    size_t dot_pos = full_code.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0) continue;
    
    std::string code = full_code.substr(0, dot_pos);
    std::string exchange = full_code.substr(dot_pos + 1);
    
    if (exchange != "SH" && exchange != "SZ") continue;
    
    std::string start_date = state_.all_dates.empty() ? "20000101" : state_.all_dates.front();
    std::string end_date = state_.all_dates.empty() ? "20991231" : state_.all_dates.back();
    
    AssetInfo asset(asset_id++, code, exchange, start_date, end_date);
    asset.init_paths(db_dir, state_.all_dates);
    asset.scan_existing_binaries();
    
    state_.assets.push_back(std::move(asset));
  }
}

void CoroScanner::load_targets_json(const std::string &config_path) {
  if (!fs::exists(config_path)) return;
  
  try {
    std::ifstream file(config_path);
    json j;
    file >> j;
    
    if (!j.is_array()) return;
    
    size_t asset_id = 0;
    for (const auto &code : j) {
      if (!code.is_string()) continue;
      
      std::string code_str = code.get<std::string>();
      if (code_str.empty() || code_str == "000000") continue;
      
      std::string exchange = infer_exchange(code_str);
      
      AssetInfo asset(asset_id++, code_str, exchange, "20000101", "20991231");
      state_.assets.push_back(std::move(asset));
    }
  } catch (...) {}
}

std::string CoroScanner::infer_exchange(const std::string &code) {
  if (code.empty()) return "SZ";
  
  char first = code[0];
  if (first == '0' || first == '3') return "SZ";
  if (first == '6') return "SH";
  if (first == '8' || first == '9') return "SH"; // B-shares, typically SH
  
  return "SZ";
}

} // namespace GUI::Database

