#include "json_config.hpp"
#include "nlohmann/json.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>


namespace JsonConfig {

std::unordered_map<std::string, StockInfo> ParseStockInfo(const std::string &stock_info_file) {
  std::unordered_map<std::string, StockInfo> result;

  std::ifstream file(stock_info_file);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open stock info file: " + stock_info_file);
  }

  nlohmann::json j;
  file >> j;

  for (const auto &[stock_code, info] : j.items()) {
    StockInfo stock_info;
    stock_info.name = info["name"];
    stock_info.industry = info["industry"];
    stock_info.sub_industry = info["sub_industry"];
    stock_info.ipo_date = ParseDateString(info["ipo_date"]);

    std::string delist_str = info["delist_date"];
    if (!delist_str.empty()) {
      stock_info.delist_date = ParseDateString(delist_str);
      stock_info.is_delisted = true;
    } else {
      stock_info.is_delisted = false;
      // Set delist date to current year + 1 for active stocks
      auto now = std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())};
      stock_info.delist_date = std::chrono::year_month{now.year() + std::chrono::years{1}, std::chrono::December};
    }

    result[stock_code] = stock_info;
  }

  return result;
}

AppConfig ParseAppConfig(const std::string &config_file) {
  std::ifstream file(config_file);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + config_file);
  }

  nlohmann::json j;
  file >> j;

  AppConfig config;
  config.snapshot_dir = j["snapshot_dir"];

  return config;
}

std::chrono::year_month ParseDateString(const std::string &date_str) {
  // Expected format: "YYYY-MM-DD"
  if (date_str.length() < 7) {
    throw std::runtime_error("Invalid date format: " + date_str);
  }

  unsigned int year = std::stoi(date_str.substr(0, 4));
  unsigned int month = std::stoi(date_str.substr(5, 2));

  // BC/BCE years can be negative
  return std::chrono::year_month{std::chrono::year{static_cast<int>(year)}, std::chrono::month{static_cast<unsigned>(month)}};
}

std::vector<std::chrono::year_month> GetMonthRange(
    const std::chrono::year_month &start,
    const std::chrono::year_month &end) {

  std::vector<std::chrono::year_month> months;
  std::chrono::year_month current = start;

  while (current <= end) {
    months.push_back(current);
    current = current + std::chrono::months{1};
  }

  return months;
}

// Example: FormatYearMonth(2024y/3) returns "2024_03"
std::string FormatYearMonth(const std::chrono::year_month &ym) {
  std::ostringstream oss;
  oss << static_cast<int>(ym.year()) << "_" << std::setfill('0') << std::setw(2) << static_cast<unsigned>(ym.month());
  return oss.str();
}

} // namespace JsonConfig