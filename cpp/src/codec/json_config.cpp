#include "codec/json_config.hpp"
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
    stock_info.start_date = ParseDateString(info["ipo_date"]);

    std::string delist_str = info["delist_date"];
    if (!delist_str.empty()) {
      stock_info.end_date = ParseDateString(delist_str);
      stock_info.is_delisted = true;
    } else {
      stock_info.is_delisted = false;
      // Set delist date to the current month for active stocks
      auto now = std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())};
      stock_info.end_date = std::chrono::year_month{now.year(), now.month()};
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
  config.dir = j["dir"];

  // lower bound date for data availability
  std::string start_date_str = j.value("start_date", "");
  config.start_date = ParseDateStringFull(start_date_str);

  // upper bound date for data availability
  std::string end_date_str = j.value("end_date", "");
  config.end_date = ParseDateStringFull(end_date_str);

  return config;
}

std::chrono::year_month ParseDateString(const std::string &date_str) {
  // Expected formats: "YYYY-MM" or "YYYY-MM-DD"
  if (date_str.length() < 7) {
    throw std::runtime_error("Invalid date format: " + date_str);
  }

  unsigned int year = std::stoi(date_str.substr(0, 4));
  unsigned int month = std::stoi(date_str.substr(5, 2));

  // BC/BCE years can be negative
  return std::chrono::year_month{std::chrono::year{static_cast<int>(year)}, std::chrono::month{static_cast<unsigned>(month)}};
}

std::chrono::year_month_day ParseDateStringFull(const std::string &date_str) {
  // Expected format: "YYYY-MM-DD"
  if (date_str.length() < 10) {
    throw std::runtime_error("Invalid date format, expected YYYY-MM-DD: " + date_str);
  }

  unsigned int year = std::stoi(date_str.substr(0, 4));
  unsigned int month = std::stoi(date_str.substr(5, 2));
  unsigned int day = std::stoi(date_str.substr(8, 2));

  return std::chrono::year_month_day{
      std::chrono::year{static_cast<int>(year)},
      std::chrono::month{static_cast<unsigned>(month)},
      std::chrono::day{static_cast<unsigned>(day)}};
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

// Example: FormatYearMonthDay(2024y/3/15) returns "20240315"
std::string FormatYearMonthDay(const std::chrono::year_month_day &ymd) {
  std::ostringstream oss;
  oss << static_cast<int>(ymd.year())
      << std::setfill('0') << std::setw(2) << static_cast<unsigned>(ymd.month())
      << std::setfill('0') << std::setw(2) << static_cast<unsigned>(ymd.day());
  return oss.str();
}

} // namespace JsonConfig