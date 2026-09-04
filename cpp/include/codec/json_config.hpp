#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>

namespace JsonConfig {

// Stock information structure
struct StockInfo {
  std::string name;
  std::string industry;
  std::string sub_industry;
  std::chrono::year_month start_date;
  std::chrono::year_month end_date; // Default constructed if not delisted
  bool is_delisted;

  StockInfo() : is_delisted(false) {}
};

// Application configuration
struct AppConfig {
  std::string dir;
  std::chrono::year_month_day start_date; // Lower bound date for data availability
  std::chrono::year_month_day end_date;   // Upper bound date for data availability
};

// Stock info parser
std::unordered_map<std::string, StockInfo> ParseStockInfo(const std::string &stock_info_file);

// Config parser
AppConfig ParseAppConfig(const std::string &config_file);

// Date utilities
// Accepts "YYYY-MM" or "YYYY-MM-DD"
std::chrono::year_month ParseDateString(const std::string &date_str);
std::chrono::year_month_day ParseDateStringFull(const std::string &date_str);
std::vector<std::chrono::year_month> GetMonthRange(
    const std::chrono::year_month &start,
    const std::chrono::year_month &end);
std::string FormatYearMonth(const std::chrono::year_month &ym);
std::string FormatYearMonthDay(const std::chrono::year_month_day &ymd);

} // namespace JsonConfig