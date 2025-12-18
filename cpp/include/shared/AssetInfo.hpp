// Asset Information - Stock metadata from Baostock
// Shared data structure for stock info, factors, and trading days
#pragma once

#include <map>
#include <string>
#include <unordered_set>
#include <vector>

// Forward declaration
struct Config;

// ============================================================================
// Stock Info - Complete stock information
// ============================================================================

struct StockInfo {
  // Weekly fields (full update on Monday)
  std::string name;
  std::string ipoDate;
  std::string outDate;
  std::string ind_code;
  std::string ind_name;

  // Daily fields (incremental update on trading days)
  std::string update_date;
  std::string volume;
  std::string amount;
  std::string turn;
  std::string tradestatus;
  std::string isST;
  std::string peTTM;
  std::string pbMRQ;
  std::string psTTM;
  std::string pcfNcfTTM;
};

// ============================================================================
// Data Containers
// ============================================================================

// StockFactor: code -> {last_update, data}
struct StockFactorData {
  std::string last_update;
  std::vector<std::vector<std::string>> data;
};
using StockFactorMap = std::map<std::string, StockFactorData>;

// StockInfo: code -> StockInfo
using StockInfoMap = std::map<std::string, StockInfo>;

// StockDays: [[date, is_trading_day], ...]
using StockDaysVec = std::vector<std::vector<std::string>>;

// ============================================================================
// AssetInfo - Stock metadata and trading calendar
// ============================================================================

struct AssetInfo {
  // ========================================
  // Core Data
  // ========================================
  StockInfoMap stock_info_;
  StockFactorMap stock_factor_;
  StockDaysVec stock_days_;
  
  // ========================================
  // Quick lookup cache (for performance)
  // ========================================
  std::unordered_set<std::string> trading_days_set_;
  
  // ========================================
  // Read-only accessors
  // ========================================
  const StockInfoMap& get_stock_info() const { return stock_info_; }
  const StockFactorMap& get_stock_factor() const { return stock_factor_; }
  const StockDaysVec& get_stock_days() const { return stock_days_; }
  
  // ========================================
  // Query methods
  // ========================================
  
  // Find stock info by code (e.g. "sh.600000")
  // Returns nullptr if not found
  const StockInfo* find_stock_info(const std::string& code) const;
  
  // Calculate market cap in billions (亿元)
  // Formula: amount * 100 / turn / 1e8
  // Returns 0.0 if data insufficient
  float calculate_market_cap(const std::string& code) const;
  
  // Check if a date is a trading day
  // Date format: YYYYMMDD or YYYY-MM-DD
  bool is_trading_day(const std::string& date) const;
  
  // ========================================
  // Persistence (synchronous I/O)
  // ========================================
  
  // Load all data from JSON files (convenience method)
  void load_from_json(const std::string& config_dir, const Config& config);
  
  // Load individual JSON files (for fine-grained control in update flows)
  void load_stock_factor_from_json(const std::string& config_dir, const Config& config);
  void load_stock_info_from_json(const std::string& config_dir, const Config& config);
  void load_stock_days_from_json(const std::string& config_dir, const Config& config);
  
  // Save all data to JSON files (convenience method)
  void save_to_json(const std::string& config_dir, const Config& config) const;
  
  // Save individual JSON files (for fine-grained control in update flows)
  void save_stock_factor_to_json(const std::string& config_dir, const Config& config) const;
  void save_stock_info_to_json(const std::string& config_dir, const Config& config) const;
  void save_stock_days_to_json(const std::string& config_dir, const Config& config) const;
  
  // ========================================
  // Internal mutators (for DataManager)
  // ========================================
  
  // Get mutable references for updates
  StockInfoMap& mutable_stock_info() { return stock_info_; }
  StockFactorMap& mutable_stock_factor() { return stock_factor_; }
  StockDaysVec& mutable_stock_days() { return stock_days_; }
  
  // Rebuild trading days cache after modifying stock_days_
  void rebuild_cache();
};

