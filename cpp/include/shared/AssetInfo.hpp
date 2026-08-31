// Asset Information - Stock metadata (基本面数据, BigQuant + Tushare)
// Shared data structure for stock info, factors, and trading days.
// 由 FundamentalService 从 output/fundamental/ parquet 构建.
#pragma once

#include <map>
#include <string>
#include <unordered_set>
#include <vector>

// ============================================================================
// Stock Info - Complete stock information
// ============================================================================

struct StockInfo {
  // 静态字段 (cn_stock_basic_info + cn_stock_industry_component)
  std::string name;
  std::string ipoDate;
  std::string outDate;
  std::string ind_code;
  std::string ind_name;

  // 日频字段 (cn_stock_real_bar1d / cn_stock_status 每股最新行)
  std::string update_date;
  std::string volume;
  std::string amount;
  std::string turn;
  std::string tradestatus;
  std::string isST;
  // peTTM/pbMRQ/psTTM/pcfNcfTTM: 特征表阶段用财务表 + 实时价格计算
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
  const StockInfoMap &get_stock_info() const { return stock_info_; }
  const StockFactorMap &get_stock_factor() const { return stock_factor_; }
  const StockDaysVec &get_stock_days() const { return stock_days_; }

  // ========================================
  // Query methods
  // ========================================

  // Find stock info by code (e.g. "sh.600000")
  // Returns nullptr if not found
  const StockInfo *find_stock_info(const std::string &code) const;

  // Calculate market cap in billions (亿元)
  // Formula: amount * 100 / turn / 1e8
  // Returns 0.0 if data insufficient
  float calculate_market_cap(const std::string &code) const;

  // Check if a date is a trading day
  // Date format: YYYYMMDD or YYYY-MM-DD
  bool is_trading_day(const std::string &date) const;

  // ========================================
  // Internal mutators (for FundamentalService)
  // ========================================

  // Get mutable references for updates
  StockInfoMap &mutable_stock_info() { return stock_info_; }
  StockFactorMap &mutable_stock_factor() { return stock_factor_; }
  StockDaysVec &mutable_stock_days() { return stock_days_; }

  // Rebuild trading days cache after modifying stock_days_
  void rebuild_cache();
};
