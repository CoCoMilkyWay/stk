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
  std::string name; // 逐日 PIT 简称 (cn_stock_instruments), 退市股回落 basic_info
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
  std::string isST; // cn_stock_status.st_status 原值: "0"=正常 "1"=ST "2"=*ST
  // 估值快照 — 分子统一为总市值 mcap = close × total_shares (不复权真价).
  // mcap 单位 [亿元]; dy* 为年化股息率 [%]; 其余为倍数.
  // 无效 → 空串 (Table 显示 "-").
  std::string mcap;
  std::string peTTM;
  std::string pbMRQ;
  std::string psTTM;
  std::string pcfNcfTTM;
  std::string dy1y; // 近 1/3/5 年分红总额年化 / mcap; 上市不足窗长的按实际年数年化
  std::string dy3y;
  std::string dy5y;
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

// Suspended: dense "YYYYMMDD" -> 该日全天停牌的股票 key ("sz.000001") 集合.
// 只存停牌的 (date, code) 对 (2015 至今约 28 万条), 未停牌日不建条目.
using SuspendedMap =
    std::map<std::string, std::unordered_set<std::string>>;

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
  SuspendedMap suspended_;

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
  const SuspendedMap &get_suspended() const { return suspended_; }

  // ========================================
  // Query methods
  // ========================================

  // Find stock info by code (e.g. "sh.600000")
  // Returns nullptr if not found
  const StockInfo *find_stock_info(const std::string &code) const;

  // 总市值 [亿元] — 直接取 StockInfo::mcap (FundamentalService 已算好).
  // 缺失 → 0.0
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
  SuspendedMap &mutable_suspended() { return suspended_; }

  // Rebuild trading days cache after modifying stock_days_
  void rebuild_cache();
};
