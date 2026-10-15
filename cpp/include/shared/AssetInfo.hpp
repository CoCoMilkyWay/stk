// Asset Information - Stock metadata (基本面数据, BigQuant + Tushare)
// Shared data structure for stock info, factors, and trading days.
// 由 FundamentalService 从 output/fundamental/ parquet 构建.
// 一律原生类型: 日期 Ymd (0 = 无), 数值 float (NaN = 无效); 格式化只在 UI.
#pragma once

#include "misc/date.hpp"

#include <limits>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

// ============================================================================
// Stock Info - Complete stock information
// ============================================================================

struct StockInfo {
  static constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

  // 静态字段 (cn_stock_basic_info + cn_stock_industry_component)
  std::string name;       // 逐日 PIT 简称 (cn_stock_instruments), 退市股回落 basic_info
  misc::Ymd ipo_date = 0; // 0 = 未知
  misc::Ymd out_date = 0; // 0 = 在市
  std::string ind_code;
  std::string ind_name;

  // 日频字段 (cn_stock_real_bar1d / cn_stock_status 每股最新行)
  misc::Ymd update_date = 0;                       // 最新行 (含停牌日) = 数据水位
  float volume = kNaN, amount = kNaN, turn = kNaN; // 最新有效报价行; 全区间无成交 → NaN
  int st = 0;                                      // cn_stock_status.st_status 原值: 0=正常 1=ST 2=*ST
  bool suspended = false;                          // 最新状态行停牌

  // 估值快照 — 分子统一为总市值 mcap = close × total_shares (不复权真价).
  // mcap 单位 [亿元]; dy* 为年化股息率 [%]; 其余为倍数. 无效 → NaN (Table 显示 "-").
  float mcap = kNaN;
  float pe_ttm = kNaN;
  float pb_mrq = kNaN;
  float ps_ttm = kNaN;
  float pcf_ttm = kNaN;
  float dy1y = kNaN; // 近 1/3/5 年分红总额年化 / mcap; 上市不足窗长的按实际年数年化
  float dy3y = kNaN;
  float dy5y = kNaN;
};

// ============================================================================
// Data Containers
// ============================================================================

// 复权因子变点 (分红/拆分事件日), date 升序
struct FactorPoint {
  misc::Ymd date;
  float factor;
};
using StockFactorMap = std::map<std::string, std::vector<FactorPoint>>; // code → 变点序列

// StockInfo: code ("sz.000001") -> StockInfo
using StockInfoMap = std::map<std::string, StockInfo>;

// 全日历 (含周末/节假日, 首末交易日之间逐日), date 升序
struct CalendarDay {
  misc::Ymd date;
  bool trading;
};
using StockDaysVec = std::vector<CalendarDay>;

// Suspended: date -> 该日全天停牌的股票 key ("sz.000001") 集合.
// 只存停牌的 (date, code) 对 (2015 至今约 28 万条), 未停牌日不建条目.
using SuspendedMap = std::map<misc::Ymd, std::unordered_set<std::string>>;

// ============================================================================
// AssetInfo - Stock metadata and trading calendar
// ============================================================================

struct AssetInfo {
  StockInfoMap stock_info_;
  StockFactorMap stock_factor_;
  StockDaysVec stock_days_;
  SuspendedMap suspended_;

  const StockInfoMap &get_stock_info() const { return stock_info_; }
  const StockFactorMap &get_stock_factor() const { return stock_factor_; }
  const StockDaysVec &get_stock_days() const { return stock_days_; }
  const SuspendedMap &get_suspended() const { return suspended_; }

  // code 如 "sh.600000"; 不存在 → nullptr
  const StockInfo *find_stock_info(const std::string &code) const;

  StockInfoMap &mutable_stock_info() { return stock_info_; }
  StockFactorMap &mutable_stock_factor() { return stock_factor_; }
  StockDaysVec &mutable_stock_days() { return stock_days_; }
  SuspendedMap &mutable_suspended() { return suspended_; }
};
